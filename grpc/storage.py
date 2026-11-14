"""Persistência local transacional. Nenhuma decisão de consenso é tomada aqui."""

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import sqlite3
from threading import RLock


DATA_DIR = Path(__file__).resolve().parent / "data"


@dataclass(frozen=True)
class Metadata:
    current_term: int
    voted_for: str | None
    commit_index: int
    last_applied: int


@dataclass(frozen=True)
class LogEntry:
    index: int
    term: int
    key: str
    value: str


class Storage:
    def __init__(self, node_id: int, data_dir: Path = DATA_DIR):
        if node_id not in range(1, 5):
            raise ValueError("ID do nó deve estar entre 1 e 4")
        self.path = Path(data_dir) / f"node_{node_id}.db"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.execute("PRAGMA synchronous = FULL")
        self.initialize()

    @contextmanager
    def _transaction(self):
        with self._lock:
            with self._connection:
                self._connection.execute("BEGIN IMMEDIATE")
                yield self._connection

    def initialize(self) -> None:
        with self._transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS metadata (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                current_term INTEGER NOT NULL CHECK (current_term >= 0),
                voted_for TEXT,
                commit_index INTEGER NOT NULL CHECK (commit_index >= 0),
                last_applied INTEGER NOT NULL CHECK (last_applied = commit_index)
            )""")
            db.execute("INSERT OR IGNORE INTO metadata VALUES (1, 0, NULL, 0, 0)")
            db.execute("""CREATE TABLE IF NOT EXISTS operation_log (
                log_index INTEGER PRIMARY KEY CHECK (log_index >= 1),
                term INTEGER NOT NULL CHECK (term >= 0),
                key TEXT NOT NULL, value TEXT NOT NULL
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS committed_state (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            )""")

    def load_metadata(self) -> Metadata:
        with self._lock:
            row = self._connection.execute(
                "SELECT current_term, voted_for, commit_index, last_applied FROM metadata"
            ).fetchone()
            return Metadata(*row)

    def save_metadata(self, current_term: int, voted_for: str | None) -> None:
        """Termo e voto são persistidos juntos; commit usa commit_and_apply."""
        with self._transaction() as db:
            if current_term < self.load_metadata().current_term:
                raise ValueError("O termo não pode retroceder")
            db.execute("UPDATE metadata SET current_term = ?, voted_for = ?",
                       (current_term, voted_for))

    def last_log_position(self) -> tuple[int, int]:
        with self._lock:
            row = self._connection.execute(
                "SELECT log_index, term FROM operation_log ORDER BY log_index DESC LIMIT 1"
            ).fetchone()
            return tuple(row) if row else (0, 0)

    def snapshot(self) -> tuple[Metadata, tuple[int, int]]:
        with self._lock:
            return self.load_metadata(), self.last_log_position()

    def append_entries(self, entries: list[LogEntry]) -> None:
        with self._transaction() as db:
            expected = self.last_log_position()[0] + 1
            for entry in entries:
                if entry.index != expected:
                    raise ValueError("Entradas devem ser contíguas e acrescentadas ao final")
                db.execute("INSERT INTO operation_log VALUES (?, ?, ?, ?)",
                           (entry.index, entry.term, entry.key, entry.value))
                expected += 1

    def get_entries(self, start_index: int = 1) -> list[LogEntry]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT log_index, term, key, value FROM operation_log "
                "WHERE log_index >= ? ORDER BY log_index", (start_index,),
            ).fetchall()
            return [LogEntry(*row) for row in rows]

    def truncate_from(self, index: int) -> None:
        """Remove o sufixo a partir de index (inclusive), nunca o prefixo comitado."""
        with self._transaction() as db:
            if index < 1 or index <= self.load_metadata().commit_index:
                raise ValueError("Não é permitido truncar entradas comitadas ou índice 0")
            db.execute("DELETE FROM operation_log WHERE log_index >= ?", (index,))

    def commit_and_apply(self, commit_index: int) -> None:
        """Aplica em ordem e atualiza os dois índices na mesma transação.

        O chamador futuro será responsável por validar o quórum.
        """
        with self._transaction() as db:
            metadata = self.load_metadata()
            if not metadata.commit_index <= commit_index <= self.last_log_position()[0]:
                raise ValueError("Commit não pode retroceder nem ultrapassar o log")
            rows = db.execute(
                "SELECT key, value FROM operation_log "
                "WHERE log_index > ? AND log_index <= ? ORDER BY log_index",
                (metadata.last_applied, commit_index),
            ).fetchall()
            for key, value in rows:
                db.execute("INSERT INTO committed_state VALUES (?, ?) "
                           "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))
            db.execute("UPDATE metadata SET commit_index = ?, last_applied = ?",
                       (commit_index, commit_index))

    def read(self, key: str) -> str | None:
        """None indica chave ausente; string vazia é um valor válido."""
        with self._lock:
            row = self._connection.execute(
                "SELECT value FROM committed_state WHERE key = ?", (key,),
            ).fetchone()
            return row[0] if row else None

    def close(self) -> None:
        with self._lock:
            self._connection.close()
