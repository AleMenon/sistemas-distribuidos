"""Estado do nó, independente do transporte gRPC, com metadados persistidos."""

from dataclasses import dataclass
from enum import Enum
from threading import RLock, Condition
import time

from config import NODE_ADDRESSES, QUORUM, WRITE_TIMEOUT
from storage import DATA_DIR, LogEntry, Storage


class Role(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass(frozen=True)
class NodeStatus:
    node_id: int
    role: Role
    current_term: int
    commit_index: int
    last_log_index: int
    leader_id: str


@dataclass(frozen=True)
class WriteResult:
    success: bool
    error: str
    leader_id: str
    index: int


class Node:
    def __init__(self, node_id: int, data_dir=DATA_DIR):
        if node_id not in NODE_ADDRESSES:
            raise ValueError("ID do nó deve estar entre 1 e 4")
        self._lock = RLock()
        self._commit_changed = Condition(self._lock)
        self._closing = False
        self._node_id = node_id
        self._role = Role.FOLLOWER
        self.storage = Storage(node_id, data_dir)
        self._leader_id = ""
        from election import Election
        self.election = Election(self)
        from replication import Replication
        self.replication = Replication(self)

    def get_status(self) -> NodeStatus:
        """Retorna uma cópia imutável e consistente do estado compartilhado."""
        with self._lock:
            metadata, (last_index, _) = self.storage.snapshot()
            return NodeStatus(
                self._node_id, self._role, metadata.current_term,
                metadata.commit_index, last_index, self._leader_id,
            )

    def close(self) -> None:
        self.begin_shutdown()
        self.replication.stop()
        self.election.stop()
        with self._lock:
            self.storage.close()

    def begin_shutdown(self):
        with self._commit_changed:
            self._closing = True
            self._commit_changed.notify_all()

    def read(self, key: str) -> tuple[str | None, int]:
        with self._lock:
            return self.storage.read(key), self.storage.load_metadata().commit_index

    def leadership_changed(self):
        """Chamado sob o lock do nó a cada mudança de papel/termo."""
        self.replication.reset_tracking()
        self._commit_changed.notify_all()

    def write(self, key: str, value: str, timeout: float = WRITE_TIMEOUT) -> WriteResult:
        # O mesmo lock serializa escritas, Fetch e transições de liderança.
        with self._commit_changed:
            if self._closing or self._role != Role.LEADER:
                return WriteResult(False, "NOT_LEADER", self._leader_id, 0)
            _, active = self.replication.activity()
            if 1 + sum(active.values()) < QUORUM:
                return WriteResult(False, "NO_QUORUM", self._leader_id, 0)
            metadata, (last_index, _) = self.storage.snapshot()
            index = last_index + 1
            self.storage.append_entries([LogEntry(index, metadata.current_term, key, value)])
            deadline = time.monotonic() + timeout
            while True:
                current = self.storage.load_metadata()
                # Verificar mandato antes do commit evita sucesso após perda de liderança.
                if (self._closing or self._role != Role.LEADER
                        or current.current_term != metadata.current_term):
                    return WriteResult(False, "NOT_LEADER", self._leader_id, index)
                if current.commit_index >= index:
                    return WriteResult(True, "", str(self._node_id), index)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return WriteResult(False, "NOT_COMMITTED", self._leader_id, index)
                self._commit_changed.wait(remaining)
