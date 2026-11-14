"""Estado do nó, independente do transporte gRPC, com metadados persistidos."""

from dataclasses import dataclass
from enum import Enum
from threading import RLock

from config import NODE_ADDRESSES
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
        self.replication.stop()
        self.election.stop()
        with self._lock:
            self.storage.close()

    def write(self, key: str, value: str) -> WriteResult:
        # O mesmo lock serializa escritas, Fetch e transições de liderança.
        with self._lock:
            if self._role != Role.LEADER:
                return WriteResult(False, "NOT_LEADER", self._leader_id, 0)
            metadata, (last_index, _) = self.storage.snapshot()
            index = last_index + 1
            self.storage.append_entries([LogEntry(index, metadata.current_term, key, value)])
            # Persistido não significa comitado. Nenhum dado é aplicado aqui.
            return WriteResult(False, "NOT_COMMITTED", str(self._node_id), index)
