"""Estado em memória, independente do transporte gRPC."""

from dataclasses import dataclass
from enum import Enum
from threading import RLock

from config import NODE_ADDRESSES


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


class Node:
    def __init__(self, node_id: int):
        if node_id not in NODE_ADDRESSES:
            raise ValueError("ID do nó deve estar entre 1 e 4")
        self._lock = RLock()
        self._node_id = node_id
        self._role = Role.FOLLOWER
        self._current_term = 0
        self._commit_index = 0
        self._last_log_index = 0
        self._leader_id = ""

    def get_status(self) -> NodeStatus:
        """Retorna uma cópia imutável e consistente do estado compartilhado."""
        with self._lock:
            return NodeStatus(
                self._node_id, self._role, self._current_term,
                self._commit_index, self._last_log_index, self._leader_id,
            )
