"""Acesso gRPC independente da interface e da lógica dos nós."""

from dataclasses import dataclass
import grpc

from cluster_protocol import cluster_pb2 as pb, cluster_pb2_grpc as rpc
from config import NODE_ADDRESSES, RPC_TIMEOUT


@dataclass(frozen=True)
class QueryResult:
    node_id: int
    response: object = None
    error: str = ""


@dataclass(frozen=True)
class WriteResult:
    success: bool
    code: str
    node_id: int | None = None
    index: int = 0
    uncertain: bool = False


class ClusterClient:
    def __init__(self, addresses=None, timeout=RPC_TIMEOUT):
        self.addresses = dict(NODE_ADDRESSES if addresses is None else addresses)
        self.timeout = timeout

    def _call(self, node_id, method, request):
        with grpc.insecure_channel(self.addresses[node_id]) as channel:
            return getattr(rpc.ClusterServiceStub(channel), method)(request, timeout=self.timeout)

    def status(self, node_id):
        try:
            return QueryResult(node_id, self._call(node_id, "GetStatus", pb.GetStatusRequest()))
        except grpc.RpcError as exc:
            return QueryResult(node_id, error=exc.code().name)

    def statuses(self):
        return [self.status(node_id) for node_id in self.addresses]

    def read(self, node_id, key):
        try:
            return QueryResult(node_id, self._call(node_id, "Read", pb.ReadRequest(key=key)))
        except grpc.RpcError as exc:
            return QueryResult(node_id, error=exc.code().name)

    def read_all(self, key):
        return [self.read(node_id, key) for node_id in self.addresses]

    def _discover(self, excluded):
        results = self.statuses()
        leaders = [r for r in results if not r.error and r.response.role == pb.LEADER
                   and r.response.node_id == str(r.node_id) and r.node_id not in excluded]
        if leaders:
            return max(leaders, key=lambda r: r.response.term).node_id, ""
        return None, "NO_LEADER" if any(not r.error for r in results) else "CONNECTION_ERROR"

    def write(self, initial_node, key, value):
        if initial_node not in self.addresses:
            raise ValueError("ID de nó inválido")
        target, visited = initial_node, set()
        # No máximo uma tentativa por nó; descoberta também é limitada por este laço.
        while len(visited) < len(self.addresses):
            visited.add(target)
            # Falha antes do envio permite descobrir outro destino sem duplicar operação.
            if self.status(target).error:
                target, error = self._discover(visited)
                if target is None:
                    return WriteResult(False, error)
                continue
            try:
                response = self._call(target, "Write", pb.WriteRequest(key=key, value=value))
            except grpc.RpcError:
                return WriteResult(False, "CONNECTION_ERROR", target, uncertain=True)
            if response.success:
                return WriteResult(True, "", target, response.index)
            if response.error != "NOT_LEADER" or response.index != 0:
                return WriteResult(False, response.error, target, response.index,
                                   uncertain=response.index != 0)
            redirect = int(response.leader_id) if response.leader_id.isdigit() else None
            if redirect in self.addresses and redirect not in visited:
                target = redirect
            else:
                target, error = self._discover(visited)
                if target is None:
                    return WriteResult(False, error)
        return WriteResult(False, "NO_LEADER")
