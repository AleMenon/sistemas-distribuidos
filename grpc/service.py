"""Adaptação do estado do nó para as mensagens do protocolo."""

from cluster_protocol import cluster_pb2 as pb
from cluster_protocol import cluster_pb2_grpc as rpc
from node_state import Node
from election import VoteRequest
from replication import FetchRequest
from threading import BoundedSemaphore
from config import MAX_WORKERS, WRITE_TIMEOUT


class ClusterService(rpc.ClusterServiceServicer):
    def __init__(self, node: Node):
        self.node = node
        # Reserva capacidade no pool gRPC para Fetch/eleição durante esperas de Write.
        self._write_slots = BoundedSemaphore(max(1, MAX_WORKERS - 1))

    def GetStatus(self, request, context):
        status = self.node.get_status()
        return pb.GetStatusResponse(
            node_id=str(status.node_id), role=pb.Role.Value(status.role.value),
            term=status.current_term, commit_index=status.commit_index,
            last_log_index=status.last_log_index, leader_id=status.leader_id,
        )

    def RequestVote(self, request, context):
        response = self.node.election.request_vote(VoteRequest(
            request.term, request.candidate_id, request.last_log_index, request.last_log_term,
        ))
        return pb.RequestVoteResponse(term=response.term, vote_granted=response.vote_granted)

    def Write(self, request, context):
        if not self._write_slots.acquire(blocking=False):
            return pb.WriteResponse(success=False, error="BUSY",
                                    leader_id=self.node.get_status().leader_id)
        try:
            remaining = context.time_remaining()
            timeout = WRITE_TIMEOUT if remaining is None else min(WRITE_TIMEOUT, remaining)
            result = self.node.write(request.key, request.value, timeout=max(0, timeout))
        finally:
            self._write_slots.release()
        return pb.WriteResponse(success=result.success, error=result.error,
                                leader_id=result.leader_id, index=result.index)

    def Fetch(self, request, context):
        response = self.node.replication.fetch(FetchRequest(
            request.term, request.follower_id, request.last_log_index, request.last_log_term))
        result = pb.FetchResponse(
            term=response.term, leader_id=response.leader_id, is_leader=response.is_leader,
            commit_index=response.commit_index, prev_log_index=response.prev_log_index,
            prev_log_term=response.prev_log_term,
            entries=[pb.LogEntry(index=e.index, term=e.term, key=e.key, value=e.value)
                     for e in response.entries])
        if response.truncate_after_index is not None:
            result.truncate_after_index = response.truncate_after_index
        return result
