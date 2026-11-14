"""Adaptação do estado do nó para as mensagens do protocolo."""

from cluster_protocol import cluster_pb2 as pb
from cluster_protocol import cluster_pb2_grpc as rpc
from node_state import Node
from election import VoteRequest


class ClusterService(rpc.ClusterServiceServicer):
    def __init__(self, node: Node):
        self.node = node

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
