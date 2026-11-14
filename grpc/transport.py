"""Transporte gRPC das solicitações de voto."""

import grpc

from cluster_protocol import cluster_pb2 as pb, cluster_pb2_grpc as rpc
from config import NODE_ADDRESSES, VOTE_RPC_TIMEOUT
from election import VoteResponse


def send_vote(peer_id, request):
    with grpc.insecure_channel(NODE_ADDRESSES[peer_id]) as channel:
        try:
            response = rpc.ClusterServiceStub(channel).RequestVote(
                pb.RequestVoteRequest(
                    term=request.term, candidate_id=request.candidate_id,
                    last_log_index=request.last_log_index, last_log_term=request.last_log_term,
                ), timeout=VOTE_RPC_TIMEOUT,
            )
            return VoteResponse(response.term, response.vote_granted)
        except grpc.RpcError:
            return None
