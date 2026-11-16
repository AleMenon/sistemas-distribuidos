"""Transporte gRPC de votação, descoberta de líder e Fetch."""

import grpc

from cluster_protocol import cluster_pb2 as pb, cluster_pb2_grpc as rpc
from config import FETCH_RPC_TIMEOUT, NODE_ADDRESSES, VOTE_RPC_TIMEOUT
from election import VoteResponse
from node_state import NodeStatus, Role
from replication import FetchResponse
from storage import LogEntry


def get_status(peer_id):
    with grpc.insecure_channel(NODE_ADDRESSES[peer_id]) as channel:
        try:
            response = rpc.ClusterServiceStub(channel).GetStatus(
                pb.GetStatusRequest(), timeout=FETCH_RPC_TIMEOUT)
            return NodeStatus(int(response.node_id), Role(pb.Role.Name(response.role)),
                              response.term, response.commit_index, response.last_log_index,
                              response.leader_id)
        except grpc.RpcError:
            return None


def send_fetch(peer_id, request):
    with grpc.insecure_channel(NODE_ADDRESSES[peer_id]) as channel:
        try:
            response = rpc.ClusterServiceStub(channel).Fetch(
                pb.FetchRequest(term=request.term, follower_id=request.follower_id,
                                last_log_index=request.last_log_index,
                                last_log_term=request.last_log_term), timeout=FETCH_RPC_TIMEOUT)
            return FetchResponse(
                response.term, response.leader_id, response.is_leader, response.commit_index,
                [LogEntry(e.index, e.term, e.key, e.value) for e in response.entries],
                response.prev_log_index, response.prev_log_term,
                response.truncate_after_index if response.HasField("truncate_after_index") else None,
            )
        except grpc.RpcError:
            return None


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
