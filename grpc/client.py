"""Consulta GetStatus dos quatro nós, inclusive quando algum está offline."""

import grpc

from cluster_protocol import cluster_pb2 as pb
from cluster_protocol import cluster_pb2_grpc as rpc
from config import NODE_ADDRESSES, RPC_TIMEOUT


def main() -> int:
    failed = False
    for node_id, address in NODE_ADDRESSES.items():
        with grpc.insecure_channel(address) as channel:
            try:
                status = rpc.ClusterServiceStub(channel).GetStatus(
                    pb.GetStatusRequest(), timeout=RPC_TIMEOUT,
                )
                print(f"{address} node_id={status.node_id} "
                      f"role={pb.Role.Name(status.role)} term={status.term} "
                      f"commit_index={status.commit_index} "
                      f"last_log_index={status.last_log_index} "
                      f"leader_id={status.leader_id or 'desconhecido'}")
            except grpc.RpcError as exc:
                failed = True
                print(f"{address} node_id={node_id} indisponível: {exc.code().name}")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
