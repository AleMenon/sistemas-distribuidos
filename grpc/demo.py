"""Controle gRPC de um nó explicitamente iniciado com --demo."""

import argparse
import grpc
from cluster_protocol import demo_pb2 as pb, demo_pb2_grpc as rpc
from config import NODE_ADDRESSES, RPC_TIMEOUT


def main():
    parser = argparse.ArgumentParser(description="Controles de demonstração (exigem node.py --demo)")
    parser.add_argument("--id", type=int, choices=NODE_ADDRESSES, required=True)
    parser.add_argument("action", choices=["status", "arm-crash", "pause-commit", "resume-commit", "reset"])
    args = parser.parse_args()
    with grpc.insecure_channel(NODE_ADDRESSES[args.id]) as channel:
        try:
            result = rpc.DemoControlStub(channel).Configure(
                pb.ControlRequest(action=pb.Action.Value(args.action.upper().replace("-", "_"))),
                timeout=RPC_TIMEOUT)
            print(f"Nó {args.id}: crash_armed={result.crash_armed} commit_paused={result.commit_paused}")
            return 0
        except grpc.RpcError as exc:
            print(f"Controle recusado/indisponível: {exc.code().name} ({exc.details()})")
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
