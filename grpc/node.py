"""Ponto de entrada: uv run --no-sync python node.py --id 1."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import logging
import signal
from threading import Event

import grpc

from cluster_protocol import cluster_pb2_grpc as rpc
from config import MAX_WORKERS, NODE_ADDRESSES, SHUTDOWN_GRACE
from node_state import Node
from service import ClusterService


def log_status(node: Node, message: str) -> None:
    status = node.get_status()
    logging.info("node_id=%s role=%s term=%s commit_index=%s %s",
                 status.node_id, status.role.value, status.current_term,
                 status.commit_index, message)


def main() -> int:
    parser = argparse.ArgumentParser(description="Nó do cluster gRPC")
    parser.add_argument("--id", type=int, choices=NODE_ADDRESSES, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    node = Node(args.id)
    stop = Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Impede que dois processos aceitem conexões na mesma porta.
        server = grpc.server(executor, options=(("grpc.so_reuseport", 0),))
        rpc.add_ClusterServiceServicer_to_server(ClusterService(node), server)
        try:
            address = NODE_ADDRESSES[args.id]
            if server.add_insecure_port(address) == 0:
                raise RuntimeError(f"Não foi possível abrir {address}")
            server.start()
            log_status(node, f"Servidor iniciado em {address}")
            stop.wait()
        except RuntimeError as exc:
            log_status(node, f"Falha ao iniciar: {exc}")
            return 1
        finally:
            log_status(node, "Encerrando servidor")
            server.stop(SHUTDOWN_GRACE).wait()
            log_status(node, "Servidor encerrado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
