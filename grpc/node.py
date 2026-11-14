"""Ponto de entrada: uv run --no-sync python node.py --id 1."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import logging
from pathlib import Path
import signal
from threading import Event

import grpc

from cluster_protocol import cluster_pb2_grpc as rpc
from config import MAX_WORKERS, NODE_ADDRESSES, SHUTDOWN_GRACE
from node_state import Node
from service import ClusterService
from storage import DATA_DIR
from transport import send_vote, get_status, send_fetch


def log_status(node: Node, message: str) -> None:
    status = node.get_status()
    logging.info("node_id=%s role=%s term=%s commit_index=%s %s",
                 status.node_id, status.role.value, status.current_term,
                 status.commit_index, message)


def main() -> int:
    parser = argparse.ArgumentParser(description="Nó do cluster gRPC")
    parser.add_argument("--id", type=int, choices=NODE_ADDRESSES, required=True)
    parser.add_argument("--demo", action="store_true", help="Habilita RPCs de injeção de falhas")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR,
                        help="Diretório dos bancos SQLite (padrão: data/ do projeto)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    node = Node(args.id, args.data_dir, demo=args.demo)
    stop = Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Impede que dois processos aceitem conexões na mesma porta.
        server = grpc.server(executor, options=(("grpc.so_reuseport", 0),))
        rpc.add_ClusterServiceServicer_to_server(ClusterService(node), server)
        if args.demo:
            from cluster_protocol import demo_pb2_grpc
            from fault_injection import DemoControl
            demo_pb2_grpc.add_DemoControlServicer_to_server(DemoControl(node), server)
        try:
            address = NODE_ADDRESSES[args.id]
            if server.add_insecure_port(address) == 0:
                raise RuntimeError(f"Não foi possível abrir {address}")
            server.start()
            log_status(node, f"Servidor iniciado em {address}")
            node.election.start(send_vote)
            node.replication.start(get_status, send_fetch)
            stop.wait()
        except RuntimeError as exc:
            log_status(node, f"Falha ao iniciar: {exc}")
            return 1
        finally:
            log_status(node, "Encerrando servidor")
            node.begin_shutdown()
            node.election.stop()
            node.replication.stop()
            server.stop(SHUTDOWN_GRACE).wait()
            executor.shutdown(wait=True)
            log_status(node, "Servidor encerrado")
            node.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
