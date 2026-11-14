"""Endereços locais fixos dos quatro processos do cluster."""

NODE_ADDRESSES = {node_id: f"127.0.0.1:{50050 + node_id}" for node_id in range(1, 5)}
RPC_TIMEOUT = 2.0
SHUTDOWN_GRACE = 2.0
MAX_WORKERS = 4
