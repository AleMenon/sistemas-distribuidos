"""Configuração compartilhada; CLUSTER_PORTS permite isolar execuções locais."""

import os


def load_addresses():
    ports = [int(value) for value in os.environ.get("CLUSTER_PORTS", "50051,50052,50053,50054").split(",")]
    if len(ports) != 4 or len(set(ports)) != 4 or any(not 1 <= port <= 65535 for port in ports):
        raise ValueError("CLUSTER_PORTS deve conter quatro portas distintas entre 1 e 65535")
    return {node_id: f"127.0.0.1:{port}" for node_id, port in enumerate(ports, 1)}


NODE_ADDRESSES = load_addresses()
RPC_TIMEOUT = 2.0
SHUTDOWN_GRACE = 2.0
MAX_WORKERS = 4
QUORUM = 3
ELECTION_TIMEOUT_MIN = 1.0
ELECTION_TIMEOUT_MAX = 2.0
VOTE_RPC_TIMEOUT = 0.5
FETCH_INTERVAL = 0.1
FETCH_RPC_TIMEOUT = 0.2
HEARTBEAT_TIMEOUT = 0.5
WRITE_TIMEOUT = 1.5
