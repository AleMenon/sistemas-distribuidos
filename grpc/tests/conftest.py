"""Isola também os testes de processos criados antes da adoção de pytest."""

import socket
import pytest
from config import NODE_ADDRESSES


@pytest.fixture
def cluster_ports():
    sockets = []
    try:
        for _ in range(4):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


@pytest.fixture(autouse=True)
def isolate_legacy_process_tests(request, monkeypatch):
    if request.node.cls and request.node.cls.__name__ in {"ClusterTests", "FaultScenarioTests"}:
        ports = request.getfixturevalue("cluster_ports")
        monkeypatch.setenv("CLUSTER_PORTS", ",".join(map(str, ports)))
        for node_id, port in enumerate(ports, 1):
            monkeypatch.setitem(NODE_ADDRESSES, node_id, f"127.0.0.1:{port}")
