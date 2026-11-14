"""Cenários obrigatórios usando somente RPCs para controlar/observar nós ativos."""

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

import grpc
import pytest

from cluster_protocol import cluster_pb2 as pb, cluster_pb2_grpc as rpc
from cluster_protocol import demo_pb2 as demo, demo_pb2_grpc as demo_rpc


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.e2e


class Cluster:
    def __init__(self, directory, ports):
        self.directory = directory
        self.env = dict(os.environ, CLUSTER_PORTS=",".join(map(str, ports)))
        self.processes = {}
        self.outputs = []
        self.channels = {i: grpc.insecure_channel(f"127.0.0.1:{port}")
                         for i, port in enumerate(ports, 1)}

    def start(self, node_id):
        output = (self.directory / f"node_{node_id}_{len(self.outputs)}.log").open("w+")
        self.outputs.append(output)
        self.processes[node_id] = subprocess.Popen(
            [sys.executable, str(ROOT / "node.py"), "--id", str(node_id), "--demo",
             "--data-dir", str(self.directory)], cwd=ROOT, env=self.env,
            stdout=output, stderr=subprocess.STDOUT)

    def stop(self, node_id):
        process = self.processes[node_id]
        process.terminate()
        assert process.wait(timeout=8) == 0

    def close(self):
        for process in self.processes.values():
            if process.poll() is None:
                process.terminate()
        for process in self.processes.values():
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for channel in self.channels.values():
            channel.close()
        for output in self.outputs:
            output.close()

    def wait(self, predicate, message, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                value = predicate()
                if value:
                    return value
            except grpc.RpcError:
                pass
            time.sleep(0.04)  # Intervalo de polling; a condição decide quando prosseguir.
        diagnostics = []
        for output in self.outputs:
            output.flush()
            output.seek(0)
            diagnostics.append(output.read()[-3000:])
        pytest.fail(message + "\n" + "\n".join(diagnostics))

    def stub(self, node_id):
        return rpc.ClusterServiceStub(self.channels[node_id])

    def status(self, node_id):
        return self.stub(node_id).GetStatus(pb.GetStatusRequest(), timeout=0.3)

    def leader(self, nodes=None, after_term=-1):
        nodes = list(self.channels) if nodes is None else nodes
        def elected():
            statuses = [self.status(i) for i in nodes]
            leaders = [s for s in statuses if s.role == pb.LEADER and s.term > after_term]
            if len(leaders) == 1 and all(s.term == leaders[0].term and
                                        s.leader_id == leaders[0].node_id for s in statuses):
                return leaders[0]
        return self.wait(elected, "Eleição/descoberta não convergiu")

    def write(self, node_id, key, value):
        return self.stub(node_id).Write(pb.WriteRequest(key=key, value=value), timeout=3)

    def read(self, node_id, key):
        return self.stub(node_id).Read(pb.ReadRequest(key=key), timeout=0.3)

    def control(self, node_id, action):
        return demo_rpc.DemoControlStub(self.channels[node_id]).Configure(
            demo.ControlRequest(action=action), timeout=1)

    def visible(self, key, value, index, nodes=None):
        def replicated():
            responses = [self.read(i, key) for i in (self.channels if nodes is None else nodes)]
            return all(r.exists and r.value == value and r.commit_index >= index for r in responses)
        self.wait(replicated, f"Dados comitados não convergiram: {key}")

    def disk_log(self, node_id):
        # Inspeção somente leitura para provar truncamento/conteúdo, sem controlar o nó.
        with sqlite3.connect(f"file:{self.directory / f'node_{node_id}.db'}?mode=ro", uri=True) as db:
            return db.execute("SELECT log_index, term, key, value FROM operation_log ORDER BY log_index").fetchall()


@pytest.fixture
def cluster(tmp_path, cluster_ports):
    instance = Cluster(tmp_path, cluster_ports)
    try:
        for node_id in instance.channels:
            instance.start(node_id)
        instance.leader()
        yield instance
    finally:
        instance.close()


def test_distributed_write_and_read(cluster):
    leader = int(cluster.leader().node_id)
    for key, value in (("name", "ação"), ("empty", ""), ("name", "updated")):
        result = cluster.write(leader, key, value)
        assert result.success, result.error
        cluster.visible(key, value, result.index)
    for node_id in cluster.channels:
        assert not cluster.read(node_id, "missing").exists


def test_election_after_leader_failure(cluster):
    before = cluster.leader()
    cluster.stop(int(before.node_id))
    survivors = [i for i in cluster.channels if str(i) != before.node_id]
    after = cluster.leader(survivors, after_term=before.term)
    assert after.node_id != before.node_id
    assert after.term > before.term
    result = cluster.write(int(after.node_id), "after-election", "ok")
    assert result.success
    cluster.visible("after-election", "ok", result.index, survivors)


def test_quorum_loss_and_recovery(cluster):
    leader = int(cluster.leader().node_id)
    followers = [i for i in cluster.channels if i != leader]
    stopped = followers[:2]
    for node_id in stopped:
        cluster.stop(node_id)
    baseline = cluster.status(leader).last_log_index
    # Esperar a detecção por heartbeat pela resposta observável, sem sleep fixo.
    def rejected():
        response = cluster.write(leader, "no-quorum", "not-confirmed")
        assert not response.success
        return response if response.error in ("NO_QUORUM", "NOT_LEADER") else None
    response = cluster.wait(rejected, "Escrita não foi rejeitada sem quórum")
    if response.error == "NO_QUORUM":
        assert response.index == 0
    for node_id in (leader, followers[2]):
        assert not cluster.read(node_id, "no-quorum").exists
    for node_id in stopped:
        cluster.start(node_id)
    restored = int(cluster.leader().node_id)
    result = cluster.write(restored, "restored", "ok")
    assert result.success
    assert result.index > baseline
    cluster.visible("restored", "ok", result.index)


def test_recovery_of_lagging_follower(cluster):
    leader = int(cluster.leader().node_id)
    follower = next(i for i in cluster.channels if i != leader)
    cluster.stop(follower)
    for i in range(4):
        result = cluster.write(leader, f"missed-{i}", str(i))
        assert result.success
    cluster.start(follower)
    cluster.visible("missed-3", "3", result.index)
    for i in range(4):
        assert cluster.read(follower, f"missed-{i}").value == str(i)
    assert cluster.disk_log(follower) == cluster.disk_log(leader)


def test_recovery_of_crashed_leader_with_conflicting_entry(cluster):
    before = cluster.leader()
    old = int(before.node_id)
    baseline = cluster.write(old, "base", "committed")
    assert baseline.success
    cluster.visible("base", "committed", baseline.index)
    cluster.control(old, demo.ARM_CRASH)
    with pytest.raises(grpc.RpcError):
        cluster.write(old, "orphan", "old")
    assert cluster.processes[old].wait(timeout=5) == 86
    assert cluster.disk_log(old)[-1][2:] == ("orphan", "old")
    survivors = [i for i in cluster.channels if i != old]
    for node_id in survivors:
        assert cluster.status(node_id).last_log_index == baseline.index
    new = int(cluster.leader(survivors, before.term).node_id)
    result = cluster.write(new, "replacement", "new")
    assert result.success
    assert result.index == baseline.index + 1
    cluster.start(old)
    cluster.visible("replacement", "new", result.index)
    assert cluster.status(old).role == pb.FOLLOWER
    assert not cluster.read(old, "orphan").exists
    assert cluster.disk_log(old) == cluster.disk_log(new)


def test_uncommitted_data_isolation(cluster):
    leader = int(cluster.leader().node_id)
    cluster.control(leader, demo.PAUSE_COMMIT)
    follower = next(i for i in cluster.channels if i != leader)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(cluster.write, leader, "hidden", "later-visible")
        cluster.wait(lambda: cluster.status(follower).last_log_index == 1,
                     "Seguidor não persistiu entrada")
        assert cluster.disk_log(follower)[-1][2:] == ("hidden", "later-visible")
        for node_id in cluster.channels:
            response = cluster.read(node_id, "hidden")
            assert not response.exists
            assert response.commit_index == 0
        result = pending.result(timeout=4)
        assert not result.success and result.error == "NOT_COMMITTED"
    cluster.control(leader, demo.RESUME_COMMIT)
    cluster.visible("hidden", "later-visible", result.index)
