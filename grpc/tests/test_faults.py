from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

import grpc

from cluster_protocol import cluster_pb2 as pb, cluster_pb2_grpc as rpc
from cluster_protocol import demo_pb2 as demo, demo_pb2_grpc as demo_rpc
from config import NODE_ADDRESSES
from storage import Storage


ROOT = Path(__file__).resolve().parents[1]


class FaultScenarioTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.processes, self.logs, self.channels = {}, [], {}
        self.addCleanup(self.cleanup)
        for node_id, address in NODE_ADDRESSES.items():
            self.channels[node_id] = grpc.insecure_channel(address)
            self.start(node_id)
        for channel in self.channels.values():
            grpc.channel_ready_future(channel).result(timeout=10)

    def cleanup(self):
        for process in self.processes.values():
            if process.poll() is None:
                process.terminate()
        for process in self.processes.values():
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        for channel in self.channels.values():
            channel.close()
        for log in self.logs:
            log.close()
        self.temp.cleanup()

    def start(self, node_id):
        output = tempfile.TemporaryFile(mode="w+")
        self.logs.append(output)
        self.processes[node_id] = subprocess.Popen(
            [sys.executable, "node.py", "--id", str(node_id), "--demo", "--data-dir", self.temp.name],
            cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)

    def stop(self, node_id):
        self.processes[node_id].terminate()
        self.assertEqual(self.processes[node_id].wait(timeout=8), 0)

    def stub(self, node_id):
        return rpc.ClusterServiceStub(self.channels[node_id])

    def status(self, node_id):
        try:
            return self.stub(node_id).GetStatus(pb.GetStatusRequest(), timeout=0.3)
        except grpc.RpcError:
            return None

    def wait(self, predicate, message):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(0.05)
        self.fail(message)

    def leader(self, nodes):
        def elected():
            statuses = [self.status(i) for i in nodes]
            if any(s is None for s in statuses):
                return None
            leaders = [s for s in statuses if s.role == pb.LEADER]
            if len(leaders) == 1 and all(s.leader_id == leaders[0].node_id for s in statuses):
                return int(leaders[0].node_id)
        return self.wait(elected, "Não houve eleição/descoberta de líder")

    def control(self, node_id, action):
        return demo_rpc.DemoControlStub(self.channels[node_id]).Configure(
            demo.ControlRequest(action=action), timeout=2)

    def read(self, node_id, key):
        return self.stub(node_id).Read(pb.ReadRequest(key=key), timeout=2)

    def write(self, leader, key, value):
        result = self.stub(leader).Write(pb.WriteRequest(key=key, value=value), timeout=3)
        self.assertTrue(result.success, result.error)
        return result

    def test_scenarios_a_b_c_and_restore(self):
        all_nodes = list(NODE_ADDRESSES)
        leader = self.leader(all_nodes)
        # A: um seguidor perde várias entradas enquanto os demais mantêm quórum.
        follower = next(i for i in all_nodes if i != leader)
        self.stop(follower)
        for i in range(3):
            self.write(leader, f"a{i}", f"value{i}")
        self.start(follower)
        self.wait(lambda: (s := self.status(follower)) and s.commit_index == 3,
                  "Seguidor não recuperou entradas perdidas")
        self.assertEqual(self.read(follower, "a2").value, "value2")

        # B: crash real após SQLite commit, ainda sob o lock compartilhado com Fetch.
        leader = self.leader(all_nodes)
        base = self.status(leader).last_log_index
        self.assertTrue(self.control(leader, demo.ARM_CRASH).crash_armed)
        with self.assertRaises(grpc.RpcError):
            self.stub(leader).Write(pb.WriteRequest(key="orphan", value="old"), timeout=3)
        self.assertEqual(self.processes[leader].wait(timeout=5), 86)
        persisted = Storage(leader, self.temp.name)
        try:
            self.assertEqual(persisted.last_log_position()[0], base + 1)
            self.assertEqual(persisted.get_entries(base + 1)[0].key, "orphan")
            self.assertEqual(persisted.load_metadata().commit_index, base)
        finally:
            persisted.close()
        survivors = [i for i in all_nodes if i != leader]
        for node_id in survivors:
            self.assertEqual(self.status(node_id).last_log_index, base)
        new_leader = self.leader(survivors)
        replacement = self.write(new_leader, "replacement", "new")
        self.assertEqual(replacement.index, base + 1)
        self.start(leader)
        self.wait(lambda: (s := self.status(leader)) and s.commit_index == replacement.index,
                  "Antigo líder não corrigiu seu log")
        self.assertEqual(self.status(leader).role, pb.FOLLOWER)
        self.assertFalse(self.read(leader, "orphan").exists)
        self.assertEqual(self.read(leader, "replacement").value, "new")
        persisted = Storage(leader, self.temp.name)
        try:
            self.assertEqual(persisted.get_entries(base + 1)[0].key, "replacement")
        finally:
            persisted.close()
        self.assertFalse(self.control(leader, demo.STATUS).crash_armed)

        # C: pausar só o commit mantém Fetch/heartbeat e permite inspecionar isolamento.
        leader = self.leader(all_nodes)
        self.assertTrue(self.control(leader, demo.PAUSE_COMMIT).commit_paused)
        result = self.stub(leader).Write(pb.WriteRequest(key="hidden", value="visible-later"), timeout=3)
        self.assertFalse(result.success)
        self.assertEqual(result.error, "NOT_COMMITTED")
        self.wait(lambda: all((s := self.status(i)) and s.last_log_index == result.index
                              for i in all_nodes), "Entrada não chegou às réplicas")
        for node_id in all_nodes:
            self.assertFalse(self.read(node_id, "hidden").exists)
            self.assertLess(self.status(node_id).commit_index, result.index)
        self.assertFalse(self.control(leader, demo.RESUME_COMMIT).commit_paused)
        self.wait(lambda: all((s := self.status(i)) and s.commit_index == result.index
                              for i in all_nodes), "Commit não retomou após liberar pausa")
        for node_id in all_nodes:
            self.assertEqual(self.read(node_id, "hidden").value, "visible-later")
        self.control(leader, demo.ARM_CRASH)
        self.control(leader, demo.PAUSE_COMMIT)
        reset = self.control(leader, demo.RESET)
        self.assertFalse(reset.crash_armed)
        self.assertFalse(reset.commit_paused)
        self.write(leader, "normal-again", "ok")
