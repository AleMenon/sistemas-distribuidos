from dataclasses import FrozenInstanceError
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

import grpc

from cluster_protocol import cluster_pb2 as pb
from cluster_protocol import cluster_pb2_grpc as rpc
from config import NODE_ADDRESSES
from node_state import Node
from client_api import ClusterClient
from cluster_protocol import demo_pb2, demo_pb2_grpc


ROOT = Path(__file__).resolve().parents[1]


class StateTests(unittest.TestCase):
    def test_instances_have_independent_state(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        first, second = Node(1, temp.name), Node(2, temp.name)
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        self.assertIsNot(first._lock, second._lock)
        first.storage.save_metadata(7, None)
        self.assertEqual(first.get_status().current_term, 7)
        self.assertEqual(second.get_status().current_term, 0)
        snapshot = second.get_status()
        with self.assertRaises(FrozenInstanceError):
            snapshot.current_term = 8

    def test_invalid_ids(self):
        for node_id in (0, 5):
            with self.assertRaises(ValueError):
                Node(node_id)


class ClusterTests(unittest.TestCase):
    def test_four_processes_and_independent_shutdown(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        processes, channels, logs = {}, {}, {}

        def cleanup():
            for channel in channels.values():
                channel.close()
            for process in processes.values():
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            for output in logs.values():
                output.close()

        self.addCleanup(cleanup)
        for node_id in NODE_ADDRESSES:
            logs[node_id] = tempfile.TemporaryFile(mode="w+")
            processes[node_id] = subprocess.Popen(
                [sys.executable, "node.py", "--id", str(node_id),
                 "--data-dir", temp.name], cwd=ROOT,
                stdout=logs[node_id], stderr=subprocess.STDOUT,
            )
        for node_id, address in NODE_ADDRESSES.items():
            channel = channels[node_id] = grpc.insecure_channel(address)
            grpc.channel_ready_future(channel).result(timeout=10)
            status = rpc.ClusterServiceStub(channel).GetStatus(
                pb.GetStatusRequest(), timeout=2,
            )
            self.assertIsNone(processes[node_id].poll())
            self.assertEqual(status.node_id, str(node_id))
            self.assertIn(status.role, (pb.FOLLOWER, pb.CANDIDATE, pb.LEADER))
            self.assertEqual((status.commit_index, status.last_log_index), (0, 0))

        client = subprocess.run([sys.executable, "client.py", "--status"], cwd=ROOT,
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(client.returncode, 0, client.stderr)
        for node_id, address in NODE_ADDRESSES.items():
            self.assertIn(f"{address} node_id={node_id}", client.stdout)

        deadline = time.monotonic() + 10
        elected = None
        while time.monotonic() < deadline and elected is None:
            for channel in channels.values():
                status = rpc.ClusterServiceStub(channel).GetStatus(pb.GetStatusRequest(), timeout=2)
                if status.role == pb.LEADER:
                    elected = status
                    break
            if elected is None:
                time.sleep(0.02)
        self.assertIsNotNone(elected, "Nenhum líder eleito em 10 segundos")
        self.assertGreaterEqual(elected.term, 1)
        self.assertEqual(elected.leader_id, elected.node_id)
        # No modo normal o serviço auxiliar nem sequer é registrado.
        with self.assertRaises(grpc.RpcError) as disabled:
            demo_pb2_grpc.DemoControlStub(channels[int(elected.node_id)]).Configure(
                demo_pb2.ControlRequest(action=demo_pb2.ARM_CRASH), timeout=2)
        self.assertEqual(disabled.exception.code(), grpc.StatusCode.UNIMPLEMENTED)

        # Vários timeouts de eleição decorrem sem troca de líder graças ao Fetch.
        stable_until = time.monotonic() + 4.5
        while time.monotonic() < stable_until:
            for channel in channels.values():
                status = rpc.ClusterServiceStub(channel).GetStatus(pb.GetStatusRequest(), timeout=2)
                self.assertEqual(status.term, elected.term)
                if status.role == pb.LEADER:
                    self.assertEqual(status.node_id, elected.node_id)
            time.sleep(0.1)

        api = ClusterClient()
        follower_id = next(i for i in NODE_ADDRESSES if str(i) != elected.node_id)
        for index, value in enumerate(("first", "second"), 1):
            result = api.write(follower_id, "key", value)
            self.assertTrue(result.success)
            self.assertEqual((result.code, result.index, result.node_id),
                             ("", index, int(elected.node_id)))
        follower_id = next(i for i in NODE_ADDRESSES if str(i) != elected.node_id)
        result = rpc.ClusterServiceStub(channels[follower_id]).Write(
            pb.WriteRequest(key="rejected", value="value"), timeout=2)
        self.assertEqual((result.error, result.index, result.leader_id),
                         ("NOT_LEADER", 0, elected.node_id))
        deadline = time.monotonic() + 5
        while True:
            statuses = [rpc.ClusterServiceStub(channel).GetStatus(pb.GetStatusRequest(), timeout=2)
                        for channel in channels.values()]
            if all(status.last_log_index == 2 and status.commit_index == 2 for status in statuses):
                break
            self.assertLess(time.monotonic(), deadline, "Fetch não replicou as escritas")
            time.sleep(0.05)
        self.assertTrue(all(status.commit_index == 2 for status in statuses))

        for channel in channels.values():
            stub = rpc.ClusterServiceStub(channel)
            found = stub.Read(pb.ReadRequest(key="key"), timeout=2)
            self.assertTrue(found.exists)
            self.assertEqual((found.value, found.commit_index), ("second", 2))
            self.assertFalse(stub.Read(pb.ReadRequest(key="missing"), timeout=2).exists)

        victim = int(elected.node_id)
        processes[victim].send_signal(signal.SIGINT)
        self.assertEqual(processes[victim].wait(timeout=8), 0)
        logs[victim].seek(0)
        output = logs[victim].read()
        self.assertIn(f"node_id={victim} role=FOLLOWER term=0 commit_index=0", output)
        self.assertIn("Servidor encerrado", output)
        # A RPC não implementada acima gera um traceback pelo próprio gRPC;
        # o encerramento, por sua vez, deve ser limpo.
        self.assertNotIn("Traceback", output.split("Encerrando servidor", 1)[1])
        with self.assertRaises(grpc.RpcError):
            rpc.ClusterServiceStub(channels[victim]).GetStatus(pb.GetStatusRequest(), timeout=2)
        survivors = [node_id for node_id in NODE_ADDRESSES if node_id != victim]
        deadline = time.monotonic() + 10
        replacement = None
        while time.monotonic() < deadline and replacement is None:
            for node_id in survivors:
                status = rpc.ClusterServiceStub(channels[node_id]).GetStatus(
                    pb.GetStatusRequest(), timeout=2)
                if status.role == pb.LEADER and status.term > elected.term:
                    replacement = status
                    break
            if replacement is None:
                time.sleep(0.05)
        self.assertIsNotNone(replacement, "Nenhum novo líder após perda do líder")
        readings = api.read_all("key")
        self.assertEqual(sum(bool(result.error) for result in readings), 1)
        for result in readings:
            if result.node_id != victim:
                self.assertTrue(result.response.exists)
                self.assertEqual(result.response.value, "second")
        for node_id in survivors:
            self.assertIsNone(processes[node_id].poll())
            status = rpc.ClusterServiceStub(channels[node_id]).GetStatus(
                pb.GetStatusRequest(), timeout=2,
            )
            self.assertEqual(status.node_id, str(node_id))
            self.assertGreaterEqual(status.term, 0)


if __name__ == "__main__":
    unittest.main()
