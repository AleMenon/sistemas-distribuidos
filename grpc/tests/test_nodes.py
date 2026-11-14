from dataclasses import FrozenInstanceError
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest

import grpc

from cluster_protocol import cluster_pb2 as pb
from cluster_protocol import cluster_pb2_grpc as rpc
from config import NODE_ADDRESSES
from node_state import Node


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
            self.assertEqual(status.role, pb.FOLLOWER)
            self.assertEqual((status.term, status.commit_index, status.last_log_index),
                             (0, 0, 0))
            self.assertEqual(status.leader_id, "")

        client = subprocess.run([sys.executable, "client.py"], cwd=ROOT,
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(client.returncode, 0, client.stderr)
        for node_id, address in NODE_ADDRESSES.items():
            self.assertIn(f"{address} node_id={node_id}", client.stdout)

        # As outras RPCs continuam explicitamente não implementadas.
        with self.assertRaises(grpc.RpcError) as error:
            rpc.ClusterServiceStub(channels[1]).RequestVote(
                pb.RequestVoteRequest(), timeout=2,
            )
        self.assertEqual(error.exception.code(), grpc.StatusCode.UNIMPLEMENTED)

        processes[1].send_signal(signal.SIGINT)
        self.assertEqual(processes[1].wait(timeout=8), 0)
        logs[1].seek(0)
        output = logs[1].read()
        self.assertIn("node_id=1 role=FOLLOWER term=0 commit_index=0", output)
        self.assertIn("Servidor encerrado", output)
        # A RPC não implementada acima gera um traceback pelo próprio gRPC;
        # o encerramento, por sua vez, deve ser limpo.
        self.assertNotIn("Traceback", output.split("Encerrando servidor", 1)[1])
        with self.assertRaises(grpc.RpcError):
            rpc.ClusterServiceStub(channels[1]).GetStatus(pb.GetStatusRequest(), timeout=2)
        for node_id in (2, 3, 4):
            self.assertIsNone(processes[node_id].poll())
            status = rpc.ClusterServiceStub(channels[node_id]).GetStatus(
                pb.GetStatusRequest(), timeout=2,
            )
            self.assertEqual(status.node_id, str(node_id))
            self.assertEqual(status.term, 0)


if __name__ == "__main__":
    unittest.main()
