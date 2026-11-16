from contextlib import redirect_stdout
from io import StringIO
import unittest
from unittest.mock import patch

import grpc

from client import main, show_read, show_write
from client_api import ClusterClient, WriteResult
from cluster_protocol import cluster_pb2 as pb


class Unavailable(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.client = ClusterClient()

    def status(self, node, leader=1):
        return pb.GetStatusResponse(node_id=str(node), role=pb.LEADER if node == leader else pb.FOLLOWER,
                                    term=1, leader_id=str(leader) if leader else "")

    def test_redirect_to_leader(self):
        writes = []
        def call(node, method, request):
            if method == "GetStatus":
                return self.status(node)
            writes.append(node)
            return (pb.WriteResponse(success=True, index=1) if node == 1 else
                    pb.WriteResponse(error="NOT_LEADER", leader_id="1"))
        with patch.object(self.client, "_call", side_effect=call):
            self.assertTrue(self.client.write(2, "key", "value").success)
        self.assertEqual(writes, [2, 1])

    def test_discover_unknown_or_unavailable_leader(self):
        for initial_offline in (False, True):
            writes = []
            def call(node, method, request):
                if node == 2 and initial_offline:
                    raise Unavailable()
                if method == "GetStatus":
                    return self.status(node)
                writes.append(node)
                return (pb.WriteResponse(success=True, index=1) if node == 1 else
                        pb.WriteResponse(error="NOT_LEADER"))
            with patch.object(self.client, "_call", side_effect=call):
                self.assertTrue(self.client.write(2, "key", "value").success)
            self.assertEqual(writes[-1], 1)

    def test_no_leader_and_connection_error_are_distinct(self):
        def no_leader(node, method, request):
            return self.status(node, leader=0) if method == "GetStatus" else pb.WriteResponse(error="NOT_LEADER")
        with patch.object(self.client, "_call", side_effect=no_leader):
            self.assertEqual(self.client.write(2, "key", "value").code, "NO_LEADER")
        with patch.object(self.client, "_call", side_effect=Unavailable()):
            self.assertEqual(self.client.write(2, "key", "value").code, "CONNECTION_ERROR")

    def test_redirect_cycle_is_bounded(self):
        writes = []
        def call(node, method, request):
            if method == "GetStatus":
                return self.status(node, leader=0)
            writes.append(node)
            return pb.WriteResponse(error="NOT_LEADER", leader_id="2" if node == 1 else "1")
        with patch.object(self.client, "_call", side_effect=call):
            self.assertFalse(self.client.write(1, "key", "value").success)
        self.assertEqual(writes, [1, 2])

    def test_no_retry_of_unconfirmed_write_and_quorum_message(self):
        for code in ("NO_QUORUM", "NOT_COMMITTED", "NOT_LEADER"):
            writes = []
            def call(node, method, request):
                if method == "GetStatus":
                    return self.status(node)
                writes.append(node)
                return pb.WriteResponse(error=code, index=0 if code == "NO_QUORUM" else 1)
            with patch.object(self.client, "_call", side_effect=call):
                result = self.client.write(1, "key", "value")
            self.assertFalse(result.success)
            self.assertEqual(writes, [1])
        output = StringIO()
        with redirect_stdout(output):
            show_write(WriteResult(False, "NO_QUORUM"))
        self.assertIn("Sem quórum", output.getvalue())

    def test_connection_failure_after_write_is_uncertain_without_retry(self):
        def call(node, method, request):
            if method == "GetStatus":
                return self.status(node)
            raise Unavailable()
        with patch.object(self.client, "_call", side_effect=call) as mocked:
            result = self.client.write(1, "key", "value")
        self.assertFalse(result.success)
        self.assertTrue(result.uncertain)
        self.assertEqual(mocked.call_count, 2)

    def test_read_all_continues_and_distinguishes_empty_value(self):
        def call(node, method, request):
            self.assertEqual(method, "Read")
            if node == 1:
                raise Unavailable()
            return pb.ReadResponse(exists=node == 2, value="", commit_index=3)
        with patch.object(self.client, "_call", side_effect=call):
            results = self.client.read_all("key")
        self.assertTrue(results[0].error)
        self.assertTrue(results[1].response.exists)
        self.assertFalse(results[2].response.exists)
        output = StringIO()
        with redirect_stdout(output):
            for result in results:
                show_read(result)
        self.assertIn("Nó 2, commit_index=3: valor=''", output.getvalue())
        self.assertIn("chave inexistente", output.getvalue())

    def test_menu_exit_and_invalid_input(self):
        with patch("sys.argv", ["client.py"]), patch("builtins.input", side_effect=["x", "3", "9", "5"]):
            with redirect_stdout(StringIO()) as output:
                self.assertEqual(main(), 0)
        self.assertIn("Opção inválida", output.getvalue())
        self.assertIn("Nó inválido", output.getvalue())
