"""Valida o contrato sem iniciar servidores ou implementar handlers."""

import importlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from scripts.generate_proto import generate


class ProtocolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.output = Path(cls.temp.name)
        if generate(cls.output) != 0:
            raise RuntimeError("Falha na geração do protocolo")
        (cls.output / "cluster_protocol" / "__init__.py").touch()
        sys.path.insert(0, cls.temp.name)
        cls.addClassCleanup(sys.path.remove, cls.temp.name)
        cls.pb = importlib.import_module("cluster_protocol.cluster_pb2")

    def test_generation_and_stub_import(self):
        result = subprocess.run(
            [sys.executable, "-c",
             "from cluster_protocol.cluster_pb2_grpc import ClusterServiceStub; "
             "from cluster_protocol.demo_pb2_grpc import DemoControlStub; "
             "from cluster_protocol.cluster_pb2 import FetchResponse; "
             "message = FetchResponse(truncate_after_index=0); "
             "assert FetchResponse.FromString(message.SerializeToString()).HasField('truncate_after_index')"],
            cwd=self.output, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        service = self.pb.DESCRIPTOR.services_by_name["ClusterService"]
        self.assertEqual([m.name for m in service.methods],
                         ["GetStatus", "RequestVote", "Fetch", "Write", "Read"])

    def test_message_round_trips(self):
        p = self.pb
        entry = p.LogEntry(index=1, term=2, key="chave", value="ação")
        messages = [
            entry, p.GetStatusRequest(),
            p.GetStatusResponse(node_id="n1", role=p.LEADER, term=2,
                                commit_index=1, last_log_index=1, leader_id="n1"),
            p.RequestVoteRequest(term=2, candidate_id="n1", last_log_index=1,
                                 last_log_term=1),
            p.RequestVoteResponse(term=2, vote_granted=True),
            p.FetchRequest(term=2, follower_id="n2", last_log_index=1,
                           last_log_term=2),
            p.FetchResponse(term=2, leader_id="n1", is_leader=True,
                            commit_index=1, entries=[entry], prev_log_index=0,
                            prev_log_term=0, truncate_after_index=0),
            p.WriteRequest(key="chave", value="ação"),
            p.WriteResponse(success=True, leader_id="n1", index=1),
            p.WriteResponse(error="Sem quórum", leader_id="n1"),
            p.ReadRequest(key="chave"),
            p.ReadResponse(value="", exists=True, commit_index=1),
            p.ReadResponse(exists=False, commit_index=1),
        ]
        for message in messages:
            with self.subTest(message=type(message).__name__):
                self.assertEqual(type(message).FromString(message.SerializeToString()),
                                 message)

    def test_empty_log_and_optional_truncation(self):
        p = self.pb
        self.assertEqual(p.GetStatusResponse().commit_index, 0)
        self.assertEqual(p.FetchRequest().last_log_index, 0)
        self.assertEqual(p.FetchRequest().last_log_term, 0)
        response = p.FetchResponse()
        self.assertFalse(response.HasField("truncate_after_index"))
        response.truncate_after_index = 0
        restored = p.FetchResponse.FromString(response.SerializeToString())
        self.assertTrue(restored.HasField("truncate_after_index"))
        self.assertEqual(restored.truncate_after_index, 0)


if __name__ == "__main__":
    unittest.main()
