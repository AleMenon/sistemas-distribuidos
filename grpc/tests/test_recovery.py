from concurrent.futures import ThreadPoolExecutor
import sqlite3
import tempfile
import unittest

import grpc

from cluster_protocol import cluster_pb2 as pb, cluster_pb2_grpc as rpc
from election import VoteResponse
from node_state import Node, Role
from replication import FetchRequest, FetchResponse
from service import ClusterService
from storage import LogEntry


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.leader = Node(1, self.temp.name)
        self.follower = Node(2, self.temp.name)
        self.addCleanup(self.leader.close)
        self.addCleanup(lambda: self.follower.close())
        self.leader.election.observe_term(1)
        request = self.leader.election.start_election()
        for peer in (2, 3):
            self.leader.election.receive_vote(peer, request.term, VoteResponse(request.term, True))
        self.follower.election.observe_term(request.term)
        self.common = LogEntry(1, 1, "base", "committed")

    def round(self):
        metadata, (index, term) = self.follower.storage.snapshot()
        response = self.leader.replication.fetch(FetchRequest(metadata.current_term, "2", index, term))
        self.assertTrue(self.follower.replication.accept(1, response))
        self.assertLessEqual(self.follower.storage.load_metadata().commit_index,
                             self.follower.storage.last_log_position()[0])
        return response

    def seed_common(self):
        for node in (self.leader, self.follower):
            node.storage.append_entries([self.common])
            node.storage.commit_and_apply(1)

    def test_lagging_follower_fetches_missing_entries(self):
        self.seed_common()
        self.leader.storage.append_entries([LogEntry(i, 2, str(i), str(i)) for i in range(2, 7)])
        self.leader.storage.commit_and_apply(5)
        response = self.round()
        self.assertIsNone(response.truncate_after_index)
        self.assertEqual([entry.index for entry in response.entries], list(range(2, 7)))
        self.assertEqual(self.follower.storage.load_metadata().commit_index, 5)
        self.assertEqual(self.follower.storage.read("5"), "5")
        self.assertIsNone(self.follower.storage.read("6"))

    def test_extra_uncommitted_suffix_is_removed_then_refetched(self):
        self.seed_common()
        self.follower.storage.append_entries([LogEntry(2, 1, "orphan", "lost")])
        response = self.round()
        self.assertEqual(response.truncate_after_index, 1)
        self.assertEqual(response.entries, [])
        self.assertEqual(self.follower.storage.get_entries(), [self.common])
        self.assertIsNone(self.follower.storage.read("orphan"))
        self.assertIsNone(self.round().truncate_after_index)

    def test_conflict_rewinds_until_common_prefix_without_premature_commit(self):
        self.seed_common()
        correct = [LogEntry(2, 2, "key", "new"), LogEntry(3, 2, "other", "new")]
        self.leader.storage.append_entries(correct)
        self.leader.storage.commit_and_apply(3)
        self.follower.storage.append_entries([LogEntry(2, 1, "key", "old"),
                                              LogEntry(3, 1, "other", "old")])
        for restart in (2, 1):
            response = self.round()
            self.assertEqual(response.truncate_after_index, restart)
            self.assertEqual(response.entries, [])
            self.assertEqual(self.follower.storage.load_metadata().commit_index, 1)
            self.assertIsNone(self.follower.storage.read("key"))
        self.round()
        self.assertEqual(self.follower.storage.get_entries(), self.leader.storage.get_entries())
        self.assertEqual(self.follower.storage.load_metadata().commit_index, 3)
        self.assertEqual(self.follower.storage.read("key"), "new")

    def test_committed_conflict_rejects_truncation(self):
        self.seed_common()
        self.leader.storage.append_entries([LogEntry(2, 2, "key", "new")])
        self.follower.storage.append_entries([LogEntry(2, 1, "key", "old")])
        self.follower.storage.commit_and_apply(2)
        before = self.follower.storage.get_entries()
        with self.assertRaises(ValueError):
            self.round()
        self.assertEqual(self.follower.storage.get_entries(), before)
        self.assertEqual(self.follower.storage.read("key"), "old")
        self.assertEqual(self.follower.storage.load_metadata().commit_index, 2)

    def test_restart_between_truncation_and_new_fetch(self):
        self.seed_common()
        self.leader.storage.append_entries([LogEntry(2, 2, "key", "new")])
        self.leader.storage.commit_and_apply(2)
        self.follower.storage.append_entries([LogEntry(2, 1, "key", "old")])
        self.assertEqual(self.round().truncate_after_index, 1)
        self.follower.close()
        self.follower = Node(2, self.temp.name)
        self.assertEqual(self.follower.get_status().role, Role.FOLLOWER)
        self.assertEqual(self.follower.storage.get_entries(), [self.common])
        self.round()
        self.assertEqual(self.follower.storage.read("key"), "new")
        self.assertEqual(self.follower.storage.get_entries(), self.leader.storage.get_entries())

    def test_rollback_during_truncation_preserves_log_after_reopen(self):
        self.seed_common()
        self.follower.storage.append_entries([LogEntry(2, 1, "a", "a"), LogEntry(3, 1, "b", "b")])
        with sqlite3.connect(self.follower.storage.path) as connection:
            connection.execute("""CREATE TRIGGER fail_delete BEFORE DELETE ON operation_log
                WHEN OLD.log_index = 3 BEGIN SELECT RAISE(ABORT, 'failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.round()
        self.follower.close()
        self.follower = Node(2, self.temp.name)
        self.assertEqual(self.follower.storage.last_log_position()[0], 3)
        self.assertEqual(self.follower.storage.read("base"), "committed")
        with sqlite3.connect(self.follower.storage.path) as connection:
            connection.execute("DROP TRIGGER fail_delete")
        self.round()
        self.assertEqual(self.follower.storage.get_entries(), [self.common])

    def test_commit_is_capped_by_received_prefix(self):
        response = FetchResponse(2, "1", True, commit_index=100,
                                 entries=[LogEntry(1, 2, "a", "value")])
        self.follower.replication.accept(1, response)
        self.assertEqual(self.follower.storage.load_metadata().commit_index, 1)

    def test_recovery_over_grpc(self):
        self.seed_common()
        self.leader.storage.append_entries([LogEntry(2, 2, "key", "new")])
        self.leader.storage.commit_and_apply(2)
        self.follower.storage.append_entries([LogEntry(2, 1, "key", "old")])
        with ThreadPoolExecutor(max_workers=2) as executor:
            server = grpc.server(executor)
            rpc.add_ClusterServiceServicer_to_server(ClusterService(self.leader), server)
            port = server.add_insecure_port("127.0.0.1:0")
            server.start()
            try:
                with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                    stub = rpc.ClusterServiceStub(channel)
                    for _ in range(2):
                        index, term = self.follower.storage.last_log_position()
                        response = stub.Fetch(pb.FetchRequest(term=2, follower_id="2",
                                              last_log_index=index, last_log_term=term), timeout=2)
                        self.follower.replication.accept(1, FetchResponse(
                            response.term, response.leader_id, response.is_leader, response.commit_index,
                            [LogEntry(e.index, e.term, e.key, e.value) for e in response.entries],
                            response.prev_log_index, response.prev_log_term,
                            response.truncate_after_index if response.HasField("truncate_after_index") else None))
            finally:
                server.stop(0).wait()
        self.assertEqual(self.follower.storage.get_entries(), self.leader.storage.get_entries())
        self.assertEqual(self.follower.storage.read("key"), "new")
