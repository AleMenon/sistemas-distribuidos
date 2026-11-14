from concurrent.futures import ThreadPoolExecutor
import tempfile
from threading import Barrier
import unittest

from election import VoteResponse
from node_state import Node, Role
from replication import FetchRequest
from storage import Storage


class WriteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.nodes = [Node(i, self.temp.name) for i in range(1, 5)]
        for node in self.nodes:
            self.addCleanup(node.close)
        self.leader = self.nodes[0]
        request = self.leader.election.start_election()
        for peer in (2, 3):
            self.leader.election.receive_vote(peer, request.term, VoteResponse(request.term, True))
        self.term = request.term

    def test_persisted_sequential_entries_without_commit(self):
        for index, value in enumerate(("first", "second"), 1):
            result = self.leader.write("key", value)
            self.assertFalse(result.success)
            self.assertEqual((result.error, result.index, result.leader_id),
                             ("NOT_COMMITTED", index, "1"))
        # Uma conexão nova enxerga as entradas antes de qualquer confirmação.
        reopened = Storage(1, self.temp.name)
        try:
            entries = reopened.get_entries()
            self.assertEqual([(e.index, e.term, e.key, e.value) for e in entries],
                             [(1, self.term, "key", "first"), (2, self.term, "key", "second")])
            self.assertIsNone(reopened.read("key"))
            self.assertEqual(reopened.load_metadata().commit_index, 0)
            self.assertEqual(reopened.load_metadata().last_applied, 0)
        finally:
            reopened.close()

    def test_follower_and_candidate_reject(self):
        node = self.nodes[1]
        self.assertEqual(node.write("a", "b").error, "NOT_LEADER")
        node.election.observe_leader(self.term, "1")
        result = node.write("a", "b")
        self.assertEqual((result.success, result.error, result.index, result.leader_id),
                         (False, "NOT_LEADER", 0, "1"))
        node.election.start_election()
        result = node.write("a", "b")
        self.assertEqual((result.error, result.leader_id), ("NOT_LEADER", ""))
        self.assertEqual(node.storage.get_entries(), [])

    def test_pull_to_all_followers_keeps_data_invisible(self):
        self.leader.write("key", "value")
        for node in self.nodes[1:]:
            node.election.observe_term(self.term)
            response = self.leader.replication.fetch(FetchRequest(self.term, str(node._node_id), 0, 0))
            self.assertTrue(node.replication.accept(1, response))
        for node in self.nodes:
            self.assertEqual(node.storage.last_log_position(), (1, self.term))
            self.assertIsNone(node.storage.read("key"))
            self.assertEqual(node.storage.load_metadata().commit_index, 0)

    def test_concurrent_writes_preserve_unique_sequence(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda i: self.leader.write("key", str(i)), range(32)))
        self.assertEqual(sorted(r.index for r in results), list(range(1, 33)))
        entries = self.leader.storage.get_entries()
        self.assertEqual([e.index for e in entries], list(range(1, 33)))
        for value, result in enumerate(results):
            self.assertEqual(entries[result.index - 1].value, str(value))
            self.assertEqual(entries[result.index - 1].term, self.term)

    def test_write_racing_with_step_down(self):
        barrier = Barrier(2)
        def write():
            barrier.wait()
            return self.leader.write("key", "value")
        def step_down():
            barrier.wait()
            self.leader.election.observe_term(self.term + 1)
        with ThreadPoolExecutor(max_workers=2) as pool:
            write_future = pool.submit(write)
            change_future = pool.submit(step_down)
            result = write_future.result(timeout=3)
            change_future.result(timeout=3)
        self.assertEqual(self.leader.get_status().role, Role.FOLLOWER)
        entries = self.leader.storage.get_entries()
        if result.error == "NOT_COMMITTED":
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].term, self.term)
        else:
            self.assertEqual(result.error, "NOT_LEADER")
            self.assertEqual(entries, [])
        self.assertEqual(self.leader.write("later", "value").error, "NOT_LEADER")
        self.assertIsNone(self.leader.storage.read("key"))
