import tempfile
import time
import unittest

from election import VoteResponse
from node_state import Node, Role
from replication import FetchRequest, FetchResponse, Replication
from storage import LogEntry
from config import HEARTBEAT_TIMEOUT


class ReplicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.leader = Node(1, self.temp.name)
        self.follower = Node(2, self.temp.name)
        self.addCleanup(self.leader.close)
        self.addCleanup(self.follower.close)
        request = self.leader.election.start_election()
        for peer in (2, 3):
            self.leader.election.receive_vote(peer, request.term, VoteResponse(request.term, True))
        self.follower.election.observe_term(request.term)

    def fetch_once(self):
        metadata, (index, term) = self.follower.storage.snapshot()
        response = self.leader.replication.fetch(FetchRequest(metadata.current_term, "2", index, term))
        return self.follower.replication.accept(1, response)

    def test_pull_uncommitted_then_commit(self):
        entry = LogEntry(1, 1, "key", "value")
        self.leader.storage.append_entries([entry])
        self.assertTrue(self.fetch_once())
        self.assertEqual(self.follower.storage.get_entries(), [entry])
        self.assertIsNone(self.follower.storage.read("key"))
        self.assertEqual(self.leader.storage.load_metadata().commit_index, 0)
        # Simula commit já decidido; não há cálculo de quórum nesta etapa.
        self.leader.storage.commit_and_apply(1)
        self.assertTrue(self.fetch_once())
        self.assertEqual(self.follower.storage.read("key"), "value")
        self.assertEqual(self.follower.storage.load_metadata().last_applied, 1)

    def test_heartbeat_activity_expires(self):
        now = [10.0]
        self.leader.replication = Replication(self.leader, clock=lambda: now[0])
        self.fetch_once()
        times, active = self.leader.replication.activity()
        self.assertEqual(times["2"], 10.0)
        self.assertTrue(active["2"])
        self.assertFalse(active["3"])
        now[0] += HEARTBEAT_TIMEOUT + 0.01
        self.assertFalse(self.leader.replication.activity()[1]["2"])
        self.fetch_once()
        self.assertTrue(self.leader.replication.activity()[1]["2"])

    def test_periodic_fetch_discovery_and_shutdown(self):
        calls = []
        def status(peer):
            return self.leader.get_status() if peer == 1 else None
        def fetch(peer, request):
            calls.append(time.monotonic())
            return self.leader.replication.fetch(request)
        self.follower.replication.start(status, fetch)
        deadline = time.monotonic() + 3
        while len(calls) < 3 and time.monotonic() < deadline:
            time.sleep(0.02)
        self.follower.replication.stop()
        self.assertGreaterEqual(len(calls), 3)
        self.assertTrue(all(b - a >= 0.08 for a, b in zip(calls, calls[1:])))
        self.assertFalse(self.follower.replication._thread.is_alive())
        self.assertEqual(self.follower.get_status().leader_id, "1")

    def test_failure_does_not_reset_timeout(self):
        self.fetch_once()
        deadline = self.follower.election._deadline
        self.follower.replication.step(lambda peer: None, lambda peer, request: None)
        self.assertEqual(self.follower.election._deadline, deadline)
        self.assertEqual(self.follower.get_status().leader_id, "")
        self.assertEqual(self.follower.get_status().role, Role.FOLLOWER)

    def test_higher_term_and_stale_leader_response(self):
        self.fetch_once()
        self.assertFalse(self.follower.replication.accept(1, FetchResponse(4, "", False)))
        self.assertEqual(self.follower.get_status().current_term, 4)
        deadline = self.follower.election._deadline
        self.assertFalse(self.follower.replication.accept(1, FetchResponse(1, "1", True)))
        self.assertEqual(self.follower.election._deadline, deadline)
        response = self.leader.replication.fetch(FetchRequest(5, "2", 0, 0))
        self.assertFalse(response.is_leader)
        self.assertEqual(self.leader.get_status().role, Role.FOLLOWER)
        self.assertEqual(self.leader.get_status().current_term, 5)

    def test_conflicting_suffix_and_committed_protection(self):
        self.leader.storage.append_entries([LogEntry(1, 1, "a", "correct")])
        self.follower.storage.append_entries([LogEntry(1, 0, "a", "wrong"),
                                              LogEntry(2, 0, "b", "extra")])
        self.fetch_once()
        self.fetch_once()
        self.fetch_once()
        self.assertEqual(self.follower.storage.get_entries(), self.leader.storage.get_entries())
        self.follower.storage.commit_and_apply(1)
        response = FetchResponse(1, "1", True, entries=[LogEntry(1, 1, "a", "bad")])
        with self.assertRaises(ValueError):
            self.follower.replication.accept(1, response)
        self.assertEqual(self.follower.storage.read("a"), "correct")
