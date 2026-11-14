from concurrent.futures import ThreadPoolExecutor
import tempfile
import unittest

from election import Election, VoteRequest, VoteResponse
from node_state import Node, Role
from storage import LogEntry, Storage


class ElectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.node = Node(1, self.temp.name)
        self.addCleanup(lambda: self.node.close())
        self.election = self.node.election

    def test_fixed_quorum_and_unique_votes(self):
        request = self.election.start_election()
        self.assertEqual(self.node.storage.load_metadata().voted_for, "1")
        self.election.receive_vote(2, request.term, VoteResponse(request.term, True))
        self.election.receive_vote(2, request.term, VoteResponse(request.term, True))
        self.assertEqual(self.node.get_status().role, Role.CANDIDATE)
        self.election.receive_vote(3, request.term, VoteResponse(request.term, False))
        self.assertEqual(self.node.get_status().role, Role.CANDIDATE)
        self.election.receive_vote(4, request.term, VoteResponse(request.term, True))
        self.assertEqual(self.node.get_status().role, Role.LEADER)

    def test_single_vote_and_restart(self):
        self.assertTrue(self.election.request_vote(VoteRequest(3, "2", 0, 0)).vote_granted)
        self.assertTrue(self.election.request_vote(VoteRequest(3, "2", 0, 0)).vote_granted)
        self.assertFalse(self.election.request_vote(VoteRequest(3, "3", 0, 0)).vote_granted)
        self.node.close()
        self.node = Node(1, self.temp.name)
        self.assertEqual(self.node.get_status().role, Role.FOLLOWER)
        self.assertEqual(self.node.storage.load_metadata().current_term, 3)
        self.assertEqual(self.node.storage.load_metadata().voted_for, "2")
        self.assertFalse(self.node.election.request_vote(VoteRequest(3, "3", 0, 0)).vote_granted)

    def test_log_comparison_term_before_index(self):
        self.node.storage.append_entries([LogEntry(1, 2, "a", "a"), LogEntry(2, 2, "b", "b")])
        for candidate_index, candidate_term, expected in (
            (100, 1, False), (1, 2, False), (2, 2, True), (3, 2, True), (1, 3, True),
        ):
            with self.subTest(index=candidate_index, term=candidate_term):
                response = self.election.request_vote(VoteRequest(4, "2", candidate_index, candidate_term))
                self.assertEqual(response.vote_granted, expected)

    def test_higher_term_in_request_demotes_leader_even_if_vote_denied(self):
        request = self.election.start_election()
        for peer in (2, 3):
            self.election.receive_vote(peer, request.term, VoteResponse(request.term, True))
        self.node.storage.append_entries([LogEntry(1, 1, "a", "a")])
        response = self.election.request_vote(VoteRequest(8, "4", 0, 0))
        self.assertFalse(response.vote_granted)
        self.assertEqual(response.term, 8)
        self.assertEqual(self.node.get_status().role, Role.FOLLOWER)
        persisted = Storage(1, self.temp.name)
        try:
            self.assertEqual(persisted.load_metadata().current_term, 8)
            self.assertIsNone(persisted.load_metadata().voted_for)
        finally:
            persisted.close()

    def test_higher_and_stale_responses(self):
        first = self.election.start_election()
        second = self.election.start_election()
        for peer in (2, 3):
            self.election.receive_vote(peer, first.term, VoteResponse(first.term, True))
        self.assertEqual(self.node.get_status().role, Role.CANDIDATE)
        self.election.receive_vote(2, first.term, VoteResponse(second.term + 5, False))
        self.assertEqual(self.node.get_status().role, Role.FOLLOWER)
        self.assertEqual(self.node.storage.load_metadata().current_term, second.term + 5)
        self.assertIsNone(self.node.storage.load_metadata().voted_for)

    def test_valid_leader_interrupts_candidacy(self):
        request = self.election.start_election()
        self.assertFalse(self.election.observe_leader(request.term - 1, "2"))
        self.assertTrue(self.election.observe_leader(request.term, "2"))
        for peer in (2, 3):
            self.election.receive_vote(peer, request.term, VoteResponse(request.term, True))
        self.assertEqual(self.node.get_status().role, Role.FOLLOWER)
        self.assertEqual(self.node.get_status().leader_id, "2")
        self.assertEqual(self.node.storage.load_metadata().voted_for, "1")
        self.assertTrue(self.election.observe_leader(request.term + 1, "3"))
        self.assertIsNone(self.node.storage.load_metadata().voted_for)

    def test_concurrent_vote_requests(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(self.election.request_vote,
                                     [VoteRequest(2, "2", 0, 0), VoteRequest(2, "3", 0, 0)]))
        self.assertEqual(sum(response.vote_granted for response in responses), 1)

    def test_timeout_and_retry(self):
        now = [0.0]
        bounds = []
        def uniform(low, high):
            bounds.append((low, high))
            return (low + high) / 2
        election = Election(self.node, clock=lambda: now[0], uniform=uniform)
        self.assertIsNone(election.start_if_due())
        now[0] = 10
        first = election.start_if_due()
        self.assertEqual(first.term, 1)
        self.assertIsNone(election.start_if_due())
        now[0] = 20
        self.assertEqual(election.start_if_due().term, 2)
        self.assertEqual(len(bounds), 3)
        self.assertLess(bounds[0][0], bounds[0][1])
