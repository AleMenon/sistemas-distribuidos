from concurrent.futures import ThreadPoolExecutor
import tempfile
import time
import unittest

from election import VoteResponse
from node_state import Node
from replication import FetchRequest
from storage import LogEntry


class CommitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.nodes = {i: Node(i, self.temp.name) for i in range(1, 5)}
        for node in self.nodes.values():
            self.addCleanup(node.close)
        self.leader = self.nodes[1]
        self.elect()

    def elect(self):
        request = self.leader.election.start_election()
        for peer in (2, 3):
            self.leader.election.receive_vote(peer, request.term, VoteResponse(request.term, True))
        self.term = request.term

    def fetch(self, peer):
        node = self.nodes[peer]
        node.election.observe_term(self.term)
        index, term = node.storage.last_log_position()
        response = self.leader.replication.fetch(FetchRequest(self.term, str(peer), index, term))
        self.assertTrue(node.replication.accept(1, response))
        return response

    def active_quorum(self):
        self.fetch(2)
        self.fetch(3)

    def wait_for_log(self, index):
        deadline = time.monotonic() + 1
        while self.leader.storage.last_log_position()[0] < index:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.005)

    def test_commit_requires_confirmations_and_propagates_on_later_fetch(self):
        self.active_quorum()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.leader.write, "key", "value", 1)
            self.wait_for_log(1)
            self.fetch(2)  # Recebe entrada, ainda não confirma recebimento.
            self.fetch(3)
            self.assertEqual(self.leader.storage.load_metadata().commit_index, 0)
            for node in self.nodes.values():
                self.assertIsNone(node.storage.read("key"))
            self.fetch(2)  # Duas cópias confirmadas, incluindo o líder.
            self.assertFalse(future.done())
            self.assertEqual(self.leader.storage.load_metadata().commit_index, 0)
            self.fetch(3)  # Terceira cópia: commit e notificação ao Write.
            result = future.result(timeout=1)
        self.assertTrue(result.success)
        self.assertEqual(result.index, 1)
        self.assertEqual(self.leader.storage.read("key"), "value")
        self.assertIsNone(self.nodes[2].storage.read("key"))
        self.assertEqual(self.nodes[3].storage.read("key"), "value")
        self.fetch(2)
        self.assertEqual(self.nodes[2].storage.read("key"), "value")

    def test_known_two_nodes_reject_without_appending(self):
        self.fetch(2)
        result = self.leader.write("key", "value")
        self.assertEqual((result.success, result.error, result.index), (False, "NO_QUORUM", 0))
        self.assertEqual(self.leader.storage.last_log_position(), (0, 0))

    def test_timeout_with_only_two_confirmed_copies(self):
        self.active_quorum()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.leader.write, "key", "value", 0.15)
            self.wait_for_log(1)
            self.fetch(2)
            self.fetch(2)
            result = future.result(timeout=1)
        self.assertEqual((result.success, result.error, result.index), (False, "NOT_COMMITTED", 1))
        self.assertEqual(self.leader.storage.load_metadata().commit_index, 0)
        self.assertIsNone(self.leader.storage.read("key"))

    def test_expired_heartbeat_rejects_write(self):
        now = [10.0]
        self.leader.replication._clock = lambda: now[0]
        self.active_quorum()
        now[0] += 10
        self.assertEqual(self.leader.write("key", "value").error, "NO_QUORUM")
        self.assertEqual(self.leader.storage.get_entries(), [])

    def test_leadership_loss_wakes_waiter_without_success(self):
        self.active_quorum()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.leader.write, "key", "value", 5)
            self.wait_for_log(1)
            self.leader.election.observe_term(self.term + 1)
            result = future.result(timeout=1)
        self.assertEqual((result.success, result.error), (False, "NOT_LEADER"))
        self.assertEqual(self.leader.replication.match_index, {})
        self.assertEqual(self.leader.storage.load_metadata().commit_index, 0)

    def test_wrong_log_term_and_index_do_not_confirm(self):
        self.leader.storage.append_entries([LogEntry(1, self.term, "key", "value")])
        for peer, index, term in ((2, 1, self.term + 1), (3, 20, self.term)):
            self.leader.replication.fetch(FetchRequest(self.term, str(peer), index, term))
        self.assertEqual(self.leader.replication.match_index, {"2": 0, "3": 0})
        self.assertEqual(self.leader.storage.load_metadata().commit_index, 0)

    def test_new_term_requires_new_confirmations_and_current_term_entry(self):
        self.leader.storage.append_entries([LogEntry(1, self.term, "old", "value")])
        self.fetch(2)
        self.fetch(2)
        old_term = self.term
        self.leader.election.observe_term(old_term + 1)
        self.elect()
        self.assertEqual(self.leader.replication.match_index, {})
        # Mensagem atrasada do mandato anterior não conta.
        self.leader.replication.fetch(FetchRequest(old_term, "3", 1, old_term))
        self.assertEqual(self.leader.replication.match_index, {})
        self.fetch(2)
        self.fetch(3)
        self.fetch(3)
        self.assertEqual(self.leader.storage.load_metadata().commit_index, 0)
        self.assertIsNone(self.leader.storage.read("old"))
        # A confirmação de entrada do mandato atual comita também o prefixo antigo.
        self.leader.storage.append_entries([LogEntry(2, self.term, "new", "value")])
        for peer in (2, 3):
            self.fetch(peer)
            self.fetch(peer)
        self.assertEqual(self.leader.storage.load_metadata().commit_index, 2)
        self.assertEqual(self.leader.storage.read("old"), "value")
        self.assertEqual(self.leader.storage.read("new"), "value")
