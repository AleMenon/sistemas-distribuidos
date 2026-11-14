from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import tempfile
import unittest

from node_state import Node, Role
from storage import LogEntry, Metadata, Storage


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Storage(1, self.temp.name)
        self.addCleanup(lambda: self.db.close())

    def reopen(self):
        self.db.close()
        self.db = Storage(1, self.temp.name)

    def test_metadata_and_log_survive_restart(self):
        self.assertEqual(self.db.load_metadata(), Metadata(0, None, 0, 0))
        self.assertEqual(self.db.last_log_position(), (0, 0))
        self.db.save_metadata(4, "2")
        entries = [LogEntry(1, 3, "a", "ação"), LogEntry(2, 4, "b", "")]
        self.db.append_entries(entries)
        self.reopen()
        self.assertEqual(self.db.load_metadata(), Metadata(4, "2", 0, 0))
        self.assertEqual(self.db.get_entries(), entries)
        self.assertEqual(self.db.get_entries(2), entries[1:])
        self.assertEqual(self.db.last_log_position(), (2, 4))
        with self.assertRaises(ValueError):
            self.db.save_metadata(3, None)
        self.assertEqual(self.db.load_metadata().voted_for, "2")
        self.db.save_metadata(5, None)
        self.reopen()
        self.assertEqual(self.db.load_metadata().voted_for, None)

    def test_commit_visibility_and_restart(self):
        self.db.append_entries([LogEntry(1, 1, "a", "old"),
                                LogEntry(2, 1, "a", "new"),
                                LogEntry(3, 1, "empty", "")])
        self.assertIsNone(self.db.read("a"))
        self.db.commit_and_apply(1)
        self.assertEqual(self.db.read("a"), "old")
        self.assertIsNone(self.db.read("empty"))
        self.reopen()
        self.assertEqual(self.db.read("a"), "old")
        self.assertEqual(self.db.load_metadata(), Metadata(0, None, 1, 1))
        self.db.commit_and_apply(3)
        self.db.commit_and_apply(3)  # Idempotente.
        self.reopen()
        self.assertEqual(self.db.read("a"), "new")
        self.assertEqual(self.db.read("empty"), "")
        self.assertEqual(self.db.load_metadata().last_applied, 3)
        for invalid in (2, 4):
            with self.assertRaises(ValueError):
                self.db.commit_and_apply(invalid)
        self.assertEqual(self.db.load_metadata().commit_index, 3)

    def test_truncation_protects_committed_prefix(self):
        self.db.append_entries([LogEntry(i, 1, str(i), str(i)) for i in range(1, 4)])
        self.db.commit_and_apply(1)
        for index in (0, 1):
            with self.assertRaises(ValueError):
                self.db.truncate_from(index)
        self.assertEqual(len(self.db.get_entries()), 3)
        self.db.truncate_from(2)
        self.reopen()
        self.assertEqual(self.db.last_log_position(), (1, 1))
        self.assertEqual(self.db.read("1"), "1")
        self.assertIsNone(self.db.read("2"))
        self.db.append_entries([LogEntry(2, 2, "2", "replacement")])
        self.db.commit_and_apply(2)
        self.assertEqual(self.db.read("2"), "replacement")

    def test_append_batch_is_atomic(self):
        with self.assertRaises(ValueError):
            self.db.append_entries([LogEntry(1, 1, "a", "a"), LogEntry(3, 1, "b", "b")])
        self.reopen()
        self.assertEqual(self.db.last_log_position(), (0, 0))
        with self.assertRaises(ValueError):
            self.db.append_entries([LogEntry(0, 1, "a", "a")])

    def test_apply_failure_rolls_back_data_and_metadata(self):
        self.db.append_entries([LogEntry(1, 1, "a", "a"), LogEntry(2, 1, "b", "b")])
        # Injeção de falha na segunda operação, após a primeira ser aplicada.
        with sqlite3.connect(self.db.path) as connection:
            connection.execute("""CREATE TRIGGER fail_apply BEFORE INSERT ON committed_state
                WHEN NEW.key = 'b' BEGIN SELECT RAISE(ABORT, 'test failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.commit_and_apply(2)
        self.reopen()
        self.assertIsNone(self.db.read("a"))
        self.assertEqual(self.db.load_metadata(), Metadata(0, None, 0, 0))
        with sqlite3.connect(self.db.path) as connection:
            connection.execute("DROP TRIGGER fail_apply")
        self.db.commit_and_apply(2)
        self.assertEqual(self.db.read("b"), "b")

    def test_four_independent_databases(self):
        for node_id in range(1, 5):
            db = Storage(node_id, self.temp.name)
            try:
                db.save_metadata(node_id, str(node_id))
                db.append_entries([LogEntry(1, node_id, "same", str(node_id))])
                db.commit_and_apply(1)
            finally:
                db.close()
        self.assertEqual(len(list(Path(self.temp.name).glob("node_*.db"))), 4)
        for node_id in range(1, 5):
            node = Node(node_id, self.temp.name)
            try:
                self.assertEqual(node.storage.read("same"), str(node_id))
                self.assertEqual(node.storage.load_metadata().voted_for, str(node_id))
                status = node.get_status()
                self.assertEqual(status.current_term, node_id)
                self.assertEqual(status.role, Role.FOLLOWER)
                self.assertEqual(status.leader_id, "")
                self.assertEqual((status.commit_index, status.last_log_index), (1, 1))
            finally:
                node.close()

    def test_concurrent_reads_and_commit(self):
        self.db.append_entries([LogEntry(i, 1, "a", str(i)) for i in range(1, 51)])

        def writer():
            for index in range(1, 51):
                self.db.commit_and_apply(index)

        def reader():
            for _ in range(100):
                metadata, (index, _) = self.db.snapshot()
                self.assertEqual(metadata.commit_index, metadata.last_applied)
                self.assertLessEqual(metadata.commit_index, index)
                value = self.db.read("a")
                self.assertTrue(value is None or 1 <= int(value) <= 50)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(reader) for _ in range(4)]
            futures.append(executor.submit(writer))
            for future in futures:
                future.result(timeout=10)
        self.assertEqual(self.db.read("a"), "50")
