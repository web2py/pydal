# -*- coding: utf-8 -*-

"""Unit coverage for connection acquisition and initialization."""

from pydal.connection import ConnectionPool

from ._compat import unittest


class RecordingCursor:
    def close(self):
        return None


class RecordingConnection:
    def __init__(self, events):
        self.events = events
        self.in_transaction = False

    def cursor(self):
        return RecordingCursor()

    def commit(self):
        self.events.append("commit")
        self.in_transaction = False


class RecordingConnectionPool(ConnectionPool):
    def __init__(self, events):
        super().__init__()
        self.events = events

    def after_connection_hook(self):
        self.events.append("hook")
        self.connection.in_transaction = True

    def test_connection(self):
        self.events.append("test")
        self.connection.in_transaction = True


class TestConnectionInitialization(unittest.TestCase):
    def test_initialization_transaction_is_committed_before_connection_is_returned(
        self,
    ):
        events = []
        connection = RecordingConnection(events)
        pool = RecordingConnectionPool(events)

        pool.set_connection(connection, run_hooks=True)

        self.assertEqual(events, ["hook", "test", "commit"])
        self.assertFalse(connection.in_transaction)

    def test_pooled_connection_check_is_committed_before_checkout(self):
        events = []
        connection = RecordingConnection(events)
        pool = RecordingConnectionPool(events)

        pool.set_connection(connection, run_hooks=False)

        self.assertEqual(events, ["test", "commit"])
        self.assertFalse(connection.in_transaction)

    def test_hooks_are_committed_when_connection_check_is_disabled(self):
        events = []
        connection = RecordingConnection(events)
        pool = RecordingConnectionPool(events)
        pool.check_active_connection = False

        pool.set_connection(connection, run_hooks=True)

        self.assertEqual(events, ["hook", "commit"])
        self.assertFalse(connection.in_transaction)


if __name__ == "__main__":
    unittest.main()
