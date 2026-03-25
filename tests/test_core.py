"""Tests for Nexus Engine — Durable Event Bus."""

import os
import sys
import json
import time
import tempfile
import unittest
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from nexus_engine.core import NexusEngine, Event, ConsumerLag, DeadLetterEntry


class NexusTestCase(unittest.TestCase):
    """Base class with temp DB setup/teardown."""

    def setUp(self):
        self.db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.db_file.name
        self.db_file.close()
        self.engine = NexusEngine({"db_path": self.db_path})

    def tearDown(self):
        self.engine.close()
        for ext in ["", "-wal", "-shm"]:
            p = self.db_path + ext
            if os.path.exists(p):
                os.unlink(p)


class TestPublish(NexusTestCase):

    def test_publish_single(self):
        event = self.engine.publish(
            event_type="test.event",
            payload={"key": "value"},
            source="test",
        )
        self.assertIsInstance(event, Event)
        self.assertEqual(event.event_type, "test.event")
        self.assertEqual(event.payload, {"key": "value"})
        self.assertEqual(event.source, "test")
        self.assertGreater(event.sequence_id, 0)
        self.assertTrue(event.event_id)

    def test_publish_with_metadata(self):
        event = self.engine.publish(
            event_type="session.start",
            payload={"role": "user"},
            source="gateway",
            correlation_id="corr-123",
            causation_id="cause-456",
            metadata={"agent_id": "default", "session_id": "sess-1"},
            partition_key="agent:default",
        )
        self.assertEqual(event.correlation_id, "corr-123")
        self.assertEqual(event.metadata["agent_id"], "default")
        self.assertEqual(event.partition_key, "agent:default")

    def test_publish_batch(self):
        events = self.engine.publish_batch([
            {"event_type": "tool.invocation", "payload": {"tool": "grep"}, "source": "exec"},
            {"event_type": "tool.result", "payload": {"success": True}, "source": "exec"},
            {"event_type": "session.message", "payload": {"role": "user"}, "source": "gateway"},
        ])
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].event_type, "tool.invocation")
        self.assertEqual(events[2].event_type, "session.message")
        # Sequence IDs should be monotonic
        self.assertLess(events[0].sequence_id, events[1].sequence_id)
        self.assertLess(events[1].sequence_id, events[2].sequence_id)

    def test_publish_monotonic_sequence(self):
        e1 = self.engine.publish("a", {}, "s")
        e2 = self.engine.publish("b", {}, "s")
        e3 = self.engine.publish("c", {}, "s")
        self.assertEqual(e2.sequence_id, e1.sequence_id + 1)
        self.assertEqual(e3.sequence_id, e2.sequence_id + 1)

    def test_unique_event_ids(self):
        ids = set()
        for _ in range(100):
            e = self.engine.publish("test", {}, "s")
            ids.add(e.event_id)
        self.assertEqual(len(ids), 100)


class TestConsumerGroups(NexusTestCase):

    def test_create_group(self):
        gid = self.engine.create_consumer_group("my-group", "Test group")
        self.assertEqual(gid, "my-group")

    def test_list_groups(self):
        self.engine.create_consumer_group("group-a")
        self.engine.create_consumer_group("group-b")
        groups = self.engine.list_consumer_groups()
        names = [g["group_id"] for g in groups]
        self.assertIn("group-a", names)
        self.assertIn("group-b", names)

    def test_delete_group(self):
        self.engine.create_consumer_group("temp-group")
        self.assertTrue(self.engine.delete_consumer_group("temp-group"))
        groups = self.engine.list_consumer_groups()
        names = [g["group_id"] for g in groups]
        self.assertNotIn("temp-group", names)


class TestSubscriptions(NexusTestCase):

    def test_subscribe(self):
        self.engine.create_consumer_group("indexer")
        sub_id = self.engine.subscribe(
            "indexer",
            event_filter={"event_types": ["session.*"]},
        )
        self.assertTrue(sub_id)

    def test_list_subscriptions(self):
        self.engine.create_consumer_group("indexer")
        self.engine.subscribe("indexer", {"event_types": ["session.*"]})
        self.engine.subscribe("indexer", {"event_types": ["tool.*"]})
        subs = self.engine.list_subscriptions("indexer")
        self.assertEqual(len(subs), 2)

    def test_unsubscribe(self):
        self.engine.create_consumer_group("indexer")
        sub_id = self.engine.subscribe("indexer", {"event_types": ["*"]})
        self.assertTrue(self.engine.unsubscribe(sub_id))
        subs = self.engine.list_subscriptions("indexer")
        self.assertEqual(len(subs), 0)


class TestPollAndAcknowledge(NexusTestCase):

    def test_poll_returns_matching_events(self):
        self.engine.create_consumer_group("reader")
        self.engine.subscribe("reader", {"event_types": ["session.*"]})

        self.engine.publish("session.start", {"id": 1}, "gw")
        self.engine.publish("tool.invocation", {"tool": "grep"}, "exec")
        self.engine.publish("session.message", {"id": 2}, "gw")

        events = self.engine.poll("reader")
        types = [e.event_type for e in events]
        self.assertIn("session.start", types)
        self.assertIn("session.message", types)
        self.assertNotIn("tool.invocation", types)

    def test_acknowledge_advances_checkpoint(self):
        self.engine.create_consumer_group("reader")
        self.engine.subscribe("reader", {"event_types": ["*"]})

        e1 = self.engine.publish("a", {}, "s")
        e2 = self.engine.publish("b", {}, "s")
        e3 = self.engine.publish("c", {}, "s")

        # Poll gets all 3
        events = self.engine.poll("reader")
        self.assertEqual(len(events), 3)

        # Acknowledge up to e2
        self.engine.acknowledge("reader", e2.sequence_id)

        # Next poll should only get e3
        events = self.engine.poll("reader")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "c")

    def test_empty_poll(self):
        self.engine.create_consumer_group("empty")
        self.engine.subscribe("empty", {"event_types": ["*"]})
        events = self.engine.poll("empty")
        self.assertEqual(len(events), 0)

    def test_source_filter(self):
        self.engine.create_consumer_group("cortex-only")
        self.engine.subscribe("cortex-only", {
            "event_types": ["*"],
            "sources": ["cortex"],
        })

        self.engine.publish("memory.stored", {}, "cortex")
        self.engine.publish("tool.invocation", {}, "executor")

        events = self.engine.poll("cortex-only")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].source, "cortex")


class TestReplay(NexusTestCase):

    def test_replay_all(self):
        for i in range(10):
            self.engine.publish(f"event.{i}", {"i": i}, "test")

        events = list(self.engine.replay())
        self.assertEqual(len(events), 10)

    def test_replay_with_type_filter(self):
        self.engine.publish("session.start", {}, "gw")
        self.engine.publish("tool.invocation", {}, "exec")
        self.engine.publish("session.end", {}, "gw")

        events = list(self.engine.replay(event_type="session.*"))
        self.assertEqual(len(events), 2)

    def test_replay_from_sequence(self):
        for i in range(5):
            self.engine.publish(f"e{i}", {}, "s")

        events = list(self.engine.replay(from_sequence=3))
        self.assertEqual(len(events), 3)  # sequences 3, 4, 5

    def test_replay_with_limit(self):
        for i in range(20):
            self.engine.publish("e", {"i": i}, "s")

        events = list(self.engine.replay(limit=5))
        self.assertEqual(len(events), 5)

    def test_replay_for_group(self):
        self.engine.create_consumer_group("filtered")
        self.engine.subscribe("filtered", {"event_types": ["session.*"]})

        self.engine.publish("session.start", {}, "gw")
        self.engine.publish("tool.call", {}, "exec")
        self.engine.publish("session.end", {}, "gw")

        events = list(self.engine.replay_for_group("filtered"))
        self.assertEqual(len(events), 2)

    def test_reset_checkpoint_enables_replay(self):
        self.engine.create_consumer_group("resettable")
        self.engine.subscribe("resettable", {"event_types": ["*"]})

        for i in range(5):
            self.engine.publish(f"e{i}", {}, "s")

        # Poll and acknowledge all
        events = self.engine.poll("resettable")
        self.engine.acknowledge("resettable", events[-1].sequence_id)

        # Should be empty now
        self.assertEqual(len(self.engine.poll("resettable")), 0)

        # Reset to beginning
        self.engine.reset_checkpoint("resettable", to_sequence=0)

        # Should get all events again
        events = self.engine.poll("resettable")
        self.assertEqual(len(events), 5)


class TestDeadLetterQueue(NexusTestCase):

    def test_nack_creates_dlq_entry(self):
        e = self.engine.publish("test", {}, "s")
        self.engine.nack("group1", e.event_id, "sub1", "Processing failed")

        dlq = self.engine.get_dlq("group1")
        self.assertEqual(len(dlq), 1)
        self.assertEqual(dlq[0].event_id, e.event_id)
        self.assertEqual(dlq[0].attempt_count, 1)
        self.assertEqual(dlq[0].last_error, "Processing failed")

    def test_nack_increments_attempt(self):
        e = self.engine.publish("test", {}, "s")
        self.engine.nack("g", e.event_id, "s1", "fail1")
        self.engine.nack("g", e.event_id, "s1", "fail2")
        self.engine.nack("g", e.event_id, "s1", "fail3")

        dlq = self.engine.get_dlq("g")
        self.assertEqual(dlq[0].attempt_count, 3)
        self.assertEqual(dlq[0].last_error, "fail3")

    def test_resolve_dlq(self):
        e = self.engine.publish("test", {}, "s")
        self.engine.nack("g", e.event_id, "s1", "fail")

        dlq = self.engine.get_dlq("g")
        self.assertTrue(self.engine.resolve_dlq(dlq[0].id))

        # Should not appear in unresolved
        self.assertEqual(len(self.engine.get_dlq("g")), 0)
        # But should appear with include_resolved
        self.assertEqual(len(self.engine.get_dlq("g", include_resolved=True)), 1)

    def test_retry_dlq(self):
        e = self.engine.publish("test", {}, "s")
        self.engine.nack("g", e.event_id, "s1", "fail")
        self.engine.nack("g", e.event_id, "s1", "fail")

        dlq = self.engine.get_dlq("g")
        self.assertEqual(dlq[0].attempt_count, 2)

        self.engine.retry_dlq(dlq[0].id)
        dlq = self.engine.get_dlq("g")
        self.assertEqual(dlq[0].attempt_count, 0)


class TestSchemaRegistry(NexusTestCase):

    def test_register_and_get_schema(self):
        schema = {"type": "object", "properties": {"role": {"type": "string"}}}
        self.engine.register_schema("session.message", "1.0", schema, "Message schema")

        result = self.engine.get_schema("session.message")
        self.assertIsNotNone(result)
        self.assertEqual(result["version"], "1.0")
        self.assertEqual(result["schema_def"]["type"], "object")

    def test_deprecate_schema(self):
        self.engine.register_schema("old.event", "1.0", {})
        self.assertTrue(self.engine.deprecate_schema("old.event", "1.0"))
        self.assertIsNone(self.engine.get_schema("old.event"))

    def test_multiple_versions(self):
        self.engine.register_schema("event", "1.0", {"v": 1})
        self.engine.register_schema("event", "2.0", {"v": 2})

        result = self.engine.get_schema("event")
        self.assertEqual(result["version"], "2.0")

        result = self.engine.get_schema("event", "1.0")
        self.assertEqual(result["schema_def"]["v"], 1)


class TestBackpressure(NexusTestCase):

    def test_get_lag(self):
        self.engine.create_consumer_group("slow")
        self.engine.subscribe("slow", {"event_types": ["*"]})

        for i in range(10):
            self.engine.publish(f"e{i}", {}, "s")

        lag = self.engine.get_lag("slow")
        self.assertIsInstance(lag, ConsumerLag)
        self.assertEqual(lag.lag, 10)

        # Acknowledge half
        events = self.engine.poll("slow", limit=5)
        self.engine.acknowledge("slow", events[-1].sequence_id)

        lag = self.engine.get_lag("slow")
        self.assertEqual(lag.lag, 5)


class TestCompaction(NexusTestCase):

    def test_compact_respects_checkpoints(self):
        self.engine.create_consumer_group("g")
        self.engine.subscribe("g", {"event_types": ["*"]})

        for i in range(10):
            self.engine.publish(f"e{i}", {}, "s")

        # Acknowledge up to seq 5
        self.engine.acknowledge("g", 5)

        # Compact should only remove events before checkpoint
        removed = self.engine.compact()
        self.assertLessEqual(removed, 5)

        # Remaining events should still be available
        remaining = list(self.engine.replay())
        self.assertGreaterEqual(len(remaining), 5)


class TestStats(NexusTestCase):

    def test_stats(self):
        self.engine.publish("a", {}, "s1")
        self.engine.publish("b", {}, "s2")
        self.engine.create_consumer_group("g1")

        stats = self.engine.stats()
        self.assertEqual(stats["total_events"], 2)
        self.assertEqual(stats["consumer_groups"], 1)
        self.assertIn("event_type_distribution", stats)
        self.assertIn("source_distribution", stats)


class TestHealthCheck(NexusTestCase):

    def test_health(self):
        health = self.engine.health_check()
        self.assertTrue(health["healthy"])
        self.assertEqual(health["engine"], "nexus")


class TestConcurrency(NexusTestCase):

    def test_concurrent_publish(self):
        """Multiple threads publishing simultaneously."""
        errors = []

        def publisher(prefix, count):
            try:
                for i in range(count):
                    self.engine.publish(f"{prefix}.{i}", {"i": i}, prefix)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=publisher, args=(f"t{i}", 50))
            for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        stats = self.engine.stats()
        self.assertEqual(stats["total_events"], 250)


class TestPartitions(NexusTestCase):

    def test_partition_isolation(self):
        self.engine.publish("e", {"p": "a"}, "s", partition_key="partition-a")
        self.engine.publish("e", {"p": "b"}, "s", partition_key="partition-b")

        self.engine.create_consumer_group("reader")
        self.engine.subscribe("reader", {"event_types": ["*"]})

        events_a = self.engine.poll("reader", partition_key="partition-a")
        self.assertEqual(len(events_a), 1)
        self.assertEqual(events_a[0].payload["p"], "a")


class TestIdempotentPublish(NexusTestCase):

    def test_idempotent_publish_deduplicates(self):
        e1 = self.engine.publish("test", {"v": 1}, "s", idempotency_key="key-1")
        e2 = self.engine.publish("test", {"v": 1}, "s", idempotency_key="key-1")
        self.assertEqual(e1.event_id, e2.event_id)
        self.assertEqual(e1.sequence_id, e2.sequence_id)
        self.assertTrue(e2.deduplicated)
        self.assertFalse(e1.deduplicated)

    def test_different_keys_not_deduplicated(self):
        e1 = self.engine.publish("test", {}, "s", idempotency_key="key-a")
        e2 = self.engine.publish("test", {}, "s", idempotency_key="key-b")
        self.assertNotEqual(e1.event_id, e2.event_id)

    def test_same_key_different_source(self):
        e1 = self.engine.publish("test", {}, "s1", idempotency_key="key-1")
        e2 = self.engine.publish("test", {}, "s2", idempotency_key="key-1")
        self.assertNotEqual(e1.event_id, e2.event_id)

    def test_no_key_no_dedupe(self):
        e1 = self.engine.publish("test", {}, "s")
        e2 = self.engine.publish("test", {}, "s")
        self.assertNotEqual(e1.event_id, e2.event_id)


class TestTTLExpiry(NexusTestCase):

    def test_event_has_expires_at(self):
        e = self.engine.publish("test", {}, "s", ttl_seconds=60)
        self.assertIsNotNone(e.expires_at)

    def test_expire_events_removes_expired(self):
        # Publish with 0 TTL (already expired)
        self.engine.publish("test", {}, "s", ttl_seconds=-1)
        self.engine.publish("test", {}, "s")  # no TTL

        removed = self.engine.expire_events()
        self.assertEqual(removed, 1)

        stats = self.engine.stats()
        self.assertEqual(stats["total_events"], 1)


class TestPayloadLimits(NexusTestCase):

    def test_payload_too_large(self):
        from nexus_engine.core import PayloadTooLargeError
        big_payload = {"data": "x" * 2_000_000}
        with self.assertRaises(PayloadTooLargeError):
            self.engine.publish("test", big_payload, "s")

    def test_normal_payload_ok(self):
        e = self.engine.publish("test", {"key": "value"}, "s")
        self.assertGreater(e.size_bytes, 0)


class TestHeadersAndTags(NexusTestCase):

    def test_headers_stored(self):
        e = self.engine.publish(
            "test", {}, "s",
            headers={"trace-id": "abc123", "priority": "high"},
        )
        # Replay and check
        events = list(self.engine.replay())
        self.assertEqual(events[0].headers["trace-id"], "abc123")

    def test_tags_stored(self):
        e = self.engine.publish(
            "test", {}, "s",
            tags={"env": "production", "region": "us-east"},
        )
        events = list(self.engine.replay())
        self.assertEqual(events[0].tags["env"], "production")


class TestSnapshot(NexusTestCase):

    def test_create_snapshot(self):
        self.engine.publish("test", {"i": 1}, "s")
        self.engine.publish("test", {"i": 2}, "s")

        snap_path = self.db_path + ".snapshot"
        result = self.engine.create_snapshot(snap_path)
        self.assertTrue(result["success"])
        self.assertTrue(os.path.exists(snap_path))
        self.assertGreater(result["size_bytes"], 0)

        # Verify snapshot has the data
        from nexus_engine.core import NexusEngine
        snap_engine = NexusEngine({"db_path": snap_path})
        stats = snap_engine.stats()
        self.assertEqual(stats["total_events"], 2)
        snap_engine.close()
        os.unlink(snap_path)

    def test_verify_integrity(self):
        result = self.engine.verify_integrity()
        self.assertEqual(result["integrity"], "ok")


class TestEnhancedStats(NexusTestCase):

    def test_stats_has_sizes(self):
        self.engine.publish("test", {"data": "hello"}, "s")
        stats = self.engine.stats()
        self.assertIn("db_size_bytes", stats)
        self.assertIn("wal_size_bytes", stats)
        self.assertIn("total_bytes", stats)
        self.assertGreater(stats["total_bytes"], 0)


class TestInputValidation(NexusTestCase):

    def test_reject_sql_injection_in_event_type(self):
        from nexus_engine.core import ValidationError
        with self.assertRaises(ValidationError):
            self.engine.publish("'; DROP TABLE events;--", {}, "s")

    def test_reject_special_chars_in_source(self):
        from nexus_engine.core import ValidationError
        with self.assertRaises(ValidationError):
            self.engine.publish("test", {}, "source with spaces")

    def test_reject_special_chars_in_group_id(self):
        from nexus_engine.core import ValidationError
        with self.assertRaises(ValidationError):
            self.engine.create_consumer_group("group; DROP TABLE")

    def test_valid_identifiers_accepted(self):
        e = self.engine.publish("session.start", {}, "gateway-v2", partition_key="agent:default")
        self.assertTrue(e.event_id)

    def test_reject_empty_event_type(self):
        from nexus_engine.core import ValidationError
        with self.assertRaises(ValidationError):
            self.engine.publish("", {}, "s")


class TestExceptions(NexusTestCase):

    def test_nexus_error_has_code(self):
        from nexus_engine.core import NexusError, PayloadTooLargeError
        e = PayloadTooLargeError("too big", {"size": 999})
        self.assertEqual(e.code, "NEXUS_PAYLOAD_TOO_LARGE")
        self.assertFalse(e.retryable)
        self.assertEqual(e.details["size"], 999)


if __name__ == "__main__":
    unittest.main()
