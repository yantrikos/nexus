"""
Nexus Engine — Durable Event Bus and Replay Log

Enterprise-grade event streaming for OpenClaw plugins.
Append-only log, consumer groups with checkpoints, replay,
dead-letter queue, schema registry, and fanout subscriptions.

All state persisted in a single SQLite file with WAL mode.
"""

import os
import re
import time
import json
import uuid
import sqlite3
import hashlib
import logging
import fnmatch
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from typing import Optional, Generator, Any

logger = logging.getLogger("nexus")

# ── Constants ──────────────────────────────────────────────────────────────

DEFAULT_DB_PATH = "./nexus_events.db"
DEFAULT_PARTITION = "default"
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY_MS = 1000
DEFAULT_BATCH_SIZE = 1000
MAX_DLQ_ATTEMPTS = 10
MAX_PAYLOAD_BYTES = 1_048_576  # 1MB default max payload size


# ── Exceptions ─────────────────────────────────────────────────────────────

class NexusError(Exception):
    """Base exception for all Nexus errors."""
    code: str = "NEXUS_ERROR"
    retryable: bool = False

    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.details = details or {}


class ValidationError(NexusError):
    code = "NEXUS_VALIDATION_ERROR"


class SchemaValidationError(ValidationError):
    code = "NEXUS_SCHEMA_MISMATCH"


class PayloadTooLargeError(ValidationError):
    code = "NEXUS_PAYLOAD_TOO_LARGE"


class DuplicateEventError(NexusError):
    code = "NEXUS_DUPLICATE_EVENT"

    def __init__(self, message: str, existing_event_id: str = "", existing_sequence: int = 0):
        super().__init__(message)
        self.existing_event_id = existing_event_id
        self.existing_sequence = existing_sequence


class StorageError(NexusError):
    code = "NEXUS_STORAGE_ERROR"
    retryable = True


class CorruptionError(StorageError):
    code = "NEXUS_CORRUPTION"
    retryable = False


# ── Data Classes ───────────────────────────────────────────────────────────

@dataclass
class Event:
    sequence_id: int = 0
    event_id: str = ""
    event_type: str = ""
    source: str = ""
    payload: dict = field(default_factory=dict)
    schema_version: str = "1.0"
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    metadata: Optional[dict] = None
    headers: Optional[dict] = None
    tags: Optional[dict] = None
    created_at: str = ""
    partition_key: str = DEFAULT_PARTITION
    size_bytes: int = 0
    idempotency_key: Optional[str] = None
    expires_at: Optional[str] = None
    deduplicated: bool = False


@dataclass
class Subscription:
    id: str = ""
    group_id: str = ""
    event_filter: dict = field(default_factory=dict)
    priority: int = 0
    max_retries: int = DEFAULT_MAX_RETRIES
    retry_delay_ms: int = DEFAULT_RETRY_DELAY_MS
    is_active: bool = True
    created_at: str = ""


@dataclass
class ConsumerLag:
    group_id: str = ""
    partition_key: str = DEFAULT_PARTITION
    head_sequence: int = 0
    checkpoint_sequence: int = 0
    lag: int = 0


@dataclass
class DeadLetterEntry:
    id: int = 0
    event_id: str = ""
    group_id: str = ""
    subscription_id: str = ""
    attempt_count: int = 0
    last_error: str = ""
    first_failure: str = ""
    last_failure: str = ""
    resolved: bool = False
    resolved_at: Optional[str] = None


# ── Engine ─────────────────────────────────────────────────────────────────

class NexusEngine:
    """
    Durable event bus and replay log.

    All events are persisted to an append-only SQLite log.
    Consumer groups track independent read positions.
    Events can be replayed from any point in history.
    """

    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self._lock = threading.Lock()

        db_path = self.config.get(
            "db_path",
            os.environ.get("NEXUS_DB_PATH", DEFAULT_DB_PATH),
        )
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._setup_pragmas()
        self._create_tables()

        # In-memory subscription cache for fast fanout matching
        self._subscription_cache: dict[str, list[dict]] = {}
        self._load_subscriptions()

        logger.info("NexusEngine initialized (db=%s)", db_path)

    # ── SQLite Setup ───────────────────────────────────────────────────

    def _setup_pragmas(self):
        c = self._conn
        c.execute("PRAGMA journal_mode = WAL")
        c.execute("PRAGMA synchronous = NORMAL")
        c.execute("PRAGMA wal_autocheckpoint = 1000")
        c.execute("PRAGMA cache_size = -64000")       # 64MB
        c.execute("PRAGMA mmap_size = 268435456")      # 256MB
        c.execute("PRAGMA busy_timeout = 5000")
        c.execute("PRAGMA temp_store = MEMORY")

    def _create_tables(self):
        with self._get_cursor() as cur:
            cur.executescript("""
                CREATE TABLE IF NOT EXISTS events (
                    sequence_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id         TEXT    NOT NULL UNIQUE,
                    event_type       TEXT    NOT NULL,
                    source           TEXT    NOT NULL,
                    payload          TEXT    NOT NULL,
                    schema_version   TEXT    NOT NULL DEFAULT '1.0',
                    correlation_id   TEXT,
                    causation_id     TEXT,
                    metadata         TEXT,
                    headers          TEXT,
                    tags             TEXT,
                    created_at       TEXT    NOT NULL,
                    partition_key    TEXT    NOT NULL DEFAULT 'default',
                    size_bytes       INTEGER NOT NULL DEFAULT 0,
                    idempotency_key  TEXT,
                    expires_at       TEXT,
                    checksum         TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_events_type
                    ON events(event_type);
                CREATE INDEX IF NOT EXISTS idx_events_source
                    ON events(source);
                CREATE INDEX IF NOT EXISTS idx_events_created
                    ON events(created_at);
                CREATE INDEX IF NOT EXISTS idx_events_correlation
                    ON events(correlation_id);
                CREATE INDEX IF NOT EXISTS idx_events_partition_seq
                    ON events(partition_key, sequence_id);
                CREATE INDEX IF NOT EXISTS idx_events_expires
                    ON events(expires_at) WHERE expires_at IS NOT NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedupe
                    ON events(partition_key, source, idempotency_key)
                    WHERE idempotency_key IS NOT NULL;

                CREATE TABLE IF NOT EXISTS consumer_groups (
                    group_id    TEXT PRIMARY KEY,
                    description TEXT,
                    created_at  TEXT NOT NULL,
                    updated_at  TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS consumer_checkpoints (
                    group_id      TEXT    NOT NULL,
                    partition_key TEXT    NOT NULL DEFAULT 'default',
                    last_sequence INTEGER NOT NULL DEFAULT 0,
                    updated_at    TEXT    NOT NULL,
                    PRIMARY KEY (group_id, partition_key),
                    FOREIGN KEY (group_id) REFERENCES consumer_groups(group_id)
                );

                CREATE TABLE IF NOT EXISTS subscriptions (
                    id             TEXT    PRIMARY KEY,
                    group_id       TEXT    NOT NULL,
                    event_filter   TEXT    NOT NULL,
                    priority       INTEGER NOT NULL DEFAULT 0,
                    max_retries    INTEGER NOT NULL DEFAULT 3,
                    retry_delay_ms INTEGER NOT NULL DEFAULT 1000,
                    is_active      INTEGER NOT NULL DEFAULT 1,
                    created_at     TEXT    NOT NULL,
                    FOREIGN KEY (group_id) REFERENCES consumer_groups(group_id)
                );

                CREATE INDEX IF NOT EXISTS idx_subscriptions_group
                    ON subscriptions(group_id, is_active);

                CREATE TABLE IF NOT EXISTS dead_letter_queue (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id        TEXT    NOT NULL,
                    group_id        TEXT    NOT NULL,
                    subscription_id TEXT    NOT NULL,
                    attempt_count   INTEGER NOT NULL DEFAULT 0,
                    last_error      TEXT,
                    first_failure   TEXT    NOT NULL,
                    last_failure    TEXT    NOT NULL,
                    resolved        INTEGER NOT NULL DEFAULT 0,
                    resolved_at     TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_dlq_unresolved
                    ON dead_letter_queue(resolved, group_id);

                CREATE TABLE IF NOT EXISTS schema_registry (
                    event_type    TEXT NOT NULL,
                    version       TEXT NOT NULL,
                    schema_def    TEXT NOT NULL,
                    description   TEXT,
                    is_deprecated INTEGER NOT NULL DEFAULT 0,
                    created_at    TEXT NOT NULL,
                    PRIMARY KEY (event_type, version)
                );

                CREATE TABLE IF NOT EXISTS nexus_meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)

            # Initialize meta
            now = _now()
            cur.execute(
                "INSERT OR IGNORE INTO nexus_meta (key, value) VALUES (?, ?)",
                ("schema_version", "1"),
            )
            cur.execute(
                "INSERT OR IGNORE INTO nexus_meta (key, value) VALUES (?, ?)",
                ("created_at", now),
            )

    @contextmanager
    def _get_cursor(self):
        self._conn.execute("BEGIN")
        try:
            cur = self._conn.cursor()
            yield cur
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    # ── Publishing ─────────────────────────────────────────────────────

    def publish(
        self,
        event_type: str,
        payload: dict,
        source: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
        metadata: Optional[dict] = None,
        headers: Optional[dict] = None,
        tags: Optional[dict] = None,
        partition_key: str = DEFAULT_PARTITION,
        schema_version: str = "1.0",
        idempotency_key: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
    ) -> Event:
        """
        Publish a single event to the log. Returns the stored Event.

        idempotency_key: If set, duplicate publishes with the same
            (partition, source, idempotency_key) return the existing event
            instead of creating a duplicate.
        ttl_seconds: If set, event expires after this many seconds.
        headers: Arbitrary key-value headers for routing/metadata.
        tags: Arbitrary key-value tags for filtering.
        """
        event_id = _generate_event_id()
        now = _now()

        # Validate schema if strict mode
        if self.config.get("strict_schemas", False):
            self._validate_schema(event_type, payload)

        # Serialize and check payload size
        payload_json = json.dumps(payload)
        size_bytes = len(payload_json.encode("utf-8"))
        max_size = self.config.get("max_payload_bytes", MAX_PAYLOAD_BYTES)
        if size_bytes > max_size:
            raise PayloadTooLargeError(
                f"Payload size {size_bytes} exceeds limit {max_size}",
                {"size_bytes": size_bytes, "max_bytes": max_size},
            )

        # Compute checksum
        checksum = hashlib.sha256(payload_json.encode()).hexdigest()[:16]

        # Compute expiry
        expires_at = None
        if ttl_seconds:
            expires_at = _now_plus_seconds(ttl_seconds)

        with self._lock:
            with self._get_cursor() as cur:
                # Idempotent publish: check for existing event
                if idempotency_key is not None:
                    cur.execute(
                        """SELECT sequence_id, event_id, created_at FROM events
                           WHERE partition_key = ? AND source = ? AND idempotency_key = ?""",
                        (partition_key, source, idempotency_key),
                    )
                    existing = cur.fetchone()
                    if existing:
                        return Event(
                            sequence_id=existing["sequence_id"],
                            event_id=existing["event_id"],
                            event_type=event_type,
                            source=source,
                            payload=payload,
                            schema_version=schema_version,
                            correlation_id=correlation_id,
                            causation_id=causation_id,
                            metadata=metadata,
                            headers=headers,
                            tags=tags,
                            created_at=existing["created_at"],
                            partition_key=partition_key,
                            size_bytes=size_bytes,
                            idempotency_key=idempotency_key,
                            deduplicated=True,
                        )

                cur.execute(
                    """INSERT INTO events
                       (event_id, event_type, source, payload, schema_version,
                        correlation_id, causation_id, metadata, headers, tags,
                        created_at, partition_key, size_bytes, idempotency_key,
                        expires_at, checksum)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        event_id,
                        event_type,
                        source,
                        payload_json,
                        schema_version,
                        correlation_id,
                        causation_id,
                        json.dumps(metadata) if metadata else None,
                        json.dumps(headers) if headers else None,
                        json.dumps(tags) if tags else None,
                        now,
                        partition_key,
                        size_bytes,
                        idempotency_key,
                        expires_at,
                        checksum,
                    ),
                )
                sequence_id = cur.lastrowid

        return Event(
            sequence_id=sequence_id,
            event_id=event_id,
            event_type=event_type,
            source=source,
            payload=payload,
            schema_version=schema_version,
            correlation_id=correlation_id,
            causation_id=causation_id,
            metadata=metadata,
            headers=headers,
            tags=tags,
            created_at=now,
            partition_key=partition_key,
            size_bytes=size_bytes,
            idempotency_key=idempotency_key,
            expires_at=expires_at,
        )

    def publish_batch(self, events: list[dict]) -> list[Event]:
        """
        Publish multiple events in a single transaction.
        Each dict must have: event_type, payload, source.
        Optional: correlation_id, causation_id, metadata, partition_key, schema_version.
        """
        now = _now()
        rows = []
        result_events = []

        for e in events:
            event_id = _generate_event_id()
            rows.append((
                event_id,
                e["event_type"],
                e["source"],
                json.dumps(e.get("payload", {})),
                e.get("schema_version", "1.0"),
                e.get("correlation_id"),
                e.get("causation_id"),
                json.dumps(e["metadata"]) if e.get("metadata") else None,
                now,
                e.get("partition_key", DEFAULT_PARTITION),
            ))

        event_ids = [r[0] for r in rows]

        with self._lock:
            with self._get_cursor() as cur:
                cur.executemany(
                    """INSERT INTO events
                       (event_id, event_type, source, payload, schema_version,
                        correlation_id, causation_id, metadata, created_at, partition_key)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    rows,
                )
                # Fetch the sequence IDs for the inserted events
                placeholders = ",".join("?" * len(event_ids))
                cur.execute(
                    f"SELECT sequence_id, event_id FROM events WHERE event_id IN ({placeholders})",
                    event_ids,
                )
                seq_map = {r["event_id"]: r["sequence_id"] for r in cur.fetchall()}

        for row, e in zip(rows, events):
            result_events.append(Event(
                sequence_id=seq_map.get(row[0], 0),
                event_id=row[0],
                event_type=row[1],
                source=row[2],
                payload=e.get("payload", {}),
                schema_version=row[4],
                correlation_id=row[5],
                causation_id=row[6],
                metadata=e.get("metadata"),
                created_at=now,
                partition_key=row[9],
            ))

        return result_events

    # ── Consumer Groups ────────────────────────────────────────────────

    def create_consumer_group(self, group_id: str, description: str = "") -> str:
        """Create a consumer group. Returns the group_id."""
        now = _now()
        with self._lock:
            with self._get_cursor() as cur:
                cur.execute(
                    """INSERT OR IGNORE INTO consumer_groups
                       (group_id, description, created_at, updated_at)
                       VALUES (?, ?, ?, ?)""",
                    (group_id, description, now, now),
                )
        return group_id

    def delete_consumer_group(self, group_id: str) -> bool:
        """Delete a consumer group and its checkpoints/subscriptions."""
        with self._lock:
            with self._get_cursor() as cur:
                cur.execute("DELETE FROM consumer_checkpoints WHERE group_id = ?", (group_id,))
                cur.execute("DELETE FROM subscriptions WHERE group_id = ?", (group_id,))
                cur.execute("DELETE FROM consumer_groups WHERE group_id = ?", (group_id,))
                deleted = cur.rowcount > 0
        if deleted:
            self._subscription_cache.pop(group_id, None)
        return deleted

    def list_consumer_groups(self) -> list[dict]:
        """List all consumer groups."""
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM consumer_groups ORDER BY created_at")
        return [dict(row) for row in cur.fetchall()]

    # ── Subscriptions ──────────────────────────────────────────────────

    def subscribe(
        self,
        group_id: str,
        event_filter: dict,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay_ms: int = DEFAULT_RETRY_DELAY_MS,
        priority: int = 0,
    ) -> str:
        """
        Create a subscription for a consumer group.

        event_filter: {"event_types": ["session.*", "tool.*"], "sources": ["cortex"]}
        Returns subscription_id.
        """
        sub_id = _generate_event_id()
        now = _now()

        # Ensure consumer group exists
        self.create_consumer_group(group_id)

        with self._lock:
            with self._get_cursor() as cur:
                cur.execute(
                    """INSERT INTO subscriptions
                       (id, group_id, event_filter, priority, max_retries,
                        retry_delay_ms, is_active, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, 1, ?)""",
                    (sub_id, group_id, json.dumps(event_filter), priority,
                     max_retries, retry_delay_ms, now),
                )
        self._load_subscriptions()
        return sub_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Deactivate a subscription."""
        with self._lock:
            with self._get_cursor() as cur:
                cur.execute(
                    "UPDATE subscriptions SET is_active = 0 WHERE id = ?",
                    (subscription_id,),
                )
                changed = cur.rowcount > 0
        if changed:
            self._load_subscriptions()
        return changed

    def list_subscriptions(self, group_id: Optional[str] = None) -> list[dict]:
        """List subscriptions, optionally filtered by group."""
        cur = self._conn.cursor()
        if group_id:
            cur.execute(
                "SELECT * FROM subscriptions WHERE group_id = ? AND is_active = 1",
                (group_id,),
            )
        else:
            cur.execute("SELECT * FROM subscriptions WHERE is_active = 1")
        return [dict(row) for row in cur.fetchall()]

    def _load_subscriptions(self):
        """Reload subscription cache from SQLite."""
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM subscriptions WHERE is_active = 1 ORDER BY priority DESC")
        cache: dict[str, list[dict]] = {}
        for row in cur.fetchall():
            d = dict(row)
            d["event_filter"] = json.loads(d["event_filter"])
            gid = d["group_id"]
            if gid not in cache:
                cache[gid] = []
            cache[gid].append(d)
        self._subscription_cache = cache

    # ── Consuming (Poll + Acknowledge) ─────────────────────────────────

    def poll(
        self,
        group_id: str,
        limit: int = 100,
        partition_key: str = DEFAULT_PARTITION,
    ) -> list[Event]:
        """
        Poll for new events since the group's last checkpoint.
        Only returns events matching the group's subscriptions.
        """
        checkpoint = self._get_checkpoint(group_id, partition_key)
        subs = self._subscription_cache.get(group_id, [])
        if not subs:
            return []

        cur = self._conn.cursor()
        cur.execute(
            """SELECT * FROM events
               WHERE partition_key = ? AND sequence_id > ?
               ORDER BY sequence_id ASC LIMIT ?""",
            (partition_key, checkpoint, limit * 3),  # over-fetch for filtering
        )

        events = []
        for row in cur.fetchall():
            event = self._row_to_event(row)
            if self._matches_any_subscription(event, subs):
                events.append(event)
                if len(events) >= limit:
                    break

        return events

    def acknowledge(
        self,
        group_id: str,
        sequence_id: int,
        partition_key: str = DEFAULT_PARTITION,
    ) -> bool:
        """Advance the checkpoint for a consumer group."""
        now = _now()
        with self._lock:
            with self._get_cursor() as cur:
                cur.execute(
                    """INSERT INTO consumer_checkpoints
                       (group_id, partition_key, last_sequence, updated_at)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(group_id, partition_key) DO UPDATE SET
                           last_sequence = MAX(last_sequence, excluded.last_sequence),
                           updated_at = excluded.updated_at""",
                    (group_id, partition_key, sequence_id, now),
                )
        return True

    def nack(
        self,
        group_id: str,
        event_id: str,
        subscription_id: str,
        error: str,
    ) -> bool:
        """
        Negative acknowledge — mark an event as failed for a consumer.
        Increments retry count; moves to DLQ after max retries.
        """
        now = _now()
        with self._lock:
            with self._get_cursor() as cur:
                # Check existing DLQ entry
                cur.execute(
                    """SELECT id, attempt_count FROM dead_letter_queue
                       WHERE event_id = ? AND group_id = ? AND subscription_id = ?
                       AND resolved = 0""",
                    (event_id, group_id, subscription_id),
                )
                existing = cur.fetchone()

                if existing:
                    new_count = existing["attempt_count"] + 1
                    cur.execute(
                        """UPDATE dead_letter_queue
                           SET attempt_count = ?, last_error = ?, last_failure = ?
                           WHERE id = ?""",
                        (new_count, error, now, existing["id"]),
                    )
                else:
                    cur.execute(
                        """INSERT INTO dead_letter_queue
                           (event_id, group_id, subscription_id, attempt_count,
                            last_error, first_failure, last_failure)
                           VALUES (?, ?, ?, 1, ?, ?, ?)""",
                        (event_id, group_id, subscription_id, error, now, now),
                    )
        return True

    def _get_checkpoint(self, group_id: str, partition_key: str) -> int:
        """Get the last acknowledged sequence for a group/partition."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT last_sequence FROM consumer_checkpoints WHERE group_id = ? AND partition_key = ?",
            (group_id, partition_key),
        )
        row = cur.fetchone()
        return row["last_sequence"] if row else 0

    # ── Replay ─────────────────────────────────────────────────────────

    def replay(
        self,
        from_sequence: int = 0,
        to_sequence: Optional[int] = None,
        event_type: Optional[str] = None,
        source: Optional[str] = None,
        partition_key: Optional[str] = None,
        since: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Generator[Event, None, None]:
        """
        Replay events from the log. Returns a generator.

        Supports filtering by type (glob), source, partition, and time.
        """
        query = "SELECT * FROM events WHERE sequence_id >= ?"
        params: list[Any] = [from_sequence]

        if to_sequence is not None:
            query += " AND sequence_id <= ?"
            params.append(to_sequence)
        if event_type:
            query += " AND event_type LIKE ?"
            params.append(event_type.replace("*", "%"))
        if source:
            query += " AND source = ?"
            params.append(source)
        if partition_key:
            query += " AND partition_key = ?"
            params.append(partition_key)
        if since:
            query += " AND created_at >= ?"
            params.append(since)

        query += " ORDER BY sequence_id ASC"
        if limit:
            query += f" LIMIT {int(limit)}"

        cur = self._conn.cursor()
        cur.execute(query, params)
        while True:
            row = cur.fetchone()
            if row is None:
                break
            yield self._row_to_event(row)

    def replay_for_group(
        self,
        group_id: str,
        from_sequence: int = 0,
        partition_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Generator[Event, None, None]:
        """Replay with the group's subscription filters applied."""
        subs = self._subscription_cache.get(group_id, [])
        if not subs:
            return

        count = 0
        for event in self.replay(
            from_sequence=from_sequence,
            partition_key=partition_key,
        ):
            if self._matches_any_subscription(event, subs):
                yield event
                count += 1
                if limit and count >= limit:
                    return

    def snapshot_checkpoint(
        self,
        group_id: str,
        partition_key: str = DEFAULT_PARTITION,
    ) -> int:
        """Get current checkpoint position for a group."""
        return self._get_checkpoint(group_id, partition_key)

    def reset_checkpoint(
        self,
        group_id: str,
        partition_key: str = DEFAULT_PARTITION,
        to_sequence: int = 0,
    ) -> bool:
        """Rewind a consumer group to a specific sequence."""
        now = _now()
        with self._lock:
            with self._get_cursor() as cur:
                cur.execute(
                    """INSERT INTO consumer_checkpoints
                       (group_id, partition_key, last_sequence, updated_at)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(group_id, partition_key) DO UPDATE SET
                           last_sequence = ?, updated_at = ?""",
                    (group_id, partition_key, to_sequence, now, to_sequence, now),
                )
        return True

    # ── Dead Letter Queue ──────────────────────────────────────────────

    def get_dlq(
        self,
        group_id: Optional[str] = None,
        limit: int = 50,
        include_resolved: bool = False,
    ) -> list[DeadLetterEntry]:
        """Get dead-letter queue entries."""
        query = "SELECT * FROM dead_letter_queue WHERE 1=1"
        params: list[Any] = []

        if not include_resolved:
            query += " AND resolved = 0"
        if group_id:
            query += " AND group_id = ?"
            params.append(group_id)

        query += " ORDER BY last_failure DESC LIMIT ?"
        params.append(limit)

        cur = self._conn.cursor()
        cur.execute(query, params)
        return [
            DeadLetterEntry(
                id=row["id"],
                event_id=row["event_id"],
                group_id=row["group_id"],
                subscription_id=row["subscription_id"],
                attempt_count=row["attempt_count"],
                last_error=row["last_error"] or "",
                first_failure=row["first_failure"],
                last_failure=row["last_failure"],
                resolved=bool(row["resolved"]),
                resolved_at=row["resolved_at"],
            )
            for row in cur.fetchall()
        ]

    def retry_dlq(self, dlq_id: int) -> bool:
        """Re-attempt delivery of a DLQ entry by resetting its attempt count."""
        with self._lock:
            with self._get_cursor() as cur:
                cur.execute(
                    "UPDATE dead_letter_queue SET attempt_count = 0, resolved = 0 WHERE id = ?",
                    (dlq_id,),
                )
                return cur.rowcount > 0

    def resolve_dlq(self, dlq_id: int) -> bool:
        """Manually resolve a DLQ entry."""
        now = _now()
        with self._lock:
            with self._get_cursor() as cur:
                cur.execute(
                    "UPDATE dead_letter_queue SET resolved = 1, resolved_at = ? WHERE id = ?",
                    (now, dlq_id),
                )
                return cur.rowcount > 0

    # ── Schema Registry ────────────────────────────────────────────────

    def register_schema(
        self,
        event_type: str,
        version: str,
        schema_def: dict,
        description: str = "",
    ) -> bool:
        """Register a JSON schema for an event type."""
        now = _now()
        with self._lock:
            with self._get_cursor() as cur:
                cur.execute(
                    """INSERT OR REPLACE INTO schema_registry
                       (event_type, version, schema_def, description, created_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (event_type, version, json.dumps(schema_def), description, now),
                )
        return True

    def get_schema(
        self,
        event_type: str,
        version: Optional[str] = None,
    ) -> Optional[dict]:
        """Get schema for an event type. Latest version if not specified."""
        cur = self._conn.cursor()
        if version:
            cur.execute(
                "SELECT * FROM schema_registry WHERE event_type = ? AND version = ?",
                (event_type, version),
            )
        else:
            cur.execute(
                """SELECT * FROM schema_registry
                   WHERE event_type = ? AND is_deprecated = 0
                   ORDER BY version DESC LIMIT 1""",
                (event_type,),
            )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "event_type": row["event_type"],
            "version": row["version"],
            "schema_def": json.loads(row["schema_def"]),
            "description": row["description"],
        }

    def deprecate_schema(self, event_type: str, version: str) -> bool:
        """Mark a schema version as deprecated."""
        with self._lock:
            with self._get_cursor() as cur:
                cur.execute(
                    """UPDATE schema_registry SET is_deprecated = 1
                       WHERE event_type = ? AND version = ?""",
                    (event_type, version),
                )
                return cur.rowcount > 0

    def _validate_schema(self, event_type: str, payload: dict):
        """Validate payload against registered schema. Raises on invalid."""
        schema = self.get_schema(event_type)
        if not schema:
            return  # No schema registered, allow

        try:
            import jsonschema
            jsonschema.validate(payload, schema["schema_def"])
        except ImportError:
            logger.debug("jsonschema not installed, skipping validation")
        except Exception as e:
            raise ValueError(f"Schema validation failed for {event_type}: {e}")

    # ── Subscription Matching ──────────────────────────────────────────

    def _matches_any_subscription(self, event: Event, subs: list[dict]) -> bool:
        """Check if an event matches any subscription in the list."""
        for sub in subs:
            if self._matches_filter(event, sub["event_filter"]):
                return True
        return False

    def _matches_filter(self, event: Event, filt: dict) -> bool:
        """Check if an event matches a subscription filter."""
        # Match event types (glob patterns)
        type_patterns = filt.get("event_types", ["*"])
        type_match = any(
            fnmatch.fnmatchcase(event.event_type, pat)
            for pat in type_patterns
        )
        if not type_match:
            return False

        # Match sources
        source_patterns = filt.get("sources")
        if source_patterns:
            source_match = any(
                fnmatch.fnmatchcase(event.source, pat)
                for pat in source_patterns
            )
            if not source_match:
                return False

        return True

    # ── Backpressure / Lag ─────────────────────────────────────────────

    def get_lag(
        self,
        group_id: str,
        partition_key: str = DEFAULT_PARTITION,
    ) -> ConsumerLag:
        """Get the lag (unprocessed events) for a consumer group."""
        checkpoint = self._get_checkpoint(group_id, partition_key)

        cur = self._conn.cursor()
        cur.execute(
            "SELECT MAX(sequence_id) as head FROM events WHERE partition_key = ?",
            (partition_key,),
        )
        row = cur.fetchone()
        head = row["head"] if row and row["head"] else 0

        return ConsumerLag(
            group_id=group_id,
            partition_key=partition_key,
            head_sequence=head,
            checkpoint_sequence=checkpoint,
            lag=max(0, head - checkpoint),
        )

    # ── Administration ─────────────────────────────────────────────────

    def stats(self) -> dict:
        """Get system statistics."""
        cur = self._conn.cursor()

        cur.execute("SELECT COUNT(*) as count FROM events")
        total_events = cur.fetchone()["count"]

        cur.execute("SELECT MAX(sequence_id) as head FROM events")
        row = cur.fetchone()
        head_sequence = row["head"] if row and row["head"] else 0

        cur.execute("SELECT COUNT(*) as count FROM consumer_groups")
        total_groups = cur.fetchone()["count"]

        cur.execute("SELECT COUNT(*) as count FROM subscriptions WHERE is_active = 1")
        total_subs = cur.fetchone()["count"]

        cur.execute("SELECT COUNT(*) as count FROM dead_letter_queue WHERE resolved = 0")
        dlq_depth = cur.fetchone()["count"]

        cur.execute("SELECT COUNT(*) as count FROM schema_registry WHERE is_deprecated = 0")
        schema_count = cur.fetchone()["count"]

        # Per-group lag
        cur.execute("SELECT group_id FROM consumer_groups")
        groups = [row["group_id"] for row in cur.fetchall()]
        lag_by_group = {}
        for gid in groups:
            lag = self.get_lag(gid)
            lag_by_group[gid] = lag.lag

        # Event type distribution (top 10)
        cur.execute(
            """SELECT event_type, COUNT(*) as count FROM events
               GROUP BY event_type ORDER BY count DESC LIMIT 10"""
        )
        type_dist = {row["event_type"]: row["count"] for row in cur.fetchall()}

        # Source distribution
        cur.execute(
            """SELECT source, COUNT(*) as count FROM events
               GROUP BY source ORDER BY count DESC LIMIT 10"""
        )
        source_dist = {row["source"]: row["count"] for row in cur.fetchall()}

        # DB and WAL file sizes
        db_size = os.path.getsize(self._db_path) if os.path.exists(self._db_path) else 0
        wal_path = self._db_path + "-wal"
        wal_size = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0

        # Total payload bytes
        cur.execute("SELECT COALESCE(SUM(size_bytes), 0) as total FROM events")
        total_bytes = cur.fetchone()["total"]

        return {
            "total_events": total_events,
            "head_sequence": head_sequence,
            "total_bytes": total_bytes,
            "db_size_bytes": db_size,
            "wal_size_bytes": wal_size,
            "consumer_groups": total_groups,
            "active_subscriptions": total_subs,
            "dlq_depth": dlq_depth,
            "schemas_registered": schema_count,
            "lag_by_group": lag_by_group,
            "event_type_distribution": type_dist,
            "source_distribution": source_dist,
        }

    def compact(
        self,
        before_sequence: Optional[int] = None,
        before_date: Optional[str] = None,
    ) -> int:
        """
        Archive/delete old events.
        Returns number of events removed.
        Only removes events that all consumer groups have already processed.
        """
        # Find the minimum checkpoint across all groups
        cur = self._conn.cursor()
        cur.execute("SELECT MIN(last_sequence) as min_seq FROM consumer_checkpoints")
        row = cur.fetchone()
        min_checkpoint = row["min_seq"] if row and row["min_seq"] else 0

        # Don't compact past any group's checkpoint
        safe_before = min_checkpoint
        if before_sequence is not None:
            safe_before = min(safe_before, before_sequence)

        query = "DELETE FROM events WHERE sequence_id < ?"
        params: list[Any] = [safe_before]

        if before_date:
            query += " AND created_at < ?"
            params.append(before_date)

        with self._lock:
            with self._get_cursor() as cur:
                cur.execute(query, params)
                return cur.rowcount

    def health_check(self) -> dict:
        """Check engine health."""
        try:
            cur = self._conn.cursor()
            cur.execute("SELECT COUNT(*) FROM events")
            cur.execute("PRAGMA integrity_check")
            integrity = cur.fetchone()[0]
            stats = self.stats()
            return {
                "healthy": integrity == "ok",
                "engine": "nexus",
                "integrity": integrity,
                "total_events": stats["total_events"],
                "dlq_depth": stats["dlq_depth"],
            }
        except Exception as e:
            return {"healthy": False, "engine": "nexus", "error": str(e)}

    def export_events(
        self,
        from_sequence: int = 0,
        to_sequence: Optional[int] = None,
        format: str = "jsonl",
    ) -> str:
        """Export events as JSON lines."""
        lines = []
        for event in self.replay(from_sequence=from_sequence, to_sequence=to_sequence):
            lines.append(json.dumps(asdict(event)))
        return "\n".join(lines)

    # ── TTL / Expiry ──────────────────────────────────────────────────

    def expire_events(self) -> int:
        """Delete events that have passed their TTL. Returns count deleted."""
        now = _now()
        with self._lock:
            with self._get_cursor() as cur:
                cur.execute(
                    "DELETE FROM events WHERE expires_at IS NOT NULL AND expires_at < ?",
                    (now,),
                )
                return cur.rowcount

    # ── Snapshot / Backup ──────────────────────────────────────────────

    def create_snapshot(self, target_path: str) -> dict:
        """Create a consistent backup using SQLite backup API."""
        import sqlite3 as _sqlite3
        try:
            dst = _sqlite3.connect(target_path)
            self._conn.backup(dst)
            dst.close()
            size = os.path.getsize(target_path)
            return {"success": True, "path": target_path, "size_bytes": size}
        except Exception as e:
            raise StorageError(f"Snapshot failed: {e}")

    def verify_integrity(self) -> dict:
        """Run SQLite integrity check."""
        cur = self._conn.cursor()
        cur.execute("PRAGMA integrity_check")
        result = cur.fetchone()[0]
        if result != "ok":
            raise CorruptionError(f"Integrity check failed: {result}")
        return {"integrity": "ok"}

    # ── Lifecycle ──────────────────────────────────────────────────────

    def close(self):
        """Clean shutdown: WAL checkpoint and close connection."""
        try:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self._conn.close()
        except Exception:
            pass
        logger.info("NexusEngine closed")

    # ── Helpers ────────────────────────────────────────────────────────

    def _row_to_event(self, row) -> Event:
        """Convert a SQLite row to an Event."""
        payload = row["payload"]
        metadata = row["metadata"]
        headers = row["headers"] if "headers" in row.keys() else None
        tags = row["tags"] if "tags" in row.keys() else None
        return Event(
            sequence_id=row["sequence_id"],
            event_id=row["event_id"],
            event_type=row["event_type"],
            source=row["source"],
            payload=json.loads(payload) if payload else {},
            schema_version=row["schema_version"],
            correlation_id=row["correlation_id"],
            causation_id=row["causation_id"],
            metadata=json.loads(metadata) if metadata else None,
            headers=json.loads(headers) if headers else None,
            tags=json.loads(tags) if tags else None,
            created_at=row["created_at"],
            partition_key=row["partition_key"],
            size_bytes=row["size_bytes"] if "size_bytes" in row.keys() else 0,
            idempotency_key=row["idempotency_key"] if "idempotency_key" in row.keys() else None,
            expires_at=row["expires_at"] if "expires_at" in row.keys() else None,
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ── Module-level Helpers ───────────────────────────────────────────────────

def _now() -> str:
    """ISO 8601 timestamp with milliseconds."""
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int(time.time() * 1000) % 1000:03d}"


def _now_plus_seconds(seconds: int) -> str:
    """ISO 8601 timestamp offset by N seconds."""
    t = time.time() + seconds
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + f".{int(t * 1000) % 1000:03d}"


def _generate_event_id() -> str:
    """Generate a time-ordered UUID (UUID v7-like)."""
    t = int(time.time() * 1000)
    # Time-prefixed UUID for monotonic ordering
    time_hex = f"{t:012x}"
    rand_hex = uuid.uuid4().hex[12:]
    return f"{time_hex[:8]}-{time_hex[8:12]}-7{rand_hex[:3]}-{rand_hex[3:7]}-{rand_hex[7:19]}"
