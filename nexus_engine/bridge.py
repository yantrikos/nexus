#!/usr/bin/env python3
"""
Nexus Bridge — JSON stdin/stdout interface for TypeScript plugin.

Supports two modes:
- One-shot: echo '{"command":"stats"}' | python3 bridge.py
- Persistent: python3 bridge.py --persistent (newline-delimited JSON)
"""

import sys
import json
import signal

from nexus_engine.core import NexusEngine

_engine: NexusEngine = None


def get_engine(config: dict = None) -> NexusEngine:
    global _engine
    if _engine is None:
        _engine = NexusEngine(config)
    return _engine


def handle_command(command: str, args: dict, config: dict) -> dict:
    engine = get_engine(config)

    try:
        if command == "publish":
            event = engine.publish(
                event_type=args["event_type"],
                payload=args.get("payload", {}),
                source=args.get("source", "unknown"),
                correlation_id=args.get("correlation_id"),
                causation_id=args.get("causation_id"),
                metadata=args.get("metadata"),
                partition_key=args.get("partition_key", "default"),
            )
            return {"success": True, "event_id": event.event_id, "sequence_id": event.sequence_id}

        elif command == "publish_batch":
            events = engine.publish_batch(args.get("events", []))
            return {
                "success": True,
                "count": len(events),
                "events": [{"event_id": e.event_id, "sequence_id": e.sequence_id} for e in events],
            }

        elif command == "create_group":
            gid = engine.create_consumer_group(
                args["group_id"],
                args.get("description", ""),
            )
            return {"success": True, "group_id": gid}

        elif command == "subscribe":
            sub_id = engine.subscribe(
                group_id=args["group_id"],
                event_filter=args.get("event_filter", {"event_types": ["*"]}),
                max_retries=args.get("max_retries", 3),
                retry_delay_ms=args.get("retry_delay_ms", 1000),
                priority=args.get("priority", 0),
            )
            return {"success": True, "subscription_id": sub_id}

        elif command == "unsubscribe":
            ok = engine.unsubscribe(args["subscription_id"])
            return {"success": ok}

        elif command == "poll":
            events = engine.poll(
                group_id=args["group_id"],
                limit=args.get("limit", 100),
                partition_key=args.get("partition_key", "default"),
            )
            return {
                "success": True,
                "events": [
                    {
                        "sequence_id": e.sequence_id,
                        "event_id": e.event_id,
                        "event_type": e.event_type,
                        "source": e.source,
                        "payload": e.payload,
                        "metadata": e.metadata,
                        "created_at": e.created_at,
                        "correlation_id": e.correlation_id,
                    }
                    for e in events
                ],
            }

        elif command == "acknowledge":
            ok = engine.acknowledge(
                group_id=args["group_id"],
                sequence_id=args["sequence_id"],
                partition_key=args.get("partition_key", "default"),
            )
            return {"success": ok}

        elif command == "nack":
            ok = engine.nack(
                group_id=args["group_id"],
                event_id=args["event_id"],
                subscription_id=args["subscription_id"],
                error=args.get("error", ""),
            )
            return {"success": ok}

        elif command == "replay":
            events = list(engine.replay(
                from_sequence=args.get("from_sequence", 0),
                to_sequence=args.get("to_sequence"),
                event_type=args.get("event_type"),
                source=args.get("source"),
                partition_key=args.get("partition_key"),
                since=args.get("since"),
                limit=args.get("limit", 1000),
            ))
            return {
                "success": True,
                "count": len(events),
                "events": [
                    {
                        "sequence_id": e.sequence_id,
                        "event_id": e.event_id,
                        "event_type": e.event_type,
                        "source": e.source,
                        "payload": e.payload,
                        "metadata": e.metadata,
                        "created_at": e.created_at,
                    }
                    for e in events
                ],
            }

        elif command == "reset_checkpoint":
            ok = engine.reset_checkpoint(
                group_id=args["group_id"],
                partition_key=args.get("partition_key", "default"),
                to_sequence=args.get("to_sequence", 0),
            )
            return {"success": ok}

        elif command == "get_lag":
            lag = engine.get_lag(
                group_id=args["group_id"],
                partition_key=args.get("partition_key", "default"),
            )
            return {
                "success": True,
                "group_id": lag.group_id,
                "head": lag.head_sequence,
                "checkpoint": lag.checkpoint_sequence,
                "lag": lag.lag,
            }

        elif command == "register_schema":
            ok = engine.register_schema(
                event_type=args["event_type"],
                version=args["version"],
                schema_def=args["schema_def"],
                description=args.get("description", ""),
            )
            return {"success": ok}

        elif command == "get_dlq":
            entries = engine.get_dlq(
                group_id=args.get("group_id"),
                limit=args.get("limit", 50),
                include_resolved=args.get("include_resolved", False),
            )
            return {
                "success": True,
                "entries": [
                    {
                        "id": e.id,
                        "event_id": e.event_id,
                        "group_id": e.group_id,
                        "attempt_count": e.attempt_count,
                        "last_error": e.last_error,
                        "first_failure": e.first_failure,
                        "last_failure": e.last_failure,
                        "resolved": e.resolved,
                    }
                    for e in entries
                ],
            }

        elif command == "retry_dlq":
            ok = engine.retry_dlq(args["dlq_id"])
            return {"success": ok}

        elif command == "resolve_dlq":
            ok = engine.resolve_dlq(args["dlq_id"])
            return {"success": ok}

        elif command == "compact":
            removed = engine.compact(
                before_sequence=args.get("before_sequence"),
                before_date=args.get("before_date"),
            )
            return {"success": True, "removed": removed}

        elif command == "stats":
            return {"success": True, **engine.stats()}

        elif command == "health_check":
            return engine.health_check()

        elif command == "list_groups":
            groups = engine.list_consumer_groups()
            return {"success": True, "groups": groups}

        elif command == "list_subscriptions":
            subs = engine.list_subscriptions(args.get("group_id"))
            return {"success": True, "subscriptions": subs}

        else:
            return {"success": False, "error": f"Unknown command: {command}"}

    except Exception as e:
        return {"success": False, "error": str(e)}


def run_oneshot():
    """One-shot mode: read JSON from stdin, write response to stdout."""
    try:
        data = json.loads(sys.stdin.read())
    except json.JSONDecodeError:
        print(json.dumps({"success": False, "error": "Invalid JSON"}))
        return

    result = handle_command(
        data.get("command", ""),
        data.get("args", {}),
        data.get("config", {}),
    )
    print(json.dumps(result))


def run_persistent():
    """Persistent mode: newline-delimited JSON on stdin/stdout."""
    def shutdown(sig, frame):
        if _engine:
            _engine.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            result = handle_command(
                data.get("command", ""),
                data.get("args", {}),
                data.get("config", {}),
            )
            result["request_id"] = data.get("request_id")
            print(json.dumps(result), flush=True)
        except json.JSONDecodeError:
            print(json.dumps({"success": False, "error": "Invalid JSON"}), flush=True)
        except Exception as e:
            print(json.dumps({"success": False, "error": str(e)}), flush=True)

    if _engine:
        _engine.close()


def main():
    if "--persistent" in sys.argv:
        run_persistent()
    else:
        run_oneshot()


if __name__ == "__main__":
    main()
