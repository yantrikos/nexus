# Nexus

Durable event bus and replay log for [OpenClaw](https://github.com/openclaw/openclaw) agents. The foundation layer for event-driven plugin architectures.

## What it does

Nexus gives OpenClaw a persistent event streaming system. Every event is durably stored in an append-only SQLite log and can be replayed from any point in history.

- **Publish/Subscribe** — Plugins publish events, other plugins consume them via filtered subscriptions
- **Consumer Groups** — Independent read positions with checkpoints, like Kafka consumer groups
- **Replay** — Rebuild any plugin's state by replaying history from sequence 0
- **Dead-Letter Queue** — Failed deliveries tracked with retry and manual resolution
- **Schema Registry** — Version and validate event schemas
- **Backpressure** — Lag monitoring per consumer group
- **High Throughput** — 1000+ events/sec via WAL mode, batch inserts, persistent subprocess

## Install

```bash
pip install nexus-eventbus
```

## Quick Start

```python
from nexus_engine import NexusEngine

engine = NexusEngine()

# Publish an event
event = engine.publish(
    event_type="session.message",
    payload={"role": "user", "content": "Hello!"},
    source="my-plugin",
    metadata={"agent_id": "default", "session_id": "sess_123"},
)

# Create a consumer group and subscribe
engine.create_consumer_group("memory-indexer")
engine.subscribe("memory-indexer", event_filter={"event_types": ["session.*"]})

# Poll for events
events = engine.poll("memory-indexer", limit=100)
for e in events:
    process(e)
    engine.acknowledge("memory-indexer", e.sequence_id)

# Replay all history
for e in engine.replay(from_sequence=0, event_type="session.*"):
    rebuild_state(e)
```

## OpenClaw Plugin

```bash
openclaw plugins install nexus
```

Other plugins integrate with Nexus via the TypeScript API:

```typescript
import { publish, subscribe, poll, acknowledge } from "nexus";

// Publish events
await publish("tool.invocation", { tool: "grep", args: {...} }, "my-plugin");

// Subscribe and consume
await subscribe("my-consumer", { event_types: ["tool.*"] });
const result = await poll("my-consumer", { limit: 50 });
for (const event of result.events) {
    await processEvent(event);
    await acknowledge("my-consumer", event.sequence_id);
}
```

## Architecture

```
┌─────────────────────────────────────────────┐
│              Publishers                      │
│  (any plugin, channel, or gateway event)     │
└─────────────────┬───────────────────────────┘
                  │ publish()
                  ▼
┌─────────────────────────────────────────────┐
│           Nexus Event Log                    │
│  Append-only SQLite with WAL mode            │
│  sequence_id → event_id, type, payload, ...  │
├─────────────────────────────────────────────┤
│  Schema Registry    │  Dead Letter Queue     │
└─────────┬───────────┴────────────┬──────────┘
          │ poll()                  │ nack()
          ▼                        ▼
┌──────────────────┐  ┌──────────────────┐
│ Consumer Group A │  │ Consumer Group B │
│ checkpoint: 142  │  │ checkpoint: 89   │
│ filter: session.*│  │ filter: tool.*   │
└──────────────────┘  └──────────────────┘
```

## Standard Event Types

| Event Type | Payload | When |
|-----------|---------|------|
| `gateway.startup` | `{version, plugins}` | Gateway boots |
| `gateway.shutdown` | `{reason, uptime}` | Gateway stops |
| `session.start` | `{session_id, agent_id}` | New session |
| `session.end` | `{session_id, summary}` | Session ends |
| `session.message` | `{role, content}` | Each message |
| `tool.invocation` | `{tool, arguments}` | Tool called |
| `tool.result` | `{tool, result, success}` | Tool returns |
| `plugin.loaded` | `{plugin_id, version}` | Plugin loaded |
| `plugin.error` | `{plugin_id, error}` | Plugin failure |

## Performance

- **Write:** 1000+ events/sec (WAL mode, batch inserts, persistent subprocess)
- **Read:** Concurrent reads via WAL — poll never blocks publish
- **Storage:** Single SQLite file, ~100 bytes per event
- **Replay:** Generator-based — replays millions of events with O(1) memory

## License

MIT
