"""Nexus — Durable Event Bus and Replay Log for OpenClaw."""

__version__ = "0.1.0"

from nexus_engine.core import (
    NexusEngine,
    Event,
    Subscription,
    ConsumerLag,
    DeadLetterEntry,
)
