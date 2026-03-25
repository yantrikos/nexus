#!/usr/bin/env python3
"""Nexus CLI."""

import os
import sys
import json


def main():
    args = sys.argv[1:]
    if not args:
        print("Usage: nexus <command>")
        print("Commands: init, stats, health, replay, compact")
        return

    command = args[0]

    if command == "init":
        from nexus_engine.core import NexusEngine
        engine = NexusEngine()
        health = engine.health_check()
        print(f"Nexus initialized: {health}")
        engine.close()
    elif command == "stats":
        from nexus_engine.core import NexusEngine
        engine = NexusEngine()
        print(json.dumps(engine.stats(), indent=2))
        engine.close()
    elif command == "health":
        from nexus_engine.core import NexusEngine
        engine = NexusEngine()
        print(json.dumps(engine.health_check(), indent=2))
        engine.close()
    elif command == "compact":
        from nexus_engine.core import NexusEngine
        engine = NexusEngine()
        removed = engine.compact()
        print(f"Compacted: {removed} events removed")
        engine.close()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
