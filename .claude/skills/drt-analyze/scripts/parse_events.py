#!/usr/bin/env python3
# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

"""Parse DRT operation events from Datadog MCP responses.

Usage:
  python3 scripts/parse_events.py run.json [cleanup.json] [failed.json] [depcheck.json]

Reads JSON files containing raw Datadog MCP event search responses and produces
a structured operations timeline. Each positional arg corresponds to:
  1. run events (phase:run)
  2. cleanup events (phase:cleanup)
  3. failed events (phase:run result:failed/panicked)
  4. dependency-check events (phase:dependency-check)

Output is a structured report to stdout with:
  - Summary stats (total ops, success/fail/panic counts, success rate)
  - Chronological timeline
  - Failure details
  - Disruptive operation windows (DISRUPTIVE_WINDOW lines for correlation)
"""

import json
import sys
import re
from datetime import datetime, timedelta, timezone

DISRUPTIVE_OPS = {
    "network-partition", "disk-stall", "license-throttle", "resize",
}


def is_disruptive(op_name: str) -> bool:
    """Check if an operation is disruptive (causes expected cluster impact)."""
    for prefix in DISRUPTIVE_OPS:
        if op_name.startswith(prefix):
            return True
    return False


def parse_timestamp(ts_str: str) -> datetime:
    """Parse various timestamp formats from Datadog events."""
    for fmt in [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    ]:
        try:
            dt = datetime.strptime(ts_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    # Try epoch seconds/ms
    try:
        val = float(ts_str)
        if val > 1e12:
            val = val / 1000
        return datetime.fromtimestamp(val, tz=timezone.utc)
    except (ValueError, OSError):
        pass
    raise ValueError(f"Cannot parse timestamp: {ts_str}")


def extract_events(data):
    """Extract event list from various MCP response formats."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Try common MCP response shapes
        for key in ["events", "data", "results", "content"]:
            if key in data:
                val = data[key]
                if isinstance(val, list):
                    return val
                if isinstance(val, dict) and "events" in val:
                    return val["events"]
        # If it has text content, try to parse JSON from it
        if "text" in data:
            try:
                inner = json.loads(data["text"])
                return extract_events(inner)
            except (json.JSONDecodeError, TypeError):
                pass
    return []


def extract_field(event, field, default=""):
    """Extract a field from an event, checking tags and attributes."""
    if field in event:
        return event[field]
    for container in ["tags", "attributes"]:
        if container in event and isinstance(event[container], dict):
            if field in event[container]:
                return event[container][field]
    # Check tags as list of "key:value" strings
    if "tags" in event and isinstance(event["tags"], list):
        for tag in event["tags"]:
            if isinstance(tag, str) and tag.startswith(f"{field}:"):
                return tag.split(":", 1)[1]
    return default


def build_timeline(run_events, cleanup_events, failed_events, depcheck_events):
    """Build structured timeline from parsed events."""
    ops = []
    for ev in run_events:
        op = {
            "name": extract_field(ev, "operation", extract_field(ev, "title", "unknown")),
            "timestamp": extract_field(ev, "date_happened", extract_field(ev, "timestamp", "")),
            "worker": extract_field(ev, "worker", ""),
            "result": extract_field(ev, "result", "success"),
            "host": extract_field(ev, "host", ""),
        }
        ops.append(op)

    # Sort by timestamp
    for op in ops:
        try:
            op["_ts"] = parse_timestamp(str(op["timestamp"]))
        except ValueError:
            op["_ts"] = datetime.min.replace(tzinfo=timezone.utc)
    ops.sort(key=lambda x: x["_ts"])

    # Build cleanup lookup: operation_name -> cleanup timestamp
    cleanup_map = {}
    for ev in cleanup_events:
        name = extract_field(ev, "operation", extract_field(ev, "title", ""))
        ts = extract_field(ev, "date_happened", extract_field(ev, "timestamp", ""))
        result = extract_field(ev, "result", "success")
        if name:
            try:
                cleanup_map[name] = {
                    "timestamp": parse_timestamp(str(ts)),
                    "result": result,
                }
            except ValueError:
                pass

    # Count results
    total = len(ops)
    failed = sum(1 for o in ops if o["result"] in ("failed", "failure"))
    panicked = sum(1 for o in ops if o["result"] == "panicked")
    succeeded = total - failed - panicked
    rate = (succeeded / total * 100) if total > 0 else 0

    # Print summary
    print(f"## Operations Summary")
    print(f"Total: {total} | Success: {succeeded} | Failed: {failed} | "
          f"Panicked: {panicked} | Success rate: {rate:.1f}%")
    print()

    # Print timeline
    print("## Timeline")
    for op in ops:
        ts_str = op["_ts"].strftime("%H:%M:%S") if op["_ts"] != datetime.min.replace(tzinfo=timezone.utc) else "??:??:??"
        result_marker = "OK" if op["result"] in ("success", "succeeded") else op["result"].upper()
        print(f"  {ts_str} | {op['name']} | w={op['worker']} | {result_marker}")
    print()

    # Print failures
    fail_ops = [o for o in ops if o["result"] in ("failed", "failure", "panicked")]
    if fail_ops:
        print("## Failures")
        for op in fail_ops:
            ts_str = op["_ts"].strftime("%H:%M:%S")
            print(f"  {ts_str} | {op['name']} | {op['result']}")
        print()

    # Print failed events detail (from query 2)
    if failed_events:
        print("## Failure Details")
        for ev in failed_events:
            name = extract_field(ev, "operation", extract_field(ev, "title", "unknown"))
            text = extract_field(ev, "text", extract_field(ev, "message", ""))
            if text:
                # Truncate long error messages
                text = text[:200] + "..." if len(text) > 200 else text
            print(f"  {name}: {text}")
        print()

    # Print cleanup issues
    cleanup_failures = [ev for ev in cleanup_events
                        if extract_field(ev, "result", "") in ("failed", "failure")]
    if cleanup_failures:
        print("## Cleanup Failures")
        for ev in cleanup_failures:
            name = extract_field(ev, "operation", "unknown")
            print(f"  {name}: cleanup failed")
        print()

    # Print dependency check failures
    if depcheck_events:
        print("## Dependency Check Failures")
        for ev in depcheck_events:
            name = extract_field(ev, "operation", extract_field(ev, "title", "unknown"))
            text = extract_field(ev, "text", "")
            print(f"  {name}: {text[:150]}")
        print()

    # Print disruptive operation windows
    disruptive = [o for o in ops if is_disruptive(o["name"])]
    if disruptive:
        print("## Disruptive Operation Windows")
        for op in disruptive:
            cleanup = cleanup_map.get(op["name"])
            if cleanup:
                cleanup_end = cleanup["timestamp"]
            else:
                # Estimate: run_start + 5 minutes
                cleanup_end = op["_ts"] + timedelta(minutes=5)
            recovery_end = cleanup_end + timedelta(minutes=10)
            print(f"DISRUPTIVE_WINDOW: {op['name']} | "
                  f"{op['_ts'].isoformat()} | "
                  f"{cleanup_end.isoformat()} | "
                  f"{recovery_end.isoformat()}")
        print()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    files = sys.argv[1:]
    datasets = []
    for f in files:
        try:
            with open(f) as fh:
                data = json.load(fh)
            datasets.append(extract_events(data))
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Warning: Could not load {f}: {e}", file=sys.stderr)
            datasets.append([])

    # Pad to 4 datasets
    while len(datasets) < 4:
        datasets.append([])

    run_events, cleanup_events, failed_events, depcheck_events = datasets[:4]
    build_timeline(run_events, cleanup_events, failed_events, depcheck_events)


if __name__ == "__main__":
    main()
