---
name: engflow-artifacts
description: Use when downloading test logs, artifacts, or outputs.zip from EngFlow build invocations. Use when investigating CockroachDB CI test failures hosted on mesolite.cluster.engflow.com.
---

# EngFlow Artifact Download

Download test logs and artifacts from EngFlow CI invocations (mesolite.cluster.engflow.com) using `engflow_artifacts.py` (co-located in this skill directory).

## Prerequisites

The `engflow_auth` CLI must be installed and authenticated:

```bash
# One-time login (opens browser for Google SSO)
engflow_auth login mesolite.cluster.engflow.com

# Verify authentication
engflow_auth export mesolite.cluster.engflow.com
```

## Quick Reference

Use `run.sh` to invoke the script — it manages a local venv with dependencies
automatically. The script is at `.claude/skills/engflow-artifacts/run.sh`:

| Command | Purpose |
|---------|---------|
| `targets <ID>` | List failed targets in an invocation |
| `list <ID> --target <LABEL>` | List per-shard artifacts for a target |
| `download <ID> --target <LABEL> --shard N` | Download artifacts for a shard |
| `download <ID> --target <LABEL> --shard N --run M` | Download a specific run |
| `blob <HASH> <SIZE>` | Download a specific CAS blob |
| `url '<FULL_URL>'` | Download from a full EngFlow invocation URL |

## Typical Investigation Workflow

```bash
EF=.claude/skills/engflow-artifacts/run.sh

# 1. Find what failed
./$EF targets daa807a0-3589-40a5-94b5-3440c7490d6a

# 2. List shards for the failed target
./$EF list daa807a0-3589-40a5-94b5-3440c7490d6a \
  --target '//pkg/sql/ttl:ttl_test'

# 3. Download the failing shard's logs
./$EF download daa807a0-3589-40a5-94b5-3440c7490d6a \
  --target '//pkg/sql/ttl:ttl_test' --shard 100 --outdir /tmp/engflow-test

# 4. Read the test log
cat /tmp/engflow-test/test.log

# 5. Inspect extracted CockroachDB server logs from outputs.zip
ls /tmp/engflow-test/
```

## Per-Shard Artifacts

Every shard has:
- **test.xml** — JUnit XML test results (referenced via CompactDigest: raw 32-byte SHA-256 hash)
- **test.log** — stdout/stderr from the test run (referenced via bytestream:// URL)

Some shards also have:
- **outputs.zip** — CockroachDB server logs captured during the test
- **manifest** — lists files in outputs.zip

## How It Works

- **`engflow_auth export`** provides a JWT token.
- **gRPC-web calls via curl** to EngFlow's internal `v1alpha` ResultStore API (GetTarget, GetTargetLabelsByStatus).
- **Protobuf parsing** via `google.protobuf` with a reverse-engineered `.proto` schema (`resultstore.proto`). Responses are properly deserialized — shard numbers and run numbers come from actual protobuf fields, not byte-pattern heuristics.
- **CAS blob downloads via curl** with the auth token as a cookie.
- `outputs.zip` is auto-extracted after download.

Each test target has multiple **shards** (parallel test splits) and **runs** (repeated executions). Use `--shard` to select a shard and `--run` to select a run (defaults to run 1). Shard numbers are 1-based and match the `testReportShard` parameter in EngFlow URLs.

EngFlow's gRPC-web endpoint requires HTTP/2, so the script uses `curl` (which negotiates HTTP/2 via ALPN) rather than Python HTTP libraries that only speak HTTP/1.1. Dependencies are installed automatically by `run.sh`.

## Extracted outputs.zip Structure

```
logTestName123456/
  test.log                                    # short log
  test.{node-id}.roach.{timestamp}.log        # full structured log
  test-health.log                             # health channel
  test-storage.log                            # storage channel
  test-kv-distribution.log                    # KV distribution channel
```

## URL Structure

Invocation page: `https://mesolite.cluster.engflow.com/invocations/default/{INVOCATION_ID}`

With filters: `?testReportRun=1&testReportShard={N}&testReportAttempt=1&treeFilterStatus=failed#targets-{BASE64_TARGET}`

## Tips

- Blob hashes are SHA-256. Sizes are in bytes.
- Use `list` to find which shard has `outputs.zip` (not all shards produce one).
- If `engflow_auth export` fails, re-authenticate with `engflow_auth login mesolite.cluster.engflow.com`.
- Default output directory is `/tmp/engflow-artifacts`.
- `--all` flag on `targets` shows both passed and failed targets.