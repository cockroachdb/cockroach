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

The script is at `.claude/skills/engflow-artifacts/engflow_artifacts.py`.

| Command | Purpose |
|---------|---------|
| `targets <ID>` | List failed targets in an invocation |
| `list <ID> --target <LABEL>` | List per-shard artifacts for a target |
| `download <ID> --target <LABEL> --shard N` | Download artifacts for a shard |
| `blob <HASH> <SIZE>` | Download a specific CAS blob |
| `url '<FULL_URL>'` | Download from a full EngFlow invocation URL |

## Typical Investigation Workflow

```bash
SCRIPT=.claude/skills/engflow-artifacts/engflow_artifacts.py

# 1. Find what failed
python3 $SCRIPT targets daa807a0-3589-40a5-94b5-3440c7490d6a

# 2. List shards for the failed target
python3 $SCRIPT list daa807a0-3589-40a5-94b5-3440c7490d6a \
  --target '//pkg/sql/ttl:ttl_test'

# 3. Download the failing shard's logs
python3 $SCRIPT download daa807a0-3589-40a5-94b5-3440c7490d6a \
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
- **CAS blob downloads via curl** with the auth token as a cookie.
- `outputs.zip` is auto-extracted after download.
- test.xml blobs are found by parsing `engflow.type.CompactDigest` entries (binary SHA-256 + uint64 size) from the protobuf response.

Python's urllib mangles headers in a way EngFlow rejects, so the script uses `subprocess` + `curl`.

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