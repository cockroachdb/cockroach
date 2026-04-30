# GitHub Issue DLQ for roachtest

This package implements a GCS-backed dead letter queue for failed
GitHub issue posts. When `issues.Post` returns an error during a
roachtest run (typically a GitHub outage), the request is serialized
and persisted to a GCS bucket so it can be replayed manually once
GitHub recovers.

## How it kicks in

In `run.go`, if the `GITHUB_DLQ_BUCKET` environment variable is set,
the `issues.Post` function passed to `*githubIssues` is wrapped with
`dlq.WrapIssuePoster`. The wrapper delegates to the inner `Post`,
returns its result unmodified, and on error persists a `dlq.Entry` to
`gs://<bucket>/failed/<branch>/<YYYYMMDD>/<test_name>-<unix_nanos>.json`.

When `GITHUB_DLQ_BUCKET` is unset, the wrapper is not installed and
behavior is identical to today's roachtest.

## Bucket layout

```
gs://<bucket>/
├── failed/<branch>/<YYYYMMDD>/<test_name>-<unix_nanos>.json   ← awaiting replay
├── processing/<same_path>                                      ← being replayed
└── processed/<same_path>                                       ← successfully replayed
```

GCS object preconditions are used to claim entries: a replay attempt
copies `failed/X.json` to `processing/X.json` with `DoesNotExist: true`,
which fails atomically if another runner already claimed it. This
prevents two engineers running the replay tool concurrently from
re-firing the same entry twice.

## Running the replay tool

Build the CLI binary:

```bash
./dev build pkg/cmd/roachtest/dlq-replay
```

The binary is written to `bin/dlq-replay`. Run it with a real GitHub
token:

```bash
GITHUB_API_TOKEN=ghp_... ./bin/dlq-replay --bucket=<bucket-name>
```

Optional flags:
- `--branch=master` — only replay entries under `failed/master/`.
- `--dry-run` — claim and verify entries without posting; releases
  claims when done. Useful for confirming entries deserialize cleanly.

GCS authentication uses Application Default Credentials. Make sure
you've run `gcloud auth application-default login` and have
`storage.objectAdmin` on the bucket.

The tool exits with status 1 if any entry's replay attempt failed.

## Testing locally without a real bucket

For rapid iteration on the writer or replay logic, the unit tests in
`dlq_test.go` and `replay_test.go` exercise both code paths against
in-memory fakes. Run with:

```bash
./dev test pkg/cmd/roachtest/dlq
```

For an end-to-end smoke test against a real bucket, see the
verification section of `.claude/plans/elegant-herding-cloud.md` (or
ask in #testeng for the dev bucket name).

## Future deployment alternatives (not built today)

The current implementation is intentionally minimal — a CLI an engineer
runs by hand. We considered and rejected (for now) the following
automated-replay options:

- **Cloud Function (Gen2, HTTP trigger)** — automated replay via Cloud
  Scheduler. Blocked by the 500 MB Cloud Function source upload limit:
  the cockroach repo is ~1 GB, and the replay code transitively pulls
  in protobuf-generated dependencies via `pkg/cmd/bazci/githubpost/issues`
  that need Bazel to build.

- **Cloud Run (container image)** — sidesteps the source-size limit by
  shipping a pre-built linux binary in a slim container. Requires a CI
  job (GitHub Actions or TeamCity) to build with Bazel
  (`./dev build --cross=linux`) and push to Artifact Registry, plus
  Workload Identity Federation setup. Adds non-trivial CI plumbing for a
  binary that almost never changes.

- **Manual `push.sh` (roachperf-style)** — a script that builds the
  binary locally with `./dev`, builds the Docker image, and pushes to
  Artifact Registry. Cloud Run service points at the `:latest` tag.
  Trades CI automation for per-update toil (an engineer runs `push.sh`
  whenever the replay code changes). Lighter than full CI but heavier
  than the CLI we have today.

- **Cloud Build** — would still need Bazel in the build environment to
  compile from source, so not significantly simpler than the alternatives
  above.

If the manual CLI proves insufficient (e.g. we want auto-replay on a
schedule when GitHub recovers without human intervention), the
`push.sh` + Cloud Run path is the most pragmatic next step. The
existing `Replay` function already takes a `*storage.BucketHandle` and
runs the same loop a Cloud Run service would; only the entry point
(`main.go` vs an HTTP handler) needs to change.

## HelpCommand pre-rendering

`PostRequest.HelpCommand` is a Go closure that emits markdown/HTML
links. Closures can't be JSON-serialized, so the writer renders the
closure once at write time (via `issues.Renderer`) and stores the
resulting string in `Entry.RenderedHelpCommand`. At replay time, we
wrap that string back into a closure that emits it verbatim via
`issues.Renderer.Raw`.

Alternative considered: capture the closure inputs (cluster name,
cloud, start/end, runID) and rebuild it at replay time using
`issues.HelpCommandAsLink`. Pre-rendering keeps the replay tool dumb
and avoids splitting help-link construction logic across write- and
replay-time. If we ever need replay-time HelpCommand customization
(e.g. enriching with new links not present at write time), we can add
the inputs as an additional field without breaking the existing
schema.
