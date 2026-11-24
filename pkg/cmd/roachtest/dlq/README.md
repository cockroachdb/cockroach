# GitHub Issue DLQ for roachtest

This directory contains the implementation for a GCS-backed dead letter queue for `roachtest` failed GitHub
`issue.Post` requests. The `dlq` package provides:
- `WrapIssuePoster`, which wraps `issues.Post` and writes failed requests to GCS.
- `Entry`, the serialized request format stored in GCS.
- Replay logic that reads stored entries and calls `issues.Post`.

The Cloud Run entrypoint lives separately in `pkg/cmd/roachtest/dlq-replay`. It defines the `dlq-replay` binary,
which uses this package to manually replay stored POST requests.

The motivation for this DLQ is that GitHub issues are the durable source of truth for `roachtest` failures. If GitHub
is unavailable when `roachtest` tries to post a failure, losing that request can hide rare or important failure modes.

When a GitHub `issues.Post` call fails, the DLQ wrapper stores the request and replay metadata in GCS. In TeamCity CI,
`roachtest` exits with code `12`, which alerts `#test-eng-ops` that manual replay is needed. After GitHub recovers,
an operator runs the Cloud Run Job to replay the queued entries.

This directory owns the GCS writer, serialized entry format, and replay library logic. The Cloud Run binary lives in
`pkg/cmd/roachtest/dlq-replay`, and the infrastructure lives in
`crl-infrastructure/terraform/gcp/cockroachlabs.com/engineering/test-engineering/cockroach-testeng-infra/
service_github_dlq_replay.tf`.

The infrastructure is deployed into 2 environments `prod` and `dev`. Testing should be done targeting infra in `dev`.

## Roachtest runtime flow
Writing to GCS is gated by the `GITHUB_DLQ_BUCKET` env var. When it is set, `issues.Post` is wrapped with
`dlq.WrapIssuePoster`. When it is unset, the wrapper is not used and behavior is unchanged.

The wrapper delegates to the original poster first. If the GitHub post succeeds, nothing is written to the dead letter
store. If the GitHub post fails, the failed request and replay metadata are serialized as a `dlq.Entry` and written to:

```text
gs://<bucket>/failed/<branch>/<YYYYMMDD>/<test_name>-<unix_nanos>.json
```

Manual replay is handled by the `dlq-replay` binary running in a Cloud Run Job.

## Bucket layout

```text
gs://<bucket>/
  failed/<branch>/<YYYYMMDD>/<test_name>-<unix_nanos>.json  # awaiting replay
  processing/<branch>/<YYYYMMDD>/<test_name>-<unix_nanos>.json  # claimed by replay
  processed/<branch>/<YYYYMMDD>/<test_name>-<unix_nanos>.json   # successfully replayed
```

### DLQ Prefix Flow

```mermaid
%%{init: {"themeVariables": {"fontSize": "16px"}} }%%
flowchart TB
  Writer["roachtest DLQ writer"]
  Replay["Cloud Run Job<br/>roachtest-github-dlq-replay-prod/dev"]
  Done["done"]
  Investigate["manual investigation<br/>avoid duplicate post"]

  subgraph Bucket["GCS bucket: selected DLQ bucket"]
    direction TB

    subgraph FailedPrefix["failed/"]
      Failed["&lt;branch&gt;/&lt;YYYYMMDD&gt;/&lt;test&gt;-&lt;nanos&gt;.json<br/>awaiting replay"]
    end

    subgraph ProcessingPrefix["processing/"]
      Processing["entry in progress"]
    end

    subgraph ProcessedPrefix["processed/"]
      Processed["entry replayed successfully"]
    end
  end

  Writer -->|"GitHub post failure"| Failed

  Replay -->|"list failed entries"| Failed
  Failed -->|"claim:<br/>copy with DoesNotExist"| Processing

  Processing -->|"unhappy path:<br/>GitHub post fails<br/>delete processing/<br/>leave failed/ for retry"| Failed
  Processing -->|"happy path:<br/>GitHub post succeeds<br/>copy to processed/"| Processed
  Processing -->|"unhappy path:<br/>post succeeds, archive fails<br/>leave processing/"| Investigate
  Processed -->|"cleanup:<br/>delete failed/ and processing/"| Done
```

The replay binary claims an entry by copying `failed/X.json` to `processing/X.json` with a `DoesNotExist`
precondition. If another runner already claimed the entry, the precondition fails and the entry is skipped. See
[GCS consistency docs](https://docs.cloud.google.com/storage/docs/consistency#strongly_consistent_operations) for the
relevant strong consistency guarantees.

On a successful replayed post to GitHub, `dlq-replay` moves the entry in `processing/` to `processed/`, then
deletes the original entry in `failed/` and the entry in `processing/`.

On a failed replayed post to GitHub, `dlq-replay` deletes the `processing/` object and leaves the original `failed/`
entry for a future attempt.

In the unlikely scenario where the GitHub post succeeds but moving the entry to `processed/` fails, `dlq-replay`
leaves the `processing/` object in place so a later run does not silently post the same issue again. This will need
to be manually moved.

## How to Replay Events
### Happy Path
The Cloud Run Job executes `dlq-replay` with a bucket name:

```bash
# Default: replay all failed/ entries in the prod bucket.
# The prod Cloud Run Job includes --bucket roachtest-github-dlq-prod
# as its default args.
gcloud run jobs execute roachtest-github-dlq-replay-prod \
  --region=us-east1 --project=cockroach-testeng-infra \
  --wait

# Branch specific e.g. only entries from failures on master
gcloud run jobs execute roachtest-github-dlq-replay-prod \
  --region=us-east1 --project=cockroach-testeng-infra \
  --args="--bucket,roachtest-github-dlq-prod,--branch,master" \
  --wait
```

Optional flags:

- `--branch=<branch_name>`: only replay entries under specified branch e.g. `failed/master/`
- Per-execution `--args=...` replace the Job's default args, so include
  `--bucket roachtest-github-dlq-prod` when overriding args on the prod Job.

Use `--wait` so `gcloud` waits for the Cloud Run execution to finish. The command does not stream the replay logs to
stdout; see [Logging and audit logs](#logging-and-audit-logs) for verification commands and per-entry output.

The CLI exits with status 1 if any entry's replay attempt failed, which causes the Cloud Run execution to fail.

### Unhappy Path
GCS preconditions make claiming concurrency-safe, but the replay workflow is not transactional across GCS and GitHub.
If replay fails before posting, the entry can be retried from `failed/`. If GitHub accepts the post but replay fails
before archiving to `processed/`, retrying may create a duplicate issue or comment.

For that reason, entries left in `processing/` should be inspected manually before retrying. A future improvement
could add an idempotency key to the GitHub issue body/comment and store replay state in the entry or an external
datastore.

## Access

Only `test-eng@cockroachlabs.com` has access to execute the prod Cloud Run Job and access the prod bucket for manual
reconciliation. The dev Cloud Run Job and dev bucket are also limited to `test-eng@cockroachlabs.com`. Reach out to
test-eng if you need access.

## Testing
### Unit Tests
Unit tests exercise the writer and replay logic against in-memory fakes:

```bash
./dev test pkg/cmd/roachtest/dlq
```

### Writing Mock Events to dev GCS bucket for Replay Testing
Functional testing using a local `dlq-replay` binary and using a custom `dlq-replay` on the dev Cloud Run Job is both
supported. For both cases, you will need mock events in `roachtest-github-dlq-dev`.

To seed the dev bucket with a real failed issue-post entry, run:
```bash
# If you need to build roachtest
./dev build pkg/cmd/roachtest

# Run script
ROACHTEST_BIN=./bin/roachtest \
  bash pkg/cmd/roachtest/dlq/smoke_test.sh
```
* test selected is a roachtest that will always fail
* test(s) executed that attempt to post github issues / comments will always fail because the script passes an invalid
  `GITHUB_API_TOKEN`

The script exercises the writer path. It expects `roachtest` on `PATH`, or a `ROACHTEST_BIN` override. It writes to
`roachtest-github-dlq-dev` and verifies an object landed under `failed/`. You can specify test(s) to run with
`ROACHTEST_NAME` (in order to test the DLQ functionality, the roachtest needs to fail).

### Testing Replay Using Local CLI and dev GCS bucket
If you are making changes to the Replay binary, you most likely want to verify locally.
If you want to verify the entire Replay flow end-to-end locally, you will need a GitHub PAT (Personal Access Token)
which you can request through GitHub. The PAT is granted after dev-inf approval. Otherwise you can do a dry run using
`--skip-github-post`. Local runs use Application Default Credentials for GCS and needs credentials with
read/write/delete access on the DLQ bucket.

Dry Run:
```bash
# Build the replay binary.
./dev build pkg/cmd/roachtest/dlq-replay

# Execute local replay binary
./bin/dlq-replay \
  --bucket=roachtest-github-dlq-dev \
  --skip-github-post
```
The GitHub issue preview URL will be printed to stdout. When clicked, the URL brings you to a GitHub Issue posting
page where you can see how the issue will be formatted.

The format and presentation of the issue should match with what you would get from running roachtest against a failing
test with `--dry-run-issue-posting`. (Note some minor difference may exist like the exact fields listed in the
Parameters section).
```bash
roachtest run "roachtest/manual/fail" --local --cockroach-stage=latest \
  --dry-run-issue-posting
```

With GitHub PAT:
```bash
# Full local replay. This posts to GitHub and requires a valid PAT.
# Close the issue after it posts.
GITHUB_API_TOKEN=<token> ./bin/dlq-replay \
  --bucket=roachtest-github-dlq-dev
```

### Testing Replay Using The Dev Cloud Run Job
If you changed `dlq-replay`, first push an updated dev replay image. See [image updates](#updating-cloud-run-images).

To verify Cloud Run replay behavior without posting to GitHub, run the dev env Cloud Run Job against the `dev bucket`
via `--bucket roachtest-github-dlq-dev` and use the `--skip-github-post` flag.
```bash
gcloud run jobs execute roachtest-github-dlq-replay-dev \
  --region=us-east1 \
  --project=cockroach-testeng-infra \
  --args="--bucket,roachtest-github-dlq-dev,--skip-github-post" \
  --wait
```
* `--skip-github-post` claims entries for processing and verifies entries without posting to GitHub, then releases the
  claims by deleting the entries from `processing/`. The original `failed/` entries remain in place, and no
  `processed/` entries are written.
* `--bucket` is included because Cloud Run `--args` replaces the Job's default args

Just like with local execution, the GitHub issue preview URL will be printed to stdout. When clicked, the URL brings
you to a GitHub Issue posting page where you can see how the issue will be formatted.

See [Logging and audit logs](#logging-and-audit-logs) for verifying the issue preview URL, Cloud Run execution status,
per-entry replay logs.

To test a real end-to-end replay from the dev Cloud Run Job, use a dev PAT and override the Job's injected
`GITHUB_API_TOKEN` for that execution. Do not include `--skip-github-post`.

```bash
read -s GITHUB_API_TOKEN
gcloud run jobs execute roachtest-github-dlq-replay-dev \
  --region=us-east1 \
  --project=cockroach-testeng-infra \
  --args="--bucket,roachtest-github-dlq-dev" \
  --update-env-vars="GITHUB_API_TOKEN=${GITHUB_API_TOKEN}" \
  --wait
unset GITHUB_API_TOKEN
```

## Updating Cloud Run Images

There is a convenience script located in `pkg/cmd/roachtest/dlq-replay/push.sh` to push new images to both `dev` and
`prod` environments. The script submits a Cloud Build that cross-compiles the linux `dlq-replay` binary with Bazel,
builds the replay image from the current committed Git SHA, pushes it to Artifact Registry as a short-SHA tag, and
runs `gcloud run jobs update <selected-job> --image=...`.
That Git SHA must already be pushed to the GitHub repo selected by `OWNER`/`REPO`, because Cloud Build clones that repo
before building. Terraform creates the Artifact Registry repo and Cloud Run Job, but intentionally ignores later Job
image changes so this manual update is not reverted by Spacelift.

See below for examples in both environments.

### Updating Dev Cloud Run Image for Testing

To test `dlq-replay` changes before updating prod, push the current committed SHA to the dev Artifact Registry repo and
update the dev Cloud Run Job:

```bash
DLQ_ENV=dev pkg/cmd/roachtest/dlq-replay/push.sh
```

This updates:

- Cloud Run Job: `roachtest-github-dlq-replay-dev`
- Artifact Registry repo: `roachtest-github-dlq-replay-dev`

To verify the push, check that the latest Cloud Build succeeded:

```bash
gcloud builds list \
  --project=cockroach-testeng-infra \
  --sort-by="~createTime" \
  --limit=5
```
Then check that the dev Cloud Run Job's image points at the recently added image in Artifact Registry:

```bash
gcloud run jobs describe roachtest-github-dlq-replay-dev \
  --region=us-east1 \
  --project=cockroach-testeng-infra \
  --format="value(spec.template.spec.template.spec.containers[0].image)"
```

The image should look like:

```text
us-east1-docker.pkg.dev/cockroach-testeng-infra/roachtest-github-dlq-replay-dev/replay:<short-sha>
```

### Updating Prod Cloud Run Image
After merging changes to `dlq-replay`, update the prod Cloud Run Job image from the Cockroach repo. By default,
`push.sh` updates the prod Job `roachtest-github-dlq-replay-prod` and pushes to the prod Artifact Registry repo
`roachtest-github-dlq-replay-prod`:

```bash
pkg/cmd/roachtest/dlq-replay/push.sh
```

To verify the push, check that the latest Cloud Build succeeded:

```bash
gcloud builds list \
  --project=cockroach-testeng-infra \
  --sort-by="~createTime" \
  --limit=5
```

Then check that the prod Cloud Run Job's image points at the recently added image in Artifact Registry:

```bash
gcloud run jobs describe roachtest-github-dlq-replay-prod \
  --region=us-east1 \
  --project=cockroach-testeng-infra \
  --format="value(spec.template.spec.template.spec.containers[0].image)"
```

The image should look like:

```text
us-east1-docker.pkg.dev/cockroach-testeng-infra/roachtest-github-dlq-replay-prod/replay:<short-sha>
```

Note: Run `push.sh` after the infra is created and before the first real replay. A newly-created Job starts with a
public bootstrap image and will not run the real replay binary until the Job image is updated.

## Rotating the GitHub token

The prod Cloud Run Job reads `GITHUB_API_TOKEN` from Secret Manager secret `roachtest-github-dlq-token-prod`, version
`latest`. The token is not stored in the image and does not require rebuilding or updating the Cloud Run Job.

To rotate the token, add a new enabled secret version:

```bash
read -s GITHUB_DLQ_REPLAY_TOKEN
printf '%s' "$GITHUB_DLQ_REPLAY_TOKEN" | gcloud secrets versions add roachtest-github-dlq-token-prod \
  --project=cockroach-testeng-infra \
  --data-file=-
unset GITHUB_DLQ_REPLAY_TOKEN
```

New Cloud Run Job executions will read the new `latest` version. Existing executions keep the environment they started
with.

Verify versions:

```bash
gcloud secrets versions list roachtest-github-dlq-token-prod \
  --project=cockroach-testeng-infra
```

## Logging and audit logs

`dlq-replay` writes one status line per visited entry to stdout and exits with status 1 if any replay attempt failed.
When replay runs through Cloud Run, stdout/stderr are written to Cloud Logging. e.g. for prod, the quickest place to
inspect a run is **Cloud Run -> Jobs -> roachtest-github-dlq-replay-prod -> Executions**, which shows per-execution
status, exit code, and logs.

### Verifying Replay Execution on dev

First, find the latest execution:

```bash
gcloud run jobs executions list \
  --job=roachtest-github-dlq-replay-dev \
  --region=us-east1 \
  --project=cockroach-testeng-infra \
  --sort-by="~metadata.creationTimestamp" \
  --limit=5
```

Optionally capture the latest execution name for the commands below:

```bash
EXECUTION="$(gcloud run jobs executions list \
  --job=roachtest-github-dlq-replay-dev \
  --region=us-east1 \
  --project=cockroach-testeng-infra \
  --sort-by="~metadata.creationTimestamp" \
  --limit=1 \
  --format="value(metadata.name)")"
```

Check execution status. A successful execution means `dlq-replay` exited with status 0; still check logs and GCS state
for per-entry outcomes:

```bash
gcloud run jobs executions describe "$EXECUTION" \
  --region=us-east1 \
  --project=cockroach-testeng-infra
```

Check replay logs for the summary line, `done: <ok> ok, <skipped> skipped, <failed> failed`:

```bash
gcloud logging read \
  'resource.type="cloud_run_job"
   AND resource.labels.job_name="roachtest-github-dlq-replay-dev"
   AND resource.labels.location="us-east1"
   AND labels."run.googleapis.com/execution_name"="'"$EXECUTION"'"
   AND textPayload:"done:"' \
  --project=cockroach-testeng-infra \
  --limit=20 \
  --format='table(timestamp,severity,textPayload)'
```

Check per-entry log lines. Successful posts are logged as `ok <failed/object/path> posted to <org>/<repo>`, and failed
posts are logged as `x <failed/object/path> post failed: ...`:

```bash
gcloud logging read \
  'resource.type="cloud_run_job"
   AND resource.labels.job_name="roachtest-github-dlq-replay-dev"
   AND resource.labels.location="us-east1"
   AND labels."run.googleapis.com/execution_name"="'"$EXECUTION"'"
   AND (textPayload:"ok " OR textPayload:"x ")' \
  --project=cockroach-testeng-infra \
  --limit=100 \
  --format='table(timestamp,severity,textPayload)'
```

For `--skip-github-post` runs, check the preview URL log line:

```bash
gcloud logging read \
  'resource.type="cloud_run_job"
   AND resource.labels.job_name="roachtest-github-dlq-replay-dev"
   AND resource.labels.location="us-east1"
   AND labels."run.googleapis.com/execution_name"="'"$EXECUTION"'"
   AND textPayload:"preview: https://github.com/"' \
  --project=cockroach-testeng-infra \
  --limit=20 \
  --format='value(textPayload)'
```

Check GCS state if needed. Replayed entries should move from `failed/` to `processed/`; `processing/` should normally
be empty. Entries left in `failed/` need another replay attempt, and entries left in `processing/` need manual
investigation before retrying. An empty or no-matches result under `processing/` is expected after a clean replay.

```bash
gcloud storage ls --recursive gs://roachtest-github-dlq-dev/failed/
gcloud storage ls --recursive gs://roachtest-github-dlq-dev/processing/
gcloud storage ls --recursive gs://roachtest-github-dlq-dev/processed/
```

General log browsing:

```bash
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="roachtest-github-dlq-replay-dev" AND resource.labels.location="us-east1"' \
  --project=cockroach-testeng-infra \
  --limit=50 \
  --format='table(timestamp,severity,textPayload)'
```

Cloud Audit Logs record control-plane actions such as who executed or updated the Cloud Run Job. Look at
`protoPayload.authenticationInfo.principalEmail`, `protoPayload.methodName`, and `timestamp`.

```bash
gcloud logging read \
  'protoPayload.serviceName="run.googleapis.com" AND protoPayload.resourceName:"/jobs/roachtest-github-dlq-replay-dev"' \
  --project=cockroach-testeng-infra \
  --limit=50 \
  --format='table(timestamp,protoPayload.methodName,protoPayload.authenticationInfo.principalEmail)'
```

### Verifying Replay Execution on prod

First, find the latest execution:

```bash
gcloud run jobs executions list \
  --job=roachtest-github-dlq-replay-prod \
  --region=us-east1 \
  --project=cockroach-testeng-infra \
  --sort-by="~metadata.creationTimestamp" \
  --limit=5
```

Optionally capture the latest execution name for the commands below:

```bash
EXECUTION="$(gcloud run jobs executions list \
  --job=roachtest-github-dlq-replay-prod \
  --region=us-east1 \
  --project=cockroach-testeng-infra \
  --sort-by="~metadata.creationTimestamp" \
  --limit=1 \
  --format="value(metadata.name)")"
```

Check execution status. A successful execution means `dlq-replay` exited with status 0; still check logs and GCS state
for per-entry outcomes:

```bash
gcloud run jobs executions describe "$EXECUTION" \
  --region=us-east1 \
  --project=cockroach-testeng-infra
```

Check replay logs for the summary line, `done: <ok> ok, <skipped> skipped, <failed> failed`:

```bash
gcloud logging read \
  'resource.type="cloud_run_job"
   AND resource.labels.job_name="roachtest-github-dlq-replay-prod"
   AND resource.labels.location="us-east1"
   AND labels."run.googleapis.com/execution_name"="'"$EXECUTION"'"
   AND textPayload:"done:"' \
  --project=cockroach-testeng-infra \
  --limit=20 \
  --format='table(timestamp,severity,textPayload)'
```

Check per-entry log lines. Successful posts are logged as `ok <failed/object/path> posted to <org>/<repo>`, and failed
posts are logged as `x <failed/object/path> post failed: ...`:

```bash
gcloud logging read \
  'resource.type="cloud_run_job"
   AND resource.labels.job_name="roachtest-github-dlq-replay-prod"
   AND resource.labels.location="us-east1"
   AND labels."run.googleapis.com/execution_name"="'"$EXECUTION"'"
   AND (textPayload:"ok " OR textPayload:"x ")' \
  --project=cockroach-testeng-infra \
  --limit=100 \
  --format='table(timestamp,severity,textPayload)'
```

Check GCS state if needed. Replayed entries should move from `failed/` to `processed/`; `processing/` should normally
be empty. Entries left in `failed/` need another replay attempt, and entries left in `processing/` need manual
investigation before retrying. An empty or no-matches result under `processing/` is expected after a clean replay.

```bash
gcloud storage ls --recursive gs://roachtest-github-dlq-prod/failed/
gcloud storage ls --recursive gs://roachtest-github-dlq-prod/processing/
gcloud storage ls --recursive gs://roachtest-github-dlq-prod/processed/
```

General log browsing:

```bash
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="roachtest-github-dlq-replay-prod" AND resource.labels.location="us-east1"' \
  --project=cockroach-testeng-infra \
  --limit=50 \
  --format='table(timestamp,severity,textPayload)'
```

Cloud Audit Logs record control-plane actions such as who executed or updated the Cloud Run Job. Look at
`protoPayload.authenticationInfo.principalEmail`, `protoPayload.methodName`, and `timestamp`.

```bash
gcloud logging read \
  'protoPayload.serviceName="run.googleapis.com" AND protoPayload.resourceName:"/jobs/roachtest-github-dlq-replay-prod"' \
  --project=cockroach-testeng-infra \
  --limit=50 \
  --format='table(timestamp,protoPayload.methodName,protoPayload.authenticationInfo.principalEmail)'
```
