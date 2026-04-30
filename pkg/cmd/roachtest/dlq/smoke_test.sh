#!/usr/bin/env bash
#
# End-to-end smoke test for the roachtest GitHub DLQ. Exercises both
# the writer (roachtest persists a failed post to GCS) and the replay
# tool (dlq-replay reads it back and posts to a target repo).
#
# This is not run automatically — it requires a real GCS bucket,
# Application Default Credentials, and a GitHub token. Run by hand from
# the repo root after meaningful changes to the writer or replay logic.
#
# Prerequisites:
#   - gcloud auth application-default login
#   - GITHUB_API_TOKEN exported (token with repo write on REPLAY_REPO)
#   - The DLQ bucket exists and you have storage.objectAdmin on it
#   - REPLAY_REPO defaults to your fork (e.g. williamchoe3/cockroach);
#     override to test against a different repo. NEVER point at
#     cockroachdb/cockroach without confirming.
#
# Usage:
#   bash pkg/cmd/roachtest/dlq/smoke_test.sh
#
# Optional env overrides:
#   BUCKET           default: roachtest-github-dlq-dev
#   REPLAY_REPO      default: $USER/cockroach (e.g. williamchoe3/cockroach)
#   ROACHTEST_NAME   default: roachtest/manual/fail
#   SKIP_WRITE       set to 1 to skip the write step (replay only)
#   SKIP_REPLAY      set to 1 to skip the replay step (write only)
#   DRY_RUN          set to 1 to make the replay step --dry-run only.
#                    No issues are posted; instead each entry prints a
#                    "create new issue" preview URL with the rendered
#                    body. Skips the org/repo rewrite step.

set -euo pipefail

BUCKET="${BUCKET:-roachtest-github-dlq-dev}"
REPLAY_REPO="${REPLAY_REPO:-${USER}/cockroach}"
ROACHTEST_NAME="${ROACHTEST_NAME:-roachtest/manual/fail}"

REPO_OWNER="${REPLAY_REPO%%/*}"
REPO_NAME="${REPLAY_REPO##*/}"

if [[ "$REPO_OWNER" == "cockroachdb" ]]; then
  echo "REFUSING: REPLAY_REPO points at cockroachdb. Set REPLAY_REPO to a fork." >&2
  exit 1
fi

if [[ -z "${GITHUB_API_TOKEN:-}" && "${SKIP_REPLAY:-0}" != "1" && "${DRY_RUN:-0}" != "1" ]]; then
  echo "GITHUB_API_TOKEN must be set (or SKIP_REPLAY=1, or DRY_RUN=1)" >&2
  exit 1
fi

echo "==> Smoke test config:"
echo "    bucket:    gs://${BUCKET}"
echo "    test:      ${ROACHTEST_NAME}"
if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo "    mode:      DRY RUN (no posts; preview URLs only)"
else
  echo "    replay to: ${REPO_OWNER}/${REPO_NAME}"
fi
echo

# ---------------------------------------------------------------------------
# Build both binaries up front. Failures here are unrelated to the smoke test.
# ---------------------------------------------------------------------------
echo "==> Building roachtest and dlq-replay"
./dev build pkg/cmd/roachtest pkg/cmd/roachtest/dlq-replay >/dev/null
echo "    bin/roachtest, bin/dlq-replay built"
echo

# ---------------------------------------------------------------------------
# Step 1: Writer side. Run roachtest with an invalid token so issues.Post
# fails. The DLQ wrapper should persist the failed request to GCS.
# ---------------------------------------------------------------------------
if [[ "${SKIP_WRITE:-0}" != "1" ]]; then
  echo "==> Step 1: Run roachtest with an invalid token to force a DLQ write"
  echo "    (--local --cockroach-stage=latest avoids GCE provisioning and a"
  echo "    full ./dev build cockroach; the staged binary is downloaded from"
  echo "    cloud storage)"
  echo

  GITHUB_DLQ_BUCKET="$BUCKET" \
  GITHUB_API_TOKEN="invalid-token-for-smoke-test" \
  TC_BUILD_BRANCH="master" \
    ./bin/roachtest run "$ROACHTEST_NAME" \
      --local \
      --cockroach-stage=latest || true

  echo
  echo "==> Verifying entry landed under failed/"
  TODAY="$(date -u +%Y%m%d)"
  PREFIX="failed/master/${TODAY}/"
  if ! gsutil ls "gs://${BUCKET}/${PREFIX}" >/dev/null 2>&1; then
    echo "FAIL: no entries found at gs://${BUCKET}/${PREFIX}" >&2
    exit 1
  fi
  gsutil ls "gs://${BUCKET}/${PREFIX}"
  echo
fi

# ---------------------------------------------------------------------------
# Step 2: Mutate every entry under failed/ so org/repo point at the test
# repo. This prevents the replay from posting to cockroachdb/cockroach.
# ---------------------------------------------------------------------------
if [[ "${SKIP_REPLAY:-0}" != "1" ]]; then
  if [[ "${DRY_RUN:-0}" == "1" ]]; then
    # Dry-run: skip the org/repo rewrite (we're not posting), and run
    # dlq-replay with --dry-run to log a preview URL per entry.
    #
    # The preview URL produced here has parity with the link emitted by
    # roachtest's existing --dry-run-issue-posting flag (dryRunPost in
    # pkg/cmd/roachtest/github.go) — same UnitTestFormatter, same
    # "create new issue" URL shape, same trailing labels footer. The
    # only intentional difference is that dlq-preview uses the entry's
    # stored branch/SHA/TeamCity URLs instead of dryRunPost's
    # "test_branch"/"test_SHA" placeholders, so the build/commit/
    # artifacts links are populated from real failure context.
    echo "==> Running dlq-replay --dry-run"
    echo "    Each entry prints a 'create new issue' URL with the rendered body."
    echo "    Open those URLs in a browser to inspect formatting."
    echo
    ./bin/dlq-replay --bucket="$BUCKET" --dry-run
  else
    echo "==> Step 2: Rewriting org/repo on every failed/ entry to ${REPO_OWNER}/${REPO_NAME}"
    while IFS= read -r path; do
      [[ -z "$path" ]] && continue
      gsutil cat "$path" \
        | jq --arg org "$REPO_OWNER" --arg repo "$REPO_NAME" \
            '.org=$org | .repo=$repo' \
        | gsutil -q cp - "$path"
      echo "    rewrote $path"
    done < <(gsutil ls "gs://${BUCKET}/failed/**" 2>/dev/null || true)
    echo

    echo "==> Step 3: Running dlq-replay"
    ./bin/dlq-replay --bucket="$BUCKET"
    echo

    echo "==> Step 4: Verifying processed/ contains the entries and failed/ is empty"
    REMAINING="$(gsutil ls "gs://${BUCKET}/failed/**" 2>/dev/null | wc -l | tr -d ' ')"
    if [[ "$REMAINING" != "0" ]]; then
      echo "WARN: $REMAINING entries still under failed/ — replay may have had failures" >&2
    fi
    PROCESSED="$(gsutil ls "gs://${BUCKET}/processed/**" 2>/dev/null | wc -l | tr -d ' ')"
    echo "    processed/: $PROCESSED objects"
    echo
    echo "==> Verify the issue was created at:"
    echo "    https://github.com/${REPO_OWNER}/${REPO_NAME}/issues"
  fi
fi

echo "==> Smoke test complete."
