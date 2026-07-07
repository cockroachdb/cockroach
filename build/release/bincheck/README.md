# bincheck

bincheck verifies the sanity of CockroachDB release binaries. At present, the
sanity checks are:

  * starting a one-node server and running a simple SQL query, and
  * verifying the output of `cockroach version`.

## When it runs

bincheck runs as part of the `Release - Build and Sign` workflow
(`.github/workflows/release-build-and-sign.yml`), once the per-platform builds
have staged their archives. Each platform is smoke-tested on a native runner
against the staged (pre-publish) archive in GCS, so a bad binary is caught
before it is published.

## Running manually

The `test-*` wrappers take the expected version and SHA, e.g.:

    ./test-linux v26.3.0-beta.1 <sha>

By default they download the published archive from
https://binaries.cockroachdb.com. Set `BINCHECK_GCS_BUCKET` together with
`BINCHECK_GCS_TOKEN` (a GCS-scoped OAuth2 access token) to fetch the staged
archive from a GCS bucket instead:

    BINCHECK_GCS_BUCKET=cockroach-release-artifacts-staged-dryrun \
      BINCHECK_GCS_TOKEN="$(gcloud auth print-access-token)" \
      ./test-linux v26.3.0-beta.1 <sha>
