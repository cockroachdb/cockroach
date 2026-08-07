# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Bucket names include the owning GCP infrastructure project so production and
# staging never share artifacts. Build-cache entries and binaries historically
# lived in the shared cockroach-nightly bucket, while performance artifacts use
# a cloud-specific bucket (except GCE, the original cloud).
function roachtest_nightly_shared_bucket {
  printf 'cockroach-nightly-%s\n' "${ROACHPROD_GCE_INFRA_PROJECT:-crl-e2e-infra}"
}

function roachtest_nightly_perf_bucket {
  local cloud="${1:-}"
  local infra_project="${ROACHPROD_GCE_INFRA_PROJECT:-crl-e2e-infra}"

  case "${cloud}" in
    gce)
      printf 'cockroach-nightly-%s\n' "${infra_project}"
      ;;
    aws|azure|ibm)
      printf 'cockroach-nightly-%s-%s\n' "${cloud}" "${infra_project}"
      ;;
    *)
      echo "unknown roachtest cloud: ${cloud}" >&2
      return 1
      ;;
  esac
}
