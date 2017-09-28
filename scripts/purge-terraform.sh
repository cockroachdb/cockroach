#!/usr/bin/env bash

# Destroy any Terraform resources found in tfstate files within the repository.

set -euo pipefail

cd "$(dirname "$0")/.."

echo -n "Are you sure you want to destroy all Terraform resources (y/n)? "
read ans
[[ "$ans" == y* ]] || exit 0

failures=()

for tfstate in $(find . -name '*.tfstate'); do
  dir=$(dirname "$tfstate")
  base=$(basename "$tfstate")
  if (set -x && cd "$dir" && terraform destroy -force -state="$base"); then
    rm "$tfstate"
  else
    failures+=("$tfstate")
  fi
done

# Terraform output is quite spammy, so we wait until all `terraform destroy`s
# are finished before warning about tfstates that failed to destroy.
for tfstate in "${failures[@]}"; do
  echo "warning: unable to destroy $tfstate" >&2
done
