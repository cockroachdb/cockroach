#!/usr/bin/env bash

# Destroy any Terraform resources found in tfstate files within the repository.

set -euo pipefail

cd "$(dirname "$0")/.."

echo -n "Are you sure you want to destroy all Terraform resources (y/n)? "
read ans
[[ "$ans" == y* ]] || exit 0

for tfstate in $(find . -name '*.tfstate'); do
  cd "$(dirname "$tfstate")"
  if ! terraform destroy -force -state="$(basename "$tfstate")"; then
    echo "fatal: unable to destroy $tfstate" >&2
    exit 1
  fi
  rm "$tfstate"
  cd -
done
