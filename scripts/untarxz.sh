#!/usr/bin/env bash

# Uncompress a tar.xz file using tar(1) or xz(1). This avoids a dependency on XZ
# Utils on systems whose tar natively supports xz compression (e.g. macOS,
# Linux). On systems where tar does not have xz support (e.g. OpenBSD), users
# can install XZ Utils manually, rather than replacing tar with an xz-supporting
# tar.

set -euo pipefail

file="${1:-}"
if [[ -z "$file" ]]
then
  echo "usage: $0 INPUT-FILE [TAR-OPTIONS...]" >&2
  exit 1
fi
shift

if tar -Jxf /dev/null 2> /dev/null
then
  exec tar -Jxf "$file" "$@"
fi

xz --decompress --stdout "$file" | tar -xf - "$@"
