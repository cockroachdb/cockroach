#!/usr/bin/env bash

# Uncompress a tar.xz file using tar(1) or xz(1). This avoids a dependency on XZ
# Utils on macOS, whose tar natively supports xz compression. We can't rely on
# tar to ask users to install xz when it's missing, because there exist systems
# where tar refuses to shell out to xz even if it's installed (e.g. OpenBSD).

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
