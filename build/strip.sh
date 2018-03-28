#!/usr/bin/env bash

# Strip a binary after extracting its debug symbols.
#
# Usage: build/strip.sh [BINARY] [TARGET-TRIPLE]

binary=$1
triple=${2-}

if [[ "$triple" ]]; then
  triple+=-
fi

have_objcopy=false
if command -v "${triple}objcopy" &> /dev/null; then
  have_objcopy=true
fi

# Create a copy of the binary with only debug symbols. If objcopy doesn't exist
# (e.g. on macOS), fall back to copying the whole binary.
if "$have_objcopy"; then
  "${triple}objcopy" --only-keep-debug "$1" "$1.dbg"
else
  cp "$1" "$1.dbg"
fi

# Strip the original binary.
"${triple}strip" "$1"

# If possible, indicate in the stripped binary where debug information lives.
if "$have_objcopy"; then
  "${triple}objcopy" --add-gnu-debuglink="$1.dbg" "$1"
fi
