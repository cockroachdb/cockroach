#!/usr/bin/env bash

set -euo pipefail

if [[ -n "${COCKROACH_BUILDER_CCACHE-}" ]]; then
  PATH=/usr/lib/ccache:/usr/local/lib/ccache:$PATH
  export CCACHE_DIR=/go/native/ccache
  export CCACHE_MAXSIZE=${COCKROACH_BUILDER_CCACHE_MAXSIZE-10G}
  # Without CCACHE_CPP2=1, ccache can generate spurious warnings. See the manpage
  # for details. This option is enabled by default in ccache v3.3+, but our
  # version Ubuntu 16.04 ships v3.2.4.
  # TODO(benesch): Remove when we upgrade to Ubuntu 18.04 or newer.
  export CCACHE_CPP2=1
fi

exec "${@-$SHELL}"
