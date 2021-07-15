#!/usr/bin/env bash
set -euo pipefail

# This script is an opinionated helper around building binaries and artifacts.
# It is particularly helpful for stressing roachtests at multiple revisions.
#
# This script is to be invoked from the repository root checked out at the revision
# to be tested.
#
# It's best practice to invoke this script with "caffeinate" on OSX and/or linux
# to avoid computer going to standby.

if [ "${GCE_PROJECT-cockroach-ephemeral}" == "cockroach-ephemeral" ]; then
  cat <<EOF
Please do not use roachstress on the cockroach-ephemeral project.
This may compete over quota with scheduled roachtest builds.
Use the andrei-jepsen project instead or reach out to dev-inf.

The project can be specified via the environment:
  export GCE_PROJECT=XXX
EOF
  exit 2
fi

# Define the artifacts base dir, within which both the built binaries and the
# artifacts will be stored.
sha=$(git rev-parse --short HEAD)
abase="artifacts/${sha}"

# Locations of the binaries.
rt="${abase}/roachtest"
rp="${abase}/roachprod"
wl="${abase}/workload"
cr="${abase}/cockroach"

# This is the artifacts dir we'll pass to the roachtest invocation. It's
# disambiguated by a timestamp because one often ends up invoking roachtest on
# the same SHA multiple times and artifacts shouldn't mix.
a="${abase}/$(date '+%H%M%S')"

# Read user input.
read -e -i "${TEST-}" -p "Test regexp: " TEST
read -e -i "${COUNT-10}" -p "Count: " COUNT

short="short"
if [ ! -f "${cr}" ]; then
  yn=""
  read -e -i "${SHORT-y}" -p "Build cockroach without the UI: " yn
  case $yn in
    [Nn]* | false | "") short=""
  esac
fi

mkdir -p "${a}"

if [ ! -f "${cr}" ]; then
  ./build/builder.sh mkrelease amd64-linux-gnu "build${short}"
  mv "cockroach${short}-linux-2.6.32-gnu-amd64" "${cr}"
fi

if [ ! -f "${rt}" ]; then
  ./build/builder.sh mkrelease amd64-linux-gnu bin/workload
  mv -f bin.docker_amd64/workload "${wl}"
  make bin/roach{prod,test}
  mv -f bin/roachprod "${rp}"
  mv -f bin/roachtest "${rt}"
fi

# Run roachtest. Use a random port so that multiple
# tests can be stressed from the same workstation.
"${rt}" run "${TEST}" \
  --port "$((8080+$RANDOM % 1000))" \
  --roachprod "${rp}" \
  --workload "${wl}" \
  --cockroach "${cr}" \
  --artifacts "${a}" \
  --count "${COUNT}"
