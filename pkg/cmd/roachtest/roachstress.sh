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

if [ -z "${BASH_VERSINFO}" ] || [ -z "${BASH_VERSINFO[0]}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  echo "This script requires Bash version >= 4"
  echo "On OSX, 'brew install bash' should do the trick."
  exit 1
fi

function help {
  cat <<EOF
Stress run roachtest
Usage:
  $(basename ${0}) [flags] [-- roachtest flags]

flags:
  -t - test to run (overrides TEST)
  -c - number of tests to run (overrides COUNT)
  -l - run test locally on host (overrides LOCAL)
  -s - y/n buildshort instead of build (exclude web ui)
  -b - cr | test | all - remove artifacts to force rebuild
       useful when running with local change like logging etc

If flag neither flag nor environment variable is set, script will prompt
for value.
EOF
}

# Process command line flags
rebuild=
while getopts ":t:c:ls:b:h-" o ; do
  case "$o" in
    t)
     TEST="${OPTARG}"
     ;;
    c)
     COUNT="${OPTARG}"
     ;;
    l)
     LOCAL=y
     ;;
    s)
     SHORT="${OPTARG}"
     ;;
    b)
     # Can use either of: cr | test | all
     rebuild="${OPTARG}"
     ;;
    h)
     help
     exit 0
     ;;
    -)
     break
     ;;
    :)
     echo -e "Option -${OPTARG} requires argument\n"
     help
     exit 1
     ;;
    *)
     echo -e "Unknown option -${OPTARG}\n"
     help
     exit 1
     ;;
  esac
done
shift $((OPTIND - 1))

# Read user input.
if [ ! -v TEST ]; then read -r -e -p "Test regexp: " TEST; fi
if [ ! -v COUNT ]; then read -r -e -i "10" -p "Count: " COUNT; fi
if [ ! -v LOCAL ]; then read -r -e -i "n" -p "Local: " LOCAL; fi
case $LOCAL in
  [Nn]* | false | "") LOCAL="";;
  *) LOCAL=".local";;
esac

if [ -z "${LOCAL}" ] && [ "${GCE_PROJECT-cockroach-ephemeral}" == "cockroach-ephemeral" ]; then
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
wl="${abase}/workload${LOCAL}"
cr="${abase}/cockroach${LOCAL}"

# Check if we need to force the build by removing artifacts
case "${rebuild}" in
  cr)
    rm -f "${cr}"
    ;;
  test)
    rm -f "${rt}" "${rp}" "${wl}"
    ;;
  all)
    rm -f "${cr}" "${rt}" "${rp}" "${wl}"
    ;;
esac

# This is the artifacts dir we'll pass to the roachtest invocation. It's
# disambiguated by a timestamp because one often ends up invoking roachtest on
# the same SHA multiple times and artifacts shouldn't mix.
a="${abase}/$(date '+%H%M%S')"

short="short"
if [ ! -f "${cr}" ]; then
  yn="${SHORT-}"
  if [ -z "${yn}" ]; then read -r -e -i "y" -p "Build cockroach without the UI: " yn; fi
  case $yn in
    [Nn]* | false | "") short=""
  esac
fi

mkdir -p "${a}"

if [ ! -f "${cr}" ]; then
  if [ -z "${LOCAL}" ]; then
    ./build/builder.sh mkrelease amd64-linux-gnu "build${short}"
    cp "cockroach${short}-linux-2.6.32-gnu-amd64" "${cr}"
  else
    make "build${short}"
    cp "cockroach${short}" "${cr}"
  fi
fi

if [ ! -f "${wl}" ]; then
  if [ -z "${LOCAL}" ]; then
    ./build/builder.sh mkrelease amd64-linux-gnu bin/workload
    cp bin.docker_amd64/workload "${wl}"
  else
    make bin/workload
    cp bin/workload "${wl}"
  fi
  make bin/roach{prod,test}
  cp bin/roachprod "${rp}"
  cp bin/roachtest "${rt}"
fi

args=(
  "run" "${TEST}"
  "--port" "$((8080+RANDOM % 1000))"
  "--roachprod" "${rp}"
  "--workload" "${wl}"
  "--cockroach" "${cr}"
  "--artifacts" "${a}"
  "--count" "${COUNT}"
)
if [ -n "${LOCAL}" ]; then
  args+=("--local")
fi
args+=("$@")

# Run roachtest. Use a random port so that multiple
# tests can be stressed from the same workstation.
"${rt}" "${args[@]}"
