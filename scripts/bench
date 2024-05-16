#!/bin/bash
set -euo pipefail

if ! which benchstat > /dev/null; then
  cat 1>&2 <<EOF
Requires golang.org/x/perf/cmd/benchstat
Run:
  go install golang.org/x/perf/cmd/benchstat@latest
EOF
  exit 1
fi

cd "$(git rev-parse --show-toplevel)"

if [[ $# < 1 || $# > 2 ]]; then
  cat 1>&2 <<EOF
Usage: BENCHES=regexp PKG=./pkg/yourpkg $0 oldbranch [newbranch]
EOF
  exit 1
fi

OLDNAME=$1
OLD=$(git rev-parse "$1")

ORIGREF=$(git symbolic-ref -q HEAD)
ORIG=${ORIGREF##refs/heads/}

if [[ $# < 2 ]]; then
  NEWNAME="HEAD"
  NEW=$ORIG
else
  NEWNAME=$2
  NEW=$(git rev-parse "$2")
fi

echo "Comparing $NEWNAME (new) with $OLDNAME (old)"
echo ""

dest=$(mktemp -d)
echo "Writing to ${dest}"

shas=($OLD $NEW)
names=($OLDNAME $NEWNAME)

benchtime=${T:-}
if [[ benchtime ]]; then
   benchtime="--bench-time=$benchtime"
fi

for (( i=0; i<${#shas[@]}; i+=1 )); do
  name=${names[i]}
  sha=${shas[i]}
  echo "Switching to $name"
  git checkout -q "$sha"
  (set -x; ./dev bench ${PKG} --timeout=${BENCHTIMEOUT:-5m} --filter=${BENCHES} --count=${N:-10} $benchtime --bench-mem -v --stream-output --ignore-cache --test-args='-test.cpu 8' | tee "${dest}/bench.${name}" 2> "${dest}/log.txt")
done

benchstat "${dest}/bench.$OLDNAME" "${dest}/bench.$NEWNAME"

git checkout "$ORIG"
