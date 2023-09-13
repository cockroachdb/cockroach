#!/bin/bash
set -euo pipefail

# Example run: BENCHES='BenchmarkTest' PKG=pkg/microbisect ./scripts/microbench-bisect.sh 8bdfeb64b3a

THRESHOLD=50
METRIC="sec/op"
METRIC_UNIT="ns/op"

if [[ $# -lt 1 || $# -gt 2 ]]; then
  cat 1>&2 <<EOF
Usage: BENCHES=regexp PKG=./pkg/yourpkg $0 oldbranch [newbranch]
EOF
  exit 1
fi

# set the old and new branches
OLDNAME=$1
OLD=$(git rev-parse "$1")

ORIGREF=$(git symbolic-ref -q HEAD)
ORIG=${ORIGREF##refs/heads/}

if [[ $# -lt 2 ]]; then
  NEWNAME="HEAD"
  NEW=$ORIG
else
  NEWNAME=$2
  NEW=$(git rev-parse "$2")
fi

# set the temp working directory
dest=$(mktemp -d)
echo "Writing to ${dest}"

# run the benchmarks for the current revision
run_bench() {
  ./dev bench "${PKG}" \
    --timeout="${BENCHTIMEOUT:-5m}" \
    --filter="${BENCHES}" \
    --count="${N:-10}" \
    --bench-time=1s \
    --bench-mem -v --stream-output --ignore-cache | tee "${dest}/bench.$1"
}

# compare the benchmarks and check if the change is greater than the threshold
check_change() {
  compare_file="${dest}/$1_$2.txt"
  benchstat -filter ".unit:($METRIC_UNIT)" -format csv "${dest}/bench.$1" "${dest}/bench.$2" | tee "${compare_file}"
  change=$(grep "^,$METRIC,CI,$METRIC,CI,vs base,P\$" -A 1 "${compare_file}" | tail -1 | awk -F, '{print $6}')
  if [ -n "$change" ] && [ "$change" != "~" ]
  then
    # remove the trailing % sign
    change_val=${change:0:${#change}-1}
    # remove the leading + sign (so that bc doesn't complain)
    if [[ ${change_val:0:1} == "+" ]]
    then
      change_val=${change_val:1:${#change_val}}
    fi

    # compare the change to the threshold
    # if the change is greater than the threshold, mark this as a good commit
    # otherwise, mark this as a bad commit
    if (( $(echo "$change_val > $THRESHOLD" | bc -l) ))
    then
      echo "Change in $METRIC is greater than threshold ($THRESHOLD): $change_val"
      echo "Marking this as a good commit"
      git bisect good
    else
      echo "Change in $METRIC is less than threshold ($THRESHOLD): $change_val"
      echo "Marking this as a bad commit"
      git bisect bad
    fi
  else
    echo "No change in $METRIC detected"
    echo "Marking this as a bad commit"
    git bisect bad
  fi
}

echo "Comparing $NEWNAME (new) with $OLDNAME (old)"

run_bench "bad"

git bisect start
git bisect bad
git bisect good "$OLD"

while true
do
  current_hash=$(git rev-parse HEAD)
  run_bench "$current_hash"
  check_change "$current_hash" "bad"

  # Check if the bisect process is complete
  if git bisect log | grep -q 'first bad commit'; then
      echo "Bisect is complete."
      exit 0
  else
      echo "Bisect is not complete yet."
  fi

  echo "Continue? Yes = 1, No = 2"
  read -r user_input
  if [[ $user_input -eq 2 ]]; then
      break
  fi

done
