#!/bin/sh
set -eux

if [ $# -eq 0 ] || [ "$1" == "" ]; then
  echo "Usage: ./bench.sh [output-name]"
  exit 1
fi

echo "output file: $1.txt"

for i in {1..10}
do
  go test -bench=. >> $1.txt.tmp
done

sed -n '/^Benchmark/p' $1.txt.tmp > $1.txt
rm $1.txt.tmp
