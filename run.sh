#!/bin/bash
set -euo pipefail

make bench PKG=./pkg/sql/tests BENCHES=-
#make bench PKG=./pkg/sql/tests BENCHES="BenchmarkKV/Scan/SQL/rows=1/sample_rate=${1}\.00" TESTFLAGS='-benchtime=5000x' | sed 's/sample_rate=[01]\.00/sample_rate=X/' | tee out.${1}
rm tests.test
go test -c ./pkg/sql/tests
for i in $(seq 0 20); do
	env GOGC=off ./tests.test -test.run - -test.bench "BenchmarkKV/Scan/SQL/rows=1/sample_rate=${1}\.00" -test.benchtime=5000x | \
		sed 's/sample_rate=[01]\.00/sample_rate=X/' | tee -a out.${1}
done

