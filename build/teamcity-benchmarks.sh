#!/usr/bin/env bash
set -exuo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

mkdir -p artifacts

build/builder.sh env \
	make bench \
	TESTFLAGS='-benchmem -count 10' \
	BENCHTIMEOUT=24h \
	2>    artifacts/bench.stderr.log \
	| tee artifacts/bench.stdout.log \
	| go-test-teamcity

build/builder.sh bin/benchstat artifacts/bench.stdout.log > artifacts/bench.log
