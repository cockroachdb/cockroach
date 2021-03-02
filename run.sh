#!/bin/bash


time caffeinate make bench PKG=./pkg/sql/tests BENCHES="BenchmarkKV/Scan/SQL/rows=1/sample_rate=${1}\.00" TESTFLAGS='-benchtime=3000x -count 20' | sed 's/sample_rate=[01]\.00/sample_rate=X/' | tee out.${1}


