#!/bin/sh
# wget https://github.com/google/flatbuffers/releases/download/v22.10.26/Mac.flatc.binary.zip

for f in transport.fbs entry.fbs; do
  ./flatc --warnings-as-errors --go --grpc --filename-suffix "_generated.go" -o ./pkg/kv/kvserver "./pkg/kv/kvserver/raftfbs/${f}"
done
