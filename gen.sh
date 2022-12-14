#!/bin/sh
# wget https://github.com/google/flatbuffers/releases/download/v22.10.26/Mac.flatc.binary.zip

./flatc --warnings-as-errors --go --grpc -o ./pkg/kv/kvserver ./pkg/kv/kvserver/rafttransport/raft.fbs
