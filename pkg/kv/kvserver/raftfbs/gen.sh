#!/bin/sh
pinned="22.10.26"
if [[ "$(flatc --version)" != "flatc version ${pinned}" ]]; then
    echo "Need to use flatc at ${pinned}"
    echo "OSX: wget https://github.com/google/flatbuffers/releases/download/v${pinned}/Mac.flatc.binary.zip"
    exit 1
fi

(cd $(dirname "${0}") && \
  flatc --warnings-as-errors --go --go-namespace raftfbs --grpc --gen-onefile -o . ./command.fbs)
