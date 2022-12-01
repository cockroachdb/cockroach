#!/usr/bin/env bash
#
# Generate all etcd protobuf bindings.
# Run from repository root directory named etcd.
#
set -e
shopt -s globstar

if ! [[ "$0" =~ scripts/genproto.sh ]]; then
  echo "must be run from repository root"
  exit 255
fi

source ./scripts/test_lib.sh

if [[ $(protoc --version | cut -f2 -d' ') != "3.14.0" ]]; then
  echo "could not find protoc 3.14.0, is it installed + in PATH?"
  exit 255
fi

GOFAST_BIN=$(tool_get_bin github.com/gogo/protobuf/protoc-gen-gofast)
GOGOPROTO_ROOT="$(tool_pkg_dir github.com/gogo/protobuf/proto)/.."

echo
echo "Resolved binary and packages versions:"
echo "  - protoc-gen-gofast:       ${GOFAST_BIN}"
echo "  - gogoproto-root:          ${GOGOPROTO_ROOT}"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

# directories containing protos to be built
DIRS="./raftpb"

log_callout -e "\\nRunning gofast (gogo) proto generation..."

for dir in ${DIRS}; do
  pushd "${dir}"
    protoc --gofast_out=. -I=".:${GOGOPROTO_PATH}:${RAFT_ROOT_DIR}/..:${RAFT_ROOT_DIR}" \
      --plugin="${GOFAST_BIN}" ./**/*.proto

    sed -i.bak -E 's|"raft/raftpb"|"go.etcd.io/etcd/raft/v3/raftpb"|g' ./**/*.pb.go
    sed -i.bak -E 's|"google/protobuf"|"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"|g' ./**/*.pb.go

    rm -f ./**/*.bak
    gofmt -s -w ./**/*.pb.go
    run_go_tool "golang.org/x/tools/cmd/goimports" -w ./**/*.pb.go
  popd
done

log_success -e "\\n./genproto SUCCESS"
