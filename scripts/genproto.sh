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
GRPC_GATEWAY_BIN=$(tool_get_bin github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway)
SWAGGER_BIN=$(tool_get_bin github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger)
GOGOPROTO_ROOT="$(tool_pkg_dir github.com/gogo/protobuf/proto)/.."
GRPC_GATEWAY_ROOT="$(tool_pkg_dir github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway)/.."

echo
echo "Resolved binary and packages versions:"
echo "  - protoc-gen-gofast:       ${GOFAST_BIN}"
echo "  - protoc-gen-grpc-gateway: ${GRPC_GATEWAY_BIN}"
echo "  - swagger:                 ${SWAGGER_BIN}"
echo "  - gogoproto-root:          ${GOGOPROTO_ROOT}"
echo "  - grpc-gateway-root:       ${GRPC_GATEWAY_ROOT}"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

# directories containing protos to be built
DIRS="./raftpb"

log_callout -e "\\nRunning gofast (gogo) proto generation..."

for dir in ${DIRS}; do
  pushd "${dir}"
    protoc --gofast_out=plugins=grpc:. -I=".:${GOGOPROTO_PATH}:${RAFT_ROOT_DIR}/..:${RAFT_ROOT_DIR}:${GRPC_GATEWAY_ROOT}/third_party/googleapis" \
      --plugin="${GOFAST_BIN}" ./**/*.proto

    sed -i.bak -E 's|"raft/raftpb"|"go.etcd.io/etcd/raft/v3/raftpb"|g' ./**/*.pb.go
    sed -i.bak -E 's|"google/protobuf"|"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"|g' ./**/*.pb.go

    rm -f ./**/*.bak
    gofmt -s -w ./**/*.pb.go
    run_go_tool "golang.org/x/tools/cmd/goimports" -w ./**/*.pb.go
  popd
done

log_success -e "\\n./genproto SUCCESS"
