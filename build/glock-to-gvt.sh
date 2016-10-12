#!/bin/bash

set -euo pipefail

# This is for migrating to using /vendor for dependencies, and will be removed
# when the transition is complete.

rm -rf vendor/*/*
rm build/gvt-manifest.json

if [ -f GLOCKFILE.tools ]; then
  rm  GLOCKFILE.tools
fi

while read -r -a i; do
  case "${i[0]}" in
  cmd)
    if [[ "${i[1]}" == "github.com/cockroachdb/cockroach/pkg/cmd/protoc-gen-gogoroach" ]]; then
      echo "cmd ./pkg/cmd/protoc-gen-gogoroach" >> GLOCKFILE.tools
    else
        echo "cmd ./vendor/${i[1]}" >> GLOCKFILE.tools
    fi
    ;;
  github.com/coreos/etcd|github.com/gogo/protobuf|github.com/grpc-ecosystem/grpc-gateway)
    gvt fetch -a -no-recurse --revision "${i[1]}" "${i[0]}"
    ;;
  *)
    gvt fetch -no-recurse  --revision "${i[1]}" "${i[0]}"
    ;;
  esac
done <GLOCKFILE

mv GLOCKFILE.tools GLOCKFILE

perl -pi -e 's/HEAD/master/g' vendor/manifest
cp build/vendor-repo.md vendor/README.md
