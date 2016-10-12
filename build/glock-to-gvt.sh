#!/bin/bash

set -ex

rm -rf vendor/*/* vendor/manifest vendor/README.md

while read -r -a i; do
  case "${i[0]}" in
  cmd)
    ;;
  github.com/cockroachdb/c-protobuf)
    gvt fetch -no-recurse --revision "${i[1]}" "${i[0]}"
    ;;
  github.com/coreos/etcd|github.com/gogo/protobuf|github.com/grpc-ecosystem/grpc-gateway)
    gvt fetch -a -no-recurse --revision "${i[1]}" "${i[0]}"
    ;;
  *)
    gvt fetch -no-recurse  --revision "${i[1]}" "${i[0]}"
    ;;
  esac
done <GLOCKFILE

perl -pi -e 's/HEAD/master/g' vendor/manifest
cp build/vendor-repo.md vendor/README.md
