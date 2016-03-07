#!/bin/bash

set -eux

$(dirname $0)/build-docker-deploy.sh

cd $(dirname $0)/../acceptance
if [ -f ./acceptance.test ]; then
  time ./acceptance.test -i cockroachdb/cockroach -b /cockroach/cockroach -nodes 3 \
    -test.v -test.timeout -5m
fi

docker tag cockroachdb/cockroach cockroachdb/cockroach:${VERSION}

# Pushing to the registry just fails sometimes, so for the time
# being just make this a best-effort action.
docker push cockroachdb/cockroach || true
