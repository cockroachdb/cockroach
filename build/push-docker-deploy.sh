#!/bin/bash

set -eux

$(dirname $0)/build-docker-deploy.sh

cd $(dirname $0)/../acceptance
if [ -f ./acceptance.test ]; then
    time ./acceptance.test -i cockroachdb/cockroach -b /cockroach/cockroach \
	 -test.v -test.timeout -5m
fi

docker tag cockroachdb/cockroach:latest cockroachdb/cockroach:${VERSION}

for version in {latest, ${VERSION}}; do
  # Pushing to the registry just fails sometimes, so for the time
  # being just make this a best-effort action.
  docker push cockroachdb/cockroach:${version} || true
done
