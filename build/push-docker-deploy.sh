#!/usr/bin/env sh

set -eux

export COCKROACH_IMAGE=cockroachdb/cockroach

./build/build-docker-deploy.sh
run/local-cluster.sh stop
run/local-cluster.sh start
run/local-cluster.sh stop

docker tag cockroachdb/cockroach:latest cockroachdb/cockroach:${VERSION}
docker tag cockroachdb/cockroach-dev:latest cockroachdb/cockroach-dev:${VERSION}

# Pushing to the registry just fails sometimes, so for the time
# being just make this a best-effort action.
set +e

docker push cockroachdb/cockroach:latest
docker push cockroachdb/cockroach:${VERSION}
docker push cockroachdb/cockroach-dev:latest
docker push cockroachdb/cockroach-dev:${VERSION}
