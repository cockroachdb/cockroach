#!/usr/bin/env sh

set -eux

export COCKROACH_IMAGE=cockroachdb/cockroach

./build/build-docker-deploy.sh
run/local-cluster.sh stop
run/local-cluster.sh start
run/local-cluster.sh stop

docker tag cockroachdb/cockroach:latest cockroachdb/cockroach:${VERSION}
docker tag cockroachdb/cockroach-dev:latest cockroachdb/cockroach-dev:${VERSION}

for type in {,-dev}; do
  for version in {latest, ${VERSION}}; do
    # Pushing to the registry just fails sometimes, so for the time
    # being just make this a best-effort action.
    docker push cockroachdb/cockroach${type}:${version} || true
  done
done
