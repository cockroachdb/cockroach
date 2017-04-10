#!/usr/bin/env bash

set -euxo pipefail

"$(dirname "${0}")"/build-docker-deploy.sh

cd "$(dirname "${0}")"/../pkg/acceptance
time ./acceptance.test -i cockroachdb/cockroach -b /cockroach/cockroach -nodes 3 -test.v -test.timeout -5m

docker tag cockroachdb/cockroach cockroachdb/cockroach:"${VERSION}"

# Pushing to the registry just fails sometimes, so for the time
# being just make this a best-effort action. Push with an explicit
# version to avoid pushing other tags or overwriting ":latest".
docker push cockroachdb/cockroach:"${VERSION}" || true

# Only update the ":latest" tag for releases that we consider stable.
if [ "${STABLE_RELEASE}" = true ] ; then
  # We don't allow such failures on this push, because we care more about
  # the latest tag being updated properly.
  docker tag cockroachdb/cockroach cockroachdb/cockroach:latest
  docker push cockroachdb/cockroach:latest
fi
