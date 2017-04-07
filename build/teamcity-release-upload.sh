#!/usr/bin/env bash
set -euxo pipefail

sed "s/<EMAIL>/$DOCKER_EMAIL/;s/<AUTH>/$DOCKER_AUTH/" < build/.dockercfg.in > ~/.dockercfg

case "$TC_BUILD_BRANCH" in
  master)
    VERSION=$(git describe || git rev-parse --short HEAD)
    STABLE_RELEASE=false
    push=build/push-aws.sh
    ;;

  beta-*)
    VERSION="$TC_BUILD_BRANCH"
    STABLE_RELEASE=true
    push=build/push-tagged-aws.sh
    ;;

  *)
    exit 1
    ;;
esac

export VERSION
export STABLE_RELEASE
echo "Deploying $VERSION..."

cat .buildinfo/tag || true
cat .buildinfo/rev || true
git status

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh build/build-static-binaries.sh static-tests.tar.gz
for archive in cockroach-latest "cockroach-${VERSION}"
do
  build/builder.sh make archive ARCHIVE_BASE="${archive}" ARCHIVE="${archive}.src.tgz"
done
build/push-docker-deploy.sh
mv build/deploy/cockroach cockroach
aws configure set region us-east-1
build/builder.sh make build TYPE=release-darwin
# TODO(tamird, #14673): make CCL compile on Windows.
build/builder.sh make buildoss TYPE=release-windows
eval $push
