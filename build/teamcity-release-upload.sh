#!/usr/bin/env bash
set -euxo pipefail

#sed "s/<EMAIL>/$DOCKER_EMAIL/;s/<AUTH>/$DOCKER_AUTH/" < build/.dockercfg.in > ~/.dockercfg

case "beta-19940829" in
  master)
    VERSION=$(git describe || git rev-parse --short HEAD)
    push=build/push-aws.sh
    ;;

  beta-*)
    VERSION="beta-19940829"
    push=build/push-tagged-aws.sh
    ;;

  *)
    exit 1
    ;;
esac

export VERSION
echo "Deploying $VERSION..."
build/builder.sh build/build-static-binaries.sh static-tests.tar.gz
for archive in cockroach-latest "cockroach-$VERSION"
do
  build/builder.sh make archive ARCHIVE_BASE="$archive" ARCHIVE="$archive.src.tgz"
done
build/push-docker-deploy.sh
mv build/deploy/cockroach cockroach
aws configure set region us-east-1
build/build-osx.sh
eval $push
