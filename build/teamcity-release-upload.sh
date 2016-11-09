#!/usr/bin/env bash
set -euxo pipefail

sed "s/<EMAIL>/$DOCKER_EMAIL/;s/<AUTH>/$DOCKER_AUTH/" < "resource/deploy_templates/.dockercfg.template" > ~/.dockercfg

case "$TC_BUILD_BRANCH" in
  master)
    VERSION=$(git describe || git rev-parse --short HEAD)
    push=build/push-aws.sh
    ;;

  beta-*)
    VERSION="$TC_BUILD_BRANCH"
    push=build/push-tagged-aws.sh
    ;;

  *)
    exit 1
    ;;
esac

export VERSION
echo "Deploying $VERSION..."
build/builder.sh build/build-static-binaries.sh static-tests.tar.gz
build/push-docker-deploy.sh
mv build/deploy/cockroach cockroach
aws configure set region us-east-1
build/build-osx.sh
eval $push
