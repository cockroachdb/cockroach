#!/usr/bin/env bash
set -euxo pipefail

build_dir="$(dirname $0)"

sed "s/<EMAIL>/$DOCKER_EMAIL/;s/<AUTH>/$DOCKER_AUTH/" < "${build_dir}"/.dockercfg.in > ~/.dockercfg

case "$TC_BUILD_BRANCH" in
  master)
    VERSION=$(git describe || git rev-parse --short HEAD)
    push="${build_dir}"/push-aws.sh
    ;;

  beta-*)
    VERSION="$TC_BUILD_BRANCH"
    push="${build_dir}"/push-tagged-aws.sh
    ;;

  *)
    exit 1
    ;;
esac

export VERSION
echo "Deploying $VERSION..."
"${build_dir}"/builder.sh "${build_dir}"/build-static-binaries.sh static-tests.tar.gz
"${build_dir}"/push-docker-deploy.sh
mv "${build_dir}"/deploy/cockroach cockroach
aws configure set region us-east-1
"${build_dir}"/build-osx.sh
eval $push
