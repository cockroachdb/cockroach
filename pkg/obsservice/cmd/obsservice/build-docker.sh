#!/bin/bash

## This script is in support of a Dockerfile that we hacked together
## in the interest of prototyping speed. The obsservice binary has
## cgo dependencies, meaning we need to build within Docker when
## cross compiling for linux. We decided to just use the `dev` build
## system to cross compile into the $CRDB_ROOT/artifacts directory, and
## then use that file as the basis for our Docker image. This might not
## be the best approach, but it works for now.

echo "Begin cross compiling obsservice binary..."

echo "Running dev in $PWD to cross compile obsservice binary..."
if [[ $(./dev build obsservice --cross=linux) ]]; then
  echo "obsservice binary successfully cross compiled"
else
  echo "obsservice binary failed to cross compile. Exiting."
  exit 1
fi

echo "Copying obsservice binary to pkg/obsservice/cmd/obsservice to make available to Dockerfile"
echo "NOTE: This is a quick hack solution made for prototyping. It should not be used beyond that."

cp artifacts/obsservice pkg/obsservice/cmd/obsservice/artifact_obsservice

echo "Building obsservice docker image."

cd pkg/obsservice/cmd/obsservice

echo "(Now running in $PWD)"

docker build -t obsservice .

echo "Finalizing... Removing copied obsservice artifact. Check $ docker image ls to see image."
rm artifact_obsservice


