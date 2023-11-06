#!/bin/bash

## This script is in support of a bazel rule that we hacked together
## in the interest of prototyping speed. The obsservice binary has
## cgo dependencies, meaning we need to build within Docker when
## cross compiling for linux. Instead of putting in all the work to
## work cross compiling in Docker into the Bazel system, in the interest
## of moving quickly, we instead decided to just use the `dev` build
## system to cross compile into the $CRDB_ROOT/artifacts directory, and
## then use that file as the basis for the rest of our Bazel build targets.
## TODO(observability): Remove this script and the .gitignore in this dir
## once we have a better Bazel rule that can handle cross compiling.

echo "Begin cross compiling obsservice binary..."

echo "Running dev in $PWD to cross compile obsservice binary..."
if [[ $(./dev build obsservice --cross=linux) ]]; then
  echo "obsservice binary successfully cross compiled"
else
  echo "obsservice binary failed to cross compile. Exiting."
  exit 1
fi

echo "Copying obsservice binary to pkg/obsservice/cmd/obsservice to make available to bazel target"
echo "NOTE: This is a quick hack solution made for prototyping. It should not be used beyond that."

cp artifacts/obsservice pkg/obsservice/cmd/obsservice/artifact_obsservice

echo "Building obsservice docker image."

if [[ $(bazel run pkg/obsservice/cmd/obsservice:tar_image) ]]; then
  echo "obsservice docker image built successfully. Run docker image ls to verify"
else
  echo "obsservice docker image failed to build"
  rm pkg/obsservice/cmd/obsservice/artifact_obsservice
  exit 1
fi

echo "Finalizing... Removing copied obsservice artifact"
rm pkg/obsservice/cmd/obsservice/artifact_obsservice


