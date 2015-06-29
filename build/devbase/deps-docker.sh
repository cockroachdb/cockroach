#!/bin/sh
# Grab dependences in the initial phases of the docker build.
#
# To minimize the number of changes that require a full rebuild,
# we install our dependencies in a base image that does not include
# the full cockroach repo, so we must copy GLOCKFILE, package.json and
# npm-shrinkwrap by hand.

set -eu

cd /go/src/github.com/cockroachdb/cockroach
cp build/devbase/GLOCKFILE .
mkdir ui
cp build/devbase/ui/package.json ui
cp build/devbase/ui/npm-shrinkwrap.json ui
build/devbase/deps.sh
rm GLOCKFILE
