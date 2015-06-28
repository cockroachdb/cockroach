#!/bin/sh
# Grab Go-related dependences in the initial phases of the docker build.
#
# To minimize the number of changes that require a full rebuild,
# we install our dependencies in a base image that does not include
# the full cockroach repo, so we must copy GLOCKFILE by hand.

set -eu

cd /go/src/github.com/cockroachdb/cockroach
cp build/devbase/GLOCKFILE .
build/devbase/godeps.sh
rm GLOCKFILE
