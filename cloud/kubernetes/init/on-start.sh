#!/bin/bash

# Simply determine if any instances exist other than this one. If there are any
# others, then assume that a cluster already exists and create a marker to
# signal that we shouldn't create a new one.
if grep -v `hostname -f`; then
  mkdir -p cockroach/cockroach-data && touch cockroach/cockroach-data/cluster_exists_marker
fi
