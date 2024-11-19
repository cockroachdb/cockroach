#!/usr/bin/env bash

# Copyright 2017 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# Bootstraps Local SSD devices.
#
# NOTE: This script is not run automatically.
#
# TODO: Adapt for Azure.

for d in $(ls /dev/disk/by-id/google-local-ssd-*); do
  let "disknum++"
  grep -e "${d}" /etc/fstab > /dev/null
  if [ $? -ne 0 ]; then
    echo "Disk ${disknum}: ${d} not mounted, creating..."
    mountpoint="/mnt/data${disknum}"
    sudo mkdir -p "${mountpoint}"
    sudo mkfs.ext4 -F ${d}
    opts="discard,defaults"
    sudo mount -o ${opts} ${d} ${mountpoint}
    sudo chown ${USER} ${mountpoint}
    echo "${d} ${mountpoint} ext4 ${opts} 1 1" | sudo tee -a /etc/fstab
  else
    echo "Disk ${disknum}: ${d} already mounted, skipping..."
  fi
done
