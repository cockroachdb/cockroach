#!/bin/bash

set -euo pipefail
ua enable fips --assume-yes
apt-get install -y dpkg-repack
