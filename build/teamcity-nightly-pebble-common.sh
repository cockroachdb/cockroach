#!/usr/bin/env bash
#
# This script contains common configuration used by the Pebble Nightly runs.

set -eo pipefail

if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

set -ux

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -N "" -f ~/.ssh/id_rsa
fi

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"

# Disable global -json flag.
PATH=$PATH:$(GOFLAGS=; go env GOPATH)/bin
export PATH

build_tag=$(git describe --abbrev=0 --tags --match=v[0-9]*)
export build_tag

# Build the roachtest binary.
make bin/roachtest

# Pull in the latest version of Pebble from upstream. The benchmarks run
# against the tip of the 'master' branch.
rm -fr vendor/github.com/cockroachdb/pebble
git clone https://github.com/cockroachdb/pebble vendor/github.com/cockroachdb/pebble
pushd vendor/github.com/cockroachdb/pebble
GOOS=linux go build -v -mod=vendor -o pebble.linux ./cmd/pebble
popd
mv vendor/github.com/cockroachdb/pebble/pebble.linux .

# Set the location of the pebble binary. This is referenced by the roachtests,
# which will push this binary out to all workers in order to run the
# benchmarks.
export PEBBLE_BIN=pebble.linux

# Unzips log files from all artifacts re-zips them in GZIP format. A new "data"
# directory is created and symlinked to the artifact directory.
function prepare_datadir() {
  # Each roachtest's artifacts are zip'd. Unzip them all and remove the .zips.
  find "$artifacts" -name '*.zip' -execdir unzip {} \;
  find "$artifacts" -name '*.zip' -execdir rm {} \;
  
  # mkbench expects artifacts to be gzip compressed.
  find "$artifacts" -name '*.log' | xargs gzip -9
  
  # mkbench expects the benchmark data to be stored in data/YYYYMMDD.
  mkdir data
  ln -sf "$PWD/artifacts" "data/$(date +"%Y%m%d")"
}

# Build the mkbench tool from within the Pebble repo. This is used to parse
# the benchmark data.
function build_mkbench() {
  go build -o mkbench github.com/cockroachdb/pebble/internal/mkbench
}

# Sync all other data within the ./data/ directory. The runner logs aren't of
# any use to us, so we exclude them.
function sync_data_dir() {
  aws s3 sync --exclude "*/_runner-logs/*"  data/ s3://pebble-benchmarks/data/
}
