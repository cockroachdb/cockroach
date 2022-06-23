#!/bin/bash
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License..

# Fail on any error
set -eo pipefail

# Display commands being run
set -x

# Only run on Go 1.17+
min_minor_ver=17

v=`go version | { read _ _ v _; echo ${v#go}; }`
comps=(${v//./ })
minor_ver=${comps[1]}

if [ "$minor_ver" -lt "$min_minor_ver" ]; then
    echo minor version $minor_ver, skipping
    exit 0
fi

export STORAGE_EMULATOR_HOST="http://localhost:9000"

DEFAULT_IMAGE_NAME='gcr.io/cloud-devrel-public-resources/storage-testbench'
DEFAULT_IMAGE_TAG='latest'
DOCKER_IMAGE=${DEFAULT_IMAGE_NAME}:${DEFAULT_IMAGE_TAG}
CONTAINER_NAME=storage_testbench

# Get the docker image for the testbench
docker pull $DOCKER_IMAGE

# Start the testbench
# Note: --net=host makes the container bind directly to the Docker host’s network, 
# with no network isolation. If we were to use port-mapping instead, reset connection errors 
# would be captured differently and cause unexpected test behaviour.
# The host networking driver works only on Linux hosts.
# See more about using host networking: https://docs.docker.com/network/host/
docker run --name $CONTAINER_NAME --rm --net=host $DOCKER_IMAGE &
echo "Running the Cloud Storage testbench: $STORAGE_EMULATOR_HOST"

# Check that the server is running - retry several times to allow for start-up time
response=$(curl -w "%{http_code}\n" $STORAGE_EMULATOR_HOST --retry-connrefused --retry 5 -o /dev/null) 

if [[ $response != 200 ]]
then
    echo "Testbench server did not start correctly"
    exit 1
fi

# Stop the testbench & cleanup environment variables
function cleanup() {
    echo "Cleanup testbench"
    docker stop $CONTAINER_NAME
    unset STORAGE_EMULATOR_HOST;
}
trap cleanup EXIT

# Run tests
go test -v -timeout 10m ./ -run="TestRetryConformance" -short 2>&1 | tee -a sponge_log.log
