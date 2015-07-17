#!/bin/bash

set -eu

image="cockroachdb/builder"

if [ "${1:-}" = "init" ]; then
    docker build --tag="${image}" - <<EOF
FROM golang:1.4.2

RUN apt-get update -y && \
 apt-get dist-upgrade -y && \
 apt-get install --no-install-recommends --auto-remove -y git build-essential file npm nodejs && \
 apt-get clean autoclean && \
 apt-get autoremove -y && \
 rm -rf /tmp/* && \
 ln -s /usr/bin/nodejs /usr/bin/node && \
 apt-get remove --auto-remove -y npm
RUN go get golang.org/x/tools/cmd/vet

CMD ["/bin/bash"]
EOF
    exit 0
fi

gopath0="${GOPATH%%:*}"
dockerVers=$(docker version | grep 'Server version:' | awk '{print $NF}')
rm=""

case "${dockerVers}" in
  0.*|1.[012345]*)
    # Removing volume containers fails on older versions of docker with
    # the error:
    #
    #   Failed to destroy btrfs snapshot: operation not permitted
    ;;
  *)
    rm="--rm"
    ;;
esac

tty=""
if test -t 0; then
  tty="--tty"
fi

# Run our build container with a set of volumes mounted that will
# allow the container to store persistent build data on the host
# computer.
docker run -i ${tty} ${rm} \
  --volume="${gopath0}/src:/go/src" \
  --volume="${PWD}:/go/src/github.com/cockroachdb/cockroach" \
  --volume="${gopath0}/pkg:/go/pkg" \
  --volume="${gopath0}/pkg/linux_amd64_netgo:/usr/src/go/pkg/linux_amd64_netgo" \
  --volume="${gopath0}/pkg/linux_amd64_race:/usr/src/go/pkg/linux_amd64_race" \
  --volume="${gopath0}/bin/linux_amd64:/go/bin" \
  --workdir="/go/src/github.com/cockroachdb/cockroach" \
  --env="CACHE=/go/pkg/cache" \
  "${image}" "$@"
