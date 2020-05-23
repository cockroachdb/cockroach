# Docker Deploy

Installing docker is a prerequisite. The instructions differ depending on the
environment. Docker is comprised of two parts: the daemon server which runs on
Linux and accepts commands, and the client which is a Go program capable of
running on MacOS, all Unix variants and Windows.

## Docker Installation

Follow the [Docker install
instructions](https://docs.docker.com/engine/installation/).

## Available images

There are development and deploy images available.

### Development

The development image is a bulky image containing a complete build toolchain.
It is well suited to hacking around and running the tests (including the
acceptance tests). To fetch this image, run `./builder.sh pull`. The image can
be run conveniently via `./builder.sh`.

Note that if you use the builder image, you should ensure that your
Docker installation grants 4GB or more of RAM to containers. On some
systems, the default configuration limits containers to 2GB memory
usage and this can be insufficient to build/link a CockroachDB
executable.

### Deployment

The deploy image is a downsized image containing a minimal environment for
running CockroachDB. It is based on Debian Jessie and contains only the main
CockroachDB binary. To fetch this image, run `docker pull
cockroachdb/cockroach` in the usual fashion.

To build the image yourself, use the Dockerfile in the `deploy` directory after
building a release version of the binary with the development image described in
the previous section. The CockroachDB binary will be built inside of that
development container, then placed into the minimal deployment container. The
resulting image `cockroachdb/cockroach` can be run via `docker run` in the
usual fashion. To be more specific, the steps to do this are:

```
go/src/github.com/cockroachdb/cockroach $ ./build/builder.sh mkrelease linux-gnu
go/src/github.com/cockroachdb/cockroach $ cp ./cockroach-linux-2.6.32-gnu-amd64 build/deploy/cockroach
go/src/github.com/cockroachdb/cockroach $ cd build/deploy && docker build -t cockroachdb/cockroach .
```

The list of valid/recognized targets is available in the script
`build/build/mkrelease.sh`, for example `amd64-linux-gnu` and
`amd64-darwin`. Note that this script supports experimental targets
which may or may not work (and are not officially supported).

# Upgrading / extending the Docker image

## Basic Process

- Edit `build/Dockerfile` as desired
- Run `build/builder.sh init` to test -- this will build the image locally. Beware this can take a lot of time. The result of `init` is a docker image version which you can subsequently stick into the `version` variable inside the `builder.sh` script for testing locally.
- Once you are happy with the result, run `build/builder.sh push` which pushes your image towards Docker hub, so that it becomes available to others. The result is again a version number, which you then *must* copy back into `builder.sh`. Then commit the change to both Dockerfile and `builder.sh` and submit a PR.
- Finally, use this version number to update the `builder.dockerImage` configuration parameter in TeamCity under the [`Cockroach`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Cockroach&tab=projectParams) and [`Internal`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Internal&tab=projectParams) projects.

## Updating the golang version

Please copy this checklist (based on [Basic Process](#basic-process)) into the relevant commit message, with a link
back to this document and perform these steps:

* [ ] Adjust version in Docker image ([source](./builder/Dockerfile#L199-L200)).
* [ ] Rebuild the Docker image and bump the `version` in `builder.sh` accordingly ([source](./builder.sh#L6)).
* [ ] Bump the version in `go-version-check.sh` ([source](./go-version-check.sh)), unless bumping to a new patch release.
* [ ] Bump the default installed version of Go in `bootstrap-debian.sh` ([source](./bootstrap/bootstrap-debian.sh#L40-42)).
* [ ] Update the `builder.dockerImage` parameter in the TeamCity [`Cockroach`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Cockroach&tab=projectParams) and [`Internal`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Internal&tab=projectParams) projects.

You can test the new builder image in TeamCity by using the custom parameters
UI (the "..." icon next to the "Run" button) to verify the image before
committing the change.

## Updating the nodejs version

Please follow the instructions above on updating the golang version, omitting the go-version-check.sh step.

#  Dependencies

Dependencies are managed using `go mod`. We use `go mod vendor` so that we can import and use non-Go files (e.g. protobuf files).

## Installing or updating a dependency

Run `go get -u <dependency>`. To get a specific version, run `go get -u <dependency>@<version>`.

You must then run `make vendor/modules.txt` to ensure the modules are installed.

Programs can then be run using `go build -mod=vendor ...` or `go test -mod=vendor ...`.

## Requiring a new tool

When installing a tool, you may need to add blank import to `pkg/cmd/import-tools/main.go` so that `go mod tidy` does not clean it up.
