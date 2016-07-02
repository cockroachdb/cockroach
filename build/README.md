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

### Deployment

The deploy image is a downsized image containing a minimal environment for
running CockroachDB. It is based on Debian Jessie and contains only the main
CockroachDB binary. To fetch this image, run `docker pull
cockroachdb/cockroach` in the usual fashion.

To build the image yourself, use `./build-docker-deploy.sh`. The script will
build and run a development container. The CockroachDB binary will be built
inside of that container. That binary is built into our minimal container. The
resulting image `cockroachdb/cockroach` can be run via `docker run` in the
usual fashion.
