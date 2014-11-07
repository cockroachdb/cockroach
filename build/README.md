# Docker Deploy

Installing docker is a prerequisite. The instructions differ depending
on the environment. Docker is comprised of two parts: the daemon
server which runs on Linux and accepts commands, and the client which
is a Go program capable of running on MacOS, all Unix variants and
Windows.

## Docker Installation

Follow the [Docker install instructions](https://docs.docker.com/installation/).

If deploying to a modern Linux-based environment, Docker is installed
locally and provides Cockroach containerization directly on top of the
host OS.

If deploying to a MacOS or Windows-based environment, the docker
installation includes a virtual machine (VirtualBox) which runs a
minimal Tiny Core Linux OS with Docker support, to provide Cockroach
containerization on top of the Linux virtual machine.

## Available images

There are development and deploy images available.

### Development
The development image is bulky, dynamically linked and contains a complete build toolchain.
It is well suited to hacking around and running the tests (including
acceptance tests and such).
To build this image, run `./build-docker-dev.sh`.

### Deployment
The deploy image is a downsized image containing a minimal environment
for running Cockroach. It is statically linked and should be considered
highly experimental at this point in time.
The image is based on busybox and contains only the main Cockroach binary
as well as the resources requiring for starting the server (certs, etc.).
To build the image yourself, use `./build-docker-deploy.sh`. The script
will build and run a development container. Inside of that container,
the statically linked binary will be built along with the (statically linked)
individual tests. These created files will be streamed out of the image
and saved in ./deploy/.out.
The deployment image is then build using ./deploy/.out/cockroach.
After the build is complete, the script will fire up the container, supplying
to it the statically linked test files in ./deploy/.out; running them
one by one and propagating failure.
