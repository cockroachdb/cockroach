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
