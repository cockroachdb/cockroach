# Overview

The Dockerfile in this directory defines a lightweight wrapper around the
[Kubernetes-maintained "peer-finder"
image](https://github.com/kubernetes/contrib/tree/master/pets/peer-finder),
which finds whether any other instances from the same StatefulSet currently
exist in the cluster.

The `on-start.sh` script in this directory is invoked by the peer-finder binary
with a newline separated list of the DNS results matching the provided
Kubernetes service name and namespace.

We use this to try to help the first CockroachDB instance decide whether it
should try to join an existing cluster or initialize a new one. We have to be
very careful about initializing a new one, since doing so when one alread
exists can cause some real problems.

# Pushing a new version

Assuming you're logged in to a Docker Hub account that can push to the
cockroachdb organization, [check the latest tag of the
cockroachdb/cockroach-k8s-init
container](https://hub.docker.com/r/cockroachdb/cockroach-k8s-init/tags/) so
that you know what tag number to use next, then cd to this directory and run:

```shell
NEW_TAG=0.0 # replace 0.0 with the next appropriate tag number
docker build -t "cockroachdb/cockroach-k8s-init:${NEW_TAG}" .
docker push "cockroachdb/cockroach-k8s-init:${NEW_TAG}"
```
