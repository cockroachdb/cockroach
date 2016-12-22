#!/usr/bin/env bash

set -exuo pipefail

# Clean up anything from a prior run:
kubectl delete statefulsets,pods,persistentvolumes,persistentvolumeclaims,services,poddisruptionbudget -l app=cockroachdb

# The persistent volume auto-provisioner will create the necessary persistent
# volumes for us, unlike when using Minikube.
kubectl create -f cockroachdb-statefulset.yaml
