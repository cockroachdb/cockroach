#!/usr/bin/env bash

set -exuo pipefail

# Clean up anything from a prior run:
kubectl delete statefulsets,pods,persistentvolumes,persistentvolumeclaims,services,poddisruptionbudget -l app=cockroachdb

# Make persistent volumes and (correctly named) claims. We must create the
# claims here manually even though that sounds counter-intuitive. For details
# see https://github.com/kubernetes/contrib/pull/1295#issuecomment-230180894.
# Note that we make an extra volume here so you can manually test scale-up.
for i in $(seq 0 3); do
  cat <<EOF | kubectl create -f -
kind: PersistentVolume
apiVersion: v1
metadata:
  name: pv${i}
  labels:
    type: local
    app: cockroachdb
spec:
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: "/tmp/${i}"
EOF

  cat <<EOF | kubectl create -f -
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: datadir-cockroachdb-${i}
  labels:
    app: cockroachdb
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
EOF
done;

kubectl create -f cockroachdb-statefulset.yaml
