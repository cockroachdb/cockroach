### Instructions for using the Kubernetes Self-Signed Certificate to deploy CockroachDB

#### Prerequisites

**REQUIRED**: the kubernetes cluster must run with the certificate controller enabled.
This is done by passing the `--cluster-signing-cert-file` and `--cluster-signing-key-file` flags.
If you are using minikube v0.23.0 or newer (run `minikube version` if you aren't sure), you can
tell it to use the minikube-generated CA by specifying:
```shell
minikube start --extra-config=controller-manager.ClusterSigningCertFile="/var/lib/localkube/certs/ca.crt" --extra-config=controller-manager.ClusterSigningKeyFile="/var/lib/localkube/certs/ca.key"
```

If you're running on an older version of minikube, you can similarly run:
```shell
minikube start --extra-config=controller-manager.ClusterSigningCertFile="/var/lib/localkube/ca.crt" --extra-config=controller-manager.ClusterSigningKeyFile="/var/lib/localkube/ca.key"
```

#### Creating the cluster

Run: `kubectl create -f cockroachdb-statefulset-secure.yaml`

If you get an error saying "attempt to grant extra privileges", you don't have
sufficient permissions in the cluster to manage certificates. If this happens
and you're running on Google Kubernetes Engine, see [the note above](#on-gce).
If not, talk to your cluster administrator.

Each new node will request a certificate from the kubernetes CA during its initialization phase.
We have configured the StatefulSet to bring up all its pods at once, so you can approve all of
the pods' certificates in quick succession.

If a pod is rescheduled, it will reuse the previously-generated certificate.

You can view pending certificates and approve them using:
```
# List CSRs:
$ kubectl get csr
NAME                         AGE       REQUESTOR                               CONDITION
default.node.cockroachdb-0   4s        system:serviceaccount:default:default   Pending
default.node.cockroachdb-1   4s        system:serviceaccount:default:default   Pending
default.node.cockroachdb-2   4s        system:serviceaccount:default:default   Pending

# Examine the CSR:
$ kubectl describe csr default.node.cockroachdb-0
Name:                   default.node.cockroachdb-0
Labels:                 <none>
Annotations:            <none>
CreationTimestamp:      Thu, 22 Jun 2017 09:56:49 -0400
Requesting User:        system:serviceaccount:default:default
Status:                 Pending
Subject:
        Common Name:    node
        Serial Number:
        Organization:   Cockroach
Subject Alternative Names:
        DNS Names:      localhost
                        cockroachdb-0.cockroachdb.default.svc.cluster.local
                        cockroachdb-public
        IP Addresses:   127.0.0.1
                        172.17.0.5
Events: <none>

# If everything checks out, approve the CSR:
$ kubectl certificate approve default.node.cockroachdb-0
certificatesigningrequest "default.node.cockroachdb-0" approved

# Otherwise, deny the CSR:
$ kubectl certificate deny default.node.cockroachdb-0
certificatesigningrequest "default.node.cockroachdb-0" denied
```

Once all the pods have started, to initialize the cluster run:
```shell
kubectl create -f cluster-init-secure.yaml
```

This will create a CSR called "default.client.root", which you can approve by
running:
```shell
kubectl certificate approve default.client.root
```

To confirm that it's done, run:
```shell
kubectl get job cluster-init-secure
```

The output should look like:
```
NAME                  DESIRED   SUCCESSFUL   AGE
cluster-init-secure   1         1            5m
```
