# CockroachDB on Kubernetes as a PetSet

This example deploys CockroachDB on [Kubernetes](https://kubernetes.io) as
a [PetSet](http://kubernetes.io/docs/user-guide/petset/). Kubernetes is an
open source system for managing containerized applications across multiple
hosts, providing basic mechanisms for deployment, maintenance, and scaling
of applications.

This is a copy of the [similar example stored in the Kubernetes
repository](https://github.com/kubernetes/kubernetes/tree/master/examples/cockroachdb).
We keep a copy here as well for faster iteration, since merging things into
Kubernetes can be quite slow, particularly during the code freeze before
releases. The copy here will typically be more up-to-date.

Note that if all you want to do is run a single cockroachdb instance for
testing and don't care about data persistence, you can do so with just a single
command instead of following this guide (which sets up a more reliable cluster):

```shell
kubectl run cockroachdb --image=cockroachdb/cockroach --restart=Never -- start
```

## Limitations

### PetSet limitations

PetSets are an alpha feature as of Kubernetes version 1.4. As such, they may
not be available in some production-ready hosting environments.

Also, there is currently no possibility to use node-local storage (outside of
single-node tests), and so there is likely a performance hit associated with
running CockroachDB on some external storage. Note that CockroachDB already
does replication and thus, for better performance, should not be deployed on a
persistent volume which already replicates internally. High-performance use
cases on a private Kubernetes cluster may want to consider a
[DaemonSet](http://kubernetes.io/docs/admin/daemons/) deployment until PetSets
support node-local storage
([open issue here](https://github.com/kubernetes/kubernetes/issues/7562)).

### Recovery after persistent storage failure

A persistent storage failure (e.g. losing the hard drive) is gracefully handled
by CockroachDB as long as enough replicas survive (two out of three by
default). Due to the bootstrapping in this deployment, a storage failure of the
first node is special in that the administrator must manually prepopulate the
"new" storage medium by running an instance of CockroachDB with the `--join`
parameter. If this is not done, the first node will bootstrap a new cluster,
which will lead to a lot of trouble.

### Dynamic volume provisioning

The deployment is written for a use case in which dynamic volume provisioning is
available. When that is not the case, the persistent volume claims need
to be created manually. See [minikube.sh](minikube.sh) for the necessary
steps. If you're on GCE or AWS, where dynamic provisioning is supported, no
manual work is needed to create the persistent volumes.

## Testing locally on minikube

Set up your minikube cluster following the
[instructions provided in the Kubernetes docs](http://kubernetes.io/docs/getting-started-guides/minikube/).

Then once you have a Kubernetes cluster running on minikube, follow the steps
in [minikube.sh](minikube.sh) (or simply run that file) to create your
cockroachdb cluster.

## Testing in the cloud on AWS

Set up your cluster following the
[instructions provided in the Kubernetes docs](http://kubernetes.io/docs/getting-started-guides/aws/).

Then once you have a Kubernetes cluster running, either run the
[aws.sh](aws.sh) script or just run `kubectl create -f cockroachdb-petset.yaml`
to create your cockroachdb cluster.

## Testing in the cloud on GCE

You can either set up your cluster following the
[instructions provided in the Kubernetes docs](http://kubernetes.io/docs/getting-started-guides/gce/)
or by creating a special
[Container Engine alpha cluster](https://cloud.google.com/container-engine/docs/alpha-clusters):

```shell
gcloud alpha container clusters create NAME --enable-kubernetes-alpha
```

Then once you have a Kubernetes cluster running, either run the
[gce.sh](gce.sh) script or just run `kubectl create -f cockroachdb-petset.yaml`
to create your cockroachdb cluster.

## Accessing the database

Along with our PetSet configuration, we expose a standard Kubernetes service
that offers a load-balanced virtual IP for clients to access the database
with. In our example, we've called this service `cockroachdb-public`.

Start up a client pod and open up an interactive, (mostly) Postgres-flavor
SQL shell using:

```console
$ kubectl run -it cockroach-client --image=cockroachdb/cockroach --restart=Never --command -- bash
root@cockroach-client # ./cockroach sql --host cockroachdb-public
```

You can see example SQL statements for inserting and querying data in the
included [demo script](demo.sh), but can use almost any Postgres-style SQL
commands. Some more basic examples can be found within
[CockroachDB's documentation](https://www.cockroachlabs.com/docs/learn-cockroachdb-sql.html).

## Simulating failures

When all (or enough) nodes are up, simulate a failure like this:

```shell
kubectl exec cockroachdb-0 -- /bin/bash -c "while true; do kill 1; done"
```

You can then reconnect to the database as demonstrated above and verify
that no data was lost. The example runs with three-fold replication, so
it can tolerate one failure of any given node at a time. Note also that
there is a brief period of time immediately after the creation of the
cluster during which the three-fold replication is established, and during
which killing a node may lead to unavailability.

The [demo script](demo.sh) gives an example of killing one instance of the
database and ensuring the other replicas have all data that was written.

## Scaling up or down

Simply patch the PetSet by running

```shell
kubectl patch petset cockroachdb -p '{"spec":{"replicas":4}}'
```

Note that you may need to create a new persistent volume claim first. If you
ran `minikube.sh`, there's a spare volume so you can immediately scale up by
one. If you're running on GCE or AWS, you can scale up by as many as you want
because new volumes will automatically be created for you. Convince yourself
that the new node immediately serves reads and writes.

## Cleaning up when you're done

Because all of the resources in this example have been tagged with the label `app=cockroachdb`,
we can clean up everything that we created in one quick command using a selector on that label:

```shell
kubectl delete petsets,pods,persistentvolumes,persistentvolumeclaims,services -l app=cockroachdb
```
