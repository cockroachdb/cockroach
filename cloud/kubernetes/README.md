# CockroachDB on Kubernetes as a StatefulSet

This example deploys CockroachDB on [Kubernetes](https://kubernetes.io) as a
[StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/).
Kubernetes is an open source system for managing containerized applications
across multiple hosts, providing basic mechanisms for deployment, maintenance,
and scaling of applications.

This is a copy of the [similar example stored in the Kubernetes
repository](https://github.com/kubernetes/kubernetes/tree/master/examples/cockroachdb).
We keep a copy here as well for faster iteration, since merging things into
Kubernetes can be quite slow, particularly during the code freeze before
releases. The copy here will typically be more up-to-date.

Note that if all you want to do is run a single cockroachdb instance for
testing and don't care about data persistence, you can do so with just a single
command instead of following this guide (which sets up a more reliable cluster):

```shell
kubectl run cockroachdb --image=cockroachdb/cockroach --restart=Never -- start-single-node --insecure --logtostderr
```

## Limitations

### Kubernetes version

The minimum Kubernetes version to successfully run the examples in this
directory without modification is `1.18`.


### Secure Mode

The use of Kubernetes certificate authority to sign CockroachDBs certificates has been deprecated by Kubernetes. Due to 
the deprecation to enable secure mode we currently only support the `bring-your-own-certs` configuration. 
The deprecated method for using the Kubernetes signing authority is in the `v1.17` directory.

**Deprecated Kubernetes Signing Authority**

For secure mode, the controller must enable `certificatesigningrequests`.
You can check if this is enabled by looking at the controller logs:
```
# On cloud platform:
# Find the controller:
$ kubectl get pods --all-namespaces | grep controller
kube-system   po/kube-controller-manager-k8s-master-5ef244d4-0   1/1       Running   0          7m

# Check the logs:
$ kubectl logs kube-controller-manager-k8s-master-5ef244d4-0 -n kube-system | grep certificate
I0628 12:38:23.471365       1 controllermanager.go:427] Starting "certificatesigningrequests"
E0628 12:38:23.473076       1 certificates.go:38] Failed to start certificate controller: open /etc/kubernetes/ca/ca.pem: no such file or directory
W0628 12:38:23.473106       1 controllermanager.go:434] Skipping "certificatesigningrequests"
# This shows that the certificate controller is not running, approved CSRs will not trigger a certificate.

# On minikube:
$ minikube logs | grep certificate
Jun 28 12:49:00 minikube localkube[3440]: I0628 12:49:00.224903    3440 controllermanager.go:437] Started "certificatesigningrequests"
Jun 28 12:49:00 minikube localkube[3440]: I0628 12:49:00.231134    3440 certificate_controller.go:120] Starting certificate controller manager
# This shows that the certificate controller is running, approved CSRs will get a certificate.

```

### StatefulSet limitations

Node-local storage for StatefulSets was only recently promoted to beta as a
Kubernetes feature (known as [local persistent
volumes](https://kubernetes.io/docs/concepts/storage/volumes/#local)), so we
don't yet recommend running that way in production. Instead, these examples use
dynamically provisioned remote persistent volumes. Note that CockroachDB already
replicates its data and thus, for the best performance, should not be deployed
on a persistent volume which already replicates internally. High-performance use
cases that need the very best performance may want to consider a
[DaemonSet](https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/)
deployment until node-local storage has been more rigorously hardened.

### Secure mode

Secure mode mounts the provided secrets into the container at pod startup. To renew or change the certificates you will
first need to generate the new certificates and update the secrets to use them before performing a rolling restart of 
your cluster.

### Geographically distributed clusters

The configuration files and instructions in this directory are limited to
deployments that exist entirely within a single Kubernetes cluster. If you'd
like to deploy CockroachDB across multiple geographically distributed Kubernetes
clusters, see the [multiregion subdirectory](multiregion).

## Creating your kubernetes cluster

### Locally on minikube

Set up your minikube cluster following the
[instructions provided in the Kubernetes docs](https://kubernetes.io/docs/getting-started-guides/minikube/).

### On AWS

Set up your cluster following the
[instructions provided in the Kubernetes docs](https://kubernetes.io/docs/getting-started-guides/aws/).

### On GCE

You can either set up your cluster following the
[instructions provided in the Kubernetes docs](https://kubernetes.io/docs/getting-started-guides/gce/)
or by using the hosted
[Google Kubernetes Engine (GKE)](https://cloud.google.com/container-engine/docs) service:

```shell
gcloud container clusters create NAME
```

If you're using GKE and want to run a secure cluster, one extra step is
required. A limitation in GKE's Role-Based Access Control (RBAC) integration
necessitates running a special command in order to let you create the RBAC
roles that CockroachDB needs to manage certificates.

1. Get the email address associated with your Google Cloud account by running:
   ```shell
   `gcloud info | grep Account`
   ```
2. Run [the following command from the GKE
   documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/role-based-access-control#prerequisites_for_using_role-based_access_control)
   with that email address:
   ```shell
   kubectl create clusterrolebinding cluster-admin-binding --clusterrole=cluster-admin --user=<your.google.cloud.email@example.org>
   ```

### On Azure

Set up your cluster following the
[instructions provided in the Kubernetes docs](https://kubernetes.io/docs/getting-started-guides/azure/).


## Creating the cockroach cluster

Once your kubernetes cluster is up and running, you can launch your cockroach cluster.

### Insecure mode

To create the cluster, run:
```shell
kubectl create -f cockroachdb-statefulset.yaml
```

Then, to initialize the cluster, run:
```shell
kubectl create -f cluster-init.yaml
```

### Secure mode

#### Creating the cluster
Run: 
```shell
kubectl create -f bring-your-own-certs/cockroachdb-statefulset.yaml`
```

Once all the pods have started, to initialize the cluster run:
```shell
kubectl create -f bring-your-own-certs/cluster-init.yaml
```

The output should look like:
```
NAME                  DESIRED   SUCCESSFUL   AGE
cluster-init-secure   1         1            5m
```


## Accessing the database

Along with our StatefulSet configuration, we expose a standard Kubernetes service
that offers a load-balanced virtual IP for clients to access the database
with. In our example, we've called this service `cockroachdb-public`.

In insecure mode, start up a client pod and open up an interactive, (mostly) Postgres-flavor
SQL shell:

```shell
kubectl run cockroachdb -it --image=cockroachdb/cockroach --rm --restart=Never \
    -- sql --insecure --host=cockroachdb-public
```

In secure mode, use our `client-secure.yaml` config to launch a pod that runs indefinitely with the `cockroach` binary inside it:

```shell
kubectl create -f client-secure.yaml
```

Check and approve the CSR for the pod as described above, and then get a shell to the pod and run:

```shell
kubectl exec -it cockroachdb-client-secure -- ./cockroach sql --certs-dir=/cockroach-certs --host=cockroachdb-public
```

You can see example SQL statements for inserting and querying data in the
included [demo script](demo.sh), but can use almost any Postgres-style SQL
commands. Some more basic examples can be found within
[CockroachDB's documentation](https://www.cockroachlabs.com/docs/stable/learn-cockroachdb-sql.html).

## Accessing the admin UI

If you want to see information about how the cluster is doing, you can try
pulling up the CockroachDB admin UI by port-forwarding from your local machine
to one of the pods:

```shell
kubectl port-forward cockroachdb-0 8080
```

Once you’ve done that, you should be able to access the admin UI by visiting
http://localhost:8080/ in your web browser.

## Running the example app

This directory contains the configuration to launch a simple load generator with one pod.

If you created an insecure cockroach cluster, run:
```shell
kubectl create -f example-app.yaml
```

If you created a secure cockroach cluster, run:
```shell
kubectl create -f example-app-secure.yaml
```

When the first pod is being initialized, you will need to approve its client certificate request:
```shell
kubectl certificate approve default.client.root
```

If more pods are then added through `kubectl scale deployment example-secure --replicas=X`, the generated
certificate will be reused.
**WARNING**: the example app in secure mode should be started with only one replica, or concurrent and
conflicting certificate requests will be sent, causing issues.


## Secure client apps with other users

Applications should be run with a user other than `root`. This can be done using the following:

#### Create the desired user and database

Connect to the `cockroachdb-client-secure` pod (created in the "Accessing the database" section):
```shell
kubectl exec -it cockroachdb-client-secure -- ./cockroach sql --certs-dir=/cockroach-certs --host=cockroachdb-public
```

Create the the app user, its database, and grant it privileges:
```shell
root@:26257/defaultdb> CREATE USER myapp;
CREATE USER 1

Time: 164.811054ms

root@:26257/defaultdb> CREATE DATABASE myappdb;
CREATE DATABASE

Time: 153.44247ms

root@:26257/defaultdb> GRANT ALL ON DATABASE myappdb TO myapp;
GRANT

Time: 90.488168ms
```

#### Create the client pod

Modify [example-app-secure.yaml](example-app-secure.yaml) to match your user and app:

The init container `init-certs` needs to request a certificate and key for your new user:
```shell
"/request-cert -namespace=${POD_NAMESPACE} -certs-dir=/cockroach-certs -type=client -user=myapp -symlink-ca-from=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
```

The `loadgen` container should be modified for your client app docker image and command, and the connection url modified to match the user and database:
```shell
"postgres://myapp@cockroachdb-public:26257/myappdb?sslmode=verify-full&sslcert=/cockroach-certs/client.myapp.crt&sslkey=/cockroach-certs/client.myapp.key&sslrootcert=/cockroach-certs/ca.crt"
```

You can now create the client pod:
```shell
kubectl create -f example-app-secure.yaml
```

#### Approve the client CSR

The init container sends a CSR and waits for approval. You can approve it using:
```shell
kubectl certificate approve default.client.myapp
```

Once approved, the init container copies the certificate and key to `/cockroach-certs` and terminates. The app container now starts, using the mounted certificate directory.


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

Scale the StatefulSet by running

```shell
kubectl scale statefulset cockroachdb --replicas=4
```

You should never scale the StatefulSet down by more than one replica at a time.
Doing so could lead to data unavailability. For best practices on safely
removing nodes from a safely, see our docs on [node
decommissioning](https://www.cockroachlabs.com/docs/stable/remove-nodes.html).

## Doing a rolling upgrade to a different CockroachDB version

Open up the StatefulSet's current configuration in your default text editor
using the command below, find the line specifying the `cockroachdb/cockroach`
image being used, change the version tag to the new one, save the file, and
quit.

```shell
kubectl edit statefulset cockroachdb
```

Kubernetes will then automatically replace the pods in your StatefulSet one by
one to run on the newly specified image. For more details on upgrading
CockroachDB, see [our
docs](https://www.cockroachlabs.com/docs/stable/upgrade-cockroach-version.html).
For how to use alternative rolling update commands such as `kubectl patch` and
`kubectl replace`, see the [Kubernetes
docs](https://kubernetes.io/docs/tutorials/stateful-application/basic-stateful-set/#rolling-update).

## Cleaning up when you're done

Because all of the resources in this example have been tagged with the label `app=cockroachdb`,
we can clean up everything that we created in one quick command using a selector on that label:

```shell
kubectl delete statefulsets,pods,persistentvolumes,persistentvolumeclaims,services,poddisruptionbudget,jobs,rolebinding,clusterrolebinding,role,clusterrole,serviceaccount -l app=cockroachdb
```

If running in secure mode, you'll want to cleanup old certificate requests:
```shell
kubectl delete csr --all
```
