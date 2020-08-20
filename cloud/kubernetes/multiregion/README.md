# Running CockroachDB across multiple Kubernetes clusters (GKE)

The script and configuration files in this directory enable deploying
CockroachDB across multiple Kubernetes clusters that are spread across different
geographic regions and hosted on [GKE](https://cloud.google.com/kubernetes-engine). It deploys a CockroachDB
[StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
into each separate cluster, and links them together using DNS.

To use the configuration provided here, check out this repository (or otherwise
download a copy of this directory), fill in the constants at the top of
[setup.py](setup.py) with the relevant information about your Kubernetes
clusters, optionally make any desired modifications to
[cockroachdb-statefulset-secure.yaml](cockroachdb-statefulset-secure.yaml) as
explained in [our Kubernetes performance tuning
guide](https://www.cockroachlabs.com/docs/stable/kubernetes-performance.html),
then finally run [setup.py](setup.py).

You should see a lot of output as it does its thing, hopefully ending after
printing out `job "cluster-init-secure" created`. This implies that everything
was created successfully, and you should soon see the CockroachDB cluster
initialized with 3 pods in the "READY" state in each Kubernetes cluster. At this
point you can manage the StatefulSet in each cluster independently if you so
desire, scaling up the number of replicas, changing their resource requests, or
making other modifications as you please.

If anything goes wrong along the way, please let us know via any of the [normal
troubleshooting
channels](https://www.cockroachlabs.com/docs/stable/support-resources.html).
While we believe this creates a highly available, maintainable multi-region
deployment, it is still pushing the boundaries of how Kubernetes is typically
used, so feedback and issue reports are very appreciated.

## Limitations

### Pod-to-pod connectivity

The deployment outlined in this directory relies on pod IP addresses being
routable even across Kubernetes clusters and regions. This achieves optimal
performance, particularly when compared to alternative solutions that route all packets between clusters through load balancers, but means that it won't work in certain environments.

This requirement is satisfied by clusters deployed in cloud environments such as Google Kubernetes Engine, and
can also be satisfied by on-prem environments depending on the [Kubernetes networking setup](https://kubernetes.io/docs/concepts/cluster-administration/networking/) used. If you want to test whether your cluster will work, you can run this basic network test:

```shell
$ kubectl run network-test --image=alpine --restart=Never -- sleep 999999
pod "network-test" created
$ kubectl describe pod network-test | grep IP
IP:           THAT-PODS-IP-ADDRESS
$ kubectl config use-context YOUR-OTHER-CLUSTERS-CONTEXT-HERE
$ kubectl run -it network-test --image=alpine --restart=Never -- ping THAT-PODS-IP-ADDRESS
If you don't see a command prompt, try pressing enter.
64 bytes from 10.12.14.10: seq=1 ttl=62 time=0.570 ms
64 bytes from 10.12.14.10: seq=2 ttl=62 time=0.449 ms
64 bytes from 10.12.14.10: seq=3 ttl=62 time=0.635 ms
64 bytes from 10.12.14.10: seq=4 ttl=62 time=0.722 ms
64 bytes from 10.12.14.10: seq=5 ttl=62 time=0.504 ms
...
```

If the pods can directly connect, you should see successful ping output like the
above. If they can't, you won't see any successful ping responses. Make sure to
delete the `network-test` pod in each cluster when you're done!

### Exposing DNS servers to the Internet

As currently configured, the way that the DNS servers from each Kubernetes
cluster are hooked together is by exposing them via a load balanced IP address
that's visible to the public Internet. This is because [Google Cloud Platform's Internal Load Balancers do not currently support clients in one region using a load balancer in another region](https://cloud.google.com/compute/docs/load-balancing/internal/#deploying_internal_load_balancing_with_clients_across_vpn_or_interconnect). 

None of the services in your Kubernetes cluster will be made accessible, but
their names could leak out to a motivated attacker. If this is unacceptable,
please let us know and we can demonstrate other options. [Your voice could also
help convince Google to allow clients from one region to use an Internal Load
Balancer in another](https://issuetracker.google.com/issues/111021512),
eliminating the problem.

## Cleaning up

To remove all the resources created in your clusters by [setup.py](setup.py),
copy the parameters you provided at the top of [setup.py](setup.py) to the top
of [teardown.py](teardown.py) and run [teardown.py](teardown.py).

## More information

For more information on running CockroachDB in Kubernetes, please see the [README
in the parent directory](../README.md).
