# Running CockroachDB across multiple Kubernetes clusters (EKS)

The configuration files in this directory enable a multi-region CockroachDB deployment on [Amazon EKS](https://aws.amazon.com/eks/), using multiple Kubernetes clusters in different geographic regions. They are primarily intended for use with our [Orchestrate CockroachDB Across Multiple Kubernetes Clusters](https://www.cockroachlabs.com/docs/stable/orchestrate-cockroachdb-with-kubernetes-multi-cluster.html#eks) tutorial, but can be modified for use with any multi-region CockroachDB deployment hosted on EKS.

Note that a successful multi-region deployment also requires configuring your EC2 network for inter-region traffic, which is covered fully in our tutorial.

## Usage

The below assumes you have created a Kubernetes cluster in each region in which you want to deploy CockroachDB.

Each of the 3 configuration files must be applied separately to each Kubernetes cluster.

### Create StatefulSets

[`cockroachdb-statefulset-secure-eks.yaml`](https://github.com/cockroachdb/cockroach/cloud/kubernetes/multiregion/eks/cockroachdb-statefulset-secure-eks.yaml) creates a StatefulSet that runs 3 CockroachDB pods in a single region.

Because the multi-region deployment requires deploying CockroachDB to a separate Kubernetes cluster in each region, you need to customize and apply a separate version of this file to each region.

Use the `namespace` field to specify a namespace other than `default` in which to run the CockroachDB pods. This should correspond to the region in which the Kubernetes cluster is deployed (e.g., `us-east-1`). 

```
namespace: <cluster-namespace>
```

Also create the namespace in the appropriate region by running `kubectl create namespace <cluster-namespace> --context=<cluster-context>`.

Change the resource `requests` and `limits` to appropriate values for the hardware that you're running. You can see the allocatable resources on each of your Kubernetes nodes by running `kubectl describe nodes`.

```
resources:
  requests:
    cpu: "16"
    memory: "8Gi"
  limits:
    memory: "8Gi"
```

Replace the placeholder values in the `--join` and `--locality` flags with the namespace of the CockroachDB cluster in each region (e.g., `us-east-1`). `--join` specifies the host addresses that connect nodes to the cluster and distribute the rest of the node addresses. `--locality` describes the location of each CockroachDB node.

```
--join cockroachdb-0.cockroachdb.<cluster-namespace-1>,cockroachdb-1.cockroachdb.<cluster-namespace-1>,cockroachdb-2.cockroachdb.<cluster-namespace-1>,cockroachdb-0.cockroachdb.<cluster-namespace-2>,cockroachdb-1.cockroachdb.<cluster-namespace-2>,cockroachdb-2.cockroachdb.<cluster-namespace-2>,cockroachdb-0.cockroachdb.<cluster-namespace-3>,cockroachdb-1.cockroachdb.<cluster-namespace-3>,cockroachdb-2.cockroachdb.<cluster-namespace-3>
--locality=region=<cluster-namespace-1>,az=$(cat /etc/cockroach-env/zone),dns=$(hostname -f)
```

You can then deploy the StatefulSet in each region, specifying the appropriate cluster context and namespace (which you defined above):

```
kubectl create -f <statefulset> --context=<cluster-context> --namespace=<cluster-namespace>
```

Before initializing the cluster, however, you must enable CockroachDB pods to communicate across regions. This includes peering the VPCs in all 3 regions with each other, setting up a [Network Load Balancer](#set-up-load-balancing) in each region, and [configuring a CoreDNS service](#configure-coredns) to route DNS traffic to the appropriate pods. For information on configuring the EC2 network, see our [documentation](https://www.cockroachlabs.com/docs/stable/orchestrate-cockroachdb-with-kubernetes-multi-cluster.html#eks).

### Set up load balancing

[`dns-lb-eks.yaml`](https://github.com/cockroachdb/cockroach/cloud/kubernetes/multiregion/eks/dns-lb-eks.yaml) creates a [Network Load Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/introduction.html) pointed at the CoreDNS service that routes DNS traffic to the appropriate pods. 

Upload the load balancer manifest to each region:

```
kubectl create -f https://raw.githubusercontent.com/cockroachdb/cockroach/master/cloud/kubernetes/multiregion/eks/dns-lb-eks.yaml --context=<cluster-context>
```

### Configure CoreDNS

[`configmap.yaml`](https://github.com/cockroachdb/cockroach/cloud/kubernetes/multiregion/eks/configmap.yaml) is a template for [modifying the ConfigMap](https://kubernetes.io/docs/tasks/administer-cluster/dns-custom-nameservers/#coredns-configmap-options) for the CoreDNS Corefile in each region.

You must define a separate ConfigMap for each region. Each unique ConfigMap lists the forwarding addresses for the pods in the 2 other regions. 

For each region, replace:

- `region2` and `region3` with the namespaces in which the CockroachDB pods will run in the other 2 regions.

- `ip1`, `ip2`, and `ip3` with the IP addresses of the Network Load Balancers in the region.

First back up the existing ConfigMap in each region:

```
kubectl -n kube-system get configmap coredns -o yaml > <configmap-backup-name>
```

Then apply the new ConfigMap:

```
kubectl apply -f <configmap-name> --context=<cluster-context>
```

## More information

For more information on running CockroachDB in Kubernetes, please see the [README in the parent directory](../../README.md).
