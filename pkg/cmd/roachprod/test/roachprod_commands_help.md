# Roachprod Commands Help

This file contains the help text for all roachprod commands.

## `roachprod create`

```
Create a local or cloud-based cluster.

A cluster is composed of a set of nodes, configured during cluster creation via
the --nodes flag. Creating a cluster does not start any processes on the nodes
other than the base system processes (e.g. sshd). See "roachprod start" for
starting cockroach nodes and "roachprod {run,ssh}" for running arbitrary
commands on the nodes of a cluster.

Cloud Clusters

  Cloud-based clusters are ephemeral and come with a lifetime (specified by the
  --lifetime flag) after which they will be automatically
  destroyed. Cloud-based clusters require the associated command line tool for
  the cloud to be installed and configured (e.g. "gcloud auth login").

  Clusters names are required to be prefixed by the authenticated user of the
  cloud service. The suffix is an arbitrary string used to distinguish
  clusters. For example, "marc-test" is a valid cluster name for the user
  "marc". The authenticated user for the cloud service is automatically
  detected and can be override by the ROACHPROD_USER environment variable or
  the --username flag.

  The machine type and the use of local SSD storage can be specified during
  cluster creation via the --{cloud}-machine-type and --local-ssd flags. The
  machine-type is cloud specified. For example, --gce-machine-type=n1-highcpu-8
  requests the "n1-highcpu-8" machine type for a GCE-based cluster. No attempt
  is made (or desired) to abstract machine types across cloud providers. See
  the cloud provider's documentation for details on the machine types
  available.

  The underlying filesystem can be provided using the --filesystem flag.
  Use --filesystem=zfs, for zfs, and --filesystem=ext4, for ext4. The default
  file system is ext4. The filesystem flag only works on gce currently.

Local Clusters

  A local cluster stores the per-node data in ${HOME}/local on the machine
  roachprod is being run on. Whether a cluster is local is specified on creation
  by using the name 'local' or 'local-<anything>'. Local clusters have no expiration.

Usage:
  roachprod create <cluster> [flags]

Flags:
      --arch string                                                              architecture override for VM [amd64, arm64, fips]; N.B. fips implies amd64 with openssl
      --aws-boot-disk-only                                                       Only attach the boot disk. No additional volumes will be provisioned even if specified.
      --aws-config aws config path                                               Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-cpu-options string                                                   Options to specify number of cores and threads per core (see https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-optimize-cpu.html#instance-specify-cpu-options)
      --aws-create-rate-limit float                                              aws rate limit (per second) for instance creation. This is used to avoid hitting the request limits from aws, which can vary based on the region, and the size of the cluster being created. Try lowering this limit when hitting 'Request limit exceeded' errors. (default 2)
      --aws-ebs-iops int                                                         Number of IOPs to provision for supported disk types (io1, io2, gp3)
      --aws-ebs-throughput int                                                   Additional throughput to provision, in MiB/s
      --aws-ebs-volume JSON                                                      Additional EBS disk to attached, repeated for extra disks; specified as JSON: {"VolumeType":"io2","VolumeSize":213,"Iops":321} (default EBSVolumeList)
      --aws-ebs-volume-count int                                                 Number of EBS volumes to create, only used if local-ssd=false and superseded by --aws-ebs-volume (default 1)
      --aws-ebs-volume-size int                                                  Size in GB of EBS volume, only used if local-ssd=false (default 500)
      --aws-ebs-volume-type string                                               Type of the EBS volume, only used if local-ssd=false (default "gp3")
      --aws-enable-multiple-stores                                               Enable the use of multiple stores by creating one store directory per disk. Default is to raid0 stripe all disks. See repeating --aws-ebs-volume for adding extra volumes.
      --aws-iam-profile string                                                   the IAM instance profile to associate with created VMs if non-empty (default "roachprod-testing")
      --aws-image-ami string                                                     Override image AMI to use.  See https://awscli.amazonaws.com/v2/documentation/api/latest/reference/ec2/describe-images.html
      --aws-machine-type string                                                  Machine type (see https://aws.amazon.com/ec2/instance-types/) (default "m6i.xlarge")
      --aws-machine-type-ssd string                                              Machine type for --local-ssd (see https://aws.amazon.com/ec2/instance-types/) (default "m6id.xlarge")
      --aws-profile string                                                       Profile to manage cluster in (default "CRLShared-541263489771")
      --aws-use-spot                                                             use AWS Spot VMs, which are significantly cheaper, but can be preempted by AWS.
      --aws-user string                                                          Name of the remote user to SSH as (default "ubuntu")
      --aws-zones strings                                                        aws availability zones to use for cluster creation. If zones are formatted
                                                                                 as AZ:N where N is an integer, the zone will be repeated N times. If > 1
                                                                                 zone specified, the cluster will be spread out evenly by zone regardless
                                                                                 of geo (default [us-east-2a,us-west-2b,eu-west-2b])
      --azure-boot-disk-only                                                     Only attach the boot disk. No additional volumes will be provisioned even if specified.
      --azure-disk-caching string                                                Disk caching behavior for attached storage.  Valid values are: none, read-only, read-write.  Not applicable to Ultra disks. (default "none")
      --azure-enable-multiple-stores                                             Enable the use of multiple stores by creating one store directory per disk. Default is to raid0 stripe all disks or use ZFS (if --file-system=zfs).
      --azure-machine-type string                                                Machine type (see https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/) (default "Standard_D4_v3")
      --azure-network-disk-count int                                             Number of network disks to attach, only used if local-ssd=false (default 1)
      --azure-network-disk-type string                                           type of network disk [standard-ssd, premium-ssd, premium-ssd-v2, ultra-disk]. only used if local-ssd is false (default "premium-ssd")
      --azure-sync-delete                                                        Wait for deletions to finish before returning
      --azure-timeout duration                                                   The maximum amount of time for an Azure API operation to take (default 10m0s)
      --azure-ultra-disk-iops int                                                Number of IOPS provisioned for ultra disk, only used if network-disk-type=ultra-disk (default 5000)
      --azure-vnet-name string                                                   The name of the VNet to use (default "common")
      --azure-volume-size int32                                                  Size in GB of network disk volume, only used if local-ssd=false (default 500)
      --azure-zones az account list-locations                                    Zones for cluster, where a zone is a location (see az account list-locations)
                                                                                 and availability zone seperated by a dash. If zones are formatted as Location-AZ:N where N is an integer,
                                                                                 the zone will be repeated N times. If > 1 zone specified, nodes will be geo-distributed
                                                                                 regardless of geo (default [eastus-1,canadacentral-1,westus3-1])
  -c, --clouds strings                                                           The cloud provider(s) to use when creating new vm instances: [gce azure ibm local aws] (default [gce])
      --filesystem string                                                        The underlying file system(ext4/zfs/xfs/f2fs/btrfs). ext4 is used by default. (default "ext4")
      --gce-boot-disk-only                                                       Only attach the boot disk. No additional volumes will be provisioned even if specified.
      --gce-boot-disk-type string                                                Type of the boot disk volume (default "pd-ssd")
      --gce-default-project string                                               google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-default-service-account string                                       Service account to use if the default project is in use and no --gce-service-account was specified (default "21965078311-compute@developer.gserviceaccount.com")
      --gce-dns-domain string                                                    zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                                                   project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                                      zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-enable-cron                                                          Enables the cron service (it is disabled by default)
      --gce-enable-multiple-stores                                               Enable the use of multiple stores by creating one store directory per disk. Default is to raid0 stripe all disks.
      --gce-image gcloud compute images list --filter="family=ubuntu-2004-lts"   Image to use to create the vm, use gcloud compute images list --filter="family=ubuntu-2004-lts" to list available images. Note: this option is ignored if --fips is passed. (default "ubuntu-2204-jammy-v20240319")
      --gce-local-ssd-count int                                                  Number of local SSDs to create, only used if local-ssd=true (default 1)
      --gce-machine-type string                                                  Machine type (see https://cloud.google.com/compute/docs/machine-types) (default "n2-standard-4")
      --gce-managed                                                              use a managed instance group (enables resizing, load balancing, and health monitoring)
      --gce-managed-dns-domain string                                            zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                                              zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-managed-spot-zones strings                                           subset of zones in managed instance groups that will use spot instances
      --gce-metadata-project string                                              google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-min-cpu-platform string                                              Minimum CPU platform (see https://cloud.google.com/compute/docs/instances/specify-min-cpu-platform) (default "Intel Ice Lake")
      --gce-pd-volume-count int                                                  Number of persistent disk volumes, only used if local-ssd=false (default 1)
      --gce-pd-volume-provisioned-iops int                                       Provisioned IOPS for the disk volume (required for hyperdisk-balanced, optional for pd-extreme)
      --gce-pd-volume-provisioned-throughput int                                 Provisioned throughput in MiB/s for the disk volume (required for hyperdisk-balanced)
      --gce-pd-volume-size int                                                   Size in GB of persistent disk volume, only used if local-ssd=false (default 500)
      --gce-pd-volume-type string                                                Type of the persistent disk volume, only used if local-ssd=false (pd-ssd, pd-balanced, pd-extreme, pd-standard, hyperdisk-balanced) (default "pd-ssd")
      --gce-preemptible                                                          use preemptible GCE instances (lifetime cannot exceed 24h)
      --gce-project GCE project name                                             GCE project to manage (default cockroach-ephemeral)
      --gce-service-account string                                               Service account to use
      --gce-terminateOnMigration                                                 use 'TERMINATE' maintenance policy (for GCE live migrations)
      --gce-threads-per-core int                                                 the number of visible threads per physical core (valid values: 1 or 2), default is 0 (auto)
      --gce-turbo-mode string                                                    enable turbo mode for the instance (only supported on C4 VM families, valid value: 'ALL_CORE_MAX')
      --gce-use-bulk-insert                                                      use GCP Compute SDK's BulkInsert API for creating VMs (more efficient for large clusters) (default true)
      --gce-use-spot                                                             use spot GCE instances (like preemptible but lifetime can exceed 24h)
      --gce-zones strings                                                        Zones for cluster. If zones are formatted as AZ:N where N is an integer, the zone
                                                                                 will be repeated N times. If > 1 zone specified, nodes will be geo-distributed
                                                                                 regardless of geo (default [us-east1-c,us-west1-b,europe-west2-b,us-east1-b,us-west1-c,europe-west2-c,us-east1-d,us-west1-a,europe-west2-a])
      --geo                                                                      Create geo-distributed cluster
  -h, --help                                                                     help for create
      --label stringToString                                                     The label(s) to be used when creating new vm instances, must be in '--label name=value' format and value can't be empty string after trimming space, a value that has space must be quoted by single quotes, gce label name only allows hyphens (-), underscores (_), lowercase characters, numbers and international characters. Examples: usage=cloud-report-2021, namewithspaceinvalue='s o s' (default [usage=roachprod])
  -l, --lifetime duration                                                        Lifetime of the cluster (default 12h0m0s)
      --local-ssd                                                                Use local SSD (default true)
      --local-ssd-no-ext4-barrier                                                Mount the local SSD with the "-o nobarrier" flag. Ignored if --local-ssd=false is specified. (default true)
  -n, --nodes int                                                                Total number of nodes, distributed across all clouds (default 4)
      --os-volume-size int                                                       OS disk volume size in GB (default 10)
  -u, --username string                                                          Username to run under, detect if blank (default "will.choe")

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod grow`

```
grow a cluster by adding the specified number of nodes to it.

Only Google Cloud and local clusters currently support adding nodes. The Google
Cloud cluster has to be a managed cluster (i.e., a cluster created with the
gce-managed flag). The new nodes will use the instance template that was used to
create the cluster originally (Nodes will be created in the same zone as the
existing nodes, or if the cluster is geographically distributed, the nodes will
be fairly distributed across the zones of the cluster).

Usage:
  roachprod grow <cluster> <num-nodes> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for grow
      --insecure                                           use an insecure cluster
      --secure                                             use a secure cluster (default true)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod populate-etc-hosts`

```
populate /etc/hosts on all nodes in a cluster with the private
IP addresses of the nodes. This is useful for running cockroach tests that use
DNS to resolve the IP addresses of the nodes (e.g. jepsen) in Cloud environments
where there is no DNS server to resolve the IP addresses of the nodes.

Usage:
  roachprod populate-etc-hosts <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for populate-etc-hosts
      --insecure                                           use an insecure cluster
      --secure                                             use a secure cluster (default true)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod shrink`

```
shrink a cluster by removing the specified number of nodes.

Only Google Cloud and local clusters currently support removing nodes. The
Google Cloud cluster has to be a managed cluster (i.e., a cluster created with
the gce-managed flag). Nodes are removed from the tail end of the cluster.
Removing nodes from the middle of the cluster is not supported yet.

Usage:
  roachprod shrink <cluster> <num-nodes> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for shrink

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod reset`

```
Reset cloud VMs in a cluster.
Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod reset marc-test:1-3,8-9

  will perform reset on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod reset <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for reset

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod destroy`

```
Destroy one or more local or cloud-based clusters.

The destroy command accepts the names of the clusters to destroy. Alternatively,
the --all-mine flag can be provided to destroy all (non-local) clusters that are
owned by the current user, or the --all-local flag can be provided to destroy
all local clusters.

Destroying a cluster releases the resources for a cluster. For a cloud-based
cluster the machine and associated disk resources are freed. For a local
cluster, any processes started by roachprod are stopped, and the node
directories inside ${HOME}/local directory are removed.

Usage:
  roachprod destroy [ --all-mine | --all-local | <cluster 1> [<cluster 2> ...] ] [flags]

Flags:
  -l, --all-local                                          Destroy all local clusters
  -m, --all-mine                                           Destroy all non-local clusters belonging to the current user
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for destroy
  -u, --username string                                    Username to run under, detect if blank (default "will.choe")

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod extend`

```
Extend the lifetime of the specified cluster to prevent it from being
destroyed:

  roachprod extend marc-test --lifetime=6h

Usage:
  roachprod extend <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for extend
  -l, --lifetime duration                                  Lifetime of the cluster (default 12h0m0s)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod load-balancer`

```
create load balancers for specific services, query the IP or postgres URL of a load balancer

Usage:
  roachprod load-balancer [command] [flags]
  roachprod load-balancer [command]

Available Commands:
  create      create a load balancer for a cluster
  pgurl       get the postgres URL of a load balancer
  ip          get the IP address of a load balancer

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for load-balancer

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)

Use "roachprod load-balancer [command] --help" for more information about a command.
```

## `roachprod list`

```
List all clusters.

The list command accepts a flag --pattern which is a regular
expression that will be matched against the cluster name pattern.  Alternatively,
the --mine flag can be provided to list the clusters that are owned by the current
user.

The default output shows one line per cluster, including the local cluster if
it exists:

  ~ roachprod list
  local:     [local]    1  (-)
  marc-test: [aws gce]  4  (5h34m35s)
  Syncing...

The second column lists the cloud providers that host VMs for the cluster.

The third and fourth columns are the number of nodes in the cluster and the
time remaining before the cluster will be automatically destroyed. Note that
local clusters do not have an expiration.

The --details flag adjusts the output format to include per-node details:

  ~ roachprod list --details
  local [local]: (no expiration)
    localhost		127.0.0.1	127.0.0.1
  marc-test: [aws gce] 5h33m57s remaining
    marc-test-0001	marc-test-0001.us-east1-b.cockroach-ephemeral	10.142.0.18	35.229.60.91
    marc-test-0002	marc-test-0002.us-east1-b.cockroach-ephemeral	10.142.0.17	35.231.0.44
    marc-test-0003	marc-test-0003.us-east1-b.cockroach-ephemeral	10.142.0.19	35.229.111.100
    marc-test-0004	marc-test-0004.us-east1-b.cockroach-ephemeral	10.142.0.20	35.231.102.125
  Syncing...

The first and second column are the node hostname and fully qualified name
respectively. The third and fourth column are the private and public IP
addresses.

The --json flag sets the format of the command output to json.

Listing clusters has the side-effect of syncing ssh keys/configs and the local
hosts file.

Usage:
  roachprod list [--details | --json] [ --mine | --pattern ] [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
  -c, --cost                                               Show cost estimates
  -d, --details                                            Show cluster details
      --dns-required-providers strings                     the cloud providers that must be active to refresh DNS entries (default [gce,aws])
      --export-ssh-config string                           export the SSH config for listed clusters (only when pattern or mine is specified
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for list
      --json                                               Show cluster specs in a json format
  -m, --mine                                               Show only clusters belonging to the current user
      --pattern string                                     Show only clusters matching the regex pattern. Empty string matches everything.
  -u, --username string                                    Username to run under, detect if blank (default "will.choe")

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod sync`

```
sync ssh keys/config and hosts files

Usage:
  roachprod sync [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
  -c, --clouds stringArray                                 Specify the cloud providers when syncing. Important: Use this flag only if you are certain that you want to sync with a specific cloud. All DNS host entries for other clouds will be erased from the DNS zone.
      --dns-required-providers strings                     the cloud providers that must be active to refresh DNS entries (default [gce,aws])
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for sync
      --include-volumes                                    Include volumes when syncing

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod gc`

```
Garbage collect expired clusters and unused SSH keypairs in AWS.

Destroys expired clusters, sending email if properly configured. Usually run
hourly by a cronjob so it is not necessary to run manually.

Usage:
  roachprod gc [flags]

Flags:
      --aws-account-ids strings                            AWS account ids as a comma-separated string
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-subscription-names strings                   Azure subscription names as a comma-separated string
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
  -n, --dry-run                                            dry run (don't perform any actions)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for gc
      --slack-token string                                 Slack bot token

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod setup-ssh`

```
Sets up the keys and host keys for the vms in the cluster.

It first resets the machine credentials as though the cluster were newly created
using the cloud provider APIs and then proceeds to ensure that the hosts can
SSH into eachother and lastly adds additional public keys to AWS hosts as read
from the GCP project. This operation is performed as the last step of creating
a new cluster but can be useful to re-run if the operation failed previously or
if the user would like to update the keys on the remote hosts.

Usage:
  roachprod setup-ssh <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for setup-ssh

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod status`

```
Retrieve the status of nodes in a cluster.

The "status" command outputs the binary and PID for the specified nodes:

  ~ roachprod status local
  local: status 3/3
     1: cockroach 29688
     2: cockroach 29687
     3: cockroach 29689

The --tag flag can be used to to associate a tag with the process. This tag can
then be used to restrict the processes which are operated on by the status and
stop commands. Tags can have a hierarchical component by utilizing a slash
separated string similar to a filesystem path. A tag matches if a prefix of the
components match. For example, the tag "a/b" will match both "a/b" and
"a/b/c/d".


Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod status marc-test:1-3,8-9

  will perform status on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod status <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for status
      --insecure-ignore-host-key                           don't check ssh host keys (default true)
      --tag string                                         the process tag

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod monitor`

```
Monitor the status of cockroach nodes in a cluster.

The "monitor" command runs until terminated. At startup it outputs a line for
each specified node indicating the status of the node (either the PID of the
node if alive, or "dead" otherwise). It then watches for changes in the status
of nodes, outputting a line whenever a change is detected:

  ~ roachprod monitor local
  1: 29688
  3: 29689
  2: 29687
  3: dead
  3: 30718

Usage:
  roachprod monitor [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for monitor
      --insecure-ignore-host-key                           don't check ssh host keys (default true)
      --oneshot                                            Report the status of all targeted nodes once, then exit. The exit status is nonzero if (and only if) any node was found not running.

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod start`

```
Start nodes on a cluster.

Nodes are started in secure mode by default and there is a one time
initialization for the cluster to create and distribute the certs.
Note that running some modes in secure mode and others in insecure
mode is not a supported Cockroach configuration. To start nodes in
insecure mode, use the --insecure flag.

The --binary flag specifies the remote binary to run. It is up to the roachprod
user to ensure this binary exists, usually via "roachprod put". Note that no
cockroach software is installed by default on a newly created cluster.

The --args and --env flags can be used to pass arbitrary command line flags and
environment variables to the cockroach process.

The --tag flag can be used to to associate a tag with the process. This tag can
then be used to restrict the processes which are operated on by the status and
stop commands. Tags can have a hierarchical component by utilizing a slash
separated string similar to a filesystem path. A tag matches if a prefix of the
components match. For example, the tag "a/b" will match both "a/b" and
"a/b/c/d".

The "start" command takes care of setting up the --join address and specifying
reasonable defaults for other flags. One side-effect of this convenience is
that node 1 is special and if started, is used to auto-initialize the cluster.
The --skip-init flag can be used to avoid auto-initialization (which can then
separately be done using the "init" command).

If the COCKROACH_DEV_LICENSE environment variable is set the enterprise.license
cluster setting will be set to its value.

Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod start marc-test:1-3,8-9

  will perform start on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod start <cluster> [flags]

Flags:
      --admin-ui-port int                                  port to serve the admin UI on
  -a, --args stringArray                                   node arguments (example: --args "--cache=25%" --args "--max-sql-memory=25%")
      --auto-restart                                       automatically restart cockroach processes that die
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
  -b, --binary string                                      the remote cockroach binary to use (default "cockroach")
      --dns-required-providers strings                     the cloud providers that must be active to refresh DNS entries (default [gce,aws])
      --enable-fluent-sink                                 whether to enable the fluent-servers attribute in the CockroachDB logging configuration
      --encrypt                                            start nodes with encryption at rest turned on
  -e, --env stringArray                                    node environment variables (default [COCKROACH_TESTING_FORCE_RELEASE_BRANCH=true,COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING=true])
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for start
      --init-target int                                    node on which to run initialization (default 1)
      --insecure                                           use an insecure cluster
      --insecure-ignore-host-key                           don't check ssh host keys (default true)
      --num-files-limit int                                limit the number of files that can be created by the cockroach process (default 532480)
  -r, --racks int                                          the number of racks to partition the nodes into
      --restart                                            restart an existing cluster (skips serial start and init)
      --schedule-backup-args string                        Recurrence and scheduled backup options specification (default "RECURRING '*/15 * * * *' FULL BACKUP '@hourly' WITH SCHEDULE OPTIONS first_run = 'now'")
      --schedule-backups                                   create a cluster backup schedule once the cluster has started (by default, full backup hourly and incremental every 15 minutes)
      --secure                                             use a secure cluster (default true)
      --skip-init                                          skip initializing the cluster
      --sql-port int                                       port on which to listen for SQL clients
      --store-count int                                    number of stores to start each node with (default 1)
      --tag string                                         the process tag

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod update-targets`

```
Update prometheus target configurations of each node of a cluster.

The "start" command updates the prometheus target configuration every time. But, in case of any
failure, this command can be used to update the configurations.

The default prometheus url is https://grafana.testeng.crdb.io/. This can be overwritten by using the
environment variable COCKROACH_PROM_HOST_URL

Note that if the cluster is started in insecure mode, set the insecure mode here as well by using the --insecure flag.

Usage:
  roachprod update-targets <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for update-targets
      --insecure                                           use an insecure cluster
      --secure                                             use a secure cluster (default true)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod stop`

```
Stop nodes on a cluster.

Stop roachprod created processes running on the nodes in a cluster, including
processes started by the "start", "run" and "ssh" commands. Every process
started by roachprod is tagged with a ROACHPROD environment variable which is
used by "stop" to locate the processes and terminate them. By default processes
are killed with signal 9 (SIGKILL) giving them no chance for a graceful exit.

The --sig flag will pass a signal to kill to allow us finer control over how we
shutdown cockroach. The --wait flag causes stop to loop waiting for all
processes with the right ROACHPROD environment variable to exit. Note that stop
will wait forever if you specify --wait with a non-terminating signal (e.g.
SIGHUP), unless you also configure --max-wait.

--wait defaults to true for signal 9 (SIGKILL) and false for all other signals.

The --tag flag can be used to to associate a tag with the process. This tag can
then be used to restrict the processes which are operated on by the status and
stop commands. Tags can have a hierarchical component by utilizing a slash
separated string similar to a filesystem path. A tag matches if a prefix of the
components match. For example, the tag "a/b" will match both "a/b" and
"a/b/c/d".


Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod stop marc-test:1-3,8-9

  will perform stop on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod stop <cluster> [--sig] [--wait] [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
      --grace-period int                                   approx number of seconds to wait for processes to exit, before a forceful shutdown (SIGKILL) is performed
  -h, --help                                               help for stop
      --insecure-ignore-host-key                           don't check ssh host keys (default true)
      --sig int                                            signal to pass to kill (default 9)
      --tag string                                         the process tag
      --wait                                               wait for processes to exit

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod start-sql`

```
Start SQL/HTTP instances for a virtual cluster as separate processes.

The --storage-cluster flag must be used to specify a storage cluster
(with optional node selector) which is already running. The command
will create the virtual cluster on the storage cluster if it does not
exist already.  If creating multiple virtual clusters on the same
node, the --sql-instance flag must be passed to differentiate them.

The instance is started in shared process (in memory) mode by
default. To start an external process instance, pass the
--external-cluster flag indicating where the SQL server processes
should be started.

Nodes are started in secure mode by default and there is a one time
initialization for the cluster to create and distribute the certs.
Note that running some modes in secure mode and others in insecure
mode is not a supported Cockroach configuration. To start nodes in
insecure mode, use the --insecure flag.

The --binary flag specifies the remote binary to run, if starting
external services. It is up to the roachprod user to ensure this
binary exists, usually via "roachprod put". Note that no cockroach
software is installed by default on a newly created cluster.

The --args and --env flags can be used to pass arbitrary command line flags and
environment variables to the cockroach process.

The --tag flag can be used to to associate a tag with the process. This tag can
then be used to restrict the processes which are operated on by the status and
stop commands. Tags can have a hierarchical component by utilizing a slash
separated string similar to a filesystem path. A tag matches if a prefix of the
components match. For example, the tag "a/b" will match both "a/b" and
"a/b/c/d".

Usage:
  roachprod start-sql <name> --storage-cluster <storage-cluster> [--external-nodes <virtual-cluster-nodes>] [flags]

Flags:
      --auto-restart                                       automatically restart cockroach processes that die
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
  -b, --binary string                                      the remote cockroach binary to use (default "cockroach")
      --enable-fluent-sink                                 whether to enable the fluent-servers attribute in the CockroachDB logging configuration
      --external-nodes string                              if set, starts service in external mode, as a separate process in the given nodes
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for start-sql
      --insecure                                           use an insecure cluster
      --num-files-limit int                                limit the number of files that can be created by the cockroach process (default 532480)
      --schedule-backup-args string                        Recurrence and scheduled backup options specification (default "RECURRING '*/15 * * * *' FULL BACKUP '@hourly' WITH SCHEDULE OPTIONS first_run = 'now'")
      --schedule-backups                                   create a cluster backup schedule once the cluster has started (by default, full backup hourly and incremental every 15 minutes)
      --secure                                             use a secure cluster (default true)
      --sql-instance int                                   specific SQL/HTTP instance to connect to (this is a roachprod abstraction for separate-process deployments distinct from the internal instance ID)
      --sql-port int                                       port on which to listen for SQL clients
  -S, --storage-cluster string                             storage cluster
      --tag string                                         the process tag

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod stop-sql`

```
Stop sql instances on a cluster.

Stop roachprod created virtual clusters (shared or separate process). By default,
separate processes are killed with signal 9 (SIGKILL) giving them no chance for a
graceful exit.

The --sig flag will pass a signal to kill to allow us finer control over how we
shutdown processes. The --wait flag causes stop to loop waiting for all
processes to exit. Note that stop will wait forever if you specify --wait with a
non-terminating signal (e.g. SIGHUP), unless you also configure --max-wait.

--wait defaults to true for signal 9 (SIGKILL) and false for all other signals.

Usage:
  roachprod stop-sql <cluster> --cluster <name> --sql-instance <instance> [--sig] [--wait] [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --cluster string                                     specific virtual cluster to connect to
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
      --grace-period int                                   approx number of seconds to wait for processes to exit, before a forceful shutdown (SIGKILL) is performed
  -h, --help                                               help for stop-sql
      --insecure                                           use an insecure cluster
      --secure                                             use a secure cluster (default true)
      --sig int                                            signal to pass to kill (default 9)
      --sql-instance int                                   specific SQL/HTTP instance to connect to (this is a roachprod abstraction distinct from the internal instance ID)
      --wait                                               wait for processes to exit

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod deploy`

```
Performs a rolling upgrade of cockroach.

The deploy command currently only supports redeploying the storage cluster
(system tenant). It should be run on a cluster that is already running
cockroach. The command will download the specified version of cockroach and
stage it on the cluster. It will then perform a rolling upgrade of the cluster,
one node at a time, to the new version.

Currently available application options are:
  cockroach  - Cockroach nightly builds. Can provide an optional SHA, otherwise
               latest build version is used.
  release    - Official CockroachDB Release. Must provide a specific release
               version.
  customized - Cockroach customized builds, usually generated by running
               ./scripts/tag-custom-build.sh. Must provide a specific tag.
  local      - Use a provided local binary, must provide the path to the binary.

Usage:
  roachprod deploy <cluster> <application> <version>|<pathToBinary> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
      --grace-period int                                   approx number of seconds to wait for processes to exit, before a forceful shutdown (SIGKILL) is performed (default 300)
  -h, --help                                               help for deploy
      --pause duration                                     duration to pause between node restarts
      --sig int                                            signal to pass to kill (default 15)
      --wait                                               wait for processes to exit (default true)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod init`

```
Initialize the cluster.

The "init" command bootstraps the cluster (using "cockroach init"). It also sets
default cluster settings. It's intended to be used in conjunction with
'roachprod start --skip-init'.

Usage:
  roachprod init <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for init
      --init-target int                                    node on which to run initialization (default 1)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod run`

```
Run a command on the nodes in a cluster.

Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod run marc-test:1-3,8-9

  will perform run on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod run <cluster> <command> [args] [flags]

Aliases:
  run, ssh

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for run
      --insecure                                           use an insecure cluster
      --insecure-ignore-host-key                           don't check ssh host keys (default true)
      --secure                                             use a secure cluster (default true)
  -O, --ssh-options string                                 extra args to pass to ssh
      --tag string                                         the process tag

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod signal`

```
Send a POSIX signal, specified by its integer code, to every process started via roachprod in a cluster.
Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod signal marc-test:1-3,8-9

  will perform signal on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod signal <cluster> <signal> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for signal

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod wipe`

```
Wipe the nodes in a cluster.

The "wipe" command first stops any processes running on the nodes in a cluster
(via the "stop" command) and then deletes the data directories used by the
nodes.

Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod wipe marc-test:1-3,8-9

  will perform wipe on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod wipe <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for wipe
      --insecure-ignore-host-key                           don't check ssh host keys (default true)
      --preserve-certs                                     do not wipe certificates

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod destroy-dns`

```
cleans up DNS entries for the cluster

Usage:
  roachprod destroy-dns <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for destroy-dns

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod reformat`

```

Reformat disks in a cluster to use the specified filesystem.

WARNING: Reformatting will delete all existing data in the cluster.

Filesystem options:
  ext4
  zfs

When running with ZFS, you can create a snapshot of the filesystem's current
state using the 'zfs snapshot' command:

  $ roachprod run <cluster> 'sudo zfs snapshot data1@pristine'

You can then nearly instantaneously restore the filesystem to this state with
the 'zfs rollback' command:

  $ roachprod run <cluster> 'sudo zfs rollback data1@pristine'

Usage:
  roachprod reformat <cluster> <filesystem> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for reformat
      --insecure-ignore-host-key                           don't check ssh host keys (default true)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod install`

```
Install third party software. Currently available installation options are:

    bzip2
    docker
    fluent-bit
    gcc
    go
    haproxy
    nmap
    ntp
    opentelemetry
    postgresql
    sysbench
    vmtouch
    zfs

Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod install marc-test:1-3,8-9

  will perform install on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod install <cluster> <software> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for install
      --insecure-ignore-host-key                           don't check ssh host keys (default true)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod distribute-certs`

```
Distribute certificates to the nodes in a cluster.
If the certificates already exist, no action is taken. Note that this command is
invoked automatically when a secure cluster is bootstrapped by "roachprod
start."

Usage:
  roachprod distribute-certs <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for distribute-certs

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod ssh-keys`

```
manage SSH public keys added to clusters created by roachprod

Usage:
  roachprod ssh-keys [command]

Available Commands:
  list        list every SSH public key installed on clusters managed by roachprod
  add         add a new SSH public key to the set of keys installed on clusters managed by roachprod
  remove      remove public keys belonging to a user from the set of keys installed on clusters managed by roachprod

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for ssh-keys

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)

Use "roachprod ssh-keys [command] --help" for more information about a command.
```

## `roachprod put`

```
Copy a local file to the nodes in a cluster.

Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod put marc-test:1-3,8-9

  will perform put on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod put <cluster> <src> [<dest>] [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for put
      --insecure-ignore-host-key                           don't check ssh host keys (default true)
      --treedist                                           use treedist copy algorithm (default true)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod get`

```
Copy a remote file from the nodes in a cluster. If the file is retrieved from
multiple nodes the destination file name will be prefixed with the node number.

Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod get marc-test:1-3,8-9

  will perform get on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod get <cluster> <src> [<dest>] [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for get
      --insecure-ignore-host-key                           don't check ssh host keys (default true)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod stage`

```
Stages release and edge binaries to the cluster.

Currently available application options are:
  cockroach  - Cockroach nightly builds. Can provide an optional SHA, otherwise
               latest build version is used.
  workload   - Cockroach workload application.
  release    - Official CockroachDB Release. Must provide a specific release
               version.
  customized - Cockroach customized builds, usually generated by running
               ./scripts/tag-custom-build.sh. Must provide a specific tag.
  lib        - Supplementary Cockroach libraries (libgeos).

Some examples of usage:
  -- stage edge build of cockroach build at a specific SHA:
  roachprod stage my-cluster cockroach e90e6903fee7dd0f88e20e345c2ddfe1af1e5a97

  -- Stage the most recent edge build of the workload tool:
  roachprod stage my-cluster workload

  -- Stage the official release binary of CockroachDB at version 2.0.5
  roachprod stage my-cluster release v2.0.5

  -- Stage customized binary of CockroachDB at version v23.2.0-alpha.2-4375-g7cd2b76ed00
  roachprod stage my-cluster customized v23.2.0-alpha.2-4375-g7cd2b76ed00

  -- Stage the most recent edge build of the libraries (libgeos):
  roachprod stage my-cluster lib

Usage:
  roachprod stage <cluster> <application> [<sha/version>] [flags]

Flags:
      --arch string                                        architecture override for staged binaries [amd64, arm64, fips]; N.B. fips implies amd64 with openssl
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --dir string                                         destination for staged binaries
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for stage
      --os string                                          operating system override for staged binaries

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod stageurl`

```
Prints URL for release and edge binaries.

Currently available application options are:
  cockroach  - Cockroach nightly builds. Can provide an optional SHA, otherwise
               latest build version is used.
  workload   - Cockroach workload application.
  release    - Official CockroachDB Release. Must provide a specific release
               version.
  customized - Cockroach customized builds, usually generated by running
               ./scripts/tag-custom-build.sh. Must provide a specific tag.

Usage:
  roachprod stageurl <application> [<sha/version>] [flags]

Flags:
      --arch string                                        architecture override for staged binaries [amd64, arm64, fips]; N.B. fips implies amd64 with openssl
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for stageurl
      --os string                                          operating system override for staged binaries

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod download`

```
Downloads 3rd party tools, using a GCS cache if possible.

Usage:
  roachprod download <cluster> <url> <sha256> [DESTINATION] [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for download

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod sql`

```
Run `cockroach sql` on a remote cluster.

Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod sql marc-test:1-3,8-9

  will perform sql on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod sql <cluster> -- [args] [flags]

Flags:
      --auth-mode string                                   form of authentication to use, valid auth-modes: [root user-password user-cert] (default "user-cert")
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
  -b, --binary string                                      the remote cockroach binary to use (default "cockroach")
      --cluster string                                     specific virtual cluster to connect to
      --database string                                    database to use
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for sql
      --insecure                                           use an insecure cluster
      --insecure-ignore-host-key                           don't check ssh host keys (default true)
      --secure                                             use a secure cluster (default true)
      --sql-instance int                                   specific SQL/HTTP instance to connect to (this is a roachprod abstraction distinct from the internal instance ID)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod ip`

```
Get the IP addresses of the nodes in a cluster.

Usage:
  roachprod ip <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --external                                           return external IP addresses
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for ip
      --insecure-ignore-host-key                           don't check ssh host keys (default true)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod pgurl`

```
Generate pgurls for the nodes in a cluster.

--auth-mode specifies the method of authentication unless --insecure is passed.
Defaults to root if not passed. Available auth-modes are:

	root: authenticates with the root user and root certificates

	user-password: authenticates with the default roachprod user and password

	user-cert: authenticates with the default roachprod user and certificates

Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod pgurl marc-test:1-3,8-9

  will perform pgurl on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod pgurl <cluster> --auth-mode <auth-mode> [flags]

Flags:
      --auth-mode string                                   form of authentication to use, valid auth-modes: [user-cert root user-password] (default "user-cert")
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --certs-dir string                                   cert dir to use for secure connections (default "certs")
      --cluster string                                     specific virtual cluster to connect to
      --database string                                    database to use
      --external                                           return pgurls for external connections
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for pgurl
      --insecure                                           use an insecure cluster
      --insecure-ignore-host-key                           don't check ssh host keys (default true)
      --secure                                             use a secure cluster (default true)
      --sql-instance int                                   specific SQL/HTTP instance to connect to (this is a roachprod abstraction distinct from the internal instance ID)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod adminurl`

```
Generate admin UI URLs for the nodes in a cluster.

Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod adminurl marc-test:1-3,8-9

  will perform adminurl on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9

Usage:
  roachprod adminurl <cluster> [flags]

Aliases:
  adminurl, admin, adminui

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --cluster string                                     specific virtual cluster to connect to
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for adminurl
      --insecure                                           use an insecure cluster
      --insecure-ignore-host-key                           don't check ssh host keys (default true)
      --ips                                                Use Public IPs instead of DNS names in URL
      --open                                               Open the url in a browser
      --path string                                        Path to add to URL (e.g. to open a same page on each node) (default "/")
      --secure                                             use a secure cluster (default true)
      --sql-instance int                                   specific SQL/HTTP instance to connect to (this is a roachprod abstraction distinct from the internal instance ID)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod logs`

```
Retrieve and merge logs in a cluster.

The "logs" command runs until terminated. It works similarly to get but is
specifically focused on retrieving logs periodically and then merging them
into a single stream.

Usage:
  roachprod logs [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --filter string                                      re to filter log messages
      --from time                                          time from which to stream logs (e.g., 2024-09-07T16:05:06Z)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for logs
      --interval duration                                  interval to poll logs from host (default 200ms)
      --logs-dir string                                    path to the logs dir, if remote, relative to username's home dir, ignored if local (default "logs")
      --logs-program string                                regular expression of the name of program in log files to search (default "^cockroach$")
      --to time                                            time to which to stream logs (e.g., 2024-09-07T17:05:06Z); if ommitted, command streams without returning

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod pprof`

```
Capture a pprof profile from the specified nodes.

Examples:

    # Capture CPU profile for all nodes in the cluster
    roachprod pprof CLUSTERNAME
    # Capture CPU profile for the first node in the cluster for 60 seconds
    roachprod pprof CLUSTERNAME:1 --duration 60s
    # Capture a Heap profile for the first node in the cluster
    roachprod pprof CLUSTERNAME:1 --heap
    # Same as above
    roachprod pprof-heap CLUSTERNAME:1

Usage:
  roachprod pprof <cluster> [flags]

Aliases:
  pprof, pprof-heap

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --duration duration                                  Duration of profile to capture (default 30s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
      --heap                                               Capture a heap profile instead of a CPU profile
  -h, --help                                               help for pprof
      --open go tool pprof -http                           Open the profile using go tool pprof -http
      --starting-port int                                  Initial port to use when opening pprof's HTTP interface (default 9000)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod cached-hosts`

```
list all clusters (and optionally their host numbers) from local cache

Usage:
  roachprod cached-hosts [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --cluster string                                     print hosts matching cluster
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for cached-hosts

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod version`

```
print version information

Usage:
  roachprod version [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for version

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod get-providers`

```
print providers state (active/inactive)

Usage:
  roachprod get-providers [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for get-providers

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod grafana-start`

```
spins up a prometheus and grafana instance on the last node in the cluster; NOTE: for arm64 clusters, use --arch arm64

Usage:
  roachprod grafana-start <cluster> [flags]

Flags:
      --arch string                                        binary architecture override [amd64, arm64]
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
      --grafana-config string                              URI to grafana json config, supports local and http(s) schemes
  -h, --help                                               help for grafana-start

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod grafana-stop`

```
spins down prometheus and grafana instances on the last node in the cluster

Usage:
  roachprod grafana-stop <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for grafana-stop

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod grafana-dump`

```
dump prometheus data to the specified directory

Usage:
  roachprod grafana-dump <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --dump-dir string                                    the absolute path to dump prometheus data to (use the contained 'prometheus-docker-run.sh' to visualize
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for grafana-dump

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod grafanaurl`

```
returns a url to the grafana dashboard

Usage:
  roachprod grafanaurl <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for grafanaurl
      --open                                               Open the url in a browser

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod grafana-annotation`

```
Adds an annotation to the specified grafana instance

By default, we assume the grafana instance needs an authentication token to connect
to. Unless the GOOGLE_EPHEMERAL_CREDENTIALS environment variable exists, the default Google Application Credentials
will be used to derive an Access Token to authenticate against Google Identity-Aware Proxy.
Use the --insecure option when a token is not necessary.

--tags specifies the tags the annotation should have.

--dashboard-uid specifies the dashboard you want the annotation to be created in. If
left empty, creates the annotation in the organization instead.

--time-range can be used to specify in epoch millisecond time the annotation's timestamp.
If left empty, creates the annotation at the current time. If only start-time is specified,
creates an annotation at start-time. If both start-time and end-time are specified,
creates an annotation over time range.

Example:
# Create an annotation over time range 1-100 on the centralized grafana instance, which needs authentication.
roachprod grafana-annotation grafana.testeng.crdb.io example-annotation-event --tags my-cluster --tags test-run-1 --dashboard-uid overview --time-range 1,100

Usage:
  roachprod grafana-annotation <host> <text> --tags [<tag1>, ...] --dashboard-uid <dashboard-uid> --time-range [<start-time>, <end-time>] [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --dashboard-uid string                               grafana dashboard UID
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for grafana-annotation
      --insecure                                           use an insecure cluster
      --secure                                             use a secure cluster (default true)
      --tags stringArray                                   grafana annotation tags
      --time-range int64Slice                              grafana annotation time range in epoch time (default [])

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod pyroscope`

```
Manage the Pyroscope continuous profiling stack for a cluster.

Usage:
  roachprod pyroscope [command]

Available Commands:
  start        start the pyroscope stack using docker compose on the target node
  stop         stop the pyroscope stack and remove containers
  add-nodes    add target nodes to the pyroscope profiling configuration
  remove-nodes remove target nodes from the pyroscope profiling configuration
  list-nodes   list nodes currently being scraped by the pyroscope profiling configuration
  init-target  initialize or re-initialize a target cluster config for Pyroscope profiling

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for pyroscope

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)

Use "roachprod pyroscope [command] --help" for more information about a command.
```

## `roachprod storage`

```
storage enables administering storage related commands and configurations

Usage:
  roachprod storage [command]

Available Commands:
  collection  the collection command allows for enable or disabling the storage workload collector for a provided cluster (including a subset of nodes). The storage workload collection is defined in pebble replay/workload_capture.go.

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for storage

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)

Use "roachprod storage [command] --help" for more information about a command.
```

## `roachprod snapshot`

```
snapshot enables creating/listing/deleting/applying cluster snapshots

Usage:
  roachprod snapshot [command]

Available Commands:
  create      snapshot a named cluster, using the given snapshot name and description
  list        list all snapshots for the given cloud provider, optionally filtering by the given name
  delete      delete all snapshots for the given cloud provider optionally filtering by the given name
  apply       apply the named snapshots from the given cloud provider to the named cluster

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for snapshot

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)

Use "roachprod snapshot [command] --help" for more information about a command.
```

## `roachprod update`

```
Attempts to download the latest roachprod binary (on master) from gs://cockroach-nightly.  Swaps the current binary with it. The current roachprod binary will be backed up and can be restored via `roachprod update --revert`.

Usage:
  roachprod update [flags]

Flags:
  -a, --arch string                                        CPU architecture (default "amd64")
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
  -b, --branch string                                      git branch (default "master")
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for update
  -o, --os string                                          OS (default "linux")
      --revert                                             restore roachprod to the previous version which would have been renamed to roachprod.bak during the update process

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod jaeger-start`

```
starts a jaeger container on the last node in the cluster

Usage:
  roachprod jaeger-start <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --cluster string                                     specific virtual cluster to connect to
      --configure-nodes string                             the nodes on which to set the relevant CRDB cluster settings
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for jaeger-start
      --insecure                                           use an insecure cluster
      --secure                                             use a secure cluster (default true)
      --sql-instance int                                   specific SQL/HTTP instance to connect to (this is a roachprod abstraction distinct from the internal instance ID)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod jaeger-stop`

```
stops a running jaeger container on the last node in the cluster

Usage:
  roachprod jaeger-stop <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for jaeger-stop

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod jaegerurl`

```
returns the URL of the cluster's jaeger UI

Usage:
  roachprod jaegerurl <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for jaegerurl
      --open                                               Open the url in a browser

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod fluent-bit-start`

```
Install and start Fluent Bit

Usage:
  roachprod fluent-bit-start <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --datadog-api-key string                             Datadog API key
      --datadog-service string                             Datadog service name for emitted logs (default "cockroachdb")
      --datadog-site string                                Datadog site to send telemetry data to (e.g., us5.datadoghq.com) (default "us5.datadoghq.com")
      --datadog-tags strings                               Datadog tags as a comma-separated list in the format KEY1:VAL1,KEY2:VAL2
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for fluent-bit-start

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod fluent-bit-stop`

```
Stop Fluent Bit

Usage:
  roachprod fluent-bit-stop <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for fluent-bit-stop

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod opentelemetry-start`

```
Install and start the OpenTelemetry Collector

Usage:
  roachprod opentelemetry-start <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --datadog-api-key string                             Datadog API key
      --datadog-site string                                Datadog site to send telemetry data to (e.g., us5.datadoghq.com) (default "us5.datadoghq.com")
      --datadog-tags strings                               Datadog tags as a comma-separated list in the format KEY1:VAL1,KEY2:VAL2
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for opentelemetry-start

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod opentelemetry-stop`

```
Stop the OpenTelemetry Collector

Usage:
  roachprod opentelemetry-stop <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for opentelemetry-stop

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod parca-agent-start`

```
Install and start the Parca Agent

Usage:
  roachprod parca-agent-start <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for parca-agent-start
      --parca-agent-token string                           Parca Agent Token

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod parca-agent-stop`

```
Stop the Parca Agent

Usage:
  roachprod parca-agent-stop <cluster> [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for parca-agent-stop

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod fetchlogs`

```
Download the logs from the cluster using "roachprod get".

The logs will be placed in the directory if specified or in the directory named as <clustername>_logs.

Usage:
  roachprod fetchlogs <cluster> <destination (optional)> [flags]

Aliases:
  fetchlogs, getlogs

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for fetchlogs
  -t, --timeout duration                                   Timeout for fetching the logs from the cluster nodes (default 5m0s)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod get-latest-pprof`

```
Downloads the latest pprof file which is created on or before the provided time-before.
The time should be of the format 2022-08-31T15:23:22Z for UTC or 2022-08-31T15:23:22+05:30 for time zone.
If the time is not provided, it downloads the latest pprof file across all clusters.

Usage:
  roachprod get-latest-pprof <cluster> [time-before] [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for get-latest-pprof

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod fetch-certs`

```

Downloads the PGUrl certs directory from the cluster. In addition to downloading the
certs, it also makes sure the files are not world readable so lib/pq doesn't complain.
If a destination is not provided, the certs will be downloaded to a default certs directory.

--certs-dir: specify the directory to download the certs from

Usage:
  roachprod fetch-certs <cluster> [<dest-dir>] [flags]

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for fetch-certs

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod chaos`

```
Failure injection related commands for testing cluster resilience.

Failure injection commands allow you to inject various types of failures into a
cluster to test its behavior under adverse conditions. Each failure type has its
own lifecycle: Setup → Inject → Wait → Recover → Cleanup.

Global flags control the duration and cleanup behavior of all chaos commands.

Usage:
  roachprod chaos [command]

Available Commands:
  network-partition Creates network partitions between nodes using iptables
  network-latency   Injects artificial network latency between nodes using tc and iptables
  disk-stall        Injects disk stalls to test storage resilience
  process-kill      Kills the cockroach process on specified nodes
  reset-vm          Resets VMs on specified nodes to test recovery behavior

Flags:
      --aws-config aws config path                         Path to json for aws configuration, defaults to predefined configuration (default see config.json)
      --aws-profile string                                 Profile to manage cluster in (default "CRLShared-541263489771")
      --azure-sync-delete                                  Wait for deletions to finish before returning
      --azure-timeout duration                             The maximum amount of time for an Azure API operation to take (default 10m0s)
      --certs-dir string                                   local path to certs directory for secure clusters (default "certs")
      --gce-default-project string                         google cloud project to use to run core roachprod services (default "cockroach-ephemeral")
      --gce-dns-domain string                              zone domian in gcloud project to use to set up public DNS records (default "roachprod.crdb.io")
      --gce-dns-project string                             project to use to set up DNS (default "cockroach-shared")
      --gce-dns-zone string                                zone file in gcloud project to use to set up public DNS records (default "roachprod")
      --gce-managed-dns-domain string                      zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed.crdb.io")
      --gce-managed-dns-zone string                        zone file in gcloud project to use to set up DNS SRV records (default "roachprod-managed")
      --gce-metadata-project string                        google cloud project to use to store and fetch SSH keys (default "cockroach-ephemeral")
      --gce-project comma-separated list of GCE projects   List of GCE projects to manage (default cockroach-ephemeral)
  -h, --help                                               help for chaos
      --replication-factor int                             expected replication factor for the cluster (0 = use default of 3)
      --run-forever                                        if set, takes precedence over --wait-before-cleanup. On graceful shutdown, cleans up the injected failure
      --stage string                                       lifecycle stage to execute. Options:
                                                             - all: runs the complete lifecycle (Setup → Inject → Wait → Recover → Cleanup)
                                                             - setup: runs only the setup phase (prepares failure dependencies)
                                                             - inject: runs only the inject phase (activates the failure)
                                                             - recover: runs only the recover phase (removes the failure)
                                                             - cleanup: runs only the cleanup phase (removes failure dependencies)
                                                             - inject-recover: runs inject, waits for --wait-before-cleanup or --run-forever, then recovers
                                                           Default: all (default "all")
      --verbose                                            if set, prints verbose logs from failure-injection library
      --wait-before-cleanup duration                       time to wait before cleaning up the failure (default 5m0s)

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)

Use "roachprod chaos [command] --help" for more information about a command.
```

## `roachprod help`

```
Help provides help for any command in the application.
Simply type roachprod help [path to command] for full details.

Usage:
  roachprod help [command] [flags]

Flags:
  -h, --help   help for help

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)
```

## `roachprod completion`

```
Generate the autocompletion script for roachprod for the specified shell.
See each sub-command's help for details on how to use the generated script.

Usage:
  roachprod completion [command]

Available Commands:
  bash        Generate the autocompletion script for bash
  zsh         Generate the autocompletion script for zsh
  fish        Generate the autocompletion script for fish
  powershell  Generate the autocompletion script for powershell

Flags:
  -h, --help   help for completion

Global Flags:
      --email-domain string   email domain for users (default "@cockroachlabs.com")
      --fast-dns              enable fast DNS resolution via the standard Go net package
      --max-concurrency int   maximum number of operations to execute on nodes concurrently, set to zero for infinite (default 32)
  -q, --quiet                 disable fancy progress output (default true)
      --use-shared-user       use the shared user "ubuntu" for ssh rather than your user "wchoe" (default true)

Use "roachprod completion [command] --help" for more information about a command.
```

