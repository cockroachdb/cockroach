***WARNING*** Use this only for development or testing. This is currently partly
functional due to GCE network load balancing configuration. Also, this does not
yet support the creation of secure clusters.

# Deploy cockroach cluster on Google Cloud Engine using Terraform

This directory contains the [Terraform](https://terraform.io/) configuration
files needed to launch a Cockroach cluster on GCE (Google Compute Engine).

Terraform stores the state of the cloud resources locally (in a file called 
`terraform.tfstate`), so this is meant to be used by a single user.
For multi-user cooperation, please see [Terraform's documentation on remote state](https://terraform.io/docs/state/remote.html).

## One-time setup steps
1. Have a [Google Cloud Platform](https://cloud.google.com/compute/) account
2. [Download terraform](https://terraform.io/downloads.html), *version 0.6.7 or greater*, unzip, and add to your `PATH`.
3. [Create and download GCE credentials](https://developers.google.com/identity/protocols/application-default-credentials#howtheywork).

## Variables

Some configuration can be performed by using the `--var` command line parameter
to override the variables in `variables.tf`.

The following variables are likely to change based on your account or setup:
* `zone`: availability zone for instances
* `gce_region`: region for forwarding rules and target pools
* `key_name`: base name of the Google Cloud SSH key
* `gce_project`: name of the GCE project for your instances
* `gce_account_file`: JSON-formatted Google Cloud app credentials

The following variables can be modified if necessary.
* `sql_port`: the port for the backends and load balancer
* `machine_type`: type of machine to run instances on
* `gce_image`: OS image for your GCE instances
* `action`: default action. Defaults to `start`. Override is specified in
  initialization step

## Create the cluster

The following command will initialize all needed GCE infrastructure in the region
`us-east-1b` and start a 3-node CockroachDB cluster:

```
terraform apply --var-file=samples/3_node_cluster.tfvars
```


To see the actions expected to be performed by terraform, use `plan` instead of `apply`.

#### Create a cockroach cluster with 3 nodes

```
$ terraform apply --var-file=samples/3_node_cluster.tfvars

Outputs:

  admin_url = http://104.196.130.132:8080/
  instances = cockroach-0, cockroach-1, cockroach-2
  sql_url   = postgresql://root@104.196.138.149:26257/?sslmode=disable
```

The cluster is now running with three nodes and is reachable through the any of the `instances` 
or through the provided URLs (see `Using the cluster`).

## Use the cluster

#### Connect to the cluster

Use the `sql_url` in the terraform output to issue SQL queries to your new
cluster:

```
$ cockroach sql --url postgresql://root@104.196.138.149:26257/?sslmode=disable
# Welcome to the cockroach SQL interface.
# All statements must be terminated by a semicolon.
# To exit: CTRL + D.
root@104.196.138.149:26257> show databases;
+----------+
| Database |
+----------+
| system   |
+----------+
```

You can view the administrative UI by going to the `admin_url` given in the 
terraform output.

#### SSH into individual instances

The names of the GCE instances are shown as a comma-separated list in the
terraform output. Use the `gcloud` tool, included with the [Google Cloud SDK](https://cloud.google.com/sdk/#Quick_Start),
to SSH into one of the machines:

```
$ gcloud compute ssh ubuntu@cockroach-1 --zone=us-east1-b

ubuntu@cockroach-1:~$ ps -Af|grep cockroach
ubuntu    1500     1  0 15:16 ?        00:00:01 ./cockroach start --log-dir=cockroach-data/logs --logtostderr=false --insecure --host=10.142.0.4 --port=26257 --http-port=8080 --join=10.142.0.3

ubuntu@cockroach-1:~$ ls logs
$ ls cockroach-data/logs
cockroach.cockroach-1.ubuntu.log.INFO.2016-04-06T15_16_45Z.1500     cockroach.INFO    cockroach.STDOUT
cockroach.cockroach-1.ubuntu.log.WARNING.2016-04-06T15_16_45Z.1500  cockroach.STDERR  cockroach.WARNING
```

Note the `ubuntu` user in the above command-line.

## Destroy the cluster

```
$ terraform destroy --var-file=samples/3_node_cluster.tfvars
```

The `destroy` command requires confirmation.