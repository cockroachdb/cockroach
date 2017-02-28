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
2. [Download terraform](https://terraform.io/downloads.html), *version 0.7.2 or greater*, unzip, and add to your `PATH`.
3. [Create and download GCE credentials](https://developers.google.com/identity/protocols/application-default-credentials#howtheywork).
4. Set your credentials in environment variables:
```
$ export GOOGLE_CREDENTIALS="contents of json credentials file"
$ export CLOUDSDK_CORE_PROJECT="my-google-project"
```
5. Create a new pair of ssh keys `ssh-keygen -t rsa -C "your-email"` for GCE and save them in `~/.ssh/google_compute_engine`,
or use an existing pair and adjust the `key_name` variable described below. Do not use a passphrase while generating the keys
or else your private key will be encrypted. You have to use an unencrypted private key with terraform. If you already have a
key and it is encrypted, you can [decrypt](http://support.citrix.com/article/CTX122930) it.
6. Add the contents of your public key `~/.ssh/google_compute_engine.pub` to GCE using the web UI `Compute Engine > Metadata > ssh-keys`.

## Variables

Some configuration can be performed by using the `--var` command line parameter
to override the variables in `variables.tf`.

The following variables are likely to change based on your account or setup:
* `gce_zone`: availability zone for instances
* `gce_region`: region for forwarding rules and target pools
* `key_name`: base name of the Google Cloud SSH key

The following variables can be modified if necessary:
* `sql_port`: the port for the backends and load balancer
* `machine_type`: type of machine to run instances on
* `gce_image`: OS image for your GCE instances
* `action`: default action. Defaults to `start`. Override is specified in
  initialization step


#### Create a cockroach cluster with 3 nodes

```
$ terraform apply --var=num_instances=\"3\"


Outputs:

  instances = 104.196.43.55,104.196.19.237,104.196.107.237
```

To see the actions that will be performed by terraform, use `plan` instead of `apply`.

The cluster is now running with three nodes and is reachable through the any of the `instances`.

## Using the cluster

#### Connect to the cluster

Use one of the `instances` in the terraform output to issue SQL queries
to your new cluster:

```
$ cockroach sql --url postgresql://root@104.196.43.55:26257/?sslmode=disable
# Welcome to the cockroach SQL interface.
# All statements must be terminated by a semicolon.
# To exit: CTRL + D.
root@104.196.43.55:26257> show databases;
+----------+
| Database |
+----------+
| system   |
+----------+
```

#### View the admin UI

To view the admin web UI, visit any of the `instances` on the `http_port` (default 8080).

#### SSH into individual instances

The names of the GCE instances are shown as a comma-separated list in the
terraform output. Use the `gcloud` tool, included with the [Google Cloud SDK](https://cloud.google.com/sdk/#Quick_Start),
to SSH into one of the machines:

```
$ ssh -i ~/.ssh/google_compute_engine ubuntu@104.196.43.55

ubuntu@cockroach-1:~$ ps -Af|grep cockroach
ubuntu    1500     1  0 15:16 ?        00:00:01 ./cockroach start --log-dir=cockroach-data/logs --logtostderr=false --insecure --host=10.240.0.12 --port=26257 --http-port=8080

ubuntu@cockroach-1:~$ ls logs
$ ls cockroach-data/logs
cockroach.cockroach-1.ubuntu.log.INFO.2016-04-06T15_16_45Z.1500     cockroach.INFO    cockroach.STDOUT
cockroach.cockroach-1.ubuntu.log.WARNING.2016-04-06T15_16_45Z.1500  cockroach.STDERR  cockroach.WARNING
```

Note the `ubuntu` user in the above command-line.

## Destroy the cluster

```
$ terraform destroy

```

The `destroy` command requires confirmation.
