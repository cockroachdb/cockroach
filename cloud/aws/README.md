# Deploy cockroach cluster on AWS using Terraform

This directory contains the [Terraform](https://terraform.io/) configuration
files needed to launch a cockroach cluster on AWS.

Terraform stores the state of the cloud resources locally (in a file called `terraform.tfstate`),
so this is meant to be used by a single user.
For multi-user cooperation, please see [Terraform's documentation on remote state](https://terraform.io/docs/state/remote.html).

## One-time setup steps
1. Have an [AWS](http://aws.amazon.com/) account
2. [Download terraform](https://terraform.io/downloads.html), *version 0.6.7 or greater*, unzip, and add to your `PATH`.
3. [Create and download AWS credentials](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-set-up.html#cli-signup).
4. [Save credentials in ~/.aws/credentials](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-config-files).
5. [Create an AWS keypair](https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#KeyPairs:sort=keyName) named `cockroach` and save the file as `~/.ssh/cockroach.pem`. If sharing an account with other users, you may want to customize the key name (eg: key_name:`cockroach-<myusername>`, key path:`~/.ssh/cockroach-<myusername>.pem`) and modify the variable as mentioned in the next section.

## Variables

Some configuration can be performed through the use of variables in `variables.tf`.

The following variables are likely to change based on your account or setup:
* `aws_availability_zone`: availability zone for instances and load balancer
* `key_name`: base name of the AWS key

The following variables can be modified if necessary.
* `cockroach_port`: the port for the backends and load balancer
* `aws_region`: region to run in. Affects `aws_availability_zone` and `aws_ami_id`
* `aws_ami_id`: image ID. depends on the region.
* `action`: default action. Defaults to `start`. Override is specified in initialization step

## Create the cluster

The following command will initialize all needed AWS infrastructure in the region `us-east-1`,
initialize the first cockroach node, then add two more nodes to the cluster.
All dynamic configuration is set through terraform command-line flags but can be set in `variables.tf`.

To see the actions expected to be performed by terraform, use `plan` instead of `apply`.

#### Create a cockroach cluster with 3 nodes

```
$ terraform apply --var=num_instances=3

Outputs:
  example_block_writer =
  instances            = ec2-54-152-252-37.compute-1.amazonaws.com
```

The cluster is now running with three nodes and is reachable through the any of the `instances` (see `Using the cluster`).

To add more nodes, simply rerun the command with `--var=num_instances` set to a different number.

## Use the cluster

#### Connect to the cluster

Use any of the instances listed by terraform.

```
$ ./cockroach sql --insecure --host=<hostname of one instance from the terraform output>
root@ec2-54-152-252-37.compute-1.amazonaws.com:26257> show databases;
+----------+
| Database |
+----------+
| system   |
+----------+
```

#### Ssh into individual instances

The DNS names of AWS instances is shown as a comma-separated list in the terraform output.

```
$ ssh -i ~/.ssh/cockroach.pem ubuntu@ec2-54-85-12-159.compute-1.amazonaws.com

ubuntu@ip-172-31-15-87:~$ ps -Af|grep cockroach
ubuntu    1448     1  4 20:03 ?        00:00:39 ./cockroach start --log-dir=logs --logtostderr=false --store=path=data --insecure --join=ec2-54-152-252-37.compute-1.amazonaws.com

ubuntu@ip-172-31-15-87:~$ ls logs
cockroach.ERROR
cockroach.INFO
cockroach.ip-172-31-15-87.ubuntu.log.ERROR.2015-11-02T20_03_14Z.1448
cockroach.ip-172-31-15-87.ubuntu.log.INFO.2015-11-02T20_03_09Z.1443
cockroach.ip-172-31-15-87.ubuntu.log.INFO.2015-11-02T20_03_09Z.1448
cockroach.ip-172-31-15-87.ubuntu.log.WARNING.2015-11-02T20_03_09Z.1448
cockroach.STDERR
cockroach.STDOUT
cockroach.WARNING

```

#### Profile servers

Using either the ELB address (will hit a random node), or a specific instance:
```
$ go tool pprof <address:port>/debug/pprof/profile
```

#### Running examples against the cockroach cluster

See `examples.tf` for sample examples and how to run them against the created cluster.
The `block_writer` can be run against the newly-created cluster by running:
```
$ terraform apply --var=num_instances=3 --var=example_block_writer_instances=1

Outputs:
  example_block_writer = ec2-54-175-206-76.compute-1.amazonaws.com
  instances            = ec2-54-152-252-37.compute-1.amazonaws.com,ec2-54-175-103-126.compute-1.amazonaws.com,ec2-54-175-166-150.compute-1.amazonaws.com
```

## Destroy the cluster

```
$ terraform destroy --var=num_instances=3
```

The destroy command requires confirmation.
