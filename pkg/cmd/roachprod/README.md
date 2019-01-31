## roachprod

⚠️ roachprod is an **internal** tool for creating and testing
CockroachDB clusters. Use at your own risk! ⚠️

## Setup

1. Make sure you have [gcloud installed] and configured (`gcloud auth list` to
check, `gcloud auth login` to authenticate). You may want to update old
installations (`gcloud components update`).
1. Build a local binary of `roachprod`: `make bin/roachprod`
1. Add `$PWD/bin` to your `PATH` so you can run `roachprod` from the root directory of `cockroach`.

## Summary

* By default, clusters are created in the [cockroach-ephemeral] GCE
  project. Use the `--gce-project` flag or `GCE_PROJECT` environment
  variable to create clusters in a different GCE project. Note that
  the `lifetime` functionality requires `roachprod gc
  --gce-project=<name>` to be run periodically (i.e. via a
  cronjob). This is only provided out-of-the-box for the
  [cockroach-ephemeral] cluster.
* Anyone can connect to any port on VMs in [cockroach-ephemeral].
  **DO NOT STORE SENSITIVE DATA**.
* Cluster names are prefixed with the user creating them. For example,
  `roachprod create test` creates the `marc-test` cluster.
* VMs have a default lifetime of 12 hours (changeable with the
  `--lifetime` flag).
* Default settings create 4 VMs (`-n 4`) with 4 CPUs, 15GB memory
  (`--machine-type=n1-standard-4`), and local SSDs (`--local-ssd`).

## Cluster quick-start using roachprod

```bash
# Create a cluster with 4 nodes and local SSD. The last node is used as a
# load generator for some tests. Note that the cluster name must always begin
# with your username.
export CLUSTER="${USER}-test"
roachprod create ${CLUSTER} -n 4 --local-ssd

# Add gcloud SSH key.
ssh-add ~/.ssh/google_compute_engine

# Stage binaries.
roachprod stage ${CLUSTER} workload
roachprod stage ${CLUSTER} release v2.0.5

# ...or using roachprod directly (e.g., for your locally-built binary).
roachprod put ${CLUSTER} cockroach

# Start a cluster.
roachprod start ${CLUSTER}

# Check the admin UI.
# http://35.196.94.196:26258

# Open a SQL connection to the first node.
cockroach sql --insecure --host=35.196.94.196

# Extend lifetime by another 6 hours.
roachprod extend ${CLUSTER} --lifetime=6h

# Destroy the cluster.
roachprod destroy ${CLUSTER}
```

## Command reference

Warning: this reference is incomplete. Be prepared to refer to the CLI help text
and the source code.

### Create a cluster

```
$ roachprod create foo
Creating cluster marc-foo with 3 nodes
OK
marc-foo: 23h59m42s remaining
  marc-foo-0000   [marc-foo-0000.us-east1-b.cockroach-ephemeral]
  marc-foo-0001   [marc-foo-0001.us-east1-b.cockroach-ephemeral]
  marc-foo-0002   [marc-foo-0002.us-east1-b.cockroach-ephemeral]
Syncing...
```

### Interact using crl-prod tools

`roachprod` populates hosts files in `~/.roachprod/hosts`. These are used by
`crl-prod` tools to map clusters to node addresses.

```
$ crl-ssh marc-foo all df -h /
1: marc-foo-0000.us-east1-b.cockroach-ephemeral
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1        49G  1.2G   48G   3% /

2: marc-foo-0001.us-east1-b.cockroach-ephemeral
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1        49G  1.2G   48G   3% /

3: marc-foo-0002.us-east1-b.cockroach-ephemeral
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1        49G  1.2G   48G   3% /
```

### Interact using `roachprod` directly

```
# Add ssh-key
$ ssh-add ~/.ssh/google_compute_engine

$ roachprod status marc-foo
marc-foo: status 3/3
   1: not running
   2: not running
   3: not running
```

### SSH into hosts

`roachprod` uses `gcloud` to sync the list of hostnames to `~/.ssh/config` and
set up keys.

```
$ ssh marc-foo-0000.us-east1-b.cockroach-ephemeral
```

### List clusters

```
$ roachprod list
marc-foo: 23h58m27s remaining
  marc-foo-0000
  marc-foo-0001
  marc-foo-0002
Syncing...
```

### Destroy cluster

```
$ roachprod destroy marc-foo
Destroying cluster marc-foo with 3 nodes
OK
```

See `roachprod help <command>` for further details.


# Future improvements

* Bigger loadgen VM (last instance)

* Ease the creation of test metadata and then running a series of tests
  using `roachprod <cluster> test <dir1> <dir2> ...`. Perhaps something like
  `roachprod prepare <test> <binary>`.

* Automatically detect stalled tests and restart tests upon unexpected
  failures. Detection of stalled tests could be done by noticing zero output
  for a period of time.

* Detect crashed cockroach nodes.

[cockroach-ephemeral]: https://console.cloud.google.com/home/dashboard?project=cockroach-ephemeral
[gcloud installed]: https://cloud.google.com/sdk/downloads
