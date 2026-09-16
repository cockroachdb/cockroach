# Scenario-based testing for CockroachDB

This directory contains example configurations to test CockroachDB
operational scenarios using the
[`shakespeare`](https://github.com/knz/shakespeare) tool.

## Setting up the environment

When using scenario tests that only run on the local machine,
set the env var `$COCKROACH` to the crdb binary to use.

When using `roachprod`-based tests, the scenarios assume
that a cluster already exists. For the TPC-C tests,
the scenarios also assume TPC-C is already loaded.

This requires several pre-test manual steps, but scripts are provided
to facilitate these steps.

- to generate the scripts, perform once:

  1. set the env vars `ROACHPROD` and `COCKROACH` to the paths to
     the `roachprod` and `cockroach` binaries, respectively.
  2. set the env var `ROACHPROD_HOME` to the roachprod home directory,
     typically `$HOME/.roachprod`.
  3. set the env var `COCKROACH_DEV_LICENSE` to an enterprise license
     string.
  4. optional: set env `ROACHPROD_USER`, if the local username is not
     the cloud username.
  5. select a cluster size (`small`, `medium`, `large`) and a topology
     (`-h` for homogeneous, `-g` for geo-distributed)
  6. run `mkenv.sh <config>` (e.g. `mkenv.sh small-g`).

  This generates cluster configuration scripts in
  `$ROACHPROD_HOME/envs/<config>` and these scripts can be reused
  henceforth. The values of all the environment variables are
  preserved inside this directory and will thus persist across
  shell sessions.

- initialize a VM pool, crdb cluster and TPC-C workload by running the
  following commands:

         eval $(~/.roachprod/envs/<config>/bin/setenv.sh)
         export PATH=$CLUSTER_ENV/bin:$PATH
         1-create-vms.sh
         2-stage-cockroach-binary.sh
         3-initial-upreplication.sh
         4-initial-zone-config.sh
         # for TPC-C tests using up to 100 warehouses:
         5-import-tpcc-fixtures.sh 100
         # if planning to run multiple scenario tests, or the same test multiple times:
         6-snapshot-data.sh

  Notes:

  - all the scripts keep a log of which script was run and when,
    in the file `~/.roachprod/envs/<config>/action-log.txt`.

  - if step 1 "create vms" times out while attempting to create on
    AWS, this may be a symptom of a misconfigured SSH key. `roachprod`
    is unfortunately silent in that case.

  - if step 3 "initial replication" fails or takes too long,
    this is a known issue. The easiest is to:

	1. interrupt the wait (eg ctrl+c)
	2. run `quit-crdb.sh` to stop the nodes
	3. retry `3-initial-upreplication.sh`

  - if step 5 "import" fails with some error, optionally use
    `get-logs.sh` to investigate, then retry the import with:

        wipe-crdb.sh
        3-initial-upreplication.sh
        4-initial-zone-config.sh
        5-import-tpcc-fixtures.sh NNN

  - step 6 snapshotting is strongly recommended! however it requires
    more than 50% free space on the data store directory on every
    node. With the default VM configuration, this space will be
    available up to about 5000 TPC-C warehouses.

## Running scenario tests

Each directory defines one test scenario. The directory name is
arbitrary but some attempt was made to name it after the main traits
of the scenario.

In each directory the scenario can be executed with:

     # Choose an execution environment.
     eval $(~/.roachprod/envs/xxxx/bin/setenv.sh)
	 # Run the scenario.
     .../shakespeare conf.cfg -I../include
     # (or alternatively `go run .../shakespeare.go`)

To upload artifacts at the end of "interesting" tests use

    --upload-url gs://shakespeare-artifacts.crdb.io/public/yourname

(You can then replace `gs://` by `http://` to access the reports in a
browser.)

