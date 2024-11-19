# Overview

`roachtest` is a tool for performing large-scale (multi-machine)
automated tests. It relies on the concurrently-developed (but
separate) library `roachprod`.

# Setup

- [Set up `roachprod`](https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachprod/README.md), if you haven't already.
- If you want to run tests that require an Enterprise license, add `COCKROACH_DEV_LICENSE=<key>` to your path. Cockroach Labs employees can find one in [Slack](https://cockroachlabs.slack.com/archives/CJX8V9SJ2/p1597348203368800?thread_ts=1597348076.367800&cid=CJX8V9SJ2).
- For Cockroach Labs employees, if your local username does not match your
  gcloud/`@cockroachlabs.com` username, add
`ROACHPROD_USER=<your_crl_username>` to your environment, or add the
`--user=$CRL_USERNAME` flag to the `roachtest run` invocations. Non-Cockroach
Labs employees will want to set `--gce-project` (and maybe `--user` too) as the
default project will not be accessible to them.

# Usage

`roachtest` has the list of tests it supports compiled in. They can be listed via:

```
$ roachtest list
acceptance/build-analyze [server]
acceptance/build-info [server]
acceptance/cli/node-status [server]
[...]
```

The list can be filtered by passing a regular expression to the `list` command which will match against the test name.
Multiple `tag:` prefixed args can be specified to further narrow by which tags are present for a test. The following 
will list all tests with name containing `admission` where test tags match `(weekly && aws) || my-tag`

```
roachtest list admission tag:weekly,aws tag:my-tag
```


## Getting the binaries

To run a test, the `roachtest run` command is used. Since a test typically
deploys a CockroachDB cluster, `roachtest` needs to know which cockroach binary
to use. It also generically requires the `workload` binary which is used by many 
tests to deploy load against the cluster. To that effect, `roachtest run` takes 
the `--cockroach`, and `--workload` flags. The default values for these are set up
so that they do not need to be specified in the common case. Besides, when
the binaries have nonstandard names, it is often more convenient to move them
to the location `roachtest` expects (it will tell you) rather than to specify
the flags. However, when multiple binaries are around, it might be preferable
to be explicit, to avoid accidentally running tests against against the wrong
version of CockroachDB.

### Building your own

The `cockroach` and `workload` binaries need to be built for the architecture
of the target cluster. In most cases, this means `amd64-linux-gnu`, i.e. OSX
users have to either download the binary they need from somewhere (see below)
or cross-compile the binary locally, which unfortunately takes a long time (due
to the overhead of virtualization for Docker on Mac).

As a general rule of thumb, if running against "real" VMs, and the version to
test is checked out, the following will build the right binaries and put them
where `roachtest` will find them:

`dev build --cross cockroach workload`

`roachtest` will look first in `$PATH`, then (except when `--local` is
specified) in `bin.docker_amd64` in the repo root, followed by `bin`. This
is complicated enough to be surprising, which might be another reason to
pass the `--cockroach` and  `--workload` flags explicitly.

Some roachtests can also be run with the `--local` flag, i.e. will use a
cluster on the local machine created via `roachprod create local`. In that
case, the `cockroach` and `workload` binary must target the local architecture,
which means

`dev build cockroach workload`

will do the trick. However, most roachtests don't make a lot of sense in local
mode or will do outright dangerous things (like trying to install packages on
the system), so don't use this option unless you know what you're doing. Often
the `--local` option is used in development.

### Getting them from elsewhere

Especially for OSX users, building a linux CockroachDB binary is an extremely time-consuming process (think 30 minutes). If the version to be tested is a released version, a somewhat convenient way of getting a binary is to run

```
# Make a single-node local cluster.
roachprod create -n 1 local
# Tell roachprod to place a CockroachDB 23.1.12 Linux binary onto node 1.
# Alternatively you can choose any other recent release from the
# releases page: https://www.cockroachlabs.com/docs/releases/
roachprod stage local release v23.1.12 --os linux --arch amd64
# Copy it from (local) node 1 to the current directory
mv ~/local/1/cockroach cockroach
# Can now use roachtest with the `--cockroach=./cockroach` flag.
```

This doesn't work for the `workload` binary but this builds a lot faster anyway
(via `dev build --cross workload`).  The above strategy also works for recent
SHAs from the master/release branches, where the incantation is
`roachprod stage local cockroach <SHA>`; the SHA is optional and defaults to a
recent build from the `master` branch, which may not be what you want. As a bonus,
`roachprod stage local workload <SHA> --os linux` does allow getting a `workload`
binary as well, meaning you can run a roachtest without compiling anything yourself.

## Running a test

With the binaries in place, it should be possible to run a simple test.
As a smoke test, the following should work and not take too long:

```
$ roachtest run acceptance/build-info
```

If this doesn't work, the output should tell you why. Perhaps the binaries you
use are not in the canonical locations and need to be specified via the
`--cockroach` and `--workload` flags. Common other errors include
not having set up roachprod properly (does `roachprod create -n 1 $USER-foo &&
roachprod destroy $USER-foo` work?), or having the binaries for the wrong
architecture in place (reminder: `roachtest` and `roachprod` run on your own
system, `cockroach` and `workload` on the typically remote VMs).

After the test, artifacts for the test are collected under the `artifacts`
directory.  For failed tests, this includes a debug.zip and the cluster logs
(assuming the test got far enough to set up a cluster and also managed to
download these after the failure). The test's "main log" is `test.log`.

The `roachtest run` command takes a number of optional flags which can be listed
via `roachtest run --help`. A few important ones are:

- `--debug`: keeps the cluster alive on failure. The cluster name can be
  obtained from the artifacts and it can be manually inspected (and must
  manually be destroyed via `roachprod destroy <name>`).
- `--count`: instructs roachtest to run the test multiple times. This may be able
  to reuse clusters and also runs multiple instances concurrently, so it's the
  option of choice when trying to reproduce a failing roachtest.
- A custom pebble executable can be specified by setting the `$PEBBLE_BIN`
  environment variable; this is used only in pebble-specific tests.
- `--clouds` specifies which cloud provider to use. This allows multiple
  comma-separated entries for multi-region testing (see `--help`) but with a
  single entry such as `aws` it will run the test against AWS instead of GCE (the
  default).

## Tips for developers

### Adding a roachtest

A new roachtest should not be added without prior due diligence. Some questions to ask are:

- what am I trying to test and could I be testing it more directly using the Go test framework
  and, for example, a narrower unit test or using an in-memory TestCluster?
- is my team willing and able to own this test going forward? Roachtests run
  against cloud-provisioned hardware and have a higher rate of spurious
  failures than other tests. They are also more likely to fail as a result of
  problems not attributable to the owning team.

The #test-eng channel is a good place to get guidance on these questions and
it is recommended that those seek to add a roachtest ask there.

This is doubly encouraged once it comes to actually writing the test. There
is no comprehensive guide on how to do so, and in practice one learns from
existing tests, not all of which are good examples to learn from. The test-eng
team will help steer you towards good examples.

While writing the test it is encouraged to use the `--local` mode to sanity-check
the test. Additionally, tests interact with the test harness through interfaces
and so it is in principle possible to mock everything. It is encouraged to make
use of this as much as possible, and to add unit tests for nontrivial pieces of
code. It might seem silly to write tests for a test but the cost of investigating
a roachtest failure is often comparable to investing a customer incident, so it
makes sense to invest heavily in prevention.

When running the roachtest during iteration (perhaps using `--local` to save
time; note that the test can use `c.IsLocal()` to take short-cuts in that
case which is helpful during development), don't forget to rebuild the binaries
you've touched (i.e. most likely only `roachtest`). For example, the command
line that you might use while iterating on a new test `foo/garble-wozzler` is

`dev build roachtest && roachtest run --local foo/garble-wozzler`.

### Reproducing a test failure

To reproduce a failing roachtest, the following flags are helpful:

- `--count`: run multiple instances of the test (concurrently)
- `--cpu-quota`: sets the number of vCPUs that roachtest can use for its clusters (determines the degree of parallelism).
- `--debug`: preserves failing clusters.
- `--artifacts`: override the default artifacts dir.

The HTTP endpoint (by default `:8080`) is useful to find the "run" numbers for
failing tests (to find the artifacts) and to get a general overview of the
progress of the invocation.

### Stressing a roachtest

A solid foundation for building the binaries and stressing a roachtest is
provided via the [roachstress.sh] script, which can either be used outright or
saved and adjusted. The script can be invoked without parameters from a clean
checkout of the cockroach repository at the revision to be tested. It will
prompt for user input on which test to stress. Each input can be provided via
the environment (replacing the prompt), and additional flags that are passed
through to `roachtest` can be provided. For example:

```bash
# Run 10 instances of mytest against real VMs (i.e. not locally), and build the
# full CRDB binary (i.e. build with the ui). Also, use a CPU quota of 1000 and
# keep clusters for failing tests running for later investigation.
TEST=mytest COUNT=10 LOCAL=n SHORT=n ./pkg/cmd/roachtest/roachstress.sh --cpu-quota 1000 --debug
```

[roachstress.sh]: https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/roachstress.sh

It's important to make sure that the machine running this invocation does not
suspend or lose network connectivity. Using a tmux session on a `./scripts/gceworker.sh`
machine has worked well for many of us in the past.

Another option is to start a [`Cockroach_Nightlies_RoachtestStress`](https://teamcity.cockroachdb.com/buildConfiguration/Cockroach_Nightlies_RoachtestStress)
CI job, which allows running a bunch of tests without having to keep your
laptop online. The CI job is run as follows:

1. Go to https://teamcity.cockroachdb.com/buildConfiguration/Cockroach_Nightlies_RoachtestStress
2. Click the ellipsis (...) next to the Run button and fill in:
  * Changes → Build branch: `<branch>`
  * Parameters → `env.TESTS`: `^<test>$`
  * Parameters → `env.COUNT`: `<runs>`
