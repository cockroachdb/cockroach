# Overview

`roachtest` is a tool for performing large-scale (multi-machine)
automated tests. It relies on the concurrently-developed (but
separate) tool `roachprod`.

# Setup

1. [Set up `roachprod`](https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachprod/README.md), if you haven't already. This includes making sure `$PWD/bin` is on your `PATH` and `gcloud` is installed and properly configured.
1. Build a linux release binary of `cockroach`: `build/builder.sh mkrelease amd64-linux-gnu`
1. Build a linux binary of the `workload` tool: `build/builder.sh mkrelease amd64-linux-gnu bin/workload`
1. Build a local binary of `roachtest`: `make bin/roachtest`

# Usage

You'll usually use `roachtest run` and specify a single test.

For Cockroach Labs employees, if your local username does not match
your gcloud/`@cockroachlabs.com` username, you'll need to add
`--user=$CRL_USERNAME` to the `roachtest` command. For non-Cockroach
Labs employees, set `--gce-project` (and maybe `--user` too).

If `$PWD/bin` isn't on your `PATH`, you'll need to add `--roachprod=<path-to-roachprod>`
to the `roachtest` command.

```shell
roachtest run jepsen/1/bank/split
```

While iterating, don't forget to rebuild whichever of the above
binaries you've touched. For example, the command line that you might use
while iterating on jepsen tests is `make bin/roachtest &&
roachtest run jepsen/1/bank/split`

To keep your cluster after the roachtest finishes, use the `--debug` flag
with `roachtest run`. You can use `roachprod` to connect to your cluster and
examine the state by using `roachprod setup-ssh <cluster name>`. This will print
out an IP address to connect to using `ssh`. To have `roachtest` reuse an
existing cluster, pass the `--cluster <clustername` argument to 
`roachtest run`. 

