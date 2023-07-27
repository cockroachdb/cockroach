## Name
**roachprod-microbench** - Execute microbenchmarks across a `roachprod` cluster

## Synopsis
**roachprod-microbench** `<working dir> [<flags>] -- [<test args>]`

## Examples
> **roachprod-microbench** ./artifacts/log -binaries=bin.tar.gz -cluster=user-bench -- -test.benchtime=1ns

Execute microbenchmarks present in the `bin.tar.gz` archive across the `user-bench` roachprod cluster.

Pass the additional test argument `-test.benchtime=1ns` to all executions.

Microbenchmark results are written to the `./artifacts/log` directory. This directory will also contain logs for any failures that occurred.

> **roachprod-microbench** ./artifacts/log -binaries=bin.tar.gz -cluster=user-bench -publishdir=gs://microbench/2042

Publish the logs captured from the microbenchmarks in the `<working dir>` `./artifacts/log` to a GCS bucket `gs://microbench/2042`.

> **roachprod-microbench** ./artifacts/log -binaries=bin.tar.gz -cluster=user-bench -comparedir=gs://microbench/1899

Compare the logs captured from the microbenchmarks in the `<working dir>` `./artifacts/log` to a GCS bucket `gs://microbench/1899` of a previously published **roachprod-microbench** run.

## Description

The **roachprod-microbench** command operates on a portable test binaries archive that has been built with the `dev test-binaries` tooling.
Compressed and uncompressed `tar` archives are supported, since the `dev` tooling can produce both.
It should be noted that the archive has to adhere to a specific layout and contains `run.sh` scripts for each package, which facilitates execution.
This tool only supports binaries archives produced in this format by the `dev test-binaries` tooling.

As part of the orchestration, test binaries are copied to the target cluster and extracted.
Once the cluster is ready, the tool will first probe for all microbenchmarks available and then execute the microbenchmarks according to any flags that have been set.

The `<working dir>` must always be specified and all results from the microbenchmarks will be captured to this directory, including any errors.
If the `publishdir` or `comparedir` flags are specified these will also use the `<working dir>` to publish from or compare against.

The publish functionality is used to store the results of a run to a specified location.
On a subsequent run the compare functionality can then be pointed to that same location to do a comparison which will publish Google sheets for further inspection. 

For additional information on flags, run the **roachprod-microbench** command without any arguments to display a helpful message.
