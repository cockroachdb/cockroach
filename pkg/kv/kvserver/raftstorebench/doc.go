// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package raftstorebench provides a simulated workload that loosely mimics the
storage pattern of multiple Raft logs and state machine on a single store. It
intentionally does not attempt to model the full complexity of a CockroachDB
cluster. Instead, it aims to provide a simple, controlled environment for
measuring and optimizing storage engine performance.

The main simplifications are:
- Single-store simulation without the raft package, proposal buffer, or locking.
- Entries are considered committed immediately and are applied as uniformly distributed writes.
- No async log appends. All work is synchronous on the worker goroutines.
- Truncations capture only the size-based truncation strategy, but do not
attempt to simulate further delays or complexity that would be incurred in CockroachDB.

# Overview

The replication layer involves two main stages:
1. Appending entries to a Raft log, and
2. Applying committed log entries to the state machine.

This package provides a simplified simulation of this process, under
configurable parameters which can be specified in a YAML file under `testdata`
and which correspond to a populated Config struct. In particular,

- Separate or combined storage engines for Raft logs and state machine
- Configurable batch sizes, memtable sizes, and other storage parameters
- Simulated log truncation with different strategies

The benchmark collects detailed metrics on the LSM and in particular
write amplification, and lends itself well to prototyping.

# Usage

The following is a step-by-step guide to running the benchmark on a GCE node.
See BenchmarkRaftStore for help configuring the benchmark.

## Create a GCE node

```
export c=$(whoami)-pd
roachprod create -n 1 --local-ssd=false --gce-machine-type n2-standard-16 --gce-pd-volume-size 1000 $c
```

## Prepare test binaries

```
./dev test-binaries ./pkg/kv/kvserver/raftstorebench --output rsb.tar.gz
roachprod put $c rsb.tar.gz
roachprod ssh $c -- tar --strip-components=5 -xzf rsb.tar.gz
```

## Run the benchmark

```
roachprod ssh $c -- '
rm -f /mnt/data1/BenchmarkRaftStore && \
outdir=/mnt/data1/BenchmarkRaftStore_$(date +"%Y%m%d-%H%M%S") && \
mkdir -p "$outdir" && \
ln -s "$outdir" /mnt/data1/BenchmarkRaftStore && \

	sudo systemd-run -p "StandardOutput=file:$outdir/output.txt" -p "StandardError=inherit" -G \
		--same-dir --uid ubuntu -u rsb -- /bin/bash run.sh \
		-test.v -test.timeout 24h -test.run - -test.outputdir /mnt/data1 -test.benchtime=1x \
		-test.bench BenchmarkRaftStore/ranges=5k

```

## Wait until done

```
roachprod ssh $c -- tail -f /mnt/data1/BenchmarkRaftStore/output.txt
# alternatively:
roachprod ssh $c -- journalctl -fu rsb
```

## Download and interpret results

```

	roachprod ssh $c 'cd /mnt/data1 && find -L BenchmarkRaftStore -type f \
		\( -name "*.txt" -o -name "*.yml" \) -print0 | \
		tar --null -cvzf /home/ubuntu/results.tar.gz --files-from=-'

roachprod get $c results.tar.gz
tar -xzvf results.tar.gz
```

```
# See https://pkg.go.dev/golang.org/x/perf/cmd/benchstat.
benchstat -alpha 1 -filter '/wal:20gb -(/eng:sep /trunc:tight)' -col '/eng,/trunc' BenchmarkRaftStore/output.txt
```
*/
package raftstorebench
