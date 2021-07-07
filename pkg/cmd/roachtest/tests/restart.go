// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func runRestart(ctx context.Context, t test.Test, c cluster.Cluster, downDuration time.Duration) {
	crdbNodes := c.Range(1, c.Spec().NodeCount)
	workloadNode := c.Node(1)
	const restartNode = 3

	t.Status("installing cockroach")
	c.Put(ctx, t.Cockroach(), "./cockroach", crdbNodes)
	c.Start(ctx, crdbNodes, option.StartArgs(`--args=--vmodule=raft_log_queue=3`))

	// We don't really need tpcc, we just need a good amount of traffic and a good
	// amount of data.
	t.Status("importing tpcc fixture")
	c.Run(ctx, workloadNode,
		"./cockroach workload fixtures import tpcc --warehouses=100 --fks=false --checks=false")

	// Wait a full scanner cycle (10m) for the raft log queue to truncate the
	// sstable entries from the import. They're huge and are not representative of
	// normal traffic.
	//
	// NB: less would probably do a good enough job, but let's play it safe.
	//
	// TODO(dan/tbg): It's awkward that this is necessary. We should be able to
	// do a better job here, for example by truncating only a smaller prefix of
	// the log instead of all of it (right now there's no notion of per-entry
	// size when we do truncate). Also having quiescing ranges truncate to
	// lastIndex will be helpful because that drives the log size down eagerly
	// when things are healthy.
	t.Status("waiting for addsstable truncations")
	time.Sleep(11 * time.Minute)

	// Stop a node.
	c.Stop(ctx, c.Node(restartNode))

	// Wait for between 10s and `server.time_until_store_dead` while sending
	// traffic to one of the nodes that are not down. This used to cause lots of
	// raft log truncation, which caused node 3 to need lots of snapshots when it
	// came back up.
	c.Run(ctx, workloadNode, "./cockroach workload run tpcc --warehouses=100 "+
		fmt.Sprintf("--tolerate-errors --wait=false --duration=%s", downDuration))

	// Bring it back up and make sure it can serve a query within a reasonable
	// time limit. For now, less time than it was down for.
	c.Start(ctx, c.Node(restartNode))

	// Dialing the formerly down node may still be prevented by the circuit breaker
	// for a short moment (seconds) after n3 restarts. If it happens, the COUNT(*)
	// can fail with a "no inbound stream connection" error. This is not what we
	// want to catch in this test, so work around it.
	//
	// See https://github.com/cockroachdb/cockroach/issues/38602.
	time.Sleep(15 * time.Second)

	start := timeutil.Now()
	restartNodeDB := c.Conn(ctx, restartNode)
	if _, err := restartNodeDB.Exec(`SELECT count(*) FROM tpcc.order_line`); err != nil {
		t.Fatal(err)
	}
	if took := timeutil.Since(start); took > downDuration {
		t.Fatalf(`expected to recover within %s took %s`, downDuration, took)
	} else {
		t.L().Printf(`connecting and query finished in %s`, took)
	}
}

func registerRestart(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "restart/down-for-2m",
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(3),
		// "cockroach workload is only in 19.1+"
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runRestart(ctx, t, c, 2*time.Minute)
		},
	})
}
