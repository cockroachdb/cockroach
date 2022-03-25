// Copyright 2021 The Cockroach Authors.
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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func runSSTableCorruption(ctx context.Context, t test.Test, c cluster.Cluster) {
	crdbNodes := c.Range(1, c.Spec().NodeCount)
	workloadNode := c.Node(1)
	// Corrupt all nodes.
	corruptNodes := crdbNodes

	t.Status("installing cockroach")
	c.Put(ctx, t.Cockroach(), "./cockroach", crdbNodes)
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), crdbNodes)

	{
		m := c.NewMonitor(ctx, crdbNodes)

		m.Go(func(ctx context.Context) error {
			// We don't really need tpcc, we just need a good amount of data. Enough
			// to have multiple ranges, and some sstables with only table keys.
			t.Status("importing tpcc fixture")
			c.Run(ctx, workloadNode,
				"./cockroach workload fixtures import tpcc --warehouses=100 --fks=false --checks=false")
			return nil
		})
		m.Wait()
	}

	opts := option.DefaultStopOpts()
	opts.RoachprodOpts.Wait = true
	c.Stop(ctx, t.L(), opts, crdbNodes)

	const findTablesCmd = "" +
		// Take the latest manifest file ...
		"ls -tr {store-dir}/MANIFEST-* | tail -n1 | " +
		// ... dump its contents ...
		"xargs ./cockroach debug pebble manifest dump | " +
		// ... shuffle the files to distribute corruption over the LSM.
		"shuf | " +
		// ... filter for up to six SSTables that contain table data.
		"grep -v added | grep -v deleted | grep '/Table/' | tail -n6"

	for _, node := range corruptNodes {
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(node), findTablesCmd)
		if err != nil {
			t.Fatalf("could not find tables to corrupt: %s\nstdout: %s\nstderr: %s", err, result.Stdout, result.Stderr)
		}
		tableSSTs := strings.Split(strings.TrimSpace(result.Stdout), "\n")
		if len(tableSSTs) == 0 {
			t.Fatal("expected at least one sst containing table keys only, got none")
		}
		// Corrupt the SSTs.
		for _, sstLine := range tableSSTs {
			sstLine = strings.TrimSpace(sstLine)
			firstFileIdx := strings.Index(sstLine, ":")
			if firstFileIdx < 0 {
				t.Fatalf("unexpected format for sst line: %s", sstLine)
			}
			_, err = strconv.Atoi(sstLine[:firstFileIdx])
			if err != nil {
				t.Fatalf("error when converting %s to int: %s", sstLine[:firstFileIdx], err.Error())
			}

			t.Status(fmt.Sprintf("corrupting sstable %s on node %d", sstLine[:firstFileIdx], node))
			c.Run(ctx, c.Node(node), fmt.Sprintf("dd if=/dev/urandom of={store-dir}/%s.sst seek=256 count=128 bs=1 conv=notrunc", sstLine[:firstFileIdx]))
		}
	}

	if err := c.StartE(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), crdbNodes); err != nil {
		// Node detected corruption on start and crashed. This is good. No need
		// to run workload; the test is complete.
		_ = c.WipeE(ctx, t.L(), corruptNodes)
		return
	}

	{
		m := c.NewMonitor(ctx)
		// Run a workload to try to get the node to notice corruption and crash.
		m.Go(func(ctx context.Context) error {
			_ = c.RunE(
				ctx, workloadNode,
				"./cockroach workload run tpcc --warehouses=100 --tolerate-errors",
			)
			// Don't return an error from the workload. We want outcome of WaitE to be
			// determined by the monitor noticing that a node died. The workload may
			// also fail, despite --tolerate-errors, if a node crashes too early.
			return nil
		})

		// There's a subtle race condition here that we want to guard against. The
		// monitor's WaitE waits on either a) the first error from a node that it is
		// monitoring, OR b) all goroutines derived from the monitor to exit. If the
		// previous goroutine exits, the wait group may return (with nil) before the
		// monitor has detected failure from a node. To guard against this, we spawn
		// a second goroutine on the monitor that acts as a timeout. We expect to
		// see corruption within the timeout, if not, this goroutine will fail the
		// test.
		const timeout = 5 * time.Minute
		m.Go(func(ctx context.Context) error {
			timer := time.NewTimer(timeout)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				t.Fatal("did not encounter corruption within the timeout")
				return nil
			}
		})

		require.Error(t, m.WaitE())
	}

	// Exempt corrupted nodes from roachtest harness' post-test liveness checks.
	_ = c.WipeE(ctx, t.L(), corruptNodes)
}

func registerSSTableCorruption(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "sstable-corruption/table",
		Owner:   registry.OwnerStorage,
		Cluster: r.MakeClusterSpec(3),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSSTableCorruption(ctx, t, c)
		},
	})
}
