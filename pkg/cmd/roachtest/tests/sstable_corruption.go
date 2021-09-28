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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func runSstableCorruption(ctx context.Context, t test.Test, c cluster.Cluster) {
	crdbNodes := c.Range(1, c.Spec().NodeCount)
	workloadNode := c.Node(1)
	const corruptNode = 3

	t.Status("installing cockroach")
	c.Put(ctx, t.Cockroach(), "./cockroach", crdbNodes)
	c.Start(ctx, crdbNodes)

	// We don't really need tpcc, we just need a good amount of data. Enough
	// to have multiple ranges, and some sstables with only table keys.
	t.Status("importing tpcc fixture")
	c.Run(ctx, workloadNode,
		"./cockroach workload fixtures import tpcc --warehouses=100 --fks=false --checks=false")

	m := c.NewMonitor(ctx, crdbNodes)
	signalChan := make(chan bool)

	m.Go(func(ctx context.Context) error {
		// Wait for a signal from the other goroutine below before running the
		// workload. This will only be called after the cluster has been restarted
		// with corrupt sstables on one node.
		select {
		case proceed := <-signalChan:
			if !proceed {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		_ = c.RunE(ctx, workloadNode, "./cockroach workload run tpcc --warehouses=100 "+
			fmt.Sprintf("--tolerate-errors --duration=%s", 5*time.Minute))
		return nil
	})

	m.Go(func(ctx context.Context) error {
		m.ExpectDeaths(3)
		c.Stop(ctx, crdbNodes)

		tableSSTs, err := c.RunWithBuffer(ctx, t.L(), c.Node(corruptNode),
			"./cockroach debug pebble manifest dump {store-dir}/MANIFEST-* | grep -v added | grep -v deleted | grep \"\\[/Table\"")
		if err != nil {
			return err
		}
		strTableSSTs := strings.Split(string(tableSSTs), "\n")
		if len(strTableSSTs) == 0 {
			t.Fatal("expected at least one sst containing table keys only, got none")
		}
		// Corrupt up to 6 SSTs containing table keys.
		corruptedFiles := 0
		for _, sstLine := range strTableSSTs {
			sstLine = strings.TrimSpace(sstLine)
			firstFileIdx := strings.Index(sstLine, ":")
			_, err = strconv.Atoi(sstLine[:firstFileIdx])
			if err != nil {
				t.Fatal("error when converting %s to int: %s", sstLine[:firstFileIdx], err.Error())
			}

			t.Status(fmt.Sprintf("corrupting sstable %s on node %d", sstLine[:firstFileIdx], corruptNode))
			c.Run(ctx, c.Node(corruptNode), fmt.Sprintf("dd if=/dev/urandom of={store-dir}/%s.sst seek=256 count=128 bs=1 conv=notrunc", sstLine[:firstFileIdx]))
			corruptedFiles++
			if corruptedFiles >= 6 {
				break
			}
		}

		m.ExpectDeath()
		if err := c.StartE(ctx, crdbNodes); err != nil {
			// Node detected corruption on start and crashed. This is good. No need
			// to run workload.
			signalChan <- false
			return nil
		}
		// Start the workload. This should cause the node to crash.
		signalChan <- true
		time.Sleep(2 * time.Minute)

		if num := m.NumExpectedDeaths(); num > 0 {
			t.Fatalf("expected node death to have occurred, still waiting on %d deaths", num)
		}
		// Reset deaths and restart the corrupt node as a clean node. This is
		// necessary for the test to not fail upon cleanup.
		m.ResetDeaths()
		_ = c.WipeE(ctx, t.L(), c.Node(corruptNode))
		c.Start(ctx, c.Node(corruptNode))
		return nil
	})
	m.Wait()
}

func registerSstableCorruption(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "sstable-corruption/table",
		Owner:   registry.OwnerStorage,
		Cluster: r.MakeClusterSpec(3),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSstableCorruption(ctx, t, c)
		},
	})
}
