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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
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

	const nTables = 6
	var dumpManifestCmd = "" +
		// Take the latest manifest file ...
		"ls -tr {store-dir}/MANIFEST-* | tail -n1 | " +
		// ... dump its contents ...
		"xargs ./cockroach debug pebble manifest dump | " +
		// ... filter for SSTables that contain table data.
		"grep -v added | grep -v deleted | grep '/Table/'"
	var findTablesCmd = dumpManifestCmd + "| " +
		// Shuffle the files to distribute corruption over the LSM ...
		"shuf | " +
		// ... take a fixed number of tables.
		fmt.Sprintf("tail -n %d", nTables)

	for _, node := range corruptNodes {
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(node), findTablesCmd)
		if err != nil {
			t.Fatalf("could not find tables to corrupt: %s\nstdout: %s\nstderr: %s", err, result.Stdout, result.Stderr)
		}
		tableSSTs := strings.Split(strings.TrimSpace(result.Stdout), "\n")
		if len(tableSSTs) != nTables {
			// We couldn't find enough tables to corrupt. As there should be an
			// abundance of tables, this warrants further investigation. To aid in
			// such an investigation, print the contents of the data directory.
			cmd := "ls -l {store-dir}"
			result, err = c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(node), cmd)
			if err == nil {
				t.Status("store dir contents:\n", result.Stdout)
			}
			// Fetch the MANIFEST files from this node.
			result, err = c.RunWithDetailsSingleNode(
				ctx, t.L(), c.Node(node),
				"tar czf {store-dir}/manifests.tar.gz {store-dir}/MANIFEST-*",
			)
			if err != nil {
				t.Fatalf("could not create manifest file archive: %s", err)
			}
			result, err = c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(node), "echo", "-n", "{store-dir}")
			if err != nil {
				t.Fatalf("could not infer store directory: %s", err)
			}
			storeDirectory := result.Stdout
			srcPath := filepath.Join(storeDirectory, "manifests.tar.gz")
			dstPath := filepath.Join(t.ArtifactsDir(), fmt.Sprintf("manifests.%d.tar.gz", node))
			err = c.Get(ctx, t.L(), srcPath, dstPath, c.Node(node))
			if err != nil {
				t.Fatalf("could not fetch manifest archive: %s", err)
			}
			t.Fatalf(
				"expected %d SSTables containing table keys, got %d: %s",
				nTables, len(tableSSTs), tableSSTs,
			)
		}
		// Corrupt the SSTs.
		for _, sstLine := range tableSSTs {
			sstLine = strings.TrimSpace(sstLine)
			firstFileIdx := strings.Index(sstLine, ":")
			if firstFileIdx < 0 {
				t.Fatalf("unexpected format for sst line: %q", sstLine)
			}
			_, err = strconv.Atoi(sstLine[:firstFileIdx])
			if err != nil {
				t.Fatalf("error when converting %s to int: %s", sstLine[:firstFileIdx], err.Error())
			}

			t.Status(fmt.Sprintf("corrupting sstable %s on node %d", sstLine[:firstFileIdx], node))
			c.Run(ctx, c.Node(node), fmt.Sprintf("dd if=/dev/urandom of={store-dir}/%s.sst seek=256 count=128 bs=1 conv=notrunc", sstLine[:firstFileIdx]))
		}
	}

	if err := c.StartE(ctx, t.L(), option.DefaultStartOptsNoBackups(), install.MakeClusterSettings(), crdbNodes); err != nil {
		// Node detected corruption on start and crashed. This is good. No need
		// to run workload; the test is complete.
		_ = c.WipeE(ctx, t.L(), corruptNodes)
		return
	}

	{
		m := c.NewMonitor(ctx)
		// Run a workload to try to get the node to notice corruption and crash.
		m.Go(func(ctx context.Context) error {
			const timeout = 10 * time.Minute
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			for {
				err := errors.CombineErrors(
					c.RunE(
						ctx, workloadNode,
						"./cockroach workload run tpcc --warehouses=100 --tolerate-errors",
					),
					errors.New("workload unexpectedly returned nil"),
				)
				// NOTE: the workload is fallible, however, we don't want to return the
				// error to the caller of WaitE (below), as we want it to see any error
				// caused due to a node death. The workload is just a means of surfacing
				// the corruption. The workload could also return early with a nil
				// error, which is unexpected. In both cases, simply log the error and
				// determine whether to continue based on the context (timeout, or other
				// context cancellation).
				if err != nil {
					t.L().Printf("workload failed: %s", err)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
					// Loop.
				}
			}
		})

		t.L().Printf("waiting for monitor to observe error ...")
		err := m.WaitE()
		t.L().Printf("monitor observed error: %s", err)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}

	// Exempt corrupted nodes from roachtest harness' post-test liveness checks.
	_ = c.WipeE(ctx, t.L(), corruptNodes)
}

func registerSSTableCorruption(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "sstable-corruption/table",
		Owner:   registry.OwnerStorage,
		Cluster: r.MakeClusterSpec(3),
		Timeout: 2 * time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSSTableCorruption(ctx, t, c)
		},
	})
}
