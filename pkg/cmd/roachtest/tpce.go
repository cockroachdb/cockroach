// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

func registerTPCE(r *testRegistry) {
	type tpceOptions struct {
		customers int
		nodes     int
		cpus      int
		ssds      int

		tags    []string
		timeout time.Duration
	}

	runTPCE := func(ctx context.Context, t *test, c *cluster, opts tpceOptions) {
		roachNodes := c.Range(1, opts.nodes)
		loadNode := c.Node(opts.nodes + 1)
		racks := opts.nodes

		t.Status("installing cockroach")
		c.Put(ctx, cockroach, "./cockroach", roachNodes)
		c.Start(ctx, t, roachNodes, startArgs(
			fmt.Sprintf("--racks=%d", racks),
			fmt.Sprintf("--store-count=%d", opts.ssds),
		))

		t.Status("installing docker")
		if err := c.Install(ctx, t.l, loadNode, "docker"); err != nil {
			t.Fatal(err)
		}

		// Configure to increase the speed of the import.
		func() {
			db := c.Conn(ctx, 1)
			defer db.Close()
			if _, err := db.ExecContext(
				ctx, "SET CLUSTER SETTING kv.bulk_io_write.concurrent_addsstable_requests = $1", 4*opts.ssds,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(
				ctx, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
			); err != nil {
				t.Fatal(err)
			}
		}()

		m := newMonitor(ctx, c, roachNodes)
		m.Go(func(ctx context.Context) error {
			const dockerRun = `sudo docker run cockroachdb/tpc-e:latest`

			roachNodeIPs := c.InternalIP(ctx, roachNodes)
			roachNodeIPFlags := make([]string, len(roachNodeIPs))
			for i, ip := range roachNodeIPs {
				roachNodeIPFlags[i] = fmt.Sprintf("--hosts=%s", ip)
			}

			t.Status("preparing workload")
			c.Run(ctx, loadNode, fmt.Sprintf("%s --customers=%d --racks=%d --init %s",
				dockerRun, opts.customers, racks, roachNodeIPFlags[0]))

			t.Status("running workload")
			duration := 2 * time.Hour
			threads := opts.nodes * opts.cpus
			out, err := c.RunWithBuffer(ctx, t.l, loadNode,
				fmt.Sprintf("%s --customers=%d --racks=%d --duration=%s --threads=%d %s",
					dockerRun, opts.customers, racks, duration, threads, strings.Join(roachNodeIPFlags, " ")))
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			outStr := string(out)
			t.l.Printf("workload output:\n%s\n", outStr)
			if strings.Contains(outStr, "Reported tpsE :    --   (not between 80% and 100%)") {
				return errors.New("invalid tpsE fraction")
			}
			return nil
		})
		m.Wait()
	}

	for _, opts := range []tpceOptions{
		// Nightly, small scale configurations.
		{customers: 5_000, nodes: 3, cpus: 4, ssds: 1},
		// Weekly, large scale configurations.
		{customers: 100_000, nodes: 5, cpus: 32, ssds: 2, tags: []string{"weekly"}, timeout: 36 * time.Hour},
	} {
		opts := opts
		r.Add(testSpec{
			Name:    fmt.Sprintf("tpce/c=%d/nodes=%d", opts.customers, opts.nodes),
			Owner:   OwnerKV,
			Tags:    opts.tags,
			Timeout: opts.timeout,
			Cluster: makeClusterSpec(opts.nodes+1, cpu(opts.cpus), ssd(opts.ssds)),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runTPCE(ctx, t, c, opts)
			},
		})
	}
}
