// Copyright 2020 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
)

func registerTPCE(r registry.Registry) {
	type tpceOptions struct {
		customers int
		nodes     int
		cpus      int
		ssds      int
	}

	runTPCE := func(ctx context.Context, t test.Test, c cluster.Cluster, opts tpceOptions) {
		roachNodes := c.Range(1, opts.nodes)
		loadNode := c.Node(opts.nodes + 1)
		racks := opts.nodes

		t.Status("installing cockroach")

		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.StoreCount = opts.ssds
		settings := install.MakeClusterSettings(install.NumRacksOption(racks))
		c.Start(ctx, t.L(), startOpts, settings, roachNodes)

		t.Status("installing docker")
		if err := c.Install(ctx, t.L(), loadNode, "docker"); err != nil {
			t.Fatal(err)
		}

		// Configure to increase the speed of the import.
		func() {
			db := c.Conn(ctx, t.L(), 1)
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

		m := c.NewMonitor(ctx, roachNodes)
		m.Go(func(ctx context.Context) error {
			const dockerRun = `sudo docker run us-east1-docker.pkg.dev/crl-ci-images/cockroach/tpc-e:latest`

			roachNodeIPs, err := c.InternalIP(ctx, t.L(), roachNodes)
			if err != nil {
				return err
			}
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
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), loadNode,
				fmt.Sprintf("%s --customers=%d --racks=%d --duration=%s --threads=%d %s",
					dockerRun, opts.customers, racks, duration, threads, strings.Join(roachNodeIPFlags, " ")))
			if err != nil {
				t.Fatal(err.Error())
			}
			t.L().Printf("workload output:\n%s\n", result.Stdout)
			if strings.Contains(result.Stdout, "Reported tpsE :    --   (not between 80% and 100%)") {
				return errors.New("invalid tpsE fraction")
			}
			return nil
		})
		m.Wait()
	}

	// Nightly, small scale configuration.
	smallNightly := tpceOptions{
		customers: 5_000,
		nodes:     3,
		cpus:      4,
		ssds:      1,
	}
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("tpce/c=%d/nodes=%d", smallNightly.customers, smallNightly.nodes),
		Owner:            registry.OwnerTestEng,
		Timeout:          4 * time.Hour,
		Cluster:          r.MakeClusterSpec(smallNightly.nodes+1, spec.CPU(smallNightly.cpus), spec.SSD(smallNightly.ssds)),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		// Never run with runtime assertions as this makes this test take
		// too long to complete.
		CockroachBinary: registry.StandardCockroach,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCE(ctx, t, c, smallNightly)
		},
	})

	// Weekly, large scale configuration.
	largeWeekly := tpceOptions{
		customers: 100_000,
		nodes:     5,
		cpus:      32,
		ssds:      2,
	}
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("tpce/c=%d/nodes=%d", largeWeekly.customers, largeWeekly.nodes),
		Owner:            registry.OwnerTestEng,
		Benchmark:        true,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Weekly),
		Tags:             registry.Tags("weekly"),
		Timeout:          36 * time.Hour,
		Cluster:          r.MakeClusterSpec(largeWeekly.nodes+1, spec.CPU(largeWeekly.cpus), spec.SSD(largeWeekly.ssds)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCE(ctx, t, c, largeWeekly)
		},
	})
}
