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
		owner           registry.Owner // defaults to test-eng
		customers       int
		activeCustomers int
		nodes           int
		cpus            int
		ssds            int

		tags    []string
		timeout time.Duration
	}

	runTPCE := func(ctx context.Context, t test.Test, c cluster.Cluster, opts tpceOptions) {
		roachNodes := c.Range(1, opts.nodes)
		loadNode := c.Node(opts.nodes + 1)
		racks := opts.nodes

		t.Status("installing cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)

		startOpts := option.DefaultStartOpts()
		settings := install.MakeClusterSettings(install.NumRacksOption(racks))
		c.Start(ctx, t.L(), startOpts, settings, roachNodes)

		t.Status("installing docker")
		if err := c.Install(ctx, t.L(), loadNode, "docker"); err != nil {
			t.Fatal(err)
		}

		importC := make(chan struct{})
		m := c.NewMonitor(ctx, roachNodes)
		m.Go(func(ctx context.Context) error {
			const dockerRun = `sudo docker run cockroachdb/tpc-e:latest`

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
			close(importC)

			t.Status("running workload")
			duration := 48 * time.Hour
			threads := 1200
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), loadNode,
				fmt.Sprintf("%s --customers=%d --active-customers=%d --racks=%d --duration=%s --threads=%d %s",
					dockerRun, opts.customers, opts.activeCustomers, racks, duration, threads, strings.Join(roachNodeIPFlags, " ")))
			if err != nil {
				t.Fatal(err.Error())
			}
			t.L().Printf("workload output:\n%s\n", result.Stdout)
			if strings.Contains(result.Stdout, "Reported tpsE :    --   (not between 80% and 100%)") {
				return errors.New("invalid tpsE fraction")
			}
			return nil
		})
		m.Go(func(ctx context.Context) error {
			// Wait for the import to complete.
			select {
			case <-importC:
			case <-ctx.Done():
				return ctx.Err()
			}

			// Wait for the load to run for 1 hour before starting the index build.
			select {
			case <-time.After(1 * time.Hour):
			case <-ctx.Done():
				return ctx.Err()
			}

			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()
			if _, err := db.ExecContext(ctx, "ALTER DATABASE tpce CONFIGURE ZONE USING gc.ttlseconds = 5000;"); err != nil {
				return err
			}
			_, err := db.ExecContext(ctx, "CREATE INDEX ON tpce.cash_transaction (ct_dts)")
			return err
		})
		m.Wait()
	}

	// To use, run the following from a GCE worker tmux session:
	//  roachtest run tpce --instance-type=n2-standard-48 --zones=us-central1-c --min-cpu-platform='Intel Ice Lake' --wipe=false --cockroach=<path_to_cockroach>
	week := 7 * 24 * time.Hour
	r.Add(registry.TestSpec{
		Name:    "tpce/index-backfill",
		Owner:   registry.OwnerKV,
		Timeout: week,
		Cluster: r.MakeClusterSpec(16, spec.VolumeSize(5000)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			opts := tpceOptions{
				customers:       2_000_000,
				activeCustomers: 600_000,
				nodes:           15,
			}
			runTPCE(ctx, t, c, opts)
		},
	})
}
