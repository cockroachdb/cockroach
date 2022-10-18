// Copyright 2022 The Cockroach Authors.
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
	"github.com/stretchr/testify/require"
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

// For a faster test use this instead
const customers = 25000
const threads = 400

func registerIndexOverloadTPCE(r registry.Registry) {
	runTPCE := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		nodes := c.Spec().NodeCount
		roachNodes := c.Range(1, nodes-1)
		workloadNode := c.Node(nodes)
		racks := nodes - 1

		t.Status("installing cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)

		startOpts := option.DefaultStartOptsNoBackups()
		settings := install.MakeClusterSettings(install.NumRacksOption(racks))
		c.Start(ctx, t.L(), startOpts, settings, roachNodes)

		t.Status("installing docker")
		require.NoError(t, c.Install(ctx, t.L(), workloadNode, "docker"))

		importC := make(chan struct{})
		m := c.NewMonitor(ctx, roachNodes)
		m.Go(func(ctx context.Context) error {
			const dockerRun = `sudo docker run cockroachdb/tpc-e:latest`

			roachNodeIPs, err := c.InternalIP(ctx, t.L(), roachNodes)
			require.NoError(t, err)

			roachNodeIPFlags := make([]string, len(roachNodeIPs))
			for i, ip := range roachNodeIPs {
				roachNodeIPFlags[i] = fmt.Sprintf("--hosts=%s", ip)
			}

			c.Run(ctx, workloadNode, fmt.Sprintf("%s --customers=%d --racks=%d --init %s",
				dockerRun, customers, racks, roachNodeIPFlags[0]))
			close(importC)

			t.Status("running workload (~1 hours)")
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), workloadNode,
				fmt.Sprintf("%s --customers=%d --racks=%d --duration=1h --threads=%d %s",
					dockerRun, customers, racks, threads, strings.Join(roachNodeIPFlags, " ")))
			require.NoError(t, err)

			t.L().Printf("workload output:\n%s\n", result.Stdout)
			if strings.Contains(result.Stdout, "Reported tpsE :    --   (not between 80% and 100%)") {
				return errors.New("invalid tpsE fraction")
			}
			return nil
		})
		m.Go(func(ctx context.Context) error {
			// Wait for the import to complete.
			t.Status("waiting for import to complete (~90m)")
			<-importC

			t.Status("waiting 10m before creating the index")
			<-time.After(10 * time.Minute)

			t.Status("creating index (~30min) - this will cause IO overload")
			db := c.Conn(ctx, t.L(), 1)
			_, err := db.ExecContext(ctx, "create index on tpce.trade (t_qty, t_bid_price, t_ca_id, t_id) storing (t_s_symb, t_exec_name)")
			require.NoError(t, err)

			t.Status("index creation complete")
			return db.Close()
		})
		m.Wait()
	}

	// This test takes about 2 hours to run on 10 N2 machines.
	r.Add(registry.TestSpec{
		Name:    "admission-control/index-overload-tpce",
		Owner:   registry.OwnerAdmissionControl,
		Cluster: spec.MakeClusterSpec(spec.GCE, "n2-standard-8", "Intel Ice Lake", 10, spec.CPU(8), spec.SSD(1)),
		Skip:    "Waiting for snapshotting of GCE images",
		Run:     runTPCE,
	})
}
