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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/errors"
)

// TODO(baptist): This shares a lot of code with tpce.go. Consider ways to refactor.
func registerIndexOverloadTPCE(r registry.Registry) {
	type tpceOptions struct {
		owner            registry.Owner // defaults to test-eng
		customers        int
		activeRatio      float64
		threads          int
		delayBeforeIndex time.Duration

		tags    []string
		timeout time.Duration
	}

	runTPCE := func(ctx context.Context, t test.Test, c cluster.Cluster, opts tpceOptions) {
		nodes := c.Spec().NodeCount
		roachNodes := c.Range(1, nodes-1)
		workloadNode := c.Node(nodes)
		racks := nodes - 1
		runDuration := 4 * opts.delayBeforeIndex

		t.Status("installing cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)

		startOpts := option.DefaultStartOpts()
		settings := install.MakeClusterSettings(install.NumRacksOption(racks))
		c.Start(ctx, t.L(), startOpts, settings, roachNodes)

		{
			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(workloadNode.InstallNodes()[0])
			promCfg.WithNodeExporter(c.All().InstallNodes())
			promCfg.WithCluster(roachNodes.InstallNodes())
			promCfg.WithGrafanaDashboard("http://go.crdb.dev/p/snapshot-admission-control-grafana")
			promCfg.ScrapeConfigs = append(promCfg.ScrapeConfigs, prometheus.MakeWorkloadScrapeConfig("workload",
				"/", makeWorkloadScrapeNodes(workloadNode.InstallNodes()[0], []workloadInstance{{nodes: workloadNode}})))
			_, cleanupFunc := setupPrometheusForRoachtest(ctx, t, c, promCfg, []workloadInstance{{nodes: workloadNode}})
			defer cleanupFunc()
		}

		t.Status("installing docker")
		if err := c.Install(ctx, t.L(), workloadNode, "docker"); err != nil {
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

			t.Status("preparing workload (~4 hours)")
			c.Run(ctx, workloadNode, fmt.Sprintf("%s --customers=%d --racks=%d --init %s",
				dockerRun, opts.customers, racks, roachNodeIPFlags[0]))
			close(importC)

			t.Status("running workload (~4 hours)")
			activeCustomers := int(float64(opts.customers) * opts.activeRatio)
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), workloadNode,
				fmt.Sprintf("%s --customers=%d --active-customers=%d --racks=%d --duration=%s --threads=%d %s",
					dockerRun, opts.customers, activeCustomers, racks, runDuration, opts.threads, strings.Join(roachNodeIPFlags, " ")))
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
			t.Status("waiting for import to complete")
			select {
			case <-importC:
			case <-ctx.Done():
				return ctx.Err()
			}

			t.Status("waiting before creating the index")

			// Wait for the load to run for a while before starting the index build.
			select {
			case <-time.After(opts.delayBeforeIndex):
			case <-ctx.Done():
				return ctx.Err()
			}

			t.Status("creating index (~30min)")

			db := c.Conn(ctx, t.L(), 1)
			_, err := db.ExecContext(ctx, "CREATE INDEX ON tpce.cash_transaction (ct_dts)")
			if err != nil {
				_ = db.Close()
				return err
			}

			t.Status("index creation complete")

			return db.Close()
		})
		m.Wait()
	}

	// This test takes about 8 hours to run on 10 N2 machines.
	r.Add(registry.TestSpec{
		Name:    "admission-control/index-overload-tpce",
		Owner:   registry.OwnerAdmissionControl,
		Cluster: spec.MakeClusterSpec(spec.GCE, "n2-standard-8", "Intel Ice Lake", 10, spec.CPU(8), spec.SSD(1)),
		Tags:    []string{`weekly`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			opts := tpceOptions{
				customers:        100_000,
				activeRatio:      0.2,
				threads:          400,
				delayBeforeIndex: 1 * time.Hour,
			}
			runTPCE(ctx, t, c, opts)
		},
	})

	// This test takes about 1 hours to run.
	r.Add(registry.TestSpec{
		Name:    "admission-control/index-overload-tpce-small",
		Owner:   registry.OwnerAdmissionControl,
		Cluster: r.MakeClusterSpec(4, spec.CPU(8)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			opts := tpceOptions{
				customers:        5_000,
				activeRatio:      0.2,
				threads:          400,
				delayBeforeIndex: 5 * time.Minute,
			}
			runTPCE(ctx, t, c, opts)
		},
	})
}
