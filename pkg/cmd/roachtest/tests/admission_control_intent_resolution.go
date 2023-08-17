// Copyright 2023 The Cockroach Authors.
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
	gosql "database/sql"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/stretchr/testify/require"
)

// This test sets up a 1 node CRDB cluster on an 8vCPU machine, runs a txn
// that creates a huge number of intents, and then commits to resolve those
// intents. When intent resolution is not subject to admission control, the
// LSM gets overloaded and has > 50 sub-levels.
func registerIntentResolutionOverload(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:      "admission-control/intent-resolution",
		Owner:     registry.OwnerAdmissionControl,
		Timeout:   time.Hour,
		Benchmark: true,
		Tags:      registry.Tags(`weekly`),
		// Second node is solely for Prometheus.
		Cluster:         r.MakeClusterSpec(2, spec.CPU(8)),
		RequiresLicense: true,
		Leases:          registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount != 2 {
				t.Fatalf("expected 2 nodes, found %d", c.Spec().NodeCount)
			}
			crdbNodes := c.Spec().NodeCount - 1
			promNode := crdbNodes + 1

			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(c.Node(promNode).InstallNodes()[0]).
				WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithGrafanaDashboardJSON(grafana.ChangefeedAdmissionControlGrafana)
			err := c.StartGrafana(ctx, t.L(), promCfg)
			require.NoError(t, err)

			c.Put(ctx, t.Cockroach(), "./cockroach", c.Range(1, crdbNodes))
			startOpts := option.DefaultStartOptsNoBackups()
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
				"--vmodule=io_load_listener=2")
			settings := install.MakeClusterSettings()
			c.Start(ctx, t.L(), startOpts, settings, c.Range(1, crdbNodes))
			setAdmissionControl(ctx, t, c, true)
			t.Status("running txn")
			m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
			m.Go(func(ctx context.Context) error {
				db := c.Conn(ctx, t.L(), crdbNodes)
				defer db.Close()
				_, err := db.Exec(`CREATE TABLE test_table(id integer PRIMARY KEY, t TEXT)`)
				if err != nil {
					return err
				}
				tx, err := db.BeginTx(ctx, &gosql.TxOptions{})
				if err != nil {
					return err
				}
				query := `INSERT INTO test_table(id, t) SELECT i, sha512(random()::text) FROM ` +
					`generate_series(0, 75000000) AS t(i);`
				_, err = tx.ExecContext(ctx, query)
				if err != nil {
					return err
				}
				t.Status("intents created, committing txn")
				err = tx.Commit()
				if err != nil {
					return err
				}
				t.Status("sleeping for async intent resolution to complete")
				// Intents take ~10min to resolve, and we're padding by another 10min.
				time.Sleep(20 * time.Minute)
				t.Status("done sleeping")
				// TODO(sumeer): use prometheus client and StatCollector to ensure
				// that max(storage_l0_sublevels) is below the threshold of 20. Also
				// confirm that the intentcount metric has a very low value.
				return nil
			})
			m.Wait()
		},
	})
}
