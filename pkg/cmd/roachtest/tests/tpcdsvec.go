// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/cmpconn"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcds"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func registerTPCDSVec(r registry.Registry) {
	const (
		timeout                         = 5 * time.Minute
		withStatsSlowerWarningThreshold = 1.25
	)

	queriesToSkip := map[int]bool{
		// These queries don't complete within 5 minutes.
		1:  true,
		64: true,

		// These queries contain unsupported function 'rollup' (#46280).
		5:  true,
		14: true,
		18: true,
		22: true,
		67: true,
		77: true,
		80: true,
	}

	tpcdsTables := []string{
		`call_center`, `catalog_page`, `catalog_returns`, `catalog_sales`,
		`customer`, `customer_address`, `customer_demographics`, `date_dim`,
		`dbgen_version`, `household_demographics`, `income_band`, `inventory`,
		`item`, `promotion`, `reason`, `ship_mode`, `store`, `store_returns`,
		`store_sales`, `time_dim`, `warehouse`, `web_page`, `web_returns`,
		`web_sales`, `web_site`,
	}

	runTPCDSVec := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

		clusterConn := c.Conn(ctx, t.L(), 1)
		t.Status("disabling automatic collection of stats")
		if _, err := clusterConn.Exec(
			`SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false;`,
		); err != nil {
			t.Fatal(err)
		}
		t.Status("restoring TPCDS dataset for Scale Factor 1")
		if _, err := clusterConn.Exec(
			`
RESTORE DATABASE tpcds FROM '/' IN 'gs://cockroach-fixtures-us-east1/workload/tpcds/scalefactor=1/backup?AUTH=implicit'
WITH unsafe_restore_incompatible_version;
`,
		); err != nil {
			t.Fatal(err)
		}

		if _, err := clusterConn.Exec("USE tpcds;"); err != nil {
			t.Fatal(err)
		}
		scatterTables(t, clusterConn, tpcdsTables)
		t.Status("waiting for full replication")
		err := roachtestutil.WaitFor3XReplication(ctx, t.L(), clusterConn)
		require.NoError(t, err)

		// TODO(yuzefovich): it seems like if cmpconn.CompareConns hits a
		// timeout, the query actually keeps on going and the connection
		// becomes kinda stale. To go around it, we set a statement timeout
		// variable on the connections and pass in 3 x timeout into
		// CompareConns hoping that the session variable is better respected.
		// We additionally open fresh connections for each query.
		setStmtTimeout := fmt.Sprintf("SET statement_timeout='%s';", timeout)
		firstNode := c.Node(1)
		urls, err := c.ExternalPGUrl(ctx, t.L(), firstNode, roachprod.PGURLOptions{})
		if err != nil {
			t.Fatal(err)
		}
		firstNodeURL := urls[0]
		openNewConnections := func() (map[string]cmpconn.Conn, func()) {
			conns := map[string]cmpconn.Conn{}
			vecOffConn, err := cmpconn.NewConn(
				ctx, firstNodeURL, setStmtTimeout+"SET vectorize=off; USE tpcds;",
			)
			if err != nil {
				t.Fatal(err)
			}
			conns["vectorize=OFF"] = vecOffConn
			vecOnConn, err := cmpconn.NewConn(
				ctx, firstNodeURL, setStmtTimeout+"SET vectorize=on; USE tpcds;",
			)
			if err != nil {
				t.Fatal(err)
			}
			conns["vectorize=ON"] = vecOnConn
			// A sanity check that we have different values of 'vectorize'
			// session variable on two connections and that the comparator will
			// emit an error because of that difference.
			if _, err := cmpconn.CompareConns(
				ctx, timeout, conns, "", "SHOW vectorize;", false, /* ignoreSQLErrors */
			); err == nil {
				t.Fatal("unexpectedly SHOW vectorize didn't trigger an error on comparison")
			}
			return conns, func() {
				vecOffConn.Close(ctx)
				vecOnConn.Close(ctx)
			}
		}

		noStatsRunTimes := make(map[int]float64)
		var errToReport error
		// We will run all queries in two scenarios: without stats and with
		// auto stats. The idea is that the plans are likely to be different,
		// so we will be testing different execution scenarios. We additionally
		// will compare the queries' run times in both scenarios and print out
		// warnings when in presence of stats we seem to be choosing worse
		// plans.
		for _, haveStats := range []bool{false, true} {
			for queryNum := 1; queryNum <= tpcds.NumQueries; queryNum++ {
				if _, toSkip := queriesToSkip[queryNum]; toSkip {
					continue
				}
				query, ok := tpcds.QueriesByNumber[queryNum]
				if !ok {
					continue
				}
				t.Status(fmt.Sprintf("running query %d\n", queryNum))
				// We will be opening fresh connections for every query to go
				// around issues with cancellation.
				conns, cleanup := openNewConnections()
				start := timeutil.Now()
				if _, err := cmpconn.CompareConns(
					ctx, 3*timeout, conns, "", query, false, /* ignoreSQLErrors */
				); err != nil {
					t.Status(fmt.Sprintf("encountered an error: %s\n", err))
					errToReport = errors.CombineErrors(errToReport, err)
				} else {
					runTimeInSeconds := timeutil.Since(start).Seconds()
					t.Status(
						fmt.Sprintf("[q%d] took about %.2fs to run on both configs",
							queryNum, runTimeInSeconds),
					)
					if haveStats {
						noStatsRunTime, ok := noStatsRunTimes[queryNum]
						if ok && noStatsRunTime*withStatsSlowerWarningThreshold < runTimeInSeconds {
							t.Status(fmt.Sprintf("WARNING: suboptimal plan when stats are present\n"+
								"no stats: %.2fs\twith stats: %.2fs", noStatsRunTime, runTimeInSeconds))
						}
					} else {
						noStatsRunTimes[queryNum] = runTimeInSeconds
					}
				}
				cleanup()
			}

			if !haveStats {
				createStatsFromTables(t, clusterConn, tpcdsTables)
			}
		}
		if errToReport != nil {
			t.Fatal(errToReport)
		}
	}

	r.Add(registry.TestSpec{
		Name:      "tpcdsvec",
		Owner:     registry.OwnerSQLQueries,
		Benchmark: true,
		Cluster:   r.MakeClusterSpec(3),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCDSVec(ctx, t, c)
		},
	})
}
