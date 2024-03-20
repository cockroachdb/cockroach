// Copyright 2024 The Cockroach Authors.
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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerSqlStatsMixedVersion(r registry.Registry) {
	// sql-tats/mixed-version tests that requesting sql stats from admin-ui works across
	// mixed version clusters.
	r.Add(registry.TestSpec{
		Name:             "sql-stats/mixed-version",
		Owner:            registry.OwnerObservability,
		Cluster:          r.MakeClusterSpec(4),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Weekly),
		Run:              runSQLStatsMixedVersion,
		Timeout:          15 * time.Minute,
	})
}

func getRequestInterval() (start time.Time, end time.Time) {
	currentTime := timeutil.Now()
	start = currentTime.Add(-time.Hour)
	end = currentTime
	return start, end
}

func getCombinedStatementStatsURL(
	adminUIAddr string,
	statsType serverpb.CombinedStatementsStatsRequest_StatsType,
	start time.Time,
	end time.Time,
) string {
	searchParams := fmt.Sprintf("?fetch_mode.stats_type=%d&start=%d&end=%d", statsType, start.Unix(), end.Unix())
	return "https://" + adminUIAddr + "/_status/combinedstmts" + searchParams
}

// Randomly select a node from gatewayNodes and requests sql stats data from
// the statusServer via the `_status/combinedstmts`.
func requestStatements(
	ctx context.Context,
	c cluster.Cluster,
	l *logger.Logger,
	r *rand.Rand,
	gatewayNodes option.NodeListOption,
) (*serverpb.StatementsResponse, error) {
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, l, gatewayNodes)
	if err != nil {
		return nil, err
	}
	addr := adminUIAddrs[r.Intn(len(adminUIAddrs))]

	start, end := getRequestInterval()
	url := getCombinedStatementStatsURL(addr, serverpb.CombinedStatementsStatsRequest_StmtStatsOnly, start, end)
	client := roachtestutil.DefaultHTTPClient(c, l)
	statsResponse := &serverpb.StatementsResponse{}
	err = client.GetJSON(ctx, url, statsResponse)
	if err != nil {
		return nil, err
	}

	return statsResponse, nil
}

// runSQLStatsMixedVersion tests that accessing sql stats works across mixed version clusters.
func runSQLStatsMixedVersion(ctx context.Context, t test.Test, c cluster.Cluster) {
	roachNodes := c.Range(1, c.Spec().NodeCount-1)
	workloadNode := c.Node(c.Spec().NodeCount)
	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All())
	flushInterval := 3 * time.Minute

	initWorkload := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", roachNodes)
	runWorkload := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", roachNodes).
		Option("tolerate-errors")

	setClusterSettings := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		// SQL Stats are flushed every 10m by default, set it to 3m to speed up the test.
		_, err := c.Conn(ctx, t.L(), roachNodes[0]).Exec(`SET CLUSTER SETTING sql.stats.flush.interval = '3m'`)
		return err
	}

	runGetRequests := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		var timer timeutil.Timer
		timer.Reset(flushInterval + 30*time.Second) // Add some buffer time to ensure the stats are flushed.
		defer timer.Stop()
		foundStmtTableSources := map[string]struct{}{}
		foundTxnTableSources := map[string]struct{}{}
		for len(foundStmtTableSources) < 2 && len(foundTxnTableSources) < 2 {
			select {
			case <-timer.C:
				res, err := requestStatements(ctx, c, l, r, roachNodes)
				if err != nil {
					return err
				}
				if len(res.Statements) > 0 {
					foundStmtTableSources[res.StmtsSourceTable] = struct{}{}
				}
				if len(res.Transactions) > 0 {
					foundTxnTableSources[res.TxnsSourceTable] = struct{}{}
				}
			case <-ctx.Done():
				return nil
			}

			// Add some buffer time to ensure a flush interval has passed.
			timer.Reset(flushInterval + 30*time.Second)
		}

		return nil
	}

	mvt.OnStartup("set-cluster-settings", setClusterSettings)
	mvt.Workload("tpcc", workloadNode, initWorkload, runWorkload)
	mvt.InMixedVersion("request-sql-stats", runGetRequests)
	mvt.Run()
}
