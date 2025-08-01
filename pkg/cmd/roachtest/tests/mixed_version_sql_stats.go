// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

const (
	crdbInternalStmtStatsCombined = "crdb_internal.statement_statistics"
	crdbInternalTxnStatsCombined  = "crdb_internal.transaction_statistics"
)

func registerSqlStatsMixedVersion(r registry.Registry) {
	// sql-tats/mixed-version tests that requesting sql stats from admin-ui works across
	// mixed version clusters.
	r.Add(registry.TestSpec{
		Name:             "sql-stats/mixed-version",
		Owner:            registry.OwnerObservability,
		Cluster:          r.MakeClusterSpec(5, spec.WorkloadNode()),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Randomized:       true,
		Run:              runSQLStatsMixedVersion,
		Timeout:          1 * time.Hour,
	})
}

// runSQLStatsMixedVersion tests that accessing sql stats works across mixed version clusters.
func runSQLStatsMixedVersion(ctx context.Context, t test.Test, c cluster.Cluster) {
	mvt := mixedversion.NewTest(ctx, t, t.L(), c,
		c.CRDBNodes(),
		// We test only upgrades from 23.2 in this test because it uses
		// the `workload fixtures import` command, which is only supported
		// reliably multi-tenant mode starting from that version.
		mixedversion.MinimumSupportedVersion("v23.2.0"),
	)
	flushInterval := 2 * time.Minute

	initWorkload := roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", c.CRDBNodes())
	runWorkload := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", c.CRDBNodes()).
		Option("tolerate-errors")

	setClusterSettings := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		// SQL Stats are flushed every 10m by default, set it to 3m to speed up the test.
		l.Printf("setting sql.stats.flush.interval to %s", flushInterval.String())
		return h.Exec(r, `SET CLUSTER SETTING sql.stats.flush.interval = $1`, flushInterval.String())
	}

	mvt.OnStartup("set cluster settings", setClusterSettings)
	stopTpcc := mvt.Workload("tpcc", c.WorkloadNode(), initWorkload, runWorkload)
	defer stopTpcc()

	requestTypes := map[string]serverpb.CombinedStatementsStatsRequest_StatsType{
		"stmts stats": serverpb.CombinedStatementsStatsRequest_StmtStatsOnly,
		"txn stats":   serverpb.CombinedStatementsStatsRequest_TxnStatsOnly,
	}

	for name, reqType := range requestTypes {
		mvt.InMixedVersion("request "+name, func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			return runGetRequests(ctx, c, l, c.CRDBNodes(), reqType)
		})
	}
	mvt.Run()
}

func runGetRequests(
	ctx context.Context,
	c cluster.Cluster,
	l *logger.Logger,
	roachNodes option.NodeListOption,
	statsType serverpb.CombinedStatementsStatsRequest_StatsType,
) error {
	// We'll validate that hitting the in-memory tables and either one of the
	// persisted (full or cached activity) does not return error on any node.
	// Ideally we'd have prior data in the persisted tables already so that we
	// can easily query data from the previous aggregation interval. For now
	// we'll just have to rely on waiting for the flush to occur.
	expectedTableCount := 2

	// Track the source tables used to service each request.
	foundTableSources := make(map[string]interface{})

	addToTableSources := func(res *serverpb.StatementsResponse) {
		if statsType == serverpb.CombinedStatementsStatsRequest_StmtStatsOnly {
			if res.StmtsSourceTable != "" {
				foundTableSources[res.StmtsSourceTable] = struct{}{}
			}
			return
		}
		if res.TxnsSourceTable != "" {
			foundTableSources[res.TxnsSourceTable] = struct{}{}
		}
	}

	s := createSQLStatsRequestHelper(c, l)
	// First, we'll attempt to get stats from the in-memory tables.
	// We can ensure we request from in-memory tables by requesting stats from
	// a time range for which no data is available, as the in-memory table
	// is only used when no data is available from system tables.
	if err := s.requestSQLStatsFromEmptyInterval(ctx, roachNodes, statsType, addToTableSources); err != nil {
		return errors.Wrap(err, "failed to request stats for empty interval")
	}
	inMemTable := crdbInternalStmtStatsCombined
	if statsType == serverpb.CombinedStatementsStatsRequest_TxnStatsOnly {
		inMemTable = crdbInternalTxnStatsCombined
	}
	if foundTableSources[inMemTable] == nil {
		return errors.Newf("expected to find in-memory tables in response, found %v", maps.Keys(foundTableSources))
	}

	// Now we'll wait for the flush to occur and request stats from the persisted tables.
	r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     30 * time.Second,
		MaxRetries:     6, // 6 * 30s = 3m, which is well past the flush interval.
	})
	foundFlushedStats := false
	for r.Next() {
		// Requesting data from the last two hours should return data from the persisted tables eventually.
		if err := s.requestSQLStatsFromLastTwoHours(ctx, roachNodes, statsType, addToTableSources); err != nil {
			return errors.Wrap(err, "failed to request stats for last two hours")
		}
		if len(foundTableSources) >= expectedTableCount {
			foundFlushedStats = true
			break
		}
		l.Printf("waiting for flushed stats...")
	}
	if !foundFlushedStats {
		return errors.Newf("failed to find flushed stats, found: %v", maps.Keys(foundTableSources))
	}

	return nil
}

func getCombinedStatementStatsURL(
	adminUIAddr string,
	statsType serverpb.CombinedStatementsStatsRequest_StatsType,
	requestedRange timeRange,
) string {
	searchParams := fmt.Sprintf("?fetch_mode.stats_type=%d&start=%d&end=%d&limit=10",
		statsType, requestedRange[0].Unix(), requestedRange[1].Unix())
	return `https://` + adminUIAddr + `/_status/combinedstmts` + searchParams
}

// timeRange is a pair of time.Time values.
type timeRange [2]time.Time

type sqlStatsRequestHelper struct {
	cluster cluster.Cluster
	client  *roachtestutil.RoachtestHTTPClient
	logger  *logger.Logger
}

func createSQLStatsRequestHelper(
	cluster cluster.Cluster, logger *logger.Logger,
) *sqlStatsRequestHelper {
	client := roachtestutil.DefaultHTTPClient(cluster, logger, roachtestutil.HTTPTimeout(15*time.Second))
	return &sqlStatsRequestHelper{
		cluster: cluster,
		logger:  logger,
		client:  client,
	}
}

// Requests stmt stats data from all nodes in `gatewayNodes` from
// the statusServer via `_status/combinedstmts`.
func (s *sqlStatsRequestHelper) requestSQLStats(
	ctx context.Context,
	gatewayNodes option.NodeListOption,
	statsType serverpb.CombinedStatementsStatsRequest_StatsType,
	requestedRange timeRange,
	respHandler func(*serverpb.StatementsResponse),
) error {
	adminUIAddrs, err := s.cluster.ExternalAdminUIAddr(ctx, s.logger, gatewayNodes)
	if err != nil {
		return err
	}

	for _, addr := range adminUIAddrs {
		url := getCombinedStatementStatsURL(addr, statsType, requestedRange)
		statsResponse := &serverpb.StatementsResponse{}
		if err := s.client.GetJSON(ctx, url, statsResponse, httputil.IgnoreUnknownFields()); err != nil {
			s.logger.Printf("error requesting stats from url: %s", url)
			return err
		}
		respHandler(statsResponse)
	}

	return nil
}

// Requests stmt stats data from the last two hours.
func (s *sqlStatsRequestHelper) requestSQLStatsFromLastTwoHours(
	ctx context.Context,
	gatewayNodes option.NodeListOption,
	statsType serverpb.CombinedStatementsStatsRequest_StatsType,
	respHandler func(*serverpb.StatementsResponse),
) error {
	start := timeutil.Now().Add(-2 * time.Hour)
	end := timeutil.Now()
	return s.requestSQLStats(ctx, gatewayNodes, statsType, timeRange{start, end}, respHandler)
}

// Requests stmt stats data  by using a time range for which no data
// is available. The source table returned in the response should be
// the in-memory table.
func (s *sqlStatsRequestHelper) requestSQLStatsFromEmptyInterval(
	ctx context.Context,
	gatewayNodes option.NodeListOption,
	statsType serverpb.CombinedStatementsStatsRequest_StatsType,
	respHandler func(*serverpb.StatementsResponse),
) error {
	end := timeutil.Unix(100, 0)
	start := timeutil.Unix(0, 0)
	return s.requestSQLStats(ctx, gatewayNodes, statsType, timeRange{start, end}, respHandler)
}
