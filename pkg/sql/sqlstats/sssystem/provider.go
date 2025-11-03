// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sssystem

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type SQLStats struct {
	localSqlStats     *sslocal.SQLStats
	reportedSqlStats  *sslocal.SQLStats
	persistedSqlStats *persistedsqlstats.PersistedSQLStats
	sqlStatsIngester  *sslocal.SQLStatsIngester
	insightsProvider  *insights.Provider
	settings          *cluster.Settings
}

func (s *SQLStats) Start(ctx context.Context, stopper *stop.Stopper) {
	s.sqlStatsIngester.Start(ctx, stopper)
	s.persistedSqlStats.Start(ctx, stopper)

	// reportedStats is periodically cleared to prevent too many SQL Stats
	// accumulated in the reporter when the telemetry server fails.
	// Usually it is telemetry's reporter's job to clear the reporting SQL Stats.
	s.reportedSqlStats.Start(ctx, stopper)
}

func (s *SQLStats) Stop(ctx context.Context) {
	s.persistedSqlStats.Stop(ctx)
}

func NewSQLStats(
	settings *cluster.Settings,
	db isql.DB,
	clusterID func() uuid.UUID,
	nodeID *base.SQLIDContainer,
	statsMetrics sqlstats.Metrics,
	insightsMetrics insights.Metrics,
	statusServer serverpb.SQLStatusServer,
	sqlStatsMonitor *mon.BytesMonitor,
	persistedSqlStatsMonitor *mon.BytesMonitor,
	testingKnobs *sqlstats.TestingKnobs,
) *SQLStats {
	insightsProvider := insights.New(settings, insightsMetrics)
	reportedSQLStats := sslocal.NewSQLStats(
		settings,
		sqlstats.MaxMemReportedSQLStatsStmtFingerprints,
		sqlstats.MaxMemReportedSQLStatsTxnFingerprints,
		statsMetrics.ReportedSQLStatsMemoryCurBytesCount,
		statsMetrics.ReportedSQLStatsMemoryMaxBytesHist,
		nil, /* discardedStatsCount */
		sqlStatsMonitor,
		nil, /* reportedProvider */
		testingKnobs,
	)
	localSQLStats := sslocal.NewSQLStats(
		settings,
		sqlstats.MaxMemSQLStatsStmtFingerprints,
		sqlstats.MaxMemSQLStatsTxnFingerprints,
		statsMetrics.SQLStatsMemoryCurBytesCount,
		statsMetrics.SQLStatsMemoryMaxBytesHist,
		statsMetrics.DiscardedStatsCount,
		sqlStatsMonitor,
		reportedSQLStats,
		testingKnobs,
	)
	sqlStatsIngester := sslocal.NewSQLStatsIngester(
		settings, testingKnobs, statsMetrics, insightsProvider, localSQLStats)
	// TODO(117690): Unify StmtStatsEnable and TxnStatsEnable into a single cluster setting.
	sqlstats.TxnStatsEnable.SetOnChange(&settings.SV, func(_ context.Context) {
		if !sqlstats.TxnStatsEnable.Get(&settings.SV) {
			sqlStatsIngester.Clear()
		}
	})

	persistedSQLStats := persistedsqlstats.New(&persistedsqlstats.Config{
		Settings:                settings,
		InternalExecutorMonitor: persistedSqlStatsMonitor,
		DB:                      db,
		ClusterID:               clusterID,
		SQLIDContainer:          nodeID,
		FanoutServer:            statusServer,
		Knobs:                   testingKnobs,
		FlushesSuccessful:       statsMetrics.SQLStatsFlushesSuccessful,
		FlushDoneSignalsIgnored: statsMetrics.SQLStatsFlushDoneSignalsIgnored,
		FlushedFingerprintCount: statsMetrics.SQLStatsFlushFingerprintCount,
		FlushesFailed:           statsMetrics.SQLStatsFlushesFailed,
		FlushLatency:            statsMetrics.SQLStatsFlushLatency,
	}, localSQLStats)

	return &SQLStats{
		localSqlStats:     localSQLStats,
		reportedSqlStats:  reportedSQLStats,
		persistedSqlStats: persistedSQLStats,
		sqlStatsIngester:  sqlStatsIngester,
		insightsProvider:  insightsProvider,
		settings:          settings,
	}
}

// ResetClusterSQLStats implements eval.SQLStatsController
func (s *SQLStats) ResetClusterSQLStats(ctx context.Context) error {
	return s.persistedSqlStats.ResetClusterSQLStats(ctx)
}

// ResetActivityTables implements eval.SQLStatsController
func (s *SQLStats) ResetActivityTables(ctx context.Context) error {
	return s.persistedSqlStats.ResetActivityTables(ctx)
}

// CreateSQLStatsCompactionSchedule implements eval.SQLStatsController
func (s *SQLStats) CreateSQLStatsCompactionSchedule(ctx context.Context) error {
	return s.persistedSqlStats.CreateSQLStatsCompactionSchedule(ctx)
}

// LOCAL SQL STATS
func (s *SQLStats) RecordStatement(ctx context.Context, stmt *sqlstats.RecordedStmtStats) {
	s.sqlStatsIngester.RecordStatement(ctx, stmt)
}
func (s *SQLStats) RecordTransaction(ctx context.Context, txn *sqlstats.RecordedTxnStats) {
	s.sqlStatsIngester.RecordTransaction(ctx, txn)
}
func (s *SQLStats) GetApplicationStats(name string) *ssmemstorage.Container {
	return s.localSqlStats.GetApplicationStats(name)
}
func (s *SQLStats) ResetLocalStats(ctx context.Context) error {
	return s.localSqlStats.Reset(ctx)
}
func (s *SQLStats) GetLastLocalReset() time.Time {
	return s.localSqlStats.GetLastReset()
}
func (s *SQLStats) AddAppStats(
	ctx context.Context, appName string, other *ssmemstorage.Container,
) error {
	return s.localSqlStats.AddAppStats(ctx, appName, other)
}
func (s *SQLStats) IterateStatementStats(
	ctx context.Context, options sqlstats.IteratorOptions, visitor sqlstats.StatementVisitor,
) error {
	return s.localSqlStats.IterateStatementStats(ctx, options, visitor)
}

func (s *SQLStats) IterateTransactionStats(
	ctx context.Context, options sqlstats.IteratorOptions, visitor sqlstats.TransactionVisitor,
) error {
	return s.localSqlStats.IterateTransactionStats(ctx, options, visitor)
}

func (s *SQLStats) IterateAggregatedTransactionStats(
	ctx context.Context,
	options sqlstats.IteratorOptions,
	visitor sqlstats.AggregatedTransactionVisitor,
) error {
	return s.localSqlStats.IterateAggregatedTransactionStats(ctx, options, visitor)
}

func (s *SQLStats) GetTotalFingerprintCount() int64 {
	return s.localSqlStats.GetTotalFingerprintCount()
}

func (s *SQLStats) GetCounters() *ssmemstorage.SQLStatsAtomicCounters {
	return s.localSqlStats.GetCounters()
}

func (s *SQLStats) DrainStats(
	ctx context.Context,
) (
	[]*appstatspb.CollectedStatementStatistics,
	[]*appstatspb.CollectedTransactionStatistics,
	int64,
) {
	return s.localSqlStats.DrainStats(ctx)
}

// PERSISTED SQL STATS
func (s *SQLStats) MaybeFlush(ctx context.Context, stopper *stop.Stopper) bool {
	return s.persistedSqlStats.MaybeFlush(ctx, stopper)
}

func (s *SQLStats) ComputeAggregatedTs() time.Time {
	return s.persistedSqlStats.ComputeAggregatedTs()
}

func (s *SQLStats) GetAggregationInterval() time.Duration {
	return s.persistedSqlStats.GetAggregationInterval()
}

func (s *SQLStats) SetFlushDoneSignalCh(sigCh chan<- struct{}) {
	s.persistedSqlStats.SetFlushDoneSignalCh(sigCh)
}

func (s *SQLStats) StmtsLimitSizeReached(ctx context.Context) (bool, error) {
	return s.persistedSqlStats.StmtsLimitSizeReached(ctx)
}

func (s *SQLStats) GetNextFlushAt() time.Time {
	return s.persistedSqlStats.GetNextFlushAt()
}

// REPORTED SQL STATS
func (s *SQLStats) ResetReportedStats(ctx context.Context) error {
	return s.reportedSqlStats.Reset(ctx)
}

func (s *SQLStats) IterateReportedStatementStats(
	ctx context.Context, options sqlstats.IteratorOptions, visitor sqlstats.StatementVisitor,
) error {
	return s.reportedSqlStats.IterateStatementStats(ctx, options, visitor)
}

// INGESTER
func (s *SQLStats) ClearSession(sessionID clusterunique.ID) {
	s.sqlStatsIngester.ClearSession(sessionID)
}

// INSIGHTS
func (s *SQLStats) GetInsightsReader() *insights.LockingStore {
	return s.insightsProvider.Store()
}

func (s *SQLStats) NewStatsCollector(
	appStats *ssmemstorage.Container, phaseTimes *sessionphase.Times,
) *sslocal.StatsCollector {

	return sslocal.NewStatsCollector(
		s.settings,
		appStats,
		s.sqlStatsIngester,
		phaseTimes,
		s.GetCounters(),
	)
}

type IterateStatementStats func(
	ctx context.Context,
	options sqlstats.IteratorOptions,
	visitor sqlstats.StatementVisitor,
) error
