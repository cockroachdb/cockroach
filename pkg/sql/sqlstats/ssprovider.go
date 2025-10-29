// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// Package sqlstats is a subsystem that is responsible for tracking the
// statistics of statements and transactions.

package sqlstats

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlcommenter"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// IteratorOptions provides the ability to the caller to change how it iterates
// the statements and transactions.
// TODO(azhng): introduce StartTime and EndTime field so we can implement
//
//	virtual indexes on crdb_internal.{statement,transaction}_statistics
//	using the iterators.
type IteratorOptions struct {
	// SortedAppNames determines whether or not the application names will be
	// sorted when iterating through statistics.
	SortedAppNames bool

	// SortedKey determines whether the key of the statistics will be sorted
	// when iterating through statistics;
	SortedKey bool
}

// StatementVisitor is the callback that is invoked when caller iterate through
// all statement statistics using IterateStatementStats(). If an error is
// encountered when calling the visitor, the iteration is aborted.
type StatementVisitor func(context.Context, *appstatspb.CollectedStatementStatistics) error

// TransactionVisitor is the callback that is invoked when caller iterate through
// all transaction statistics using IterateTransactionStats(). If an error is
// encountered when calling the visitor, the iteration is aborted.
type TransactionVisitor func(context.Context, *appstatspb.CollectedTransactionStatistics) error

// AggregatedTransactionVisitor is the callback invoked when iterate through
// transaction statistics collected at the application level using
// IterateAggregatedTransactionStats(). If an error is encountered when calling
// the visitor, the iteration is aborted.
type AggregatedTransactionVisitor func(appName string, statistics *appstatspb.TxnStats) error

// RecordedStmtStats stores the statistics of a statement to be recorded.
type RecordedStmtStats struct {
	FingerprintID            appstatspb.StmtFingerprintID
	Query                    string
	App                      string
	DistSQL                  bool
	ImplicitTxn              bool
	Vec                      bool
	FullScan                 bool
	Database                 string
	PlanHash                 uint64
	QuerySummary             string
	TransactionFingerprintID appstatspb.TransactionFingerprintID
	SessionID                clusterunique.ID
	StatementID              clusterunique.ID
	AutoRetryCount           int
	Failed                   bool
	Generic                  bool
	AutoRetryReason          error
	RowsAffected             int
	IdleLatencySec           float64
	ParseLatencySec          float64
	PlanLatencySec           float64
	RunLatencySec            float64
	ServiceLatencySec        float64
	OverheadLatencySec       float64
	BytesRead                int64
	RowsRead                 int64
	RowsWritten              int64
	Nodes                    []int64
	KVNodeIDs                []int32
	StatementType            tree.StatementType
	Plan                     *appstatspb.ExplainTreePlanNode
	PlanGist                 string
	StatementError           error
	IndexRecommendations     []string
	StartTime                time.Time
	EndTime                  time.Time
	ExecStats                *execstats.QueryLevelStats
	Indexes                  []string
	QueryTags                []sqlcommenter.QueryTag
	UnderOuterTxn            bool
}

// RecordedTxnStats stores the statistics of a transaction to be recorded.
type RecordedTxnStats struct {
	FingerprintID           appstatspb.TransactionFingerprintID
	SessionID               clusterunique.ID
	TransactionID           uuid.UUID
	TransactionTimeSec      float64
	StartTime               time.Time
	EndTime                 time.Time
	Committed               bool
	ImplicitTxn             bool
	RetryCount              int64
	AutoRetryReason         error
	StatementFingerprintIDs []appstatspb.StmtFingerprintID
	ServiceLatency          time.Duration
	RetryLatency            time.Duration
	CommitLatency           time.Duration
	IdleLatency             time.Duration
	RowsAffected            int
	CollectedExecStats      bool
	ExecStats               execstats.QueryLevelStats
	RowsRead                int64
	RowsWritten             int64
	BytesRead               int64
	Priority                roachpb.UserPriority
	TxnErr                  error
	Application             string
	// Normalized user name.
	UserNormalized   string
	InternalExecutor bool
}

// SSDrainer is the interface for draining or resetting sql stats.
type SSDrainer interface {
	// DrainStats Stats that are drained will permanently be removed from their
	// source. Once the stats are drained, they cannot be processed again.
	// DrainStats returns the collected statement and transaction statistics, as
	// well as the total number of fingerprints drained.
	DrainStats(ctx context.Context) (
		[]*appstatspb.CollectedStatementStatistics,
		[]*appstatspb.CollectedTransactionStatistics,
		int64,
	)
	// Reset will reset all the stats in the drainer. Once reset, the stats will
	// be lost.
	Reset(ctx context.Context) error
}

type StatementLatencyRecorder interface {
	RunLatency() time.Duration
	IdleLatency() time.Duration
	ServiceLatency() time.Duration
	ParsingLatency() time.Duration
	PlanningLatency() time.Duration
	ProcessingLatency() time.Duration
	ExecOverheadLatency() time.Duration
	StartTime() time.Time
	EndTime() time.Time
}

type QueryStats interface {
	BytesRead() int64
	RowsRead() int64
	RowsWritten() int64
}

type PlanInfo interface {
	Generic() bool
	DistSQL() bool
	Vectorized() bool
	ImplicitTxn() bool
	FullScan() bool
	Gist() string
	Hash() uint64
}

type RecordedStatementStatsBuilder[L StatementLatencyRecorder, Q QueryStats, P PlanInfo] struct {
	stmtStats *RecordedStmtStats
}

func NewRecordedStatementStatsBuilder[L StatementLatencyRecorder, Q QueryStats, P PlanInfo](
	fingerprintId appstatspb.StmtFingerprintID,
	sessionId clusterunique.ID,
	database string,
	fingerprint string,
	summary string,
	queryID clusterunique.ID,
	stmtType tree.StatementType,
	appName string,
	info P,
) RecordedStatementStatsBuilder[L, Q, P] {
	return RecordedStatementStatsBuilder[L, Q, P]{
		stmtStats: &RecordedStmtStats{
			FingerprintID: fingerprintId,
			QuerySummary:  summary,
			StatementType: stmtType,
			SessionID:     sessionId,
			StatementID:   queryID,
			Query:         fingerprint,
			Database:      database,
			App:           appName,
			Generic:       info.Generic(),
			DistSQL:       info.DistSQL(),
			Vec:           info.Vectorized(),
			ImplicitTxn:   info.ImplicitTxn(),
			FullScan:      info.FullScan(),
			PlanGist:      info.Gist(),
			PlanHash:      info.Hash(),
		},
	}
}

func (b RecordedStatementStatsBuilder[L, Q, P]) LatencyRecorder(
	recorder L,
) RecordedStatementStatsBuilder[L, Q, P] {
	b.stmtStats.RunLatencySec = recorder.RunLatency().Seconds()
	b.stmtStats.IdleLatencySec = recorder.IdleLatency().Seconds()
	b.stmtStats.ServiceLatencySec = recorder.ServiceLatency().Seconds()
	b.stmtStats.ParseLatencySec = recorder.ParsingLatency().Seconds()
	b.stmtStats.PlanLatencySec = recorder.PlanningLatency().Seconds()
	b.stmtStats.OverheadLatencySec = recorder.ExecOverheadLatency().Seconds()
	b.stmtStats.StartTime = recorder.StartTime()
	b.stmtStats.EndTime = recorder.EndTime()
	return b
}

func (b RecordedStatementStatsBuilder[L, Q, P]) QueryLevelStats(
	stats Q,
) RecordedStatementStatsBuilder[L, Q, P] {
	b.stmtStats.BytesRead = stats.BytesRead()
	b.stmtStats.RowsRead = stats.RowsRead()
	b.stmtStats.RowsWritten = stats.RowsWritten()
	return b
}

func (b RecordedStatementStatsBuilder[L, Q, P]) ExecStats(
	execStats *execstats.QueryLevelStats,
) RecordedStatementStatsBuilder[L, Q, P] {
	if execStats == nil {
		return b
	}

	var sqlInstanceIDs []int64
	var kvNodeIDs []int32
	sqlInstanceIDs = make([]int64, 0, len(execStats.SQLInstanceIDs))
	for _, sqlInstanceID := range execStats.SQLInstanceIDs {
		sqlInstanceIDs = append(sqlInstanceIDs, int64(sqlInstanceID))
	}
	kvNodeIDs = execStats.KVNodeIDs
	b.stmtStats.KVNodeIDs = kvNodeIDs
	b.stmtStats.Nodes = sqlInstanceIDs
	b.stmtStats.ExecStats = execStats
	return b
}

func (b RecordedStatementStatsBuilder[L, Q, P]) Indexes(
	indexes []string,
) RecordedStatementStatsBuilder[L, Q, P] {
	b.stmtStats.Indexes = indexes
	return b
}

func (b RecordedStatementStatsBuilder[L, Q, P]) StatementError(
	stmtErr error,
) RecordedStatementStatsBuilder[L, Q, P] {
	if stmtErr == nil {
		return b
	}
	b.stmtStats.StatementError = stmtErr
	b.stmtStats.Failed = true
	return b
}

func (b RecordedStatementStatsBuilder[L, Q, P]) AutoRetry(
	autoRetryCount int, autoRetryReason error,
) RecordedStatementStatsBuilder[L, Q, P] {
	b.stmtStats.AutoRetryCount = autoRetryCount
	b.stmtStats.AutoRetryReason = autoRetryReason
	return b
}

func (b RecordedStatementStatsBuilder[L, Q, P]) RowsAffected(
	rowsAffected int,
) RecordedStatementStatsBuilder[L, Q, P] {
	b.stmtStats.RowsAffected = rowsAffected
	return b
}

func (b RecordedStatementStatsBuilder[L, Q, P]) IndexRecommendations(
	idxRecommendations []string,
) RecordedStatementStatsBuilder[L, Q, P] {
	b.stmtStats.IndexRecommendations = idxRecommendations
	return b
}

func (b RecordedStatementStatsBuilder[L, Q, P]) UnderOuterTxn() RecordedStatementStatsBuilder[L, Q, P] {
	b.stmtStats.UnderOuterTxn = true
	return b
}
func (b RecordedStatementStatsBuilder[L, Q, P]) QueryTags(
	queryTags []sqlcommenter.QueryTag,
) RecordedStatementStatsBuilder[L, Q, P] {
	b.stmtStats.QueryTags = queryTags
	return b
}

func (b RecordedStatementStatsBuilder[L, Q, P]) Build() *RecordedStmtStats {
	return b.stmtStats
}
