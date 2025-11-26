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
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/crlib/crtime"
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
	AppliedStmtHints         bool
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
	ExecOverheadLatency() time.Duration
	StartTime() time.Time
	EndTime() time.Time
}

// RecordedStatementStatsBuilder is a builder for RecordedStmtStats that
// provides a constructor and chained methods for setting optional fields or
// fields that may not be accessible at the time of construction. To build,
// PlanMetadata and LatencyRecorder must have been set.
type RecordedStatementStatsBuilder struct {
	stmtStats         *RecordedStmtStats
	planMetadataSet   bool
	latenciesRecorded bool
}

func NewRecordedStatementStatsBuilder(
	fingerprintId appstatspb.StmtFingerprintID,
	database string,
	fingerprint string,
	summary string,
	stmtType tree.StatementType,
	appName string,
) *RecordedStatementStatsBuilder {
	return &RecordedStatementStatsBuilder{
		stmtStats: &RecordedStmtStats{
			FingerprintID: fingerprintId,
			QuerySummary:  summary,
			StatementType: stmtType,
			Query:         fingerprint,
			Database:      database,
			App:           appName,
		},
	}
}

func (b *RecordedStatementStatsBuilder) PlanMetadata(
	generic bool, distSQL bool, vectorized bool, implicitTxn bool, fullScan bool,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.Generic = generic
	b.stmtStats.DistSQL = distSQL
	b.stmtStats.Vec = vectorized
	b.stmtStats.ImplicitTxn = implicitTxn
	b.stmtStats.FullScan = fullScan
	b.planMetadataSet = true
	return b
}

func (b *RecordedStatementStatsBuilder) PlanGist(
	gist string, hash uint64,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.PlanGist = gist
	b.stmtStats.PlanHash = hash
	return b
}

func (b *RecordedStatementStatsBuilder) LatencyRecorder(
	recorder StatementLatencyRecorder,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.RunLatencySec = recorder.RunLatency().Seconds()
	b.stmtStats.IdleLatencySec = recorder.IdleLatency().Seconds()
	b.stmtStats.ServiceLatencySec = recorder.ServiceLatency().Seconds()
	b.stmtStats.ParseLatencySec = recorder.ParsingLatency().Seconds()
	b.stmtStats.PlanLatencySec = recorder.PlanningLatency().Seconds()
	b.stmtStats.OverheadLatencySec = recorder.ExecOverheadLatency().Seconds()
	b.stmtStats.StartTime = recorder.StartTime()
	b.stmtStats.EndTime = recorder.EndTime()
	b.latenciesRecorded = true
	return b
}

func (b *RecordedStatementStatsBuilder) QueryLevelStats(
	bytesRead int64, rowsRead int64, rowsWritten int64,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.BytesRead = bytesRead
	b.stmtStats.RowsRead = rowsRead
	b.stmtStats.RowsWritten = rowsWritten
	return b
}

func (b *RecordedStatementStatsBuilder) ExecStats(
	execStats *execstats.QueryLevelStats,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
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

func (b *RecordedStatementStatsBuilder) Indexes(indexes []string) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.Indexes = indexes
	return b
}

func (b *RecordedStatementStatsBuilder) StatementError(
	stmtErr error,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	if stmtErr == nil {
		return b
	}
	b.stmtStats.StatementError = stmtErr
	b.stmtStats.Failed = true
	return b
}

func (b *RecordedStatementStatsBuilder) AutoRetry(
	autoRetryCount int, autoRetryReason error,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.AutoRetryCount = autoRetryCount
	b.stmtStats.AutoRetryReason = autoRetryReason
	return b
}

func (b *RecordedStatementStatsBuilder) RowsAffected(
	rowsAffected int,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.RowsAffected = rowsAffected
	return b
}

func (b *RecordedStatementStatsBuilder) IndexRecommendations(
	idxRecommendations []string,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.IndexRecommendations = idxRecommendations
	return b
}

func (b *RecordedStatementStatsBuilder) UnderOuterTxn() *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.UnderOuterTxn = true
	return b
}

func (b *RecordedStatementStatsBuilder) QueryTags(
	queryTags []sqlcommenter.QueryTag,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.QueryTags = queryTags
	return b
}

func (b *RecordedStatementStatsBuilder) QueryID(
	queryID clusterunique.ID,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.StatementID = queryID
	return b
}

func (b *RecordedStatementStatsBuilder) SessionID(
	sessionID clusterunique.ID,
) *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.SessionID = sessionID
	return b
}

func (b *RecordedStatementStatsBuilder) AppliedStatementHints() *RecordedStatementStatsBuilder {
	if b == nil {
		return b
	}
	b.stmtStats.AppliedStmtHints = true
	return b
}

// Build returns the final RecordedStmtStats struct. It returns nil if not all
// required fields have been set or if the builder itself is nil. In test
// builds, it panics if not all required fields have been set.
func (b *RecordedStatementStatsBuilder) Build() *RecordedStmtStats {
	if !b.validate() {
		if buildutil.CrdbTestBuild {
			panic("RecordedStatementStatsBuilder: not all required fields set. " +
				"plan metadata and latencies are required.")
		}
		return nil
	}
	return b.stmtStats
}

func (b *RecordedStatementStatsBuilder) validate() bool {
	if b == nil {
		return false
	}

	return b.planMetadataSet && b.latenciesRecorded
}

type StatsBuilderWithLatencyRecorder struct {
	StatsBuilder    *RecordedStatementStatsBuilder
	LatencyRecorder *LatencyRecorder
}

type StatementPhase int

const (
	StatementStarted StatementPhase = iota
	StatementStartParsing
	StatementEndParsing
	StatementStartPlanning
	StatementEndPlanning
	StatementStartExec
	StatementEndExec
	StatementEnd
	statementNumPhases
)

type LatencyRecorder struct {
	times [statementNumPhases]crtime.Mono
}

var _ StatementLatencyRecorder = &LatencyRecorder{}

func NewStatementLatencyRecorder() *LatencyRecorder {
	return &LatencyRecorder{}
}

func (r *LatencyRecorder) Reset() {
	*r = LatencyRecorder{}
}

func (r *LatencyRecorder) RecordPhase(phase StatementPhase, time crtime.Mono) {
	r.times[phase] = time
}

func (r *LatencyRecorder) RunLatency() time.Duration {
	return r.times[StatementEndExec].Sub(r.times[StatementStartExec])
}

func (r *LatencyRecorder) IdleLatency() time.Duration {
	return 0
}

func (r *LatencyRecorder) ServiceLatency() time.Duration {
	return r.times[StatementEndExec].Sub(r.times[StatementStarted])
}
func (r *LatencyRecorder) ParsingLatency() time.Duration {
	return r.times[StatementEndParsing].Sub(r.times[StatementStartParsing])
}
func (r *LatencyRecorder) PlanningLatency() time.Duration {
	return r.times[StatementEndPlanning].Sub(r.times[StatementStartPlanning])
}

func (r *LatencyRecorder) ProcessingLatency() time.Duration {
	return r.ParsingLatency() + r.PlanningLatency() + r.RunLatency()
}

func (r *LatencyRecorder) ExecOverheadLatency() time.Duration {
	return r.ServiceLatency() - r.ProcessingLatency()
}

func (r *LatencyRecorder) StartTime() time.Time {
	return r.times[StatementStarted].ToUTC()
}

func (r *LatencyRecorder) EndTime() time.Time {
	return r.times[StatementEnd].ToUTC()
}

func RecordStatementPhase(r *LatencyRecorder, phase StatementPhase) {
	if r != nil {
		r.RecordPhase(phase, crtime.NowMono())
	}
}
