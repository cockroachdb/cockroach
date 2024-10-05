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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Writer is the interface that provides methods to record statement and
// transaction stats.
type Writer interface {
	// RecordStatement records statistics for a statement.
	RecordStatement(ctx context.Context, key appstatspb.StatementStatisticsKey, value RecordedStmtStats) (appstatspb.StmtFingerprintID, error)

	// RecordStatementExecStats records execution statistics for a statement.
	// This is sampled and not recorded for every single statement.
	RecordStatementExecStats(key appstatspb.StatementStatisticsKey, stats execstats.QueryLevelStats) error

	// ShouldSample returns two booleans, the first one indicates whether we
	// ever sampled (i.e. collected statistics for) the given combination of
	// statement metadata, and the second one whether we should save the logical
	// plan description for it.
	ShouldSample(fingerprint string, implicitTxn bool, database string) (previouslySampled, savePlanForStats bool)

	// RecordTransaction records statistics for a transaction.
	RecordTransaction(ctx context.Context, key appstatspb.TransactionFingerprintID, value RecordedTxnStats) error
}

// Reader provides methods to retrieve transaction/statement statistics from
// the Storage.
type Reader interface {
	// IterateStatementStats iterates through all the collected statement statistics
	// by using StatementVisitor. Caller can specify iteration behavior, such
	// as ordering, through IteratorOptions argument. StatementVisitor can return
	// error, if an error is returned after the execution of the visitor, the
	// iteration is aborted.
	IterateStatementStats(context.Context, IteratorOptions, StatementVisitor) error

	// IterateTransactionStats iterates through all the collected transaction
	// statistics by using TransactionVisitor. It behaves similarly to
	// IterateStatementStats.
	IterateTransactionStats(context.Context, IteratorOptions, TransactionVisitor) error

	// IterateAggregatedTransactionStats iterates through all the collected app-level
	// transactions statistics. It behaves similarly to IterateStatementStats.
	IterateAggregatedTransactionStats(context.Context, IteratorOptions, AggregatedTransactionVisitor) error
}

// ApplicationStats is an interface to read from or write to the statistics
// belongs to an application.
type ApplicationStats interface {
	Reader
	Writer

	// MergeApplicationStatementStats merges the other application's statement
	// statistics into the current ApplicationStats. It returns how many number
	// of statistics were being discarded due to memory constraint. If the
	// transformer is non-nil, then it is applied to other's statement statistics
	// before other's statement statistics are merged into the current
	// ApplicationStats.
	MergeApplicationStatementStats(
		ctx context.Context,
		other ApplicationStats,
		transformer func(statistics *appstatspb.CollectedStatementStatistics),
	) uint64

	// MergeApplicationTransactionStats merges the other application's transaction
	// statistics into the current ApplicationStats. It returns how many number
	// of statistics were being discarded due to memory constraint.
	MergeApplicationTransactionStats(
		ctx context.Context,
		other ApplicationStats,
	) uint64

	// MaybeLogDiscardMessage is used to possibly log a message when statistics
	// are being discarded because of memory limits.
	MaybeLogDiscardMessage(ctx context.Context)

	// NewApplicationStatsWithInheritedOptions returns a new ApplicationStats
	// interface that inherits all memory limits of the existing
	NewApplicationStatsWithInheritedOptions() ApplicationStats

	// Free frees the current ApplicationStats and zeros out the memory counts
	// and fingerprint counts.
	Free(context.Context)
}

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

// Storage provides clients with interface to perform read and write operations
// to sql statistics.
type Storage interface {
	Reader

	// GetLastReset returns the last time when the sqlstats is being reset.
	GetLastReset() time.Time

	// GetApplicationStats returns an ApplicationStats instance for the given
	// application name.
	GetApplicationStats(appName string, internal bool) ApplicationStats

	// Reset resets all the statistics stored in-memory in the current Storage.
	Reset(context.Context) error
}

// Provider is a wrapper around sqlstats subsystem for external consumption.
type Provider interface {
	Storage

	Start(ctx context.Context, stopper *stop.Stopper)
}

// RecordedStmtStats stores the statistics of a statement to be recorded.
type RecordedStmtStats struct {
	SessionID            clusterunique.ID
	StatementID          clusterunique.ID
	TransactionID        uuid.UUID
	AutoRetryCount       int
	AutoRetryReason      error
	RowsAffected         int
	IdleLatencySec       float64
	ParseLatencySec      float64
	PlanLatencySec       float64
	RunLatencySec        float64
	ServiceLatencySec    float64
	OverheadLatencySec   float64
	BytesRead            int64
	RowsRead             int64
	RowsWritten          int64
	Nodes                []int64
	StatementType        tree.StatementType
	Plan                 *appstatspb.ExplainTreePlanNode
	PlanGist             string
	StatementError       error
	IndexRecommendations []string
	Query                string
	StartTime            time.Time
	EndTime              time.Time
	FullScan             bool
	ExecStats            *execstats.QueryLevelStats
	Indexes              []string
	Database             string
}

// RecordedTxnStats stores the statistics of a transaction to be recorded.
type RecordedTxnStats struct {
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
	SessionData             *sessiondata.SessionData
	TxnErr                  error
}
