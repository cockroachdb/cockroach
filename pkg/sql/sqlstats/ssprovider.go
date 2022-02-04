// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// Package sqlstats is a subsystem that is responsible for tracking the
// statistics of statements and transactions.

package sqlstats

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Writer is the interface that provides methods to record statement and
// transaction stats.
type Writer interface {
	// RecordStatement records statistics for a statement.
	RecordStatement(ctx context.Context, key roachpb.StatementStatisticsKey, value RecordedStmtStats) (roachpb.StmtFingerprintID, error)

	// RecordStatementExecStats records execution statistics for a statement.
	// This is sampled and not recorded for every single statement.
	RecordStatementExecStats(key roachpb.StatementStatisticsKey, stats execstats.QueryLevelStats) error

	// ShouldSaveLogicalPlanDesc returns whether we should save the logical plan
	// description for a given combination of statement metadata.
	ShouldSaveLogicalPlanDesc(fingerprint string, implicitTxn bool, database string) bool

	// RecordTransaction records statistics for a transaction.
	RecordTransaction(ctx context.Context, key roachpb.TransactionFingerprintID, value RecordedTxnStats) error
}

// Reader provides methods to retrieve transaction/statement statistics from
// the Storage.
type Reader interface {
	// IterateStatementStats iterates through all the collected statement statistics
	// by using StatementVisitor. Caller can specify iteration behavior, such
	// as ordering, through IteratorOptions argument. StatementVisitor can return
	// error, if an error is returned after the execution of the visitor, the
	// iteration is aborted.
	IterateStatementStats(context.Context, *IteratorOptions, StatementVisitor) error

	// IterateTransactionStats iterates through all the collected transaction
	// statistics by using TransactionVisitor. It behaves similarly to
	// IterateStatementStats.
	IterateTransactionStats(context.Context, *IteratorOptions, TransactionVisitor) error

	// IterateAggregatedTransactionStats iterates through all the collected app-level
	// transactions statistics. It behaves similarly to IterateStatementStats.
	IterateAggregatedTransactionStats(context.Context, *IteratorOptions, AggregatedTransactionVisitor) error
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
		transformer func(statistics *roachpb.CollectedStatementStatistics),
	) uint64

	// MergeApplicationTransactionStats merges the other application's transaction
	// statistics into the current ApplicationStats. It returns how many number
	// of statistics were being discarded due to memory constraint.
	MergeApplicationTransactionStats(
		ctx context.Context,
		other ApplicationStats,
	) uint64

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
//  virtual indexes on crdb_internal.{statement,transaction}_statistics
//  using the iterators.
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
type StatementVisitor func(context.Context, *roachpb.CollectedStatementStatistics) error

// TransactionVisitor is the callback that is invoked when caller iterate through
// all transaction statistics using IterateTransactionStats(). If an error is
// encountered when calling the visitor, the iteration is aborted.
type TransactionVisitor func(context.Context, *roachpb.CollectedTransactionStatistics) error

// AggregatedTransactionVisitor is the callback invoked when iterate through
// transaction statistics collected at the application level using
// IterateAggregatedTransactionStats(). If an error is encountered when calling
// the visitor, the iteration is aborted.
type AggregatedTransactionVisitor func(appName string, statistics *roachpb.TxnStats) error

// StatsCollector is an interface that collects statistics for transactions and
// statements for the entire lifetime of a session.
type StatsCollector interface {
	Writer

	// PhaseTimes returns the sessionphase.Times that this StatsCollector is
	// currently tracking.
	PhaseTimes() *sessionphase.Times

	// PreviousPhaseTimes returns the sessionphase.Times that this StatsCollector
	// was previously tracking before being Reset.
	PreviousPhaseTimes() *sessionphase.Times

	// Reset resets the StatsCollector with a new ApplicationStats and a new copy
	// of the sessionphase.Times.
	Reset(ApplicationStats, *sessionphase.Times)

	// StartExplicitTransaction informs StatsCollector that all subsequent
	// statements will be executed in the context an explicit transaction.
	StartExplicitTransaction()

	// EndExplicitTransaction informs the StatsCollector that the explicit txn has
	// finished execution. (Either COMMITTED or ABORTED). This means the txn's
	// fingerprint ID is now available. StatsCollector will now go back to update
	// the transaction fingerprint ID field of all the statement statistics for that
	// txn.
	EndExplicitTransaction(ctx context.Context, transactionFingerprintID roachpb.TransactionFingerprintID,
	)
}

// Storage provides clients with interface to perform read and write operations
// to sql statistics.
type Storage interface {
	Reader

	// GetLastReset returns the last time when the sqlstats is being reset.
	GetLastReset() time.Time

	// GetApplicationStats returns an ApplicationStats instance for the given
	// application name.
	GetApplicationStats(appName string) ApplicationStats

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
	AutoRetryCount  int
	RowsAffected    int
	ParseLatency    float64
	PlanLatency     float64
	RunLatency      float64
	ServiceLatency  float64
	OverheadLatency float64
	BytesRead       int64
	RowsRead        int64
	RowsWritten     int64
	Nodes           []int64
	StatementType   tree.StatementType
	Plan            *roachpb.ExplainTreePlanNode
	PlanGist        string
	StatementError  error
}

// RecordedTxnStats stores the statistics of a transaction to be recorded.
type RecordedTxnStats struct {
	TransactionTimeSec      float64
	Committed               bool
	ImplicitTxn             bool
	RetryCount              int64
	StatementFingerprintIDs []roachpb.StmtFingerprintID
	ServiceLatency          time.Duration
	RetryLatency            time.Duration
	CommitLatency           time.Duration
	RowsAffected            int
	CollectedExecStats      bool
	ExecStats               execstats.QueryLevelStats
	RowsRead                int64
	RowsWritten             int64
	BytesRead               int64
}
