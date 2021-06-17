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

// StatementWriter is the interface that provides methods to record
// statement stats for a given application name.
type StatementWriter interface {
	// RecordStatement records statistics for a statement.
	RecordStatement(ctx context.Context, key roachpb.StatementStatisticsKey, value RecordedStmtStats) (roachpb.StmtFingerprintID, error)

	// RecordStatementExecStats records execution statistics for a statement.
	// This is sampled and not recorded for every single statement.
	RecordStatementExecStats(key roachpb.StatementStatisticsKey, stats execstats.QueryLevelStats) error

	// ShouldSaveLogicalPlanDesc returns whether we should save the logical plan
	// description for a given combination of statement metadata.
	ShouldSaveLogicalPlanDesc(fingerprint string, implicitTxn bool, database string) bool
}

// TransactionWriter is the interface that provides methods to record
// transaction stats for a given application name..
type TransactionWriter interface {
	RecordTransaction(ctx context.Context, key roachpb.TransactionFingerprintID, value RecordedTxnStats) error
}

// Writer is the interface that provides methods to record statement and
// transaction stats.
type Writer interface {
	StatementWriter
	TransactionWriter
}

// Reader provides methods to retrieve transaction/statement statistics from
// the Storage.
type Reader interface {
	// GetLastReset returns the last time when the sqlstats is being reset.
	GetLastReset() time.Time

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

	// GetStatementStats performs a point lookup of statement statistics for a
	// given key.
	GetStatementStats(key *roachpb.StatementStatisticsKey) (*roachpb.CollectedStatementStatistics, error)

	// GetTransactionStats performs a point lookup of a transaction fingerprint key.
	GetTransactionStats(appName string, key roachpb.TransactionFingerprintID) (*roachpb.CollectedTransactionStatistics, error)
}

// IteratorOptions provides the ability to the caller to change how it iterates
// the statements and transactions.
// TODO(azhng): we want to support pagination/continuation tokens as well as
//  different error handling behaviors when error is encountered once we start
//  to support cluster-wide implementation of the sqlstats.Reader interface.
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
type StatementVisitor func(*roachpb.CollectedStatementStatistics) error

// TransactionVisitor is the callback that is invoked when caller iterate through
// all transaction statistics using IterateTransactionStats(). If an error is
// encountered when calling the visitor, the iteration is aborted.
type TransactionVisitor func(roachpb.TransactionFingerprintID, *roachpb.CollectedTransactionStatistics) error

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

	// Reset resets the StatsCollector with a new Writer and a new copy of the
	// sessionphase.Times.
	Reset(Writer, *sessionphase.Times)
}

// Storage provides clients with interface to perform read and write operations
// to sql statistics.
type Storage interface {
	Reader

	// GetWriterForApplication returns a Writer instance for the given application
	// name.
	GetWriterForApplication(appName string) Writer

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
	Nodes           []int64
	StatementType   tree.StatementType
	Plan            *roachpb.ExplainTreePlanNode
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
	BytesRead               int64
}
