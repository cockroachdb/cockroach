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
	FingerprintID        appstatspb.StmtFingerprintID
	SessionID            clusterunique.ID
	StatementID          clusterunique.ID
	TransactionID        uuid.UUID
	AutoRetryCount       int
	Failed               bool
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
	KVNodeIDs            []int32
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
