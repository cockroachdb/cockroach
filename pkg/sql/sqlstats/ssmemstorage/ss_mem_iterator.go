// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ssmemstorage

import (
	"slices"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
)

type baseIterator struct {
	container *Container
	idx       int
}

// StmtStatsIterator is an iterator that iterates over the statement statistics
// inside of a ssmemstorage.Container.
type StmtStatsIterator struct {
	baseIterator
	stmtKeys     stmtList
	currentValue *appstatspb.CollectedStatementStatistics
}

// NewStmtStatsIterator returns a StmtStatsIterator.
func NewStmtStatsIterator(
	container *Container, options sqlstats.IteratorOptions,
) StmtStatsIterator {
	var stmtKeys stmtList
	func() {
		container.mu.Lock()
		defer container.mu.Unlock()
		for k := range container.mu.stmts {
			stmtKeys = append(stmtKeys, k)
		}
	}()

	if options.SortedKey {
		sort.Sort(stmtKeys)
	}

	return StmtStatsIterator{
		baseIterator: baseIterator{
			container: container,
			idx:       -1,
		},
		stmtKeys: stmtKeys,
	}
}

// Initialized returns true if the iterator has been initialized, false otherwise.
func (s *StmtStatsIterator) Initialized() bool {
	return s.container != nil
}

// Next updates the current value returned by the subsequent Cur() call. Next()
// returns true if the following Cur() call is valid, false otherwise.
func (s *StmtStatsIterator) Next() bool {
	s.idx++
	if s.idx >= len(s.stmtKeys) {
		return false
	}

	stmtKey := s.stmtKeys[s.idx]

	statementStats := s.container.getStatsForStmtWithKey(stmtKey)

	// If the key is not found (and we expected to find it), the table must
	// have been cleared between now and the time we read all the keys. In
	// that case we simply skip this key and keep advancing the iterator.
	if statementStats == nil {
		return s.Next()
	}

	statementStats.mu.Lock()
	data := copyDataLocked(statementStats)
	distSQLUsed := statementStats.mu.distSQLUsed
	vectorized := statementStats.mu.vectorized
	fullScan := statementStats.mu.fullScan
	database := statementStats.mu.database
	querySummary := statementStats.mu.querySummary
	statementStats.mu.Unlock()

	s.currentValue = &appstatspb.CollectedStatementStatistics{
		Key: appstatspb.StatementStatisticsKey{
			Query:                    stmtKey.stmtNoConstants,
			QuerySummary:             querySummary,
			DistSQL:                  distSQLUsed,
			Vec:                      vectorized,
			ImplicitTxn:              stmtKey.implicitTxn,
			FullScan:                 fullScan,
			App:                      s.container.appName,
			Database:                 database,
			PlanHash:                 stmtKey.planHash,
			TransactionFingerprintID: stmtKey.transactionFingerprintID,
		},
		ID:    statementStats.ID,
		Stats: data,
	}

	return true
}

// copyDataLocked Copies the statement stat's data object under the mutex.
// This function requires that there is a lock on the stats object.
func copyDataLocked(stats *stmtStats) appstatspb.StatementStatistics {
	stats.mu.AssertHeld()
	data := stats.mu.data
	data.Nodes = slices.Clone(data.Nodes)
	data.KVNodeIDs = slices.Clone(data.KVNodeIDs)
	data.Regions = slices.Clone(data.Regions)
	data.PlanGists = slices.Clone(data.PlanGists)
	data.IndexRecommendations = slices.Clone(data.IndexRecommendations)
	data.Indexes = slices.Clone(data.Indexes)
	return data
}

// Cur returns the appstatspb.CollectedStatementStatistics at the current internal
// counter.
func (s *StmtStatsIterator) Cur() *appstatspb.CollectedStatementStatistics {
	return s.currentValue
}

// TxnStatsIterator is an iterator that iterates over the transaction statistics
// inside a ssmemstorage.Container.
type TxnStatsIterator struct {
	baseIterator
	txnKeys  txnList
	curValue *appstatspb.CollectedTransactionStatistics
}

// NewTxnStatsIterator returns a new instance of TxnStatsIterator.
func NewTxnStatsIterator(container *Container, options sqlstats.IteratorOptions) TxnStatsIterator {
	var txnKeys txnList
	container.mu.Lock()
	for k := range container.mu.txns {
		txnKeys = append(txnKeys, k)
	}
	container.mu.Unlock()
	if options.SortedKey {
		sort.Sort(txnKeys)
	}

	return TxnStatsIterator{
		baseIterator: baseIterator{
			container: container,
			idx:       -1,
		},
		txnKeys: txnKeys,
	}
}

// Initialized returns true if the iterator has been initialized, false otherwise.
func (t *TxnStatsIterator) Initialized() bool {
	return t.container != nil
}

// Next updates the current value returned by the subsequent Cur() call. Next()
// returns true if the following Cur() call is valid, false otherwise.
func (t *TxnStatsIterator) Next() bool {
	t.idx++
	if t.idx >= len(t.txnKeys) {
		return false
	}

	txnKey := t.txnKeys[t.idx]

	// We don't want to create the key if it doesn't exist, so it's okay to
	// pass nil for the statementFingerprintIDs, as they are only set when a key is
	// constructed.
	txnStats := t.container.getStatsForTxnWithKey(txnKey)

	// If the key is not found (and we expected to find it), the table must
	// have been cleared between now and the time we read all the keys. In
	// that case we simply skip this key and advance the iterator.
	if txnStats == nil {
		return t.Next()
	}

	txnStats.mu.Lock()
	defer txnStats.mu.Unlock()

	t.curValue = &appstatspb.CollectedTransactionStatistics{
		StatementFingerprintIDs:  txnStats.statementFingerprintIDs,
		App:                      t.container.appName,
		Stats:                    txnStats.mu.data,
		TransactionFingerprintID: txnKey,
	}

	return true
}

// Cur returns the appstatspb.CollectedTransactionStatistics at the current internal
// counter.
func (t *TxnStatsIterator) Cur() *appstatspb.CollectedTransactionStatistics {
	return t.curValue
}
