// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ssmemstorage

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	currentValue *roachpb.CollectedStatementStatistics
}

// NewStmtStatsIterator returns a StmtStatsIterator.
func NewStmtStatsIterator(
	container *Container, options *sqlstats.IteratorOptions,
) *StmtStatsIterator {
	var stmtKeys stmtList
	container.mu.Lock()
	for k := range container.mu.stmts {
		stmtKeys = append(stmtKeys, k)
	}
	container.mu.Unlock()
	if options.SortedKey {
		sort.Sort(stmtKeys)
	}

	return &StmtStatsIterator{
		baseIterator: baseIterator{
			container: container,
			idx:       -1,
		},
		stmtKeys: stmtKeys,
	}
}

// Next updates the current value returned by the subsequent Cur() call. Next()
// returns true if the following Cur() call is valid, false otherwise.
func (s *StmtStatsIterator) Next() bool {
	s.idx++
	if s.idx >= len(s.stmtKeys) {
		return false
	}

	stmtKey := s.stmtKeys[s.idx]

	stmtFingerprintID := constructStatementFingerprintIDFromStmtKey(stmtKey)
	statementStats, _, _ :=
		s.container.getStatsForStmtWithKey(stmtKey, invalidStmtFingerprintID, false /* createIfNonexistent */)

	// If the key is not found (and we expected to find it), the table must
	// have been cleared between now and the time we read all the keys. In
	// that case we simply skip this key and keep advancing the iterator.
	if statementStats == nil {
		return s.Next()
	}

	statementStats.mu.Lock()
	data := statementStats.mu.data
	distSQLUsed := statementStats.mu.distSQLUsed
	vectorized := statementStats.mu.vectorized
	fullScan := statementStats.mu.fullScan
	database := statementStats.mu.database
	querySummary := statementStats.mu.querySummary
	statementStats.mu.Unlock()

	s.currentValue = &roachpb.CollectedStatementStatistics{
		Key: roachpb.StatementStatisticsKey{
			Query:                    stmtKey.anonymizedStmt,
			QuerySummary:             querySummary,
			DistSQL:                  distSQLUsed,
			Vec:                      vectorized,
			ImplicitTxn:              stmtKey.implicitTxn,
			FullScan:                 fullScan,
			Failed:                   stmtKey.failed,
			App:                      s.container.appName,
			Database:                 database,
			PlanHash:                 stmtKey.planHash,
			TransactionFingerprintID: stmtKey.transactionFingerprintID,
		},
		ID:    stmtFingerprintID,
		Stats: data,
	}

	return true
}

// Cur returns the roachpb.CollectedStatementStatistics at the current internal
// counter.
func (s *StmtStatsIterator) Cur() *roachpb.CollectedStatementStatistics {
	return s.currentValue
}

// TxnStatsIterator is an iterator that iterates over the transaction statistics
// inside a ssmemstorage.Container.
type TxnStatsIterator struct {
	baseIterator
	txnKeys  txnList
	curValue *roachpb.CollectedTransactionStatistics
}

// NewTxnStatsIterator returns a new instance of TxnStatsIterator.
func NewTxnStatsIterator(
	container *Container, options *sqlstats.IteratorOptions,
) *TxnStatsIterator {
	var txnKeys txnList
	container.mu.Lock()
	for k := range container.mu.txns {
		txnKeys = append(txnKeys, k)
	}
	container.mu.Unlock()
	if options.SortedKey {
		sort.Sort(txnKeys)
	}

	return &TxnStatsIterator{
		baseIterator: baseIterator{
			container: container,
			idx:       -1,
		},
		txnKeys: txnKeys,
	}
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
	txnStats, _, _ := t.container.getStatsForTxnWithKey(txnKey, nil /* stmtFingerprintIDs */, false /* createIfNonexistent */)

	// If the key is not found (and we expected to find it), the table must
	// have been cleared between now and the time we read all the keys. In
	// that case we simply skip this key and advance the iterator.
	if txnStats == nil {
		return t.Next()
	}

	txnStats.mu.Lock()
	defer txnStats.mu.Unlock()

	t.curValue = &roachpb.CollectedTransactionStatistics{
		StatementFingerprintIDs:  txnStats.statementFingerprintIDs,
		App:                      t.container.appName,
		Stats:                    txnStats.mu.data,
		TransactionFingerprintID: txnKey,
	}

	return true
}

// Cur returns the roachpb.CollectedTransactionStatistics at the current internal
// counter.
func (t *TxnStatsIterator) Cur() *roachpb.CollectedTransactionStatistics {
	return t.curValue
}
