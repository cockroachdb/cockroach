// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal

import (
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
)

type baseIterator struct {
	sqlStats *SQLStats
	idx      int
	appNames []string
	options  sqlstats.IteratorOptions
}

// StmtStatsIterator is an iterator that can be used to iterate over the
// statement statistics stored in SQLStats.
type StmtStatsIterator struct {
	baseIterator
	curIter ssmemstorage.StmtStatsIterator
}

// NewStmtStatsIterator returns a new instance of the StmtStatsIterator.
func NewStmtStatsIterator(s *SQLStats, options sqlstats.IteratorOptions) StmtStatsIterator {
	appNames := s.getAppNames(options.SortedAppNames)

	return StmtStatsIterator{
		baseIterator: baseIterator{
			sqlStats: s,
			idx:      -1,
			appNames: appNames,
			options:  options,
		},
	}
}

// Next increments the internal counter of the StmtStatsIterator. It returns
// true if the following Cur() call is valid, false otherwise.
//
// This iterator iterates through a slice of appNames that it stores. For each
// appName in the slice, it creates an iterator that iterate through the
// ssmemstorage.Container for that appName.
func (s *StmtStatsIterator) Next() bool {
	// If we haven't called Next() for the first time or our current child
	// iterator has finished iterator, then we increment s.idx.
	if !s.curIter.Initialized() || !s.curIter.Next() {
		s.idx++
		if s.idx >= len(s.appNames) {
			return false
		}
		statsContainer := s.sqlStats.getStatsForApplication(s.appNames[s.idx])
		s.curIter = statsContainer.StmtStatsIterator(s.options)
		return s.Next()
	}

	// This happens when our child iterator is valid and s.curIter.Next() call
	// is true.
	return true
}

// Cur returns the appstatspb.CollectedStatementStatistics at the current internal
// counter.
func (s *StmtStatsIterator) Cur() *appstatspb.CollectedStatementStatistics {
	return s.curIter.Cur()
}

// TxnStatsIterator is an iterator that can be used to iterate over the
// statement statistics stored in SQLStats.
type TxnStatsIterator struct {
	baseIterator
	curIter ssmemstorage.TxnStatsIterator
}

// NewTxnStatsIterator returns a new instance of the TxnStatsIterator.
func NewTxnStatsIterator(s *SQLStats, options sqlstats.IteratorOptions) TxnStatsIterator {
	appNames := s.getAppNames(options.SortedAppNames)

	return TxnStatsIterator{
		baseIterator: baseIterator{
			sqlStats: s,
			idx:      -1,
			appNames: appNames,
			options:  options,
		},
	}
}

// Next increments the internal counter of the TxnStatsIterator. It returns
// true if the following Cur() call is valid, false otherwise.
//
// This iterator iterates through a slice of appNames that it stores. For each
// appName in the slice, it creates an iterator that iterate through the
// ssmemstorage.Container for that appName.
func (t *TxnStatsIterator) Next() bool {
	// If we haven't called Next() for the first time or our current child
	// iterator has finished iterator, then we increment s.idx.
	if !t.curIter.Initialized() || !t.curIter.Next() {
		t.idx++
		if t.idx >= len(t.appNames) {
			return false
		}
		statsContainer := t.sqlStats.getStatsForApplication(t.appNames[t.idx])
		t.curIter = statsContainer.TxnStatsIterator(t.options)
		return t.Next()
	}

	// This happens when our child iterator is valid and s.curIter.Next() call
	// is true.
	return true
}

// Cur returns the appstatspb.CollectedTransactionStatistics at the current internal
// counter.
func (t *TxnStatsIterator) Cur() *appstatspb.CollectedTransactionStatistics {
	return t.curIter.Cur()
}
