// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// CombinedStmtStatsIterator is an iterator that iterates through both
// in-memory and persisted stmt stats provided by the in-memory iterator and
// the on-disk iterator.
type CombinedStmtStatsIterator struct {
	// state dictates which of the two underlying iterators will be used by Cur()
	// and advanced by Next().
	state iterState

	mem struct {
		// valid is set if the mem iterator has not been exhausted.
		valid bool
		it    *memStmtStatsIterator
	}

	disk struct {
		// valid is set if the disk iterator has not been exhausted.
		valid bool
		it    *stmtStatsWrapper
	}
}

// NewCombinedStmtStatsIterator returns a new instance of
// CombinedStmtStatsIterator.
//
// The combined iterator take ownership of the args.
//
// Close() needs to be called when the iterator is not needed any more in order
// to free up resources.
func NewCombinedStmtStatsIterator(
	memIter *memStmtStatsIterator, diskIter *stmtStatsWrapper,
) *CombinedStmtStatsIterator {
	c := &CombinedStmtStatsIterator{
		// We start in stateEqual so that the first Next() call will advance both
		// iterators.
		state: stateEqual,
	}
	c.mem.it = memIter
	c.disk.it = diskIter
	return c
}

// Close needs to be called when the iteration is done to release resources.
func (c *CombinedStmtStatsIterator) Close() error {
	return c.disk.it.Close()
}

func (c *CombinedStmtStatsIterator) advanceMem() {
	c.mem.valid = c.mem.it.Next()
}

func (c *CombinedStmtStatsIterator) advanceDisk(ctx context.Context) error {
	var err error
	c.disk.valid, err = c.disk.it.Next(ctx)
	return err
}

type iterState int

const (
	stateEqual iterState = iota
	stateMemSmaller
	stateDiskSmaller
)

// Next increments the internal counter of the CombinedStmtStatsIterator. It
// returns true if the following Cur() call will be valid, false otherwise.
func (c *CombinedStmtStatsIterator) Next(ctx context.Context) (bool, error) {
	switch c.state {
	case stateEqual:
		c.advanceMem()
		if err := c.advanceDisk(ctx); err != nil {
			return false, err
		}
	case stateMemSmaller:
		c.advanceMem()
	case stateDiskSmaller:
		if err := c.advanceDisk(ctx); err != nil {
			return false, err
		}
	default:
		panic(fmt.Sprintf("unexpected state: %v", c.state))
	}

	if !c.mem.valid && !c.disk.valid {
		return false, nil
	}

	if !c.mem.valid {
		c.state = stateDiskSmaller
		return true, nil
	}
	if !c.disk.valid {
		c.state = stateMemSmaller
		return true, nil
	}

	cmp := compareStmtStats(c.mem.it.Cur(), c.disk.it.Cur())
	if cmp < 0 {
		c.state = stateMemSmaller
	} else if cmp == 0 {
		c.state = stateEqual
	} else {
		c.state = stateDiskSmaller
	}

	return true, nil
}

// Cur returns the roachpb.CollectedStatementStatistics at the current internal
// counter.
func (c *CombinedStmtStatsIterator) Cur() *roachpb.CollectedStatementStatistics {
	if !c.mem.valid && !c.disk.valid {
		panic("iterator exhausted")
	}

	switch c.state {
	case stateEqual:
		memVal := c.mem.it.Cur()
		diskVal := c.disk.it.Cur()
		// Combine the stats.
		memVal.Stats.Add(&diskVal.Stats)
		return memVal
	case stateDiskSmaller:
		return c.disk.it.Cur()
	case stateMemSmaller:
		return c.mem.it.Cur()
	default:
		panic(fmt.Sprintf("unexpected state: %v", c.state))
	}
}

func compareStmtStats(lhs, rhs *roachpb.CollectedStatementStatistics) int {
	// 1. we compare their aggregated_ts
	if lhs.AggregatedTs.Before(rhs.AggregatedTs) {
		return -1
	}
	if lhs.AggregatedTs.After(rhs.AggregatedTs) {
		return 1
	}

	// 2. we compare their app name.
	cmp := strings.Compare(lhs.Key.App, rhs.Key.App)
	if cmp != 0 {
		return cmp
	}

	// 3. we compare their fingerprint ID.
	if lhs.ID < rhs.ID {
		return -1
	}
	if lhs.ID > rhs.ID {
		return 1
	}

	// 4. we compare their transaction fingerprint ID
	if lhs.Key.TransactionFingerprintID < rhs.Key.TransactionFingerprintID {
		return -1
	}
	if lhs.Key.TransactionFingerprintID > rhs.Key.TransactionFingerprintID {
		return 1
	}

	// 5. we compare their plan hash.
	if lhs.Key.PlanHash < rhs.Key.PlanHash {
		return -1
	}
	if lhs.Key.PlanHash > rhs.Key.PlanHash {
		return 1
	}

	return 0
}

type stmtStatsWrapper struct {
	// rows represents the underlying iterator. The stmtStatsWrapper owns it and
	// will Close() it.
	rows sqlutil.InternalRows
	cur  *roachpb.CollectedStatementStatistics
}

// Next advances the iterator by one row, returning false if there are no
// more rows in this iterator or if an error is encountered (the latter is
// then returned).
//
// The iterator is automatically closed when false is returned, consequent
// calls to Next will return the same values as when the iterator was
// closed.
func (s *stmtStatsWrapper) Next(ctx context.Context) (bool, error) {
	ok, err := s.rows.Next(ctx)
	if !ok || err != nil {
		return ok, err
	}
	s.cur, err = rowToStmtStats(s.rows.Cur())
	if err != nil {
		return false, err
	}
	return true, nil
}

// Cur returns the row at the current position of the iterator. The row is
// safe to hold onto (meaning that calling Next() or Close() will not
// invalidate it).
func (s *stmtStatsWrapper) Cur() *roachpb.CollectedStatementStatistics {
	return s.cur
}

// Close needs to be called when the iteration is done to release resources.
func (s *stmtStatsWrapper) Close() error {
	return s.rows.Close()
}

// CombinedTxnStatsIterator is an iterator that iterates through both
// in-memory and persisted txn stats provided by the in-memory iterator and
// the on-disk iterator.
type CombinedTxnStatsIterator struct {
	state iterState

	mem struct {
		valid bool
		it    *memTxnStatsIterator
	}

	disk struct {
		valid bool
		it    *txnStatsWrapper
	}
}

// NewCombinedTxnStatsIterator returns a new instance of
// CombinedTxnStatsIterator.
//
// The combined iterator take ownership of the args.
//
// Close() needs to be called when the iterator is not needed any more in order
// to free up resources.
func NewCombinedTxnStatsIterator(
	memIter *memTxnStatsIterator, diskIter *txnStatsWrapper,
) *CombinedTxnStatsIterator {
	c := &CombinedTxnStatsIterator{
		// We start in stateEqual so that the first Next() call will advance both
		// iterators.
		state: stateEqual,
	}
	c.mem.it = memIter
	c.disk.it = diskIter
	return c
}

// Close needs to be called when the iteration is done to release resources.
func (c *CombinedTxnStatsIterator) Close() error {
	return c.disk.it.Close()
}

func (c *CombinedTxnStatsIterator) advanceMem() {
	c.mem.valid = c.mem.it.Next()
}

func (c *CombinedTxnStatsIterator) advanceDisk(ctx context.Context) error {
	var err error
	c.disk.valid, err = c.disk.it.Next(ctx)
	return err
}

// Next increments the internal counter of the CombinedTxnStatsIterator. It
// returns true if the following Cur() call will be valid, false otherwise.
func (c *CombinedTxnStatsIterator) Next(ctx context.Context) (bool, error) {
	switch c.state {
	case stateEqual:
		c.advanceMem()
		if err := c.advanceDisk(ctx); err != nil {
			return false, err
		}
	case stateMemSmaller:
		c.advanceMem()
	case stateDiskSmaller:
		if err := c.advanceDisk(ctx); err != nil {
			return false, err
		}
	default:
		panic(fmt.Sprintf("unexpected state: %v", c.state))
	}

	if !c.mem.valid && !c.disk.valid {
		return false, nil
	}

	if !c.mem.valid {
		c.state = stateDiskSmaller
		return true, nil
	}
	if !c.disk.valid {
		c.state = stateMemSmaller
		return true, nil
	}

	cmp := compareTxnStats(c.mem.it.Cur(), c.disk.it.Cur())
	if cmp < 0 {
		c.state = stateMemSmaller
	} else if cmp == 0 {
		c.state = stateEqual
	} else {
		c.state = stateDiskSmaller
	}

	return true, nil
}

// Cur returns the roachpb.CollectedTransactionStatistics at the current internal
// counter.
func (c *CombinedTxnStatsIterator) Cur() *roachpb.CollectedTransactionStatistics {
	if !c.mem.valid && !c.disk.valid {
		panic("iterator exhausted")
	}

	switch c.state {
	case stateEqual:
		memVal := c.mem.it.Cur()
		diskVal := c.disk.it.Cur()
		// Combine the stats.
		memVal.Stats.Add(&diskVal.Stats)
		return memVal
	case stateDiskSmaller:
		return c.disk.it.Cur()
	case stateMemSmaller:
		return c.mem.it.Cur()
	default:
		panic(fmt.Sprintf("unexpected state: %v", c.state))
	}
}

func compareTxnStats(lhs, rhs *roachpb.CollectedTransactionStatistics) int {
	// 1. we compare their aggregated_ts
	if lhs.AggregatedTs.Before(rhs.AggregatedTs) {
		return -1
	}
	if lhs.AggregatedTs.After(rhs.AggregatedTs) {
		return 1
	}

	// 2. we compare their app name.
	cmp := strings.Compare(lhs.App, rhs.App)
	if cmp != 0 {
		return cmp
	}

	// 3. we compared the transaction fingerprint ID.
	if lhs.TransactionFingerprintID < rhs.TransactionFingerprintID {
		return -1
	}
	if lhs.TransactionFingerprintID > rhs.TransactionFingerprintID {
		return 1
	}

	return 0
}

type txnStatsWrapper struct {
	rows sqlutil.InternalRows
	cur  *roachpb.CollectedTransactionStatistics
}

// Next advances the iterator by one row, returning false if there are no
// more rows in this iterator or if an error is encountered (the latter is
// then returned).
//
// The iterator is automatically closed when false is returned, consequent
// calls to Next will return the same values as when the iterator was
// closed.
func (s *txnStatsWrapper) Next(ctx context.Context) (bool, error) {
	ok, err := s.rows.Next(ctx)
	if !ok || err != nil {
		return ok, err
	}
	s.cur, err = rowToTxnStats(s.rows.Cur())
	if err != nil {
		return false, err
	}
	return true, nil
}

// Cur returns the row at the current position of the iterator. The row is
// safe to hold onto (meaning that calling Next() or Close() will not
// invalidate it).
func (s *txnStatsWrapper) Cur() *roachpb.CollectedTransactionStatistics {
	return s.cur
}

// Close needs to be called when the iteration is done to release resources.
func (s *txnStatsWrapper) Close() error {
	return s.rows.Close()
}
