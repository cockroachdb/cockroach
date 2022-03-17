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
		it    sqlutil.InternalRows
	}
}

// NewCombinedStmtStatsIterator returns a new instance of
// CombinedStmtStatsIterator.
func NewCombinedStmtStatsIterator(
	memIter *memStmtStatsIterator, diskIter sqlutil.InternalRows,
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

	memVal := c.mem.it.Cur()
	diskVal, err := rowToStmtStats(c.disk.it.Cur())
	if err != nil {
		return false, err
	}
	cmp := compareStmtStats(memVal, diskVal)
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
func (c *CombinedStmtStatsIterator) Cur() (*roachpb.CollectedStatementStatistics, error) {
	if !c.mem.valid && !c.disk.valid {
		panic("iterator exhausted")
	}

	switch c.state {
	case stateEqual:
		memVal := c.mem.it.Cur()
		diskVal, err := rowToStmtStats(c.disk.it.Cur())
		if err != nil {
			return nil, err
		}
		// Combine the stats.
		memVal.Stats.Add(&diskVal.Stats)
		return memVal, nil
	case stateDiskSmaller:
		return rowToStmtStats(c.disk.it.Cur())
	case stateMemSmaller:
		return c.mem.it.Cur(), nil
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
		it    sqlutil.InternalRows
	}
}

// NewCombinedTxnStatsIterator returns a new instance of
// CombinedTxnStatsIterator.
func NewCombinedTxnStatsIterator(
	memIter *memTxnStatsIterator, diskIter sqlutil.InternalRows,
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

	memVal := c.mem.it.Cur()
	diskVal, err := rowToTxnStats(c.disk.it.Cur())
	if err != nil {
		return false, err
	}
	cmp := compareTxnStats(memVal, diskVal)
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
func (c *CombinedTxnStatsIterator) Cur() (*roachpb.CollectedTransactionStatistics, error) {
	if !c.mem.valid && !c.disk.valid {
		panic("iterator exhausted")
	}

	switch c.state {
	case stateEqual:
		memVal := c.mem.it.Cur()
		diskVal, err := rowToTxnStats(c.disk.it.Cur())
		if err != nil {
			return nil, err
		}
		// Combine the stats.
		memVal.Stats.Add(&diskVal.Stats)
		return memVal, nil
	case stateDiskSmaller:
		return rowToTxnStats(c.disk.it.Cur())
	case stateMemSmaller:
		return c.mem.it.Cur(), nil
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
