// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/errors"
)

// CombinedStmtStatsIterator is an iterator that iterates through both
// in-memory and persisted stmt stats provided by the in-memory iterator and
// the on-disk iterator.
type CombinedStmtStatsIterator struct {
	nextToRead     *appstatspb.CollectedStatementStatistics
	expectedColCnt int

	mem struct {
		canBeAdvanced bool
		paused        bool
		it            memStmtStatsIterator
	}

	disk struct {
		canBeAdvanced bool
		paused        bool
		it            isql.Rows
	}
}

// NewCombinedStmtStatsIterator returns a new instance of
// CombinedStmtStatsIterator.
func NewCombinedStmtStatsIterator(
	memIter memStmtStatsIterator, diskIter isql.Rows, expectedColCnt int,
) CombinedStmtStatsIterator {
	c := CombinedStmtStatsIterator{
		expectedColCnt: expectedColCnt,
	}

	c.mem.it = memIter
	c.mem.canBeAdvanced = true

	c.disk.it = diskIter
	c.disk.canBeAdvanced = true

	return c
}

// Next increments the internal counter of the CombinedStmtStatsIterator. It
// returns true if the following Cur() call will be valid, false otherwise.
func (c *CombinedStmtStatsIterator) Next(ctx context.Context) (bool, error) {
	var err error

	if c.mem.canBeAdvanced && !c.mem.paused {
		c.mem.canBeAdvanced = c.mem.it.Next()
	}

	if c.disk.canBeAdvanced && !c.disk.paused {
		c.disk.canBeAdvanced, err = c.disk.it.Next(ctx)
		if err != nil {
			return false, err
		}
	}

	// Both iterators are exhausted, no new value can be produced.
	if !c.mem.canBeAdvanced && !c.disk.canBeAdvanced {
		// Sanity check.
		if c.mem.paused || c.disk.paused {
			return false, errors.AssertionFailedf("bug: leaked iterator")
		}
		return false, nil
	}

	// If memIter is exhausted, but disk iterator can still move forward.
	// We promote the disk.Cur() and resume the disk iterator if it was paused.
	if !c.mem.canBeAdvanced {
		row := c.disk.it.Cur()
		if row == nil {
			return false, errors.New("unexpected nil row")
		}

		if len(row) != c.expectedColCnt {
			return false, errors.AssertionFailedf("unexpectedly received %d columns", len(row))
		}

		c.nextToRead, err = rowToStmtStats(c.disk.it.Cur())
		if err != nil {
			return false, err
		}

		if c.disk.canBeAdvanced {
			c.disk.paused = false
		}
		return true, nil
	}

	// If diskIter is exhausted, but mem iterator can still move forward.
	// We promote the mem.Cur() and resume the mem iterator if it was paused.
	if !c.disk.canBeAdvanced {
		c.nextToRead = c.mem.it.Cur()

		if c.mem.canBeAdvanced {
			c.mem.paused = false
		}
		return true, nil
	}

	// Both iterators can be moved forward. Now we check the value of Cur()
	// for both iterators. We will have a few scenarios:
	// 1. mem.Cur() < disk.Cur():
	//    we promote mem.Cur() to c.nextToRead. We then pause
	//    the disk iterator and resume the mem iterator for next iteration.
	// 2. mem.Cur() == disk.Cur():
	//    we promote both mem.Cur() and disk.Cur() by merging both
	//    stats. We resume both iterators for next iteration.
	// 3. mem.Cur() > disk.Cur():
	//    we promote disk.Cur() to c.nextToRead. We then pause
	//    mem iterator and resume disk iterator for next iteration.
	memCurVal := c.mem.it.Cur()
	diskCurVal, err := rowToStmtStats(c.disk.it.Cur())
	if err != nil {
		return false, err
	}

	switch compareStmtStats(memCurVal, diskCurVal) {
	case -1:
		// First Case.
		c.nextToRead = memCurVal
		c.mem.paused = false
		c.disk.paused = true
	case 0:
		// Second Case.
		c.nextToRead = memCurVal
		c.nextToRead.Stats.Add(&diskCurVal.Stats)
		c.mem.paused = false
		c.disk.paused = false
	case 1:
		// Third Case.
		c.nextToRead = diskCurVal
		c.mem.paused = true
		c.disk.paused = false
	default:
		return false, errors.AssertionFailedf("bug: impossible state")
	}

	return true, nil
}

// Cur returns the appstatspb.CollectedStatementStatistics at the current internal
// counter.
func (c *CombinedStmtStatsIterator) Cur() *appstatspb.CollectedStatementStatistics {
	return c.nextToRead
}

func compareStmtStats(lhs, rhs *appstatspb.CollectedStatementStatistics) int {
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
	nextToReadVal  *appstatspb.CollectedTransactionStatistics
	expectedColCnt int

	mem struct {
		canBeAdvanced bool
		paused        bool
		it            memTxnStatsIterator
	}

	disk struct {
		canBeAdvanced bool
		paused        bool
		it            isql.Rows
	}
}

// NewCombinedTxnStatsIterator returns a new instance of
// CombinedTxnStatsIterator.
func NewCombinedTxnStatsIterator(
	memIter memTxnStatsIterator, diskIter isql.Rows, expectedColCnt int,
) CombinedTxnStatsIterator {
	c := CombinedTxnStatsIterator{
		expectedColCnt: expectedColCnt,
	}

	c.mem.it = memIter
	c.mem.canBeAdvanced = true

	c.disk.it = diskIter
	c.disk.canBeAdvanced = true

	return c
}

// Next increments the internal counter of the CombinedTxnStatsIterator. It
// returns true if the following Cur() call will be valid, false otherwise.
func (c *CombinedTxnStatsIterator) Next(ctx context.Context) (bool, error) {
	var err error

	if c.mem.canBeAdvanced && !c.mem.paused {
		c.mem.canBeAdvanced = c.mem.it.Next()
	}

	if c.disk.canBeAdvanced && !c.disk.paused {
		c.disk.canBeAdvanced, err = c.disk.it.Next(ctx)
		if err != nil {
			return false, err
		}
	}

	// Both iterators are exhausted, no new value can be produced.
	if !c.mem.canBeAdvanced && !c.disk.canBeAdvanced {
		// Sanity check.
		if c.mem.paused || c.disk.paused {
			return false, errors.AssertionFailedf("bug: leaked iterator")
		}
		return false, nil
	}

	// If memIter is exhausted, but disk iterator can still move forward.
	// We promote the disk.Cur() and resume the disk iterator if it was paused.
	if !c.mem.canBeAdvanced {
		row := c.disk.it.Cur()
		if row == nil {
			return false, errors.New("unexpected nil row")
		}

		if len(row) != c.expectedColCnt {
			return false, errors.AssertionFailedf("unexpectedly received %d columns", len(row))
		}

		c.nextToReadVal, err = rowToTxnStats(c.disk.it.Cur())
		if err != nil {
			return false, err
		}

		if c.disk.canBeAdvanced {
			c.disk.paused = false
		}
		return true, nil
	}

	// If diskIter is exhausted, but mem iterator can still move forward.
	// We promote the mem.Cur() and resume the mem iterator if it was paused.
	if !c.disk.canBeAdvanced {
		c.nextToReadVal = c.mem.it.Cur()

		if c.mem.canBeAdvanced {
			c.mem.paused = false
		}
		return true, nil
	}

	// Both iterators can be moved forward. Now we check the value of Cur()
	// for both iterators. We will have a few scenarios:
	// 1. mem.Cur() < disk.Cur():
	//    we promote mem.Cur() to c.nextToRead. We then pause
	//    the disk iterator and resume the mem iterator for next iteration.
	// 2. mem.Cur() == disk.Cur():
	//    we promote both mem.Cur() and disk.Cur() by merging both
	//    stats. We resume both iterators for next iteration.
	// 3. mem.Cur() > disk.Cur():
	//    we promote disk.Cur() to c.nextToRead. We then pause
	//    mem iterator and resume disk iterator for next iteration.
	memCurVal := c.mem.it.Cur()
	diskCurVal, err := rowToTxnStats(c.disk.it.Cur())
	if err != nil {
		return false, err
	}

	switch compareTxnStats(memCurVal, diskCurVal) {
	case -1:
		// First Case.
		c.nextToReadVal = memCurVal
		c.mem.paused = false
		c.disk.paused = true
	case 0:
		// Second Case.
		c.nextToReadVal = memCurVal
		c.nextToReadVal.Stats.Add(&diskCurVal.Stats)
		c.mem.paused = false
		c.disk.paused = false
	case 1:
		// Third Case.
		c.nextToReadVal = diskCurVal
		c.mem.paused = true
		c.disk.paused = false
	default:
		return false, errors.AssertionFailedf("bug: impossible state")
	}

	return true, nil
}

// Cur returns the appstatspb.CollectedTransactionStatistics at the current internal
// counter.
func (c *CombinedTxnStatsIterator) Cur() *appstatspb.CollectedTransactionStatistics {
	return c.nextToReadVal
}

func compareTxnStats(lhs, rhs *appstatspb.CollectedTransactionStatistics) int {
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
