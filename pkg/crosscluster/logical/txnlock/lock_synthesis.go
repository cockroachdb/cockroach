// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type Lock struct {
	Hash uint64
	Read bool
}

type LockSet struct {
	Locks      []Lock
	SortedRows []ldrdecoder.DecodedRow
}

type LockSynthesizer struct {
	tableConstraints map[descpb.ID]*tableConstraints
}

func NewLockSynthesizer(
	ctx context.Context, lm *lease.Manager, clock *hlc.Clock, tableMappings []ldrdecoder.TableMapping,
) (*LockSynthesizer, error) {
	ls := &LockSynthesizer{
		tableConstraints: make(map[descpb.ID]*tableConstraints, len(tableMappings)),
	}

	// Acquire leases for all destination tables and build their constraints
	timestamp := lease.TimestampToReadTimestamp(clock.Now())
	for _, mapping := range tableMappings {
		leasedDesc, err := lm.Acquire(ctx, timestamp, mapping.DestID)
		if err != nil {
			return nil, err
		}

		// Build table constraints from the destination descriptor
		tableDesc := leasedDesc.Underlying().(catalog.TableDescriptor)
		ls.tableConstraints[mapping.DestID] = newTableConstraints(tableDesc)

		leasedDesc.Release(ctx)
	}

	return ls, nil
}

type lockWithRows struct {
	rows    []int32
	scratch [8]int32
	isRead  bool
}

func (ls *LockSynthesizer) DeriveLocks(rows []ldrdecoder.DecodedRow) (LockSet, error) {
	// We build a table of locks for two reasons:
	// 1. We use it to eliminate duplicate locks for the same row.
	// 2. We need to topologically sort the rows to get an apply order and
	//    we can use the set of lock conflicts to identify possible dependencies.
	locks := map[uint64]lockWithRows{}
	rowLocks := make([][]Lock, len(rows))

	for i, row := range rows {
		rowLocks[i] = ls.deriveLocks(row, nil)
		for _, lock := range rowLocks[i] {
			lr, ok := locks[lock.Hash]
			if !ok {
				lr.rows = lr.scratch[:0]
				lr.rows = append(lr.rows, int32(i))
				lr.isRead = lock.Read
				locks[lock.Hash] = lr
				continue
			}

			if !lock.Read && lr.isRead {
				lr.isRead = false
			}

			if lr.rows[len(lr.rows)-1] != int32(i) {
				lr.rows = append(lr.rows, int32(i))
			}
			locks[lock.Hash] = lr
		}
	}

	sorted, err := ls.sort(rows, rowLocks, locks)
	if err != nil {
		return LockSet{}, err
	}

	lockSet := LockSet{
		SortedRows: sorted,
		Locks:      make([]Lock, 0, len(locks)),
	}

	for hash, lr := range locks {
		lockSet.Locks = append(lockSet.Locks, Lock{
			Hash: hash,
			Read: lr.isRead,
		})
	}

	return lockSet, nil
}

func (ls *LockSynthesizer) deriveLocks(row ldrdecoder.DecodedRow, locks []Lock) []Lock {
	tc, ok := ls.tableConstraints[row.TableID]
	if !ok {
		// No constraints for this table, return empty lock set
		return nil
	}
	return tc.deriveLocks(row, locks)
}

// DependsOn returns true if row a must be applied before row b.
func (ls *LockSynthesizer) dependsOn(a, b ldrdecoder.DecodedRow) bool {
	// If rows are from different tables, no ordering constraint
	if a.TableID != b.TableID {
		return false
	}

	// Look up the table constraints
	tc, ok := ls.tableConstraints[a.TableID]
	if !ok {
		// No constraints for this table
		return false
	}

	return tc.DependsOn(a, b)
}
