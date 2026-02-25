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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type Lock struct {
	Hash uint64
	// TODO(jeffswenson): Read will be set to true for foreign key locks that
	// only require a shared/read lock rather than an exclusive/write lock.
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
	ctx context.Context,
	evalCtx *eval.Context,
	lm *lease.Manager,
	clock *hlc.Clock,
	tableMappings []ldrdecoder.TableMapping,
) (*LockSynthesizer, error) {
	ls := &LockSynthesizer{
		tableConstraints: make(map[descpb.ID]*tableConstraints, len(tableMappings)),
	}

	timestamp := lease.TimestampToReadTimestamp(clock.Now())
	for _, mapping := range tableMappings {
		tc, err := func() (*tableConstraints, error) {
			leasedDesc, err := lm.Acquire(ctx, timestamp, mapping.DestID)
			if err != nil {
				return nil, err
			}
			defer leasedDesc.Release(ctx)

			tableDesc, ok := leasedDesc.Underlying().(catalog.TableDescriptor)
			if !ok {
				return nil, errors.AssertionFailedf(
					"expected table descriptor for %d, got %T",
					mapping.DestID, leasedDesc.Underlying(),
				)
			}
			return newTableConstraints(evalCtx, tableDesc)
		}()
		if err != nil {
			return nil, err
		}
		ls.tableConstraints[mapping.DestID] = tc
	}

	return ls, nil
}

type lockWithRows struct {
	rows   []int32
	isRead bool
}

func (ls *LockSynthesizer) DeriveLocks(
	ctx context.Context, rows []ldrdecoder.DecodedRow,
) (LockSet, error) {
	// We build a table of locks for two reasons:
	// 1. We use it to eliminate duplicate locks for the same row.
	// 2. We need to topologically sort the rows to get an apply order and
	//    we can use the set of lock conflicts to identify possible dependencies.
	locks := map[uint64]*lockWithRows{}
	rowLocks := make([][]Lock, len(rows))

	for i, row := range rows {
		var err error
		rowLocks[i], err = ls.deriveLocks(ctx, row, nil)
		if err != nil {
			return LockSet{}, err
		}
		for _, lock := range rowLocks[i] {
			lr, ok := locks[lock.Hash]
			if !ok {
				lr = &lockWithRows{}
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
		}
	}

	sorted, err := ls.sort(ctx, rows, rowLocks, locks)
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

func (ls *LockSynthesizer) deriveLocks(
	ctx context.Context, row ldrdecoder.DecodedRow, locks []Lock,
) ([]Lock, error) {
	tc, ok := ls.tableConstraints[row.TableID]
	if !ok {
		return nil, nil
	}
	return tc.deriveLocks(ctx, row, locks)
}

// dependsOn returns true if row a must be applied before row b. It can be read
// as a `dependsOn` b.
func (ls *LockSynthesizer) dependsOn(
	ctx context.Context, a, b ldrdecoder.DecodedRow,
) (bool, error) {
	if a.TableID != b.TableID {
		return false, nil
	}
	tc, ok := ls.tableConstraints[a.TableID]
	if !ok {
		return false, nil
	}

	return tc.DependsOn(ctx, a, b)
}
