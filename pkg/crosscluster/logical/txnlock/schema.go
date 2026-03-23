// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/sqlwriter"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/errors"
)

// tableConstraints is used to represent all of the constraints that are
// relevant for lock synthesis.
type tableConstraints struct {
	evalCtx *eval.Context
	// PrimaryKeyConstraint is the column set for the primary key constraint. We
	// can't represent this as a unique constraint because unique constraints
	// have an optimization to only acquire a write lock if the value is
	// changing.
	PrimaryKeyConstraint columnSet
	UniqueConstraints    []columnSet
	// TODO(jeffswenson): add support for foreign key ordering
}

func newTableConstraints(
	evalCtx *eval.Context, table catalog.TableDescriptor,
) (*tableConstraints, error) {
	columnSchema := sqlwriter.GetColumnSchema(table)
	colIDToIndex := make(map[descpb.ColumnID]int32, len(columnSchema))
	for i, col := range columnSchema {
		colIDToIndex[col.Column.GetID()] = int32(i)
	}

	tc := &tableConstraints{evalCtx: evalCtx}

	makeUniqueConstraint := func(uc catalog.UniqueConstraint) (columnSet, error) {
		colIDs := uc.CollectKeyColumnIDs().Ordered()
		cols := make([]int32, len(colIDs))
		for i, colID := range colIDs {
			cols[i] = colIDToIndex[colID]
		}
		ucMixin, err := uniqueIndexMixin(
			table.GetID(),
			uc.GetConstraintID(),
		)
		if err != nil {
			return columnSet{}, err
		}
		return columnSet{
			columns: cols,
			mixin:   ucMixin,
		}, nil
	}

	for _, uc := range table.EnforcedUniqueConstraintsWithIndex() {
		constraint, err := makeUniqueConstraint(uc)
		if err != nil {
			return nil, err
		}
		if uc.Primary() {
			tc.PrimaryKeyConstraint = constraint
		} else {
			tc.UniqueConstraints = append(tc.UniqueConstraints, constraint)
		}
	}

	for _, uc := range table.EnforcedUniqueConstraintsWithoutIndex() {
		constraint, err := makeUniqueConstraint(uc)
		if err != nil {
			return nil, err
		}
		tc.UniqueConstraints = append(tc.UniqueConstraints, constraint)
	}

	if len(tc.PrimaryKeyConstraint.columns) == 0 {
		return nil, errors.AssertionFailedf("expected primary key constraint for table %d", table.GetID())
	}

	return tc, nil
}

// addLock adds the lock to the list of locks if it is not already present. If
// the lock is present, it upgrades the lock to a write lock if necessary. This
// operation is O(n), but we expect the number of locks per row to be small.
func addLock(locks []Lock, lock Lock) []Lock {
	for i := range locks {
		if locks[i].Hash == lock.Hash {
			locks[i].Read = locks[i].Read && lock.Read
			return locks
		}
	}
	return append(locks, lock)
}

func (t *tableConstraints) deriveLocks(
	ctx context.Context, row ldrdecoder.DecodedRow,
) ([]Lock, error) {
	locks := make([]Lock, 0, len(t.UniqueConstraints)*2+1)

	pkRow := row.Row
	if row.IsDeleteRow() {
		pkRow = row.PrevRow
	}
	lock, err := t.PrimaryKeyConstraint.hash(ctx, pkRow)
	if err != nil {
		return nil, err
	}
	locks = append(locks, Lock{
		Hash: lock,
		Read: false,
	})

	for _, uc := range t.UniqueConstraints {
		if uc.null(row.Row) && uc.null(row.PrevRow) {
			continue
		}
		eq, err := uc.equal(ctx, t.evalCtx, row.Row, row.PrevRow)
		if err != nil {
			return nil, err
		}
		if eq {
			continue
		}
		if !uc.null(row.Row) {
			h, err := uc.hash(ctx, row.Row)
			if err != nil {
				return nil, err
			}
			locks = addLock(locks, Lock{
				Hash: h,
				Read: true,
			})
		}
		if !uc.null(row.PrevRow) {
			h, err := uc.hash(ctx, row.PrevRow)
			if err != nil {
				return nil, err
			}
			locks = addLock(locks, Lock{
				Hash: h,
				Read: false,
			})
		}
	}
	return locks, nil
}

// DependsOn returns true if b must be applied before a can be applied.
func (t *tableConstraints) DependsOn(
	ctx context.Context, a, b ldrdecoder.DecodedRow,
) (bool, error) {
	for _, uc := range t.UniqueConstraints {
		// We check PrevRow == Row because we are looking for a case where applying
		// b "frees" a value that a "takes". In that case `a` depends on `b`.
		eq, err := uc.equal(ctx, t.evalCtx, a.Row, b.PrevRow)
		if err != nil {
			return false, err
		}
		if eq {
			return true, nil
		}
	}
	return false, nil
}
