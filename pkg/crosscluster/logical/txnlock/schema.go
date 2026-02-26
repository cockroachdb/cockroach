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
	// OutboundForeignKeyConstraints are the column sets for the foreign key constraints
	// where this is the origin table, i.e. the child. It maps the constraint mixin
	// to the column set for the constraint.
	OutboundForeignKeyConstraints map[uint64]columnSet
	// InboundForeignKeyConstraints are the column sets for the foreign key constraints
	// where this is the referenced table, i.e. the parent.
	//
	// Note that for every inbound FK constraint, there must be a corresponding PK/UC constraint
	// that we've created either in this table or another. However, we opt for separate column sets
	// for locking since:
	// 1. We always emit a write lock for the primary key constraint, but want to make the same
	// optimization as unique constraints where we only acquire a write lock if the value is changing.
	// 2. We need to be able to cross-reference which outbound FK constraint corresponds to which inbound
	// FK constraint to easily determine equality of rows.
	InboundForeignKeyConstraints map[uint64]columnSet
}

func newTableConstraints(
	evalCtx *eval.Context, table catalog.TableDescriptor,
) (*tableConstraints, error) {
	columnSchema := sqlwriter.GetColumnSchema(table)
	colIDToIndex := make(map[descpb.ColumnID]int32, len(columnSchema))
	for i, col := range columnSchema {
		colIDToIndex[col.Column.GetID()] = int32(i)
	}

	tc := &tableConstraints{
		evalCtx:                       evalCtx,
		OutboundForeignKeyConstraints: make(map[uint64]columnSet),
		InboundForeignKeyConstraints:  make(map[uint64]columnSet),
	}

	makeUniqueConstraint := func(uc catalog.UniqueConstraint) (columnSet, error) {
		colIDs := uc.CollectKeyColumnIDs().Ordered()
		cols := make([]int32, len(colIDs))
		for i, colID := range colIDs {
			cols[i] = colIDToIndex[colID]
		}
		ucMixin, err := constraintMixin(
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

	for _, fk := range table.EnforcedOutboundForeignKeys() {
		originColIDs := fk.CollectOriginColumnIDs().Ordered()
		cols := make([]int32, len(originColIDs))
		for i, colID := range originColIDs {
			cols[i] = colIDToIndex[colID]
		}
		// N.B. we use the origin table ID and constraint ID for the mixin for both sides
		// so they collide. We use the origin side for uniqueness since there can be one
		// to many referenced to origin relationships but not the other way around.
		fkMixin, err := constraintMixin(fk.GetOriginTableID(), fk.GetConstraintID())
		if err != nil {
			return nil, err
		}
		tc.OutboundForeignKeyConstraints[fkMixin] = columnSet{
			columns: cols,
			mixin:   fkMixin,
		}
	}

	for _, fk := range table.InboundForeignKeys() {
		referencedColIDs := fk.CollectReferencedColumnIDs().Ordered()
		cols := make([]int32, len(referencedColIDs))
		for i, colID := range referencedColIDs {
			cols[i] = colIDToIndex[colID]
		}
		// Use the origin table ID so both sides of the FK produce the same mixin.
		fkMixin, err := constraintMixin(fk.GetOriginTableID(), fk.GetConstraintID())
		if err != nil {
			return nil, err
		}
		tc.InboundForeignKeyConstraints[fkMixin] = columnSet{
			columns: cols,
			mixin:   fkMixin,
		}
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

	addFKLocks := func(constraints map[uint64]columnSet, read bool) error {
		for _, fk := range constraints {
			if fk.null(row.Row) && fk.null(row.PrevRow) {
				continue
			}
			// If it's a delete row, and we didn't continue above, it must be
			// that our FK references the primary key and was deleted.
			if row.IsDeleteRow() {
				if !fk.null(row.PrevRow) {
					h, err := fk.hash(ctx, row.PrevRow)
					if err != nil {
						return err
					}
					locks = addLock(locks, Lock{
						Hash: h,
						Read: read,
					})
				}
				continue
			}
			eq, err := fk.equal(ctx, t.evalCtx, row.Row, row.PrevRow)
			if err != nil {
				return err
			}
			if eq {
				continue
			}
			if !fk.null(row.Row) {
				h, err := fk.hash(ctx, row.Row)
				if err != nil {
					return err
				}
				locks = addLock(locks, Lock{
					Hash: h,
					Read: read,
				})
			}
			if !fk.null(row.PrevRow) {
				h, err := fk.hash(ctx, row.PrevRow)
				if err != nil {
					return err
				}
				locks = addLock(locks, Lock{
					Hash: h,
					Read: read,
				})
			}
		}
		return nil
	}
	if err := addFKLocks(t.OutboundForeignKeyConstraints, true); err != nil {
		return nil, err
	}
	if err := addFKLocks(t.InboundForeignKeyConstraints, false); err != nil {
		return nil, err
	}

	return locks, nil
}

// DependsOn returns true if b must be applied before a can be applied. Only
// unique constraint conflicts are checked here. FK ordering is handled by fkDependsOn.
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

// fkDependsOn returns true if b must be applied before a can be applied.
// t is a's tableConstraints, otherTC is b's tableConstraints.
func (t *tableConstraints) fkDependsOn(
	ctx context.Context, otherTC *tableConstraints, a, b ldrdecoder.DecodedRow,
) (bool, error) {
	// a is the child, b is the parent. a depends on b if b is inserting a
	// parent value that a's new row needs to reference.
	for _, outbound := range t.OutboundForeignKeyConstraints {
		inbound, ok := otherTC.InboundForeignKeyConstraints[outbound.mixin]
		if !ok {
			continue
		}
		eq, err := crossTableEqual(ctx, t.evalCtx, outbound, inbound, a.Row, b.Row)
		if err != nil {
			return false, err
		}
		if eq {
			return true, nil
		}
	}

	// a is the parent, b is the child. a depends on b if b is removing a
	// child reference that currently prevents a's parent row from being
	// deleted.
	for _, inbound := range t.InboundForeignKeyConstraints {
		outbound, ok := otherTC.OutboundForeignKeyConstraints[inbound.mixin]
		if !ok {
			continue
		}
		eq, err := crossTableEqual(ctx, t.evalCtx, inbound, outbound, a.PrevRow, b.PrevRow)
		if err != nil {
			return false, err
		}
		if eq {
			return true, nil
		}
	}

	return false, nil
}
