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
			tableID: table.GetID(),
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
		// Use the origin column ID ordering here in the case of composite FKs where the ordering
		// of the outbound and inbound columns may differ, but we still need the hash to collide.
		numCols := fk.NumOriginColumns()
		cols := make([]int32, numCols)
		for i := range numCols {
			cols[i] = colIDToIndex[fk.GetOriginColumnID(i)]
		}
		// N.B. we use the origin table ID and constraint ID for the mixin for both sides
		// so they collide. We use the origin side for uniqueness since there can be one
		// to many referenced to origin relationships but not the other way around.
		fkMixin, err := constraintMixin(fk.GetOriginTableID(), fk.GetConstraintID())
		if err != nil {
			return nil, err
		}
		tc.OutboundForeignKeyConstraints[fkMixin] = columnSet{
			tableID: table.GetID(),
			columns: cols,
			mixin:   fkMixin,
		}
	}

	for _, fk := range table.InboundForeignKeys() {
		numCols := fk.NumReferencedColumns()
		cols := make([]int32, numCols)
		for i := range numCols {
			cols[i] = colIDToIndex[fk.GetReferencedColumnID(i)]
		}
		// Use the origin table ID so both sides of the FK produce the same mixin.
		fkMixin, err := constraintMixin(fk.GetOriginTableID(), fk.GetConstraintID())
		if err != nil {
			return nil, err
		}
		tc.InboundForeignKeyConstraints[fkMixin] = columnSet{
			tableID: table.GetID(),
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

	var err error
	locks, err = t.derivePKLocks(ctx, row, locks)
	if err != nil {
		return nil, err
	}
	locks, err = t.deriveUniqueLocks(ctx, row, locks)
	if err != nil {
		return nil, err
	}
	locks, err = t.deriveFKLocks(ctx, row, locks)
	if err != nil {
		return nil, err
	}

	return locks, nil
}

func (t *tableConstraints) derivePKLocks(
	ctx context.Context, row ldrdecoder.DecodedRow, locks []Lock,
) ([]Lock, error) {
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
	return locks, nil
}

func (t *tableConstraints) deriveUniqueLocks(
	ctx context.Context, row ldrdecoder.DecodedRow, locks []Lock,
) ([]Lock, error) {
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
				Read: false,
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

func (t *tableConstraints) deriveFKLocks(
	ctx context.Context, row ldrdecoder.DecodedRow, locks []Lock,
) ([]Lock, error) {
	addFKLocks := func(constraints map[uint64]columnSet, read bool) error {
		for _, fk := range constraints {
			// Skip emitting locks if the FK columns did not change.
			if row.IsUpdateRow() {
				eq, err := fk.equal(ctx, t.evalCtx, row.Row, row.PrevRow)
				if err != nil {
					return err
				}
				if eq {
					continue
				}
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
	// Check if row a is dependent on row b based on parent and child dependencies.
	// Note that row a can have both child and parent dependencies on row b, i.e.
	// we break the check into two parts.

	// Check if a and b have any fk dependencies where a is the child
	// table and b is the parent table.
	parent, child := b, a
	for _, parentConstraint := range otherTC.InboundForeignKeyConstraints {
		childConstraint, ok := t.OutboundForeignKeyConstraints[parentConstraint.mixin]
		if !ok {
			continue
		}
		applyOrder, err := deriveFKApplyOrder(ctx, t.evalCtx, parent, child, parentConstraint, childConstraint)
		if err != nil {
			return false, err
		}
		switch applyOrder {
		case noApplyOrder:
		case parentFirst:
			// parent (row b) first means that a is dependent on b.
			return true, nil
		case childFirst:
			// child (row a) first means that b is dependent on a, but we don't
			// return early here because we want to detect cycles. To show a cycle,
			// we need to prove that (a → b) and (b → a). We will (or already did)
			// call `fkDependsOn(b, a)` which will check for (b → a), so we only
			// need to worry about (a → b), i.e. if we return false early here
			// we might end up proving (b → a) twice and miss the cycle.
		}
	}

	// Check if a and b have any fk dependencies where a is the parent
	// table and b is the child table.
	parent, child = a, b
	for _, parentConstraint := range t.InboundForeignKeyConstraints {
		childConstraint, ok := otherTC.OutboundForeignKeyConstraints[parentConstraint.mixin]
		if !ok {
			continue
		}
		applyOrder, err := deriveFKApplyOrder(ctx, t.evalCtx, parent, child, parentConstraint, childConstraint)
		if err != nil {
			return false, err
		}
		switch applyOrder {
		case noApplyOrder:
		case parentFirst:
			// Don't return to detect cycles, same logic as above.
		case childFirst:
			return true, nil
		}
	}
	return false, nil
}

type fkApplyOrder int

const (
	noApplyOrder fkApplyOrder = iota
	childFirst
	parentFirst
)

// deriveFKApplyOrder takes two rows and a single FK constraint, and determines if the two
// rows have a dependency ordering.
//
// When ordering rows based on FK constraints, we need to avoid a state where we have
// a FK reference that points to a non-existent value. This can happen in two ways:
// we haven't yet created the referenced value, or we deleted the referenced value.
// Consider the following table which maps row operations to the effect it has
// on the referenced value:
//
//	┌──────────────────────┬────────────────────────────────────────┐
//	│        Row op        │               column Op                │
//	├──────────────────────┼────────────────────────────────────────┤
//	│ INSERT               │ create (null → new)                    │
//	├──────────────────────┼────────────────────────────────────────┤
//	│ DELETE               │ delete (old → null)                    │
//	├──────────────────────┼────────────────────────────────────────┤
//	│ UPDATE, FK unchanged │ none                                   │
//	├──────────────────────┼────────────────────────────────────────┤
//	│ UPDATE, FK changed   │ both delete and create (old → new)     │
//	├──────────────────────┼────────────────────────────────────────┤
//	│ TOMBSTONE UPDATE     │ none                                   │
//	└──────────────────────┴────────────────────────────────────────┘
//
// We can consider each FK constraint in isolation. For any two rows A and B, all
// FK constraints must agree with the dependency direction (or lack of) or we will
// get a cycle conflict.
//
// For any given FK constraint, there is a parent and child row. The parent is the
// row with the column being referenced by the child row's FK. The following table
// returns for the given parent and child column ops:
//
//  1. [parent]: the child may be depedent on the parent row
//
//  2. [child]: the parent may be dependent on the child row
//
//  3. [n/a]: the rows _must_ have no dependency on each other
//
//  4. [either]: either the parent or child may have a dependency on each other
//
//     ┌──────────────────┬─────────────┬─────────────┬─────────────┬─────────────┐
//     │  parent \ child  │    none     │   create    │   delete    │    both     │
//     ├──────────────────┼─────────────┼─────────────┼─────────────┼─────────────┤
//     │ none             │     n/a     │     n/a     │     n/a     │     n/a     │
//     ├──────────────────┼─────────────┼─────────────┼─────────────┼─────────────┤
//     │ create           │     n/a     │    parent   │     n/a     │    parent   │
//     ├──────────────────┼─────────────┼─────────────┼─────────────┼─────────────┤
//     │ delete           │     n/a     │     n/a     │    child    │    child    │
//     ├──────────────────┼─────────────┼─────────────┼─────────────┼─────────────┤
//     │ both             │     n/a     │    parent   │    child    │    either   │
//     └──────────────────┴─────────────┴─────────────┴─────────────┴─────────────┘
//
// The following case analyses implicitly consider the trivial disjoint
// case so we will address it once here and handwave/refer back to it later.
// If the parent/child rows are not touching the same value at any point then
// it's not possible for there to be a dependency. A FK dependency means that
// if we must apply the rows in a certain way or we will have a dangling FK
// reference that points to a non-existent row on the child. However, because
// the values are disjoint, this is not possible.
//
// 1-7 - [none/*], [*/none]: If either parent/child row is none, then we can trivially show
// there must be no dependency. Consider the parent, none means that the referenced value must
// exist before and after our row was applied. Likewise, if the child's col op
// is none it must mean we are pointing to a value that already exists and must continue
// to exist after the child row. If the parent row created the value, the former would be
// false, and if the parent row deleted the value, the latter would be false. In other words,
// if either row is a none col op, then we hit the disjoint case.
//
// 8 - [create\create]: If both parent and child are creating a value and the FK's are not disjoint,
// then it must be that the parent is creating a value that the child is referencing. We need to
// apply the parent row first in this case.
//
// 9 - [create\delete]: Assume the parent and child touch the same referenced FK value. If the parent
// is creating the value, then it does not exist before we apply this row. However, if the child
// is deleting the value, then it must be referencing a value that already exists before the delete.
// i.e. The assumption is impossible and this is always the disjoint case.
//
// 10 - [create\both]: Combining the above [create\create] and [create\delete], we can see the
// delete side is always disjoint and the create side means we need to apply the parent row
// first if the values aren't disjoint.
//
// 11 - [delete\create]:  Assume the parent and child touch the same referenced FK value. If the parent
// is deleting the value, the child's reference would be dangling after the delete. Therefore this
// assumption is impossible and it's always the disjoint case.
//
// 12 - [delete\delete]: If both parent and child are deleting a value and the FK's are not disjoint,
// then it must be the parent deleting the value that the child is referencing. We need to
// delete the child row first in this case.
//
// 13 - [delete\both]: Combining the above [delete\create] and [delete\delete], we can see the
// create side is always disjoint and the delete side means we need to apply the child side
// first if the values aren't disjoint.
//
// 14 - [both\create]: Combining the above [create\create] and [delete\create], we can see the
// delete side is always disjoint and the create side means we need to apply the parent
// first if the values aren't disjoint.
//
// 15 - [both\delete]: Combining the above [create\delete] and [delete\delete], we can see the
// create side is always disjoint and the delete side means we need to apply the child
// first if the values aren't disjoint.
//
// 16 - [both\both]: Let the parent rows update on the referenced column be from values (i → j):
//
//			a. child (i → j): This case is impossible without deferred constraints as we cannot
//			apply the child first before j exists and we cannot apply the parent first or child will
//			have a dangling reference to i.
//
//			b. child (i → k): We need to apply the child first to free up the reference to i.
//
//			c. child (k → j): We need to apply the parent first so j exists for our child to point at.
//
//			d. child (k → l): Disjoint values, no dependency.
//
//			e. child (k → i), (j → k): These both always have no dependencies. In order for the child to start
//	   	referencing a value that the parent deleted, some other row must have inserted the value back or
//	   	deleted our reference first. In either case, we have a dependency on this other row, not the
//	   	parent. Similar logic applies for if the child no longer references the value the parent creates:
//	   	some other row must have deleted the value first or created the child reference first.
func deriveFKApplyOrder(
	ctx context.Context,
	evalCtx *eval.Context,
	parent, child ldrdecoder.DecodedRow,
	parentConstraint, childConstraint columnSet,
) (fkApplyOrder, error) {
	parentOp, err := parentConstraint.colOp(ctx, evalCtx, parent)
	if err != nil {
		return noApplyOrder, err
	}
	childOp, err := childConstraint.colOp(ctx, evalCtx, child)
	if err != nil {
		return noApplyOrder, err
	}

	// Cases 1-7:
	if parentOp == noOp || childOp == noOp {
		return noApplyOrder, nil
	}

	switch parentOp {
	// Cases 8-10, all three cases can be reduced down to:
	// Parent first if the columns are not disjoint. We only need to check the new
	// row value as the parent create will always be null (disjoint) for the prev row.
	case createOp:
		eq, err := columnSetEqual(
			ctx, evalCtx, &childConstraint, &parentConstraint, child.Row, parent.Row,
		)
		if err != nil {
			return noApplyOrder, err
		}
		if eq {
			return parentFirst, nil
		}
	// Cases 11-13, all three cases can be reduced down to:
	// Child first if the columns are not disjoint. We only need to check the prev
	// row value as the parent delete will always be null (disjoint) for the new row.
	case deleteOp:
		eq, err := columnSetEqual(
			ctx, evalCtx, &childConstraint, &parentConstraint, child.PrevRow, parent.PrevRow,
		)
		if err != nil {
			return noApplyOrder, err
		}
		if eq {
			return childFirst, nil
		}
	// Cases 14-16:
	case createAndDeleteOp:
		// Case 14 and 16:
		eq, err := columnSetEqual(
			ctx, evalCtx, &childConstraint, &parentConstraint, child.Row, parent.Row,
		)
		if err != nil {
			return noApplyOrder, err
		}
		if eq {
			return parentFirst, nil
		}

		// Case 15 and 16:
		eq, err = columnSetEqual(
			ctx, evalCtx, &childConstraint, &parentConstraint, child.PrevRow, parent.PrevRow,
		)
		if err != nil {
			return noApplyOrder, err
		}
		if eq {
			return childFirst, nil
		}
	default:
		return noApplyOrder, errors.AssertionFailedf("unexpected column op %d", parentOp)
	}
	return noApplyOrder, nil
}

// colOp describes how a row affects a column.
type colOp int

const (
	noOp colOp = iota
	createOp
	deleteOp
	createAndDeleteOp
)

// colOp returns the given colOp for a row.
func (c *columnSet) colOp(
	ctx context.Context, evalCtx *eval.Context, row ldrdecoder.DecodedRow,
) (colOp, error) {
	switch {
	case row.IsDeleteRow():
		return deleteOp, nil
	case row.IsInsertRow():
		return createOp, nil
	case row.IsUpdateRow():
		eq, err := c.equal(ctx, evalCtx, row.Row, row.PrevRow)
		if err != nil {
			return noOp, err
		}
		if eq {
			return noOp, nil
		}
		return createAndDeleteOp, nil
	case row.IsTombstoneUpdate():
		return noOp, nil
	default:
		return noOp, errors.AssertionFailedf("unexpected row type")
	}
}
