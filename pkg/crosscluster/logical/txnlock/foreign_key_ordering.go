// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/errors"
)

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

	// Cases 8-10, 14, 16, all cases can be reduced down to:
	// Parent first if the columns are not disjoint. We only need to check the new
	// row value as the parent create will always be null (disjoint) for the prev row.
	if parentOp == createOp || parentOp == createAndDeleteOp {
		eq, err := columnSetEqual(
			ctx, evalCtx, &childConstraint, &parentConstraint, child.Row, parent.Row,
		)
		if err != nil {
			return noApplyOrder, err
		}
		if eq {
			return parentFirst, nil
		}
	}
	// Cases 11-13, 15, 16, all cases can be reduced down to:
	// Child first if the columns are not disjoint. We only need to check the prev
	// row value as the parent delete will always be null (disjoint) for the new row.
	if parentOp == deleteOp || parentOp == createAndDeleteOp {
		eq, err := columnSetEqual(
			ctx, evalCtx, &childConstraint, &parentConstraint, child.PrevRow, parent.PrevRow,
		)
		if err != nil {
			return noApplyOrder, err
		}
		if eq {
			return childFirst, nil
		}
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
