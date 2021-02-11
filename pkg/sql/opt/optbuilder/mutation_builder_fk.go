// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// This file contains methods that populate mutationBuilder.fkChecks and cascades.
//
// -- Checks --
//
// The foreign key checks are queries that run after the statement (including
// the relevant mutation) completes. They check the integrity of the foreign key
// relations that involve modified rows; any row that is returned by these FK
// check queries indicates a foreign key violation.
//
// -- Cacades --
//
// The foreign key cascades are "potential" future queries that perform
// cascading mutations of child tables. These queries are constructed later as
// necessary; mb.cascades stores metadata that include CascadeBuilder instances
// which are used to construct these queries.

// buildFKChecksForInsert builds FK check queries for an insert.
//
// See the comment at the top of the file for general information on checks and
// cascades.
//
// In the case of insert, each FK check query is an anti-join with the left side
// being a WithScan of the mutation input and the right side being the
// referenced table. A simple example of an insert with a FK check:
//
//   insert child
//    ├── ...
//    ├── input binding: &1
//    └── f-k-checks
//         └── f-k-checks-item: child(p) -> parent(p)
//              └── anti-join (hash)
//                   ├── columns: column2:5!null
//                   ├── with-scan &1
//                   │    ├── columns: column2:5!null
//                   │    └── mapping:
//                   │         └──  column2:4 => column2:5
//                   ├── scan parent
//                   │    └── columns: parent.p:6!null
//                   └── filters
//                        └── column2:5 = parent.p:6
//
// See testdata/fk-checks-insert for more examples.
func (mb *mutationBuilder) buildFKChecksForInsert() {
	if mb.tab.OutboundForeignKeyCount() == 0 {
		// No relevant FKs.
		return
	}

	// TODO(radu): if the input is a VALUES with constant expressions, we don't
	// need to buffer it. This could be a normalization rule, but it's probably
	// more efficient if we did it in here (or we'd end up building the entire FK
	// subtrees twice).
	mb.ensureWithID()

	h := &mb.fkCheckHelper
	for i, n := 0, mb.tab.OutboundForeignKeyCount(); i < n; i++ {
		if h.initWithOutboundFK(mb, i) {
			mb.fkChecks = append(mb.fkChecks, h.buildInsertionCheck())
		}
	}
	telemetry.Inc(sqltelemetry.ForeignKeyChecksUseCounter)
}

// buildFKChecksAndCascadesForDelete builds FK check and cascades for a delete.
//
// See the comment at the top of the file for general information on checks and
// cascades.
//
// -- Checks --
//
// In the case of delete, each FK check query is a semi-join with the left side
// being a WithScan of the mutation input and the right side being the
// referencing table. For example:
//   delete parent
//    ├── ...
//    ├── input binding: &1
//    └── f-k-checks
//         └── f-k-checks-item: child(p) -> parent(p)
//              └── semi-join (hash)
//                   ├── columns: p:7!null
//                   ├── with-scan &1
//                   │    ├── columns: p:7!null
//                   │    └── mapping:
//                   │         └──  parent.p:5 => p:7
//                   ├── scan child
//                   │    └── columns: child.p:9!null
//                   └── filters
//                        └── p:7 = child.p:9
//
// See testdata/fk-checks-delete for more examples.
//
// -- Cascades --
//
// See onDeleteCascadeBuilder, onDeleteFastCascadeBuilder, onDeleteSetBuilder
// for details.
//
func (mb *mutationBuilder) buildFKChecksAndCascadesForDelete() {
	if mb.tab.InboundForeignKeyCount() == 0 {
		// No relevant FKs.
		return
	}

	for i, n := 0, mb.tab.InboundForeignKeyCount(); i < n; i++ {
		h := &mb.fkCheckHelper
		if !h.initWithInboundFK(mb, i) {
			continue
		}
		// The action dictates how a foreign key reference is handled:
		//  - with Cascade/SetNull/SetDefault, we create a cascading mutation to
		//    modify or delete "orphaned" rows in the child table.
		//  - with Restrict/NoAction, we create a check that causes an error if
		//    there are any "orphaned" rows in the child table.
		if a := h.fk.DeleteReferenceAction(); a != tree.Restrict && a != tree.NoAction {
			telemetry.Inc(sqltelemetry.ForeignKeyCascadesUseCounter)
			var builder memo.CascadeBuilder
			switch a {
			case tree.Cascade:
				// Try the fast builder first; if it cannot be used, use the regular builder.
				var ok bool
				builder, ok = tryNewOnDeleteFastCascadeBuilder(
					mb.b.ctx, mb.md, mb.b.catalog, h.fk, i, mb.tab, h.otherTab, mb.outScope,
				)
				if !ok {
					mb.ensureWithID()
					builder = newOnDeleteCascadeBuilder(mb.tab, i, h.otherTab)
				}
			case tree.SetNull, tree.SetDefault:
				mb.ensureWithID()
				builder = newOnDeleteSetBuilder(mb.tab, i, h.otherTab, a)
			default:
				panic(errors.AssertionFailedf("unhandled action type %s", a))
			}

			cols := make(opt.ColList, len(h.tabOrdinals))
			for i, tabOrd := range h.tabOrdinals {
				cols[i] = mb.fetchColIDs[tabOrd]
			}
			mb.cascades = append(mb.cascades, memo.FKCascade{
				FKName:    h.fk.Name(),
				Builder:   builder,
				WithID:    mb.withID,
				OldValues: cols,
				NewValues: nil,
			})
			continue
		}

		mb.ensureWithID()
		withScanScope, _ := mb.buildCheckInputScan(checkInputScanFetchedVals, h.tabOrdinals)
		mb.fkChecks = append(mb.fkChecks, h.buildDeletionCheck(withScanScope.expr, withScanScope.colList()))
	}
	telemetry.Inc(sqltelemetry.ForeignKeyChecksUseCounter)
}

// buildFKChecksForUpdate builds FK check queries for an update.
//
// See the comment at the top of the file for general information on checks and
// cascades.
//
// In the case of update, there are two types of FK check queries:
//
//  - insertion-side checks are very similar to the checks we issue for insert;
//    they are an anti-join with the left side being a WithScan of the "new"
//    values for each row. For example:
//      update child
//       ├── ...
//       ├── input binding: &1
//       └── f-k-checks
//            └── f-k-checks-item: child(p) -> parent(p)
//                 └── anti-join (hash)
//                      ├── columns: column5:6!null
//                      ├── with-scan &1
//                      │    ├── columns: column5:6!null
//                      │    └── mapping:
//                      │         └──  column5:5 => column5:6
//                      ├── scan parent
//                      │    └── columns: parent.p:8!null
//                      └── filters
//                           └── column5:6 = parent.p:8
//
//  - deletion-side checks are similar to the checks we issue for delete; they
//    are a semi-join but the left side input is more complicated: it is an
//    Except between a WithScan of the "old" values and a WithScan of the "new"
//    values for each row (this is the set of values that are effectively
//    removed from the table). For example:
//      update parent
//       ├── ...
//       ├── input binding: &1
//       └── f-k-checks
//            └── f-k-checks-item: child(p) -> parent(p)
//                 └── semi-join (hash)
//                      ├── columns: p:8!null
//                      ├── except
//                      │    ├── columns: p:8!null
//                      │    ├── left columns: p:8!null
//                      │    ├── right columns: column7:9
//                      │    ├── with-scan &1
//                      │    │    ├── columns: p:8!null
//                      │    │    └── mapping:
//                      │    │         └──  parent.p:5 => p:8
//                      │    └── with-scan &1
//                      │         ├── columns: column7:9!null
//                      │         └── mapping:
//                      │              └──  column7:7 => column7:9
//                      ├── scan child
//                      │    └── columns: child.p:11!null
//                      └── filters
//                           └── p:8 = child.p:11
//
// Only FK relations that involve updated columns result in FK checks.
//
func (mb *mutationBuilder) buildFKChecksForUpdate() {
	if mb.tab.OutboundForeignKeyCount() == 0 && mb.tab.InboundForeignKeyCount() == 0 {
		return
	}

	mb.ensureWithID()

	// An Update can be thought of an insertion paired with a deletion, so for an
	// Update we can emit both semi-joins and anti-joins.

	// Each row input to the Update operator contains both the existing and the
	// new value for each updated column. From this we can construct the effective
	// insertion and deletion.

	// Say the table being updated by an update is:
	//
	//   x | y | z
	//   --+---+--
	//   1 | 3 | 5
	//
	// And we are executing UPDATE t SET y = 10, then the input to the Update
	// operator will look like:
	//
	//   x | y | z | new_y
	//   --+---+---+------
	//   1 | 3 | 5 |  10
	//
	// The insertion check will happen on the "new" row (x, new_y, z); the deletion
	// check will happen on the "old" row (x, y, z).

	h := &mb.fkCheckHelper
	for i, n := 0, mb.tab.OutboundForeignKeyCount(); i < n; i++ {
		// Verify that at least one FK column is actually updated.
		if mb.outboundFKColsUpdated(i) {
			if h.initWithOutboundFK(mb, i) {
				mb.fkChecks = append(mb.fkChecks, h.buildInsertionCheck())
			}
		}
	}

	// The "deletion" incurred by an update is the rows deleted for a given
	// inbound FK minus the rows inserted.
	for i, n := 0, mb.tab.InboundForeignKeyCount(); i < n; i++ {
		// Verify that at least one FK column is actually updated.
		if !mb.inboundFKColsUpdated(i) {
			continue
		}
		if !h.initWithInboundFK(mb, i) {
			// The FK constraint can safely be ignored.
			continue
		}

		if a := h.fk.UpdateReferenceAction(); a != tree.Restrict && a != tree.NoAction {
			telemetry.Inc(sqltelemetry.ForeignKeyCascadesUseCounter)
			builder := newOnUpdateCascadeBuilder(mb.tab, i, h.otherTab, a)

			oldCols := make(opt.ColList, len(h.tabOrdinals))
			newCols := make(opt.ColList, len(h.tabOrdinals))
			for i, tabOrd := range h.tabOrdinals {
				fetchColID := mb.fetchColIDs[tabOrd]
				updateColID := mb.updateColIDs[tabOrd]
				if updateColID == 0 {
					updateColID = fetchColID
				}

				oldCols[i] = fetchColID
				newCols[i] = updateColID
			}
			mb.cascades = append(mb.cascades, memo.FKCascade{
				FKName:    h.fk.Name(),
				Builder:   builder,
				WithID:    mb.withID,
				OldValues: oldCols,
				NewValues: newCols,
			})
			continue
		}

		// Construct an Except expression for the set difference between "old"
		// FK values and "new" FK values.
		//
		// The simplest example to see why this is necessary is when we are
		// "updating" a value to the same value, e.g:
		//   UPDATE child SET c = c
		// Here we are not removing any values from the column, so we must not
		// check for orphaned rows or we will be generating bogus FK violation
		// errors.
		//
		// There are more complicated cases where one row replaces the value from
		// another row, e.g.
		//   UPDATE child SET c = c+1
		// when we have existing consecutive values. These cases are sketchy because
		// depending on the order in which the mutations are applied, they may or
		// may not result in unique index violations (but if they go through, the FK
		// checks should be accurate).
		//
		// Note that the same reasoning could be applied to the insertion checks,
		// but in that case, it is not a correctness issue: it's always ok to
		// recheck that an existing row is not orphan. It's not really desirable for
		// performance either: we would be incurring extra cost (more complicated
		// expressions, scanning the input buffer twice) for a rare case.

		oldRowsScope, _ := mb.buildCheckInputScan(checkInputScanFetchedVals, h.tabOrdinals)
		newRowsScope, _ := mb.buildCheckInputScan(checkInputScanNewVals, h.tabOrdinals)
		colsForOldRow := oldRowsScope.colList()
		colsForNewRow := newRowsScope.colList()

		// The rows that no longer exist are the ones that were "deleted" by virtue
		// of being updated _from_, minus the ones that were "added" by virtue of
		// being updated _to_.
		deletedRows := mb.b.factory.ConstructExcept(
			oldRowsScope.expr,
			newRowsScope.expr,
			&memo.SetPrivate{
				LeftCols:  colsForOldRow,
				RightCols: colsForNewRow,
				OutCols:   colsForOldRow,
			},
		)

		mb.fkChecks = append(mb.fkChecks, h.buildDeletionCheck(deletedRows, colsForOldRow))
	}
	telemetry.Inc(sqltelemetry.ForeignKeyChecksUseCounter)
}

// buildFKChecksForUpsert builds FK check queries for an upsert.
//
// See the comment at the top of the file for general information on checks and
// cascades.
//
// The case of upsert is very similar to update; see buildFKChecksForUpdate.
// The main difference is that for update, the "new" values were readily
// available, whereas for upsert, the "new" values can be the result of an
// expression of the form:
//   CASE WHEN canary IS NULL THEN inserter-value ELSE updated-value END
// These expressions are already projected as part of the mutation input and are
// directly accessible through WithScan.
//
// Only FK relations that involve updated columns result in deletion-side FK
// checks. The insertion-side FK checks are always needed (similar to insert)
// because any of the rows might result in an insert rather than an update.
//
func (mb *mutationBuilder) buildFKChecksForUpsert() {
	numOutbound := mb.tab.OutboundForeignKeyCount()
	numInbound := mb.tab.InboundForeignKeyCount()

	if numOutbound == 0 && numInbound == 0 {
		return
	}

	mb.ensureWithID()

	h := &mb.fkCheckHelper
	for i := 0; i < numOutbound; i++ {
		if h.initWithOutboundFK(mb, i) {
			mb.fkChecks = append(mb.fkChecks, h.buildInsertionCheck())
		}
	}

	for i := 0; i < numInbound; i++ {
		// Verify that at least one FK column is updated by the Upsert; columns that
		// are not updated can get new values (through the insert path) but existing
		// values are never removed.
		if !mb.inboundFKColsUpdated(i) {
			continue
		}

		if !h.initWithInboundFK(mb, i) {
			continue
		}

		if a := h.fk.UpdateReferenceAction(); a != tree.Restrict && a != tree.NoAction {
			telemetry.Inc(sqltelemetry.ForeignKeyCascadesUseCounter)
			builder := newOnUpdateCascadeBuilder(mb.tab, i, h.otherTab, a)

			oldCols := make(opt.ColList, len(h.tabOrdinals))
			newCols := make(opt.ColList, len(h.tabOrdinals))
			for i, tabOrd := range h.tabOrdinals {
				fetchColID := mb.fetchColIDs[tabOrd]
				// Here we don't need to use the upsertColIDs because the rows that
				// correspond to inserts will be ignored in the cascade.
				updateColID := mb.updateColIDs[tabOrd]
				if updateColID == 0 {
					updateColID = fetchColID
				}

				oldCols[i] = fetchColID
				newCols[i] = updateColID
			}
			mb.cascades = append(mb.cascades, memo.FKCascade{
				FKName:    h.fk.Name(),
				Builder:   builder,
				WithID:    mb.withID,
				OldValues: oldCols,
				NewValues: newCols,
			})
			continue
		}

		// Construct an Except expression for the set difference between "old" FK
		// values and "new" FK values. See buildFKChecksForUpdate for more details.
		//
		// Note that technically, to get "old" values for the updated rows we should
		// be selecting only the rows that correspond to updates, as opposed to
		// insertions (using a "canaryCol IS NOT NULL" condition). But the rows we
		// would filter out have all-null fetched values anyway and will never match
		// in the semi join.
		oldRowsScope, _ := mb.buildCheckInputScan(checkInputScanFetchedVals, h.tabOrdinals)
		newRowsScope, _ := mb.buildCheckInputScan(checkInputScanNewVals, h.tabOrdinals)
		colsForOldRow := oldRowsScope.colList()
		colsForNewRow := newRowsScope.colList()

		// The rows that no longer exist are the ones that were "deleted" by virtue
		// of being updated _from_, minus the ones that were "added" by virtue of
		// being updated _to_.
		deletedRows := mb.b.factory.ConstructExcept(
			oldRowsScope.expr,
			newRowsScope.expr,
			&memo.SetPrivate{
				LeftCols:  colsForOldRow,
				RightCols: colsForNewRow,
				OutCols:   colsForOldRow,
			},
		)
		mb.fkChecks = append(mb.fkChecks, h.buildDeletionCheck(deletedRows, oldRowsScope.colList()))
	}
	telemetry.Inc(sqltelemetry.ForeignKeyChecksUseCounter)
}

// outboundFKColsUpdated returns true if any of the FK columns for an outbound
// constraint are being updated (according to updateColIDs).
func (mb *mutationBuilder) outboundFKColsUpdated(fkOrdinal int) bool {
	fk := mb.tab.OutboundForeignKey(fkOrdinal)
	for i, n := 0, fk.ColumnCount(); i < n; i++ {
		if ord := fk.OriginColumnOrdinal(mb.tab, i); mb.updateColIDs[ord] != 0 {
			return true
		}
	}
	return false
}

// inboundFKColsUpdated returns true if any of the FK columns for an inbound
// constraint are being updated (according to updateColIDs).
func (mb *mutationBuilder) inboundFKColsUpdated(fkOrdinal int) bool {
	fk := mb.tab.InboundForeignKey(fkOrdinal)
	for i, n := 0, fk.ColumnCount(); i < n; i++ {
		if ord := fk.ReferencedColumnOrdinal(mb.tab, i); mb.updateColIDs[ord] != 0 {
			return true
		}
	}
	return false
}

// ensureWithID makes sure that withID is initialized (and thus that the input
// to the mutation will be buffered).
//
// Assumes that outScope.expr is the input to the mutation.
func (mb *mutationBuilder) ensureWithID() {
	if mb.withID != 0 {
		return
	}

	mb.withID = mb.b.factory.Memo().NextWithID()
	mb.md.AddWithBinding(mb.withID, mb.outScope.expr)
}

// fkCheckHelper is a type associated with a single FK constraint and is used to
// build the "leaves" of a FK check expression, namely the WithScan of the
// mutation input and the Scan of the other table.
type fkCheckHelper struct {
	mb *mutationBuilder

	fk         cat.ForeignKeyConstraint
	fkOrdinal  int
	fkOutbound bool

	otherTab cat.Table

	// tabOrdinals are the table ordinals of the FK columns in the table that is
	// being mutated. They correspond 1-to-1 to the columns in the
	// ForeignKeyConstraint.
	tabOrdinals []int
	// otherTabOrdinals are the table ordinals of the FK columns in the "other"
	// table. They correspond 1-to-1 to the columns in the ForeignKeyConstraint.
	otherTabOrdinals []int
}

// initWithOutboundFK initializes the helper with an outbound FK constraint.
//
// Returns false if the FK relation should be ignored (e.g. because the new
// values for the FK columns are known to be always NULL).
func (h *fkCheckHelper) initWithOutboundFK(mb *mutationBuilder, fkOrdinal int) bool {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*h = fkCheckHelper{
		mb:         mb,
		fk:         mb.tab.OutboundForeignKey(fkOrdinal),
		fkOrdinal:  fkOrdinal,
		fkOutbound: true,
	}

	refID := h.fk.ReferencedTableID()
	h.otherTab = resolveTable(mb.b.ctx, mb.b.catalog, refID)
	if h.otherTab == nil {
		// The other table is in the process of being added; ignore the FK relation.
		return false
	}
	// We need SELECT privileges on the referenced table.
	mb.b.checkPrivilege(opt.DepByID(refID), h.otherTab, privilege.SELECT)

	numCols := h.fk.ColumnCount()
	h.allocOrdinals(numCols)
	for i := 0; i < numCols; i++ {
		h.tabOrdinals[i] = h.fk.OriginColumnOrdinal(mb.tab, i)
		h.otherTabOrdinals[i] = h.fk.ReferencedColumnOrdinal(h.otherTab, i)
	}

	// Check if we are setting NULL values for the FK columns, like when this
	// mutation is the result of a SET NULL cascade action.
	numNullCols := 0
	for _, tabOrd := range h.tabOrdinals {
		colID := mb.mapToReturnColID(tabOrd)
		if memo.OutputColumnIsAlwaysNull(mb.outScope.expr, colID) {
			numNullCols++
		}
	}
	if numNullCols == numCols {
		// All FK columns are getting NULL values; FK check not needed.
		return false
	}
	if numNullCols > 0 && h.fk.MatchMethod() == tree.MatchSimple {
		// At least one FK column is getting a NULL value and we are using MATCH
		// SIMPLE; FK check not needed.
		return false
	}

	return true
}

// initWithInboundFK initializes the helper with an inbound FK constraint.
//
// Returns false if the FK relation should be ignored (because the other table
// is in the process of being created).
func (h *fkCheckHelper) initWithInboundFK(mb *mutationBuilder, fkOrdinal int) (ok bool) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*h = fkCheckHelper{
		mb:         mb,
		fk:         mb.tab.InboundForeignKey(fkOrdinal),
		fkOrdinal:  fkOrdinal,
		fkOutbound: false,
	}

	originID := h.fk.OriginTableID()
	h.otherTab = resolveTable(mb.b.ctx, mb.b.catalog, originID)
	if h.otherTab == nil {
		return false
	}
	// We need SELECT privileges on the origin table.
	mb.b.checkPrivilege(opt.DepByID(originID), h.otherTab, privilege.SELECT)

	numCols := h.fk.ColumnCount()
	h.allocOrdinals(numCols)
	for i := 0; i < numCols; i++ {
		h.tabOrdinals[i] = h.fk.ReferencedColumnOrdinal(mb.tab, i)
		h.otherTabOrdinals[i] = h.fk.OriginColumnOrdinal(h.otherTab, i)
	}

	return true
}

// resolveTable resolves a table StableID. Returns nil if the table is in the
// process of being added, in which case it is safe to ignore any FK
// relation with the table.
func resolveTable(ctx context.Context, catalog cat.Catalog, id cat.StableID) cat.Table {
	ref, isAdding, err := catalog.ResolveDataSourceByID(ctx, cat.Flags{}, id)
	if err != nil {
		if isAdding {
			// The table is in the process of being added.
			return nil
		}
		panic(err)
	}
	return ref.(cat.Table)
}

// buildOtherTableScan builds a Scan of the "other" table.
func (h *fkCheckHelper) buildOtherTableScan() (outScope *scope, tabMeta *opt.TableMeta) {
	otherTabMeta := h.mb.b.addTable(h.otherTab, tree.NewUnqualifiedTableName(h.otherTab.Name()))
	return h.mb.b.buildScan(
		otherTabMeta,
		h.otherTabOrdinals,
		&tree.IndexFlags{IgnoreForeignKeys: true},
		noRowLocking,
		h.mb.b.allocScope(),
	), otherTabMeta
}

func (h *fkCheckHelper) allocOrdinals(numCols int) {
	buf := make([]int, numCols*2)
	h.tabOrdinals = buf[:numCols]
	h.otherTabOrdinals = buf[numCols:]
}

// buildInsertionCheck creates a FK check for rows which are added to a table.
// The input to the insertion check will be produced from the input to the
// mutation operator.
func (h *fkCheckHelper) buildInsertionCheck() memo.FKChecksItem {
	withScanScope, notNullWithScanCols := h.mb.buildCheckInputScan(
		checkInputScanNewVals, h.tabOrdinals,
	)

	numCols := len(withScanScope.cols)
	f := h.mb.b.factory
	if notNullWithScanCols.Len() < numCols {
		// The columns we are inserting might have NULLs. These require special
		// handling, depending on the match method:
		//  - MATCH SIMPLE: allows any column(s) to be NULL and the row doesn't
		//                  need to have a match in the referenced table.
		//  - MATCH FULL: only the case where *all* the columns are NULL is
		//                allowed, and the row doesn't need to have a match in the
		//                referenced table.
		//
		// Note that rows that have NULLs will never have a match in the anti
		// join and will generate errors. To handle these cases, we filter the
		// mutated rows (before the anti join) to remove those which don't need a
		// match.
		//
		// For SIMPLE, we filter out any rows which have a NULL. For FULL, we
		// filter out any rows where all the columns are NULL (rows which have
		// NULLs a subset of columns are let through and will generate FK errors
		// because they will never have a match in the anti join).
		switch m := h.fk.MatchMethod(); m {
		case tree.MatchSimple:
			// Filter out any rows which have a NULL; build filters of the form
			//   (a IS NOT NULL) AND (b IS NOT NULL) ...
			filters := make(memo.FiltersExpr, 0, numCols-notNullWithScanCols.Len())
			for _, col := range withScanScope.cols {
				if !notNullWithScanCols.Contains(col.id) {
					filters = append(filters, f.ConstructFiltersItem(
						f.ConstructIsNot(
							f.ConstructVariable(col.id),
							memo.NullSingleton,
						),
					))
				}
			}
			withScanScope.expr = f.ConstructSelect(withScanScope.expr, filters)

		case tree.MatchFull:
			// Filter out any rows which have NULLs on all referencing columns.
			if !notNullWithScanCols.Empty() {
				// We statically know that some of the referencing columns can't be
				// NULL. In this case, we don't need to filter anything (the case
				// where all the origin columns are NULL is not possible).
				break
			}
			// Build a filter of the form
			//   (a IS NOT NULL) OR (b IS NOT NULL) ...
			var condition opt.ScalarExpr
			for _, col := range withScanScope.cols {
				is := f.ConstructIsNot(
					f.ConstructVariable(col.id),
					memo.NullSingleton,
				)
				if condition == nil {
					condition = is
				} else {
					condition = f.ConstructOr(condition, is)
				}
			}
			withScanScope.expr = f.ConstructSelect(
				withScanScope.expr,
				memo.FiltersExpr{f.ConstructFiltersItem(condition)},
			)

		default:
			panic(errors.AssertionFailedf("match method %s not supported", m))
		}
	}

	// Build an anti-join, with the origin FK columns on the left and the
	// referenced columns on the right.

	scanScope, refTabMeta := h.buildOtherTableScan()

	// Build the join filters:
	//   (origin_a = referenced_a) AND (origin_b = referenced_b) AND ...
	antiJoinFilters := make(memo.FiltersExpr, numCols)
	for j := 0; j < numCols; j++ {
		antiJoinFilters[j] = f.ConstructFiltersItem(
			f.ConstructEq(
				f.ConstructVariable(withScanScope.cols[j].id),
				f.ConstructVariable(scanScope.cols[j].id),
			),
		)
	}
	var p memo.JoinPrivate
	if h.mb.b.evalCtx.SessionData.PreferLookupJoinsForFKs {
		p.Flags = memo.PreferLookupJoinIntoRight
	}
	antiJoin := f.ConstructAntiJoin(withScanScope.expr, scanScope.expr, antiJoinFilters, &p)

	return f.ConstructFKChecksItem(antiJoin, &memo.FKChecksItemPrivate{
		OriginTable:     h.mb.tabID,
		ReferencedTable: refTabMeta.MetaID,
		FKOutbound:      true,
		FKOrdinal:       h.fkOrdinal,
		KeyCols:         withScanScope.colList(),
		OpName:          h.mb.opName,
	})
}

// buildDeletionCheck creates a FK check for rows which are removed from a
// table. deletedRows is used as the input to the deletion check, and deleteCols
// is a list of the columns for the rows being deleted, containing values for
// the referenced FK columns in the table we are mutating.
func (h *fkCheckHelper) buildDeletionCheck(
	deletedRows memo.RelExpr, deleteCols opt.ColList,
) memo.FKChecksItem {
	// Build a semi join, with the referenced FK columns on the left and the
	// origin columns on the right.
	scanScope, origTabMeta := h.buildOtherTableScan()

	// Note that it's impossible to orphan a row whose FK key columns contain a
	// NULL, since by definition a NULL never refers to an actual row (in
	// either MATCH FULL or MATCH SIMPLE).
	// Build the join filters:
	//   (origin_a = referenced_a) AND (origin_b = referenced_b) AND ...
	f := h.mb.b.factory
	semiJoinFilters := make(memo.FiltersExpr, len(deleteCols))
	for j := range deleteCols {
		semiJoinFilters[j] = f.ConstructFiltersItem(
			f.ConstructEq(
				f.ConstructVariable(deleteCols[j]),
				f.ConstructVariable(scanScope.cols[j].id),
			),
		)
	}
	var p memo.JoinPrivate
	if h.mb.b.evalCtx.SessionData.PreferLookupJoinsForFKs {
		p.Flags = memo.PreferLookupJoinIntoRight
	}
	semiJoin := f.ConstructSemiJoin(deletedRows, scanScope.expr, semiJoinFilters, &p)

	return f.ConstructFKChecksItem(semiJoin, &memo.FKChecksItemPrivate{
		OriginTable:     origTabMeta.MetaID,
		ReferencedTable: h.mb.tabID,
		FKOutbound:      false,
		FKOrdinal:       h.fkOrdinal,
		KeyCols:         deleteCols,
		OpName:          h.mb.opName,
	})
}
