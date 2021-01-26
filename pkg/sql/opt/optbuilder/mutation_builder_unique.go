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
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// buildUniqueChecksForInsert builds uniqueness check queries for an insert.
// These check queries are used to enforce UNIQUE WITHOUT INDEX constraints.
func (mb *mutationBuilder) buildUniqueChecksForInsert() {
	// We only need to build unique checks if there is at least one unique
	// constraint without an index.
	if !mb.hasUniqueWithoutIndexConstraints() {
		return
	}

	mb.ensureWithID()
	h := &mb.uniqueCheckHelper

	for i, n := 0, mb.tab.UniqueCount(); i < n; i++ {
		// If this constraint is already enforced by an index we don't need to plan
		// a check.
		if mb.tab.Unique(i).WithoutIndex() && h.init(mb, i) {
			mb.uniqueChecks = append(mb.uniqueChecks, h.buildInsertionCheck())
		}
	}
	telemetry.Inc(sqltelemetry.UniqueChecksUseCounter)
}

// buildUniqueChecksForUpdate builds uniqueness check queries for an update.
// These check queries are used to enforce UNIQUE WITHOUT INDEX constraints.
func (mb *mutationBuilder) buildUniqueChecksForUpdate() {
	// We only need to build unique checks if there is at least one unique
	// constraint without an index.
	if !mb.hasUniqueWithoutIndexConstraints() {
		return
	}

	mb.ensureWithID()
	h := &mb.uniqueCheckHelper

	for i, n := 0, mb.tab.UniqueCount(); i < n; i++ {
		// If this constraint is already enforced by an index or doesn't include
		// the updated columns we don't need to plan a check.
		if mb.tab.Unique(i).WithoutIndex() && mb.uniqueColsUpdated(i) && h.init(mb, i) {
			// The insertion check works for updates too since it simply checks that
			// the unique columns in the newly inserted or updated rows do not match
			// any existing rows. The check prevents rows from matching themselves by
			// adding a filter based on the primary key.
			mb.uniqueChecks = append(mb.uniqueChecks, h.buildInsertionCheck())
		}
	}
	telemetry.Inc(sqltelemetry.UniqueChecksUseCounter)
}

// buildUniqueChecksForUpsert builds uniqueness check queries for an upsert.
// These check queries are used to enforce UNIQUE WITHOUT INDEX constraints.
func (mb *mutationBuilder) buildUniqueChecksForUpsert() {
	// We only need to build unique checks if there is at least one unique
	// constraint without an index.
	if !mb.hasUniqueWithoutIndexConstraints() {
		return
	}

	mb.ensureWithID()
	h := &mb.uniqueCheckHelper

	for i, n := 0, mb.tab.UniqueCount(); i < n; i++ {
		// If this constraint is already enforced by an index we don't need to plan
		// a check.
		if mb.tab.Unique(i).WithoutIndex() && h.init(mb, i) {
			// The insertion check works for upserts too since it simply checks that
			// the unique columns in the newly inserted or updated rows do not match
			// any existing rows. The check prevents rows from matching themselves by
			// adding a filter based on the primary key.
			mb.uniqueChecks = append(mb.uniqueChecks, h.buildInsertionCheck())
		}
	}
	telemetry.Inc(sqltelemetry.UniqueChecksUseCounter)
}

// hasUniqueWithoutIndexConstraints returns true if there are any
// UNIQUE WITHOUT INDEX constraints on the table.
func (mb *mutationBuilder) hasUniqueWithoutIndexConstraints() bool {
	for i, n := 0, mb.tab.UniqueCount(); i < n; i++ {
		if mb.tab.Unique(i).WithoutIndex() {
			return true
		}
	}
	return false
}

// uniqueColsUpdated returns true if any of the columns for a unique
// constraint are being updated (according to updateColIDs).
func (mb *mutationBuilder) uniqueColsUpdated(uniqueOrdinal int) bool {
	uc := mb.tab.Unique(uniqueOrdinal)
	for i, n := 0, uc.ColumnCount(); i < n; i++ {
		if ord := uc.ColumnOrdinal(mb.tab, i); mb.updateColIDs[ord] != 0 {
			return true
		}
	}
	return false
}

// uniqueCheckHelper is a type associated with a single unique constraint and
// is used to build the "leaves" of a unique check expression, namely the
// WithScan of the mutation input and the Scan of the table.
type uniqueCheckHelper struct {
	mb *mutationBuilder

	unique        cat.UniqueConstraint
	uniqueOrdinal int

	// uniqueOrdinals are the table ordinals of the unique columns in the table
	// that is being mutated. They correspond 1-to-1 to the columns in the
	// UniqueConstraint.
	uniqueOrdinals []int

	// uniqueAndPrimaryKeyOrdinals includes all the ordinals from uniqueOrdinals,
	// plus the ordinals from any primary key columns that are not already
	// included in uniqueOrdinals.
	uniqueAndPrimaryKeyOrdinals []int
}

// init initializes the helper with a unique constraint.
//
// Returns false if the constraint should be ignored (e.g. because the new
// values for the unique columns are known to be always NULL).
func (h *uniqueCheckHelper) init(mb *mutationBuilder, uniqueOrdinal int) bool {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*h = uniqueCheckHelper{
		mb:            mb,
		unique:        mb.tab.Unique(uniqueOrdinal),
		uniqueOrdinal: uniqueOrdinal,
	}

	uniqueCount := h.unique.ColumnCount()

	var uniqueOrds util.FastIntSet
	for i := 0; i < uniqueCount; i++ {
		uniqueOrds.Add(h.unique.ColumnOrdinal(mb.tab, i))
	}

	// Find the primary key columns that are not part of the unique constraint.
	// If there aren't any, we don't need a check.
	primaryOrds := getIndexLaxKeyOrdinals(mb.tab.Index(cat.PrimaryIndex))
	primaryOrds.DifferenceWith(uniqueOrds)
	if primaryOrds.Empty() {
		// The primary key columns are a subset of the unique columns; unique check
		// not needed.
		return false
	}

	h.uniqueAndPrimaryKeyOrdinals = append(uniqueOrds.Ordered(), primaryOrds.Ordered()...)
	h.uniqueOrdinals = h.uniqueAndPrimaryKeyOrdinals[:uniqueCount]

	// Check if we are setting NULL values for the unique columns, like when this
	// mutation is the result of a SET NULL cascade action.
	numNullCols := 0
	for _, tabOrd := range h.uniqueOrdinals {
		colID := mb.mapToReturnColID(tabOrd)
		if memo.OutputColumnIsAlwaysNull(mb.outScope.expr, colID) {
			numNullCols++
		}
	}

	// If at least one unique column is getting a NULL value, unique check not
	// needed.
	return numNullCols == 0
}

// buildInsertionCheck creates a unique check for rows which are added to a
// table. The input to the insertion check will be produced from the input to
// the mutation operator.
func (h *uniqueCheckHelper) buildInsertionCheck() memo.UniqueChecksItem {
	checkInput, withScanCols, _ := h.mb.makeCheckInputScan(
		checkInputScanNewVals, h.uniqueAndPrimaryKeyOrdinals,
	)

	numCols := len(withScanCols)
	f := h.mb.b.factory

	// Build a self semi-join, with the new values on the left and the
	// existing values on the right.

	scanScope, _ := h.buildTableScan()

	// Build the join filters:
	//   (new_a = existing_a) AND (new_b = existing_b) AND ...
	//
	// Set the capacity to len(h.uniqueOrdinals)+1 since we'll have an equality
	// condition for each column in the unique constraint, plus one additional
	// condition to prevent rows from matching themselves (see below).
	semiJoinFilters := make(memo.FiltersExpr, 0, len(h.uniqueOrdinals)+1)
	for i := 0; i < len(h.uniqueOrdinals); i++ {
		semiJoinFilters = append(semiJoinFilters, f.ConstructFiltersItem(
			f.ConstructEq(
				f.ConstructVariable(withScanCols[i]),
				f.ConstructVariable(scanScope.cols[i].id),
			),
		))
	}

	// We need to prevent rows from matching themselves in the semi join. We can
	// do this by adding another filter that uses the primary keys to check if
	// two rows are identical:
	//    (new_pk1 != existing_pk1) OR (new_pk2 != existing_pk2) OR ...
	var pkFilter opt.ScalarExpr
	for i := len(h.uniqueOrdinals); i < numCols; i++ {
		pkFilterLocal := f.ConstructNe(
			f.ConstructVariable(withScanCols[i]),
			f.ConstructVariable(scanScope.cols[i].id),
		)
		if pkFilter == nil {
			pkFilter = pkFilterLocal
		} else {
			pkFilter = f.ConstructOr(pkFilter, pkFilterLocal)
		}
	}
	semiJoinFilters = append(semiJoinFilters, f.ConstructFiltersItem(pkFilter))

	semiJoin := f.ConstructSemiJoin(checkInput, scanScope.expr, semiJoinFilters, memo.EmptyJoinPrivate)

	return f.ConstructUniqueChecksItem(semiJoin, &memo.UniqueChecksItemPrivate{
		Table:        h.mb.tabID,
		CheckOrdinal: h.uniqueOrdinal,
		// uniqueOrdinals is always a prefix of uniqueAndPrimaryKeyOrdinals, which
		// maps 1-to-1 to the columns in withScanCols. The remaining columns are
		// primary key columns and should not be included in the KeyCols.
		KeyCols: withScanCols[:len(h.uniqueOrdinals)],
		OpName:  h.mb.opName,
	})
}

// buildTableScan builds a Scan of the table.
func (h *uniqueCheckHelper) buildTableScan() (outScope *scope, tabMeta *opt.TableMeta) {
	tabMeta = h.mb.b.addTable(h.mb.tab, tree.NewUnqualifiedTableName(h.mb.tab.Name()))
	return h.mb.b.buildScan(
		tabMeta,
		h.uniqueAndPrimaryKeyOrdinals,
		nil, /* indexFlags */
		noRowLocking,
		h.mb.b.allocScope(),
	), tabMeta
}
