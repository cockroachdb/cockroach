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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// UniquenessChecksForGenRandomUUIDClusterMode controls the cluster setting for
// enabling uniqueness checks for UUID columns set to gen_random_uuid().
var UniquenessChecksForGenRandomUUIDClusterMode = settings.RegisterBoolSetting(
	"sql.optimizer.uniqueness_checks_for_gen_random_uuid.enabled",
	"if enabled, uniqueness checks may be planned for mutations of UUID columns updated with"+
		" gen_random_uuid(); otherwise, uniqueness is assumed due to near-zero collision probability",
	false,
).WithPublic()

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
		// If this constraint is already enforced by an index, we don't need to plan
		// a check.
		if !mb.tab.Unique(i).WithoutIndex() {
			continue
		}
		// If this constraint is an arbiter of an INSERT ... ON CONFLICT ... DO
		// NOTHING clause, we don't need to plan a check (ON CONFLICT ... DO UPDATE
		// does not go through this code path; that's handled by
		// buildUniqueChecksForUpsert).
		if mb.uniqueConstraintIsArbiter(i) {
			continue
		}
		if h.init(mb, i) {
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
		// If this constraint is already enforced by an index, we don't need to plan
		// a check.
		if !mb.tab.Unique(i).WithoutIndex() {
			continue
		}
		// If this constraint doesn't include the updated columns we don't need to
		// plan a check.
		if !mb.uniqueColsUpdated(i) {
			continue
		}
		if h.init(mb, i) {
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
		// If this constraint is already enforced by an index, we don't need to plan
		// a check.
		if !mb.tab.Unique(i).WithoutIndex() {
			continue
		}
		// If this constraint is an arbiter of an INSERT ... ON CONFLICT ... DO
		// UPDATE clause and not updated by the DO UPDATE clause, we don't need to
		// plan a check (ON CONFLICT ... DO NOTHING does not go through this code
		// path; that's handled by buildUniqueChecksForInsert). Note that that if
		// the constraint is partial and columns referenced in the predicate are
		// updated, we'll still plan the check (this is handled correctly by
		// mb.uniqueColsUpdated).
		if mb.uniqueConstraintIsArbiter(i) && !mb.uniqueColsUpdated(i) {
			continue
		}
		if h.init(mb, i) {
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
// constraint are being updated (according to updateColIDs). When the unique
// constraint has a partial predicate, it also returns true if the predicate
// references any of the columns being updated.
func (mb *mutationBuilder) uniqueColsUpdated(uniqueOrdinal cat.UniqueOrdinal) bool {
	uc := mb.tab.Unique(uniqueOrdinal)

	for i, n := 0, uc.ColumnCount(); i < n; i++ {
		if ord := uc.ColumnOrdinal(mb.tab, i); mb.updateColIDs[ord] != 0 {
			return true
		}
	}

	if _, isPartial := uc.Predicate(); isPartial {
		pred := mb.parseUniqueConstraintPredicateExpr(uniqueOrdinal)
		typedPred := mb.fetchScope.resolveAndRequireType(pred, types.Bool)

		var predCols opt.ColSet
		mb.b.buildScalar(typedPred, mb.fetchScope, nil, nil, &predCols)
		for colID, ok := predCols.Next(0); ok; colID, ok = predCols.Next(colID + 1) {
			ord := mb.md.ColumnMeta(colID).Table.ColumnOrdinal(colID)
			if mb.updateColIDs[ord] != 0 {
				return true
			}
		}
	}

	return false
}

// uniqueConstraintIsArbiter returns true if the given unique constraint is used
// as an arbiter to detect conflicts in an  INSERT ... ON CONFLICT statement.
func (mb *mutationBuilder) uniqueConstraintIsArbiter(uniqueOrdinal int) bool {
	return mb.arbiters.ContainsUniqueConstraint(uniqueOrdinal)
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
	uniqueOrdinals util.FastIntSet

	// primaryKeyOrdinals includes the ordinals from any primary key columns
	// that are not included in uniqueOrdinals.
	primaryKeyOrdinals util.FastIntSet

	// The scope and column ordinals of the scan that will serve as the right
	// side of the semi join for the uniqueness checks.
	scanScope    *scope
	scanOrdinals []int
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

	var uniqueOrds util.FastIntSet
	for i, n := 0, h.unique.ColumnCount(); i < n; i++ {
		uniqueOrds.Add(h.unique.ColumnOrdinal(mb.tab, i))
	}

	// Find the primary key columns that are not part of the unique constraint.
	// If there aren't any, we don't need a check.
	// TODO(mgartner): We also don't need a check if there exists a unique index
	// with columns that are a subset of the unique constraint columns.
	// Similarly, we don't need a check for a partial unique constraint if there
	// exists a non-partial unique constraint with columns that are a subset of
	// the partial unique constraint columns.
	primaryOrds := getIndexLaxKeyOrdinals(mb.tab.Index(cat.PrimaryIndex))
	primaryOrds.DifferenceWith(uniqueOrds)
	if primaryOrds.Empty() {
		// The primary key columns are a subset of the unique columns; unique check
		// not needed.
		return false
	}

	h.uniqueOrdinals = uniqueOrds
	h.primaryKeyOrdinals = primaryOrds

	for tabOrd, ok := h.uniqueOrdinals.Next(0); ok; tabOrd, ok = h.uniqueOrdinals.Next(tabOrd + 1) {
		colID := mb.mapToReturnColID(tabOrd)
		// Check if we are setting NULL values for the unique columns, like when
		// this mutation is the result of a SET NULL cascade action. If at least one
		// unique column is getting a NULL value, unique check not needed.
		if memo.OutputColumnIsAlwaysNull(mb.outScope.expr, colID) {
			return false
		}

		// If one of the columns is a UUID set to gen_random_uuid() and we don't
		// require uniqueness checks for gen_random_uuid(), unique check not needed.
		if mb.md.ColumnMeta(colID).Type.Family() == types.UuidFamily &&
			columnIsGenRandomUUID(mb.outScope.expr, colID) {
			requireCheck := UniquenessChecksForGenRandomUUIDClusterMode.Get(&mb.b.evalCtx.Settings.SV)
			if !requireCheck {
				return false
			}
		}
	}

	// Build the scan that will serve as the right side of the semi join in the
	// uniqueness check. We need to build the scan now so that we can use its
	// FDs below.
	h.scanScope, h.scanOrdinals = h.buildTableScan()

	// Check that the columns in the unique constraint aren't already known to
	// form a lax key. This can happen if there is a unique index on a superset of
	// these columns, where all other columns are computed columns that depend
	// only on our columns. This is especially important for multi-region tables
	// when the region column is computed.
	//
	// For example:
	//
	//   CREATE TABLE tab (
	//     k INT PRIMARY KEY,
	//     region crdb_internal_region AS (
	//       CASE WHEN k < 10 THEN 'us-east1' ELSE 'us-west1' END
	//     ) STORED
	//   ) LOCALITY REGIONAL BY ROW AS region
	//
	// Because this is a REGIONAL BY ROW table, the region column is implicitly
	// added to the front of every index, including the primary index. As a
	// result, we would normally need to add a uniqueness check to all mutations
	// to ensure that the primary key column (k in this case) remains unique.
	// However, because the region column is computed and depends only on k, the
	// presence of the unique index on (region, k) (i.e., the primary index) is
	// sufficient to guarantee the uniqueness of k.
	var uniqueCols opt.ColSet
	h.uniqueOrdinals.ForEach(func(ord int) {
		colID := h.scanScope.cols[ord].id
		uniqueCols.Add(colID)
	})
	fds := &h.scanScope.expr.Relational().FuncDeps
	return !fds.ColsAreLaxKey(uniqueCols)
}

// buildInsertionCheck creates a unique check for rows which are added to a
// table. The input to the insertion check will be produced from the input to
// the mutation operator.
func (h *uniqueCheckHelper) buildInsertionCheck() memo.UniqueChecksItem {
	f := h.mb.b.factory

	// Build a self semi-join, with the new values on the left and the
	// existing values on the right.

	withScanScope, _ := h.mb.buildCheckInputScan(
		checkInputScanNewVals, h.scanOrdinals,
	)

	// Build the join filters:
	//   (new_a = existing_a) AND (new_b = existing_b) AND ...
	//
	// Set the capacity to h.uniqueOrdinals.Len()+1 since we'll have an equality
	// condition for each column in the unique constraint, plus one additional
	// condition to prevent rows from matching themselves (see below). If the
	// constraint is partial, add 2 to account for filtering both the WithScan
	// and the Scan by the partial unique constraint predicate.
	numFilters := h.uniqueOrdinals.Len() + 1
	_, isPartial := h.unique.Predicate()
	if isPartial {
		numFilters += 2
	}
	semiJoinFilters := make(memo.FiltersExpr, 0, numFilters)
	for i, ok := h.uniqueOrdinals.Next(0); ok; i, ok = h.uniqueOrdinals.Next(i + 1) {
		semiJoinFilters = append(semiJoinFilters, f.ConstructFiltersItem(
			f.ConstructEq(
				f.ConstructVariable(withScanScope.cols[i].id),
				f.ConstructVariable(h.scanScope.cols[i].id),
			),
		))
	}

	// If the unique constraint is partial, we need to filter out inserted rows
	// that don't satisfy the predicate. We also need to make sure that rows do
	// not match existing rows in the the table that do not satisfy the
	// predicate. So we add the predicate as a filter on both the WithScan
	// columns and the Scan columns.
	if isPartial {
		pred := h.mb.parseUniqueConstraintPredicateExpr(h.uniqueOrdinal)

		typedPred := withScanScope.resolveAndRequireType(pred, types.Bool)
		withScanPred := h.mb.b.buildScalar(typedPred, withScanScope, nil, nil, nil)
		semiJoinFilters = append(semiJoinFilters, f.ConstructFiltersItem(withScanPred))

		typedPred = h.scanScope.resolveAndRequireType(pred, types.Bool)
		scanPred := h.mb.b.buildScalar(typedPred, h.scanScope, nil, nil, nil)
		semiJoinFilters = append(semiJoinFilters, f.ConstructFiltersItem(scanPred))
	}

	// We need to prevent rows from matching themselves in the semi join. We can
	// do this by adding another filter that uses the primary keys to check if
	// two rows are identical:
	//    (new_pk1 != existing_pk1) OR (new_pk2 != existing_pk2) OR ...
	var pkFilter opt.ScalarExpr
	for i, ok := h.primaryKeyOrdinals.Next(0); ok; i, ok = h.primaryKeyOrdinals.Next(i + 1) {
		pkFilterLocal := f.ConstructNe(
			f.ConstructVariable(withScanScope.cols[i].id),
			f.ConstructVariable(h.scanScope.cols[i].id),
		)
		if pkFilter == nil {
			pkFilter = pkFilterLocal
		} else {
			pkFilter = f.ConstructOr(pkFilter, pkFilterLocal)
		}
	}
	semiJoinFilters = append(semiJoinFilters, f.ConstructFiltersItem(pkFilter))

	semiJoin := f.ConstructSemiJoin(withScanScope.expr, h.scanScope.expr, semiJoinFilters, memo.EmptyJoinPrivate)

	// Collect the key columns that will be shown in the error message if there
	// is a duplicate key violation resulting from this uniqueness check.
	keyCols := make(opt.ColList, 0, h.uniqueOrdinals.Len())
	for i, ok := h.uniqueOrdinals.Next(0); ok; i, ok = h.uniqueOrdinals.Next(i + 1) {
		keyCols = append(keyCols, withScanScope.cols[i].id)
	}

	// Create a Project that passes-through only the key columns. This allows
	// normalization rules to prune any unnecessary columns from the expression.
	// The key columns are always needed in order to display the constraint
	// violation error.
	project := f.ConstructProject(semiJoin, nil /* projections */, keyCols.ToSet())

	return f.ConstructUniqueChecksItem(project, &memo.UniqueChecksItemPrivate{
		Table:        h.mb.tabID,
		CheckOrdinal: h.uniqueOrdinal,
		KeyCols:      keyCols,
		OpName:       h.mb.opName,
	})
}

// buildTableScan builds a Scan of the table. The ordinals of the columns
// scanned are also returned.
func (h *uniqueCheckHelper) buildTableScan() (outScope *scope, ordinals []int) {
	tabMeta := h.mb.b.addTable(h.mb.tab, tree.NewUnqualifiedTableName(h.mb.tab.Name()))
	ordinals = tableOrdinals(tabMeta.Table, columnKinds{
		includeMutations:       false,
		includeSystem:          false,
		includeVirtualInverted: false,
		includeVirtualComputed: true,
	})
	return h.mb.b.buildScan(
		tabMeta,
		ordinals,
		// After the update we can't guarantee that the constraints are unique
		// (which is why we need the uniqueness checks in the first place).
		&tree.IndexFlags{IgnoreUniqueWithoutIndexKeys: true},
		noRowLocking,
		h.mb.b.allocScope(),
	), ordinals
}

// columnIsGenRandomUUID returns true if the expression returns the function
// gen_random_uuid() for the given column.
func columnIsGenRandomUUID(e memo.RelExpr, col opt.ColumnID) bool {
	isGenRandomUUIDFunction := func(scalar opt.ScalarExpr) bool {
		if function, ok := scalar.(*memo.FunctionExpr); ok {
			if function.Name == "gen_random_uuid" {
				return true
			}
		}
		return false
	}

	switch e.Op() {
	case opt.ProjectOp:
		p := e.(*memo.ProjectExpr)
		if p.Passthrough.Contains(col) {
			return columnIsGenRandomUUID(p.Input, col)
		}
		for i := range p.Projections {
			if p.Projections[i].Col == col {
				return isGenRandomUUIDFunction(p.Projections[i].Element)
			}
		}

	case opt.ValuesOp:
		v := e.(*memo.ValuesExpr)
		colOrdinal, ok := v.Cols.Find(col)
		if !ok {
			return false
		}
		for i := range v.Rows {
			if !isGenRandomUUIDFunction(v.Rows[i].(*memo.TupleExpr).Elems[colOrdinal]) {
				return false
			}
		}
		return true
	}

	return false
}
