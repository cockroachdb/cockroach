// Copyright 2021 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partialidx"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// findArbiters returns a set of arbiters for an INSERT ON CONFLICT statement.
// Both unique indexes and unique constraints can be arbiters. Arbiters may be
// selected either by explicitly passing a named constraint (the ON CONFLICT ON
// CONSTRAINT form) or by passing a list of columns on which to resolve
// conflicts. This function panics if no arbiters are found.
//
// Arbiter constraints ensure that the columns designated by conflictOrds
// reference at most one target row of a UNIQUE index or constraint. Using ANTI
// JOINs and LEFT OUTER JOINs to detect conflicts relies upon this being true
// (otherwise result cardinality could increase). This is also a Postgres
// requirement.
//
// When inferring arbiters from a list of columns, there are rules about which
// index or constraint may be returned.
//
// An arbiter index:
//
//   1. Must have lax key columns that match the columns in the ON CONFLICT
//      clause.
//   2. If it is a partial index, its predicate must be implied by the
//      arbiter predicate supplied by the user.
//
// An arbiter constraint:
//
//   1. Must have columns that match the columns in the ON CONFLICT clause.
//   2. If it is a partial constraint, its predicate must be implied by the
//      arbiter predicate supplied by the user.
func (mb *mutationBuilder) findArbiters(onConflict *tree.OnConflict) arbiterSet {
	if onConflict == nil {
		// No on conflict constraint means that we're in the UPSERT case, which should
		// use the primary constraint as the arbiter.
		primaryOrds := getExplicitPrimaryKeyOrdinals(mb.tab)
		return mb.inferArbitersFromConflictOrds(primaryOrds, nil /* arbiterPredicate */)
	} else if onConflict.Constraint != "" {
		// We have a constraint explicitly named, so we can set the arbiter to use
		// it directly.
		for i, ic := 0, mb.tab.IndexCount(); i < ic; i++ {
			index := mb.tab.Index(i)
			if !index.IsUnique() {
				continue
			}
			if index.Name() == onConflict.Constraint {
				if _, partial := index.Predicate(); partial {
					panic(partialIndexArbiterError(onConflict, mb.tab.Name()))

				}
				return makeSingleIndexArbiterSet(mb, i)
			}
		}
		for i, uc := 0, mb.tab.UniqueCount(); i < uc; i++ {
			constraint := mb.tab.Unique(i)
			if constraint.Name() == string(onConflict.Constraint) {
				if _, partial := constraint.Predicate(); partial {
					panic(partialIndexArbiterError(onConflict, mb.tab.Name()))
				}
				return makeSingleUniqueConstraintArbiterSet(mb, i)
			}
		}
		// Found nothing, we have to return an error.
		panic(pgerror.Newf(
			pgcode.UndefinedObject,
			"constraint %q for table %q does not exist",
			onConflict.Constraint, mb.tab.Name(),
		))
	}
	// We have to infer an arbiter set.
	var ords util.FastIntSet
	for _, name := range onConflict.Columns {
		found := false
		for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
			tabCol := mb.tab.Column(i)
			if tabCol.ColName() == name && !tabCol.IsMutation() && tabCol.Kind() != cat.System {
				ords.Add(i)
				found = true
				break
			}
		}

		if !found {
			panic(colinfo.NewUndefinedColumnError(string(name)))
		}
	}
	return mb.inferArbitersFromConflictOrds(ords, onConflict.ArbiterPredicate)
}

func partialIndexArbiterError(onConflict *tree.OnConflict, tableName tree.Name) error {
	return errors.WithHint(
		pgerror.Newf(
			pgcode.WrongObjectType,
			"unique constraint %q for table %q is partial, "+
				"so it cannot be used as an arbiter via the ON CONSTRAINT syntax",
			onConflict.Constraint,
			tableName,
		),
		"use the ON CONFLICT (columns...) WHERE <predicate> form "+
			"to select this partial unique constraint as an arbiter",
	)
}

// inferArbitersFromConflictOrds is a helper function for findArbiters that
// infers a set of conflict arbiters from a list of column ordinals that a
// user specified in an ON CONFLICT clause. See the comment above findArbiters
// for more information about what arbiters are.
//
// If conflictOrds is empty then all unique indexes and unique without index
// constraints are returned as arbiters. This is required to support a
// DO NOTHING with no ON CONFLICT columns. In this case, all unique indexes
// and constraints are used to check for conflicts.
//
// If conflictOrds is non-empty, there is an intentional preference for certain
// types of arbiters. Indexes are preferred over constraints because they will
// likely lead to a more efficient query plan. Non-partial or pseudo-partial
// indexes and constraints are preferred over partial indexes and constraints
// because a non-partial or pseudo-partial index or constraint guarantees
// uniqueness of its columns across all rows; there is no need for an additional
// arbiter for a subset of rows. If there are no non-partial or pseudo-partial
// indexes or constraints found, all valid partial indexes and partial
// constraints are returned so that uniqueness is guaranteed on the respective
// subsets of rows. In summary, if conflictOrds is non-empty, this function:
//
//   1. Returns a single non-partial or pseudo-partial arbiter index, if found.
//   2. Return a single non-partial or pseudo-partial arbiter constraint, if
//      found.
//   3. Otherwise, returns all partial arbiter indexes and constraints.
//
func (mb *mutationBuilder) inferArbitersFromConflictOrds(
	conflictOrds util.FastIntSet, arbiterPredicate tree.Expr,
) arbiterSet {
	// If conflictOrds is empty, then all unique indexes and unique without
	// index constraints are arbiters.
	if conflictOrds.Empty() {
		// Use a minArbiterSet which automatically removes arbiter indexes that
		// are made redundant by arbiter unique constraints.
		arbiters := makeMinArbiterSet(mb)
		for idx, idxCount := 0, mb.tab.IndexCount(); idx < idxCount; idx++ {
			if mb.tab.Index(idx).IsUnique() {
				arbiters.AddIndex(idx)
			}
		}
		for uc, ucCount := 0, mb.tab.UniqueCount(); uc < ucCount; uc++ {
			if mb.tab.Unique(uc).WithoutIndex() {
				arbiters.AddUniqueConstraint(uc)
			}
		}
		return arbiters.ArbiterSet()
	}

	arbiters := makeArbiterSet(mb)
	h := &mb.arbiterPredicateHelper
	h.init(mb, arbiterPredicate)
	for idx, idxCount := 0, mb.tab.IndexCount(); idx < idxCount; idx++ {
		index := mb.tab.Index(idx)

		// Skip non-unique indexes. Use lax key columns, which always contain
		// the minimum columns that ensure uniqueness. Null values are
		// considered to be *not* equal, but that's OK because the join
		// condition rejects nulls anyway.
		if !index.IsUnique() || index.LaxKeyColumnCount() != conflictOrds.Len() {
			continue
		}

		// Determine whether the conflict columns match the columns in the lax
		// key. If not, the index cannot be an arbiter index.
		indexOrds := getIndexLaxKeyOrdinals(index)
		if !indexOrds.Equals(conflictOrds) {
			continue
		}

		// If the index is not a partial index, it can always be an arbiter.
		// Furthermore, it is the only arbiter needed because it guarantees
		// uniqueness of its columns across all rows.
		if _, isPartial := index.Predicate(); !isPartial {
			return makeSingleIndexArbiterSet(mb, idx)
		}

		// If the index is a pseudo-partial index, it can always be an arbiter.
		// Furthermore, it is the only arbiter needed because it guarantees
		// uniqueness of its columns across all rows.
		pred := h.partialIndexPredicate(idx)
		if pred.IsTrue() {
			return makeSingleIndexArbiterSet(mb, idx)
		}

		// If the index is a partial index, then it can only be an arbiter if
		// the arbiterPredicate implies it.
		if h.predicateIsImpliedByArbiterPredicate(pred) {
			arbiters.AddIndex(idx)
		}
	}

	// Try to find a matching unique constraint. We should always return a full
	// unique constraint if it exists before returning any partial indexes.
	for uc, ucCount := 0, mb.tab.UniqueCount(); uc < ucCount; uc++ {
		uniqueConstraint := mb.tab.Unique(uc)
		if !uniqueConstraint.WithoutIndex() {
			// Unique constraints with an index were handled above.
			continue
		}

		// Determine whether the conflict columns match the columns in the
		// unique constraint. If not, the constraint cannot be an arbiter. We
		// check the number of columns first to avoid unnecessarily collecting
		// the unique constraint ordinals and determining set equality.
		if uniqueConstraint.ColumnCount() != conflictOrds.Len() {
			continue
		}
		ucOrds := getUniqueConstraintOrdinals(mb.tab, uniqueConstraint)
		if !ucOrds.Equals(conflictOrds) {
			continue
		}

		// If the unique constraint is not partial, it should be returned
		// without any partial index arbiters.
		if _, isPartial := uniqueConstraint.Predicate(); !isPartial {
			return makeSingleUniqueConstraintArbiterSet(mb, uc)
		}

		// If the constraint is a pseudo-partial unique constraint, it can
		// always be an arbiter. It should be returned without any partial index
		// arbiters.
		pred := h.partialUniqueConstraintPredicate(uc)
		if pred.IsTrue() {
			return makeSingleUniqueConstraintArbiterSet(mb, uc)
		}

		// If the unique constraint is partial, then it can only be an arbiter
		// if the arbiterPredicate implies it.
		if h.predicateIsImpliedByArbiterPredicate(pred) {
			arbiters.AddUniqueConstraint(uc)
		}
	}

	// Err if we did not previously return and did not find any partial
	// arbiters.
	if arbiters.Empty() {
		panic(pgerror.Newf(pgcode.InvalidColumnReference,
			"there is no unique or exclusion constraint matching the ON CONFLICT specification"))
	}

	return arbiters
}

// buildAntiJoinForDoNothingArbiter builds an anti-join for a single arbiter
// index or constraint for an INSERT ON CONFLICT DO NOTHING mutation. The
// anti-join wraps the current mb.outScope.expr (which produces the insert rows)
// and removes rows that would conflict with existing rows.
//
// 	 - conflictOrds is the set of table column ordinals that the arbiter
//     guarantees uniqueness of.
// 	 - pred is the partial index or constraint predicate. If the arbiter is
//     not a partial index or constraint, pred is nil.
//
func (mb *mutationBuilder) buildAntiJoinForDoNothingArbiter(
	inScope *scope, conflictOrds util.FastIntSet, pred tree.Expr,
) {
	// Build the right side of the anti-join. Use a new metadata instance
	// of the mutation table so that a different set of column IDs are used for
	// the two tables in the self-join.
	fetchScope := mb.b.buildScan(
		mb.b.addTable(mb.tab, &mb.alias),
		tableOrdinals(mb.tab, columnKinds{
			includeMutations: false,
			includeSystem:    false,
			includeInverted:  false,
		}),
		nil, /* indexFlags */
		noRowLocking,
		inScope,
	)

	// If the index is a unique partial index, then rows that are not in the
	// partial index cannot conflict with insert rows. Therefore, a Select
	// wraps the scan on the right side of the anti-join with the partial
	// index predicate expression as the filter.
	if pred != nil {
		texpr := fetchScope.resolveAndRequireType(pred, types.Bool)
		predScalar := mb.b.buildScalar(texpr, fetchScope, nil, nil, nil)
		fetchScope.expr = mb.b.factory.ConstructSelect(
			fetchScope.expr,
			memo.FiltersExpr{mb.b.factory.ConstructFiltersItem(predScalar)},
		)
	}

	// Build the join condition by creating a conjunction of equality conditions
	// that test each conflict column:
	//
	//   ON ins.x = scan.a AND ins.y = scan.b
	//
	var on memo.FiltersExpr
	for i, ok := conflictOrds.Next(0); ok; i, ok = conflictOrds.Next(i + 1) {
		fetchCol := fetchScope.getColumnForTableOrdinal(i)
		if fetchCol == nil {
			panic(errors.AssertionFailedf("missing column in fetchScope"))
		}

		condition := mb.b.factory.ConstructEq(
			mb.b.factory.ConstructVariable(mb.insertColIDs[i]),
			mb.b.factory.ConstructVariable(fetchCol.id),
		)
		on = append(on, mb.b.factory.ConstructFiltersItem(condition))
	}

	// If the index is a unique partial index, then insert rows that do not
	// satisfy the partial index predicate cannot conflict with existing
	// rows in the unique partial index. Therefore, the partial index
	// predicate expression is added to the ON filters.
	if pred != nil {
		texpr := mb.outScope.resolveAndRequireType(pred, types.Bool)
		predScalar := mb.b.buildScalar(texpr, mb.outScope, nil, nil, nil)
		on = append(on, mb.b.factory.ConstructFiltersItem(predScalar))
	}

	// Construct the anti-join.
	mb.outScope.expr = mb.b.factory.ConstructAntiJoin(
		mb.outScope.expr,
		fetchScope.expr,
		on,
		memo.EmptyJoinPrivate,
	)
}

// buildLeftJoinForUpsertArbiter builds a left-join for a single arbiter index
// or constraint for an UPSERT or INSERT ON CONFLICT DO UPDATE mutation. It
// left-joins each insert row to the target table, using the given conflict
// columns as the join condition.
//
//   - conflictOrds is the set of table column ordinals that the arbiter
//     guarantees uniqueness of.
//   - pred is the partial index predicate. If the arbiter is not a partial
//     index, pred is nil.
//   - partialIndexDistinctCol is a column that allows the UpsertDistinctOn to
//     only de-duplicate insert rows that satisfy the partial index predicate.
//     If the arbiter is not a partial index, partialIndexDistinctCol is nil.
//
func (mb *mutationBuilder) buildLeftJoinForUpsertArbiter(
	inScope *scope, conflictOrds util.FastIntSet, pred tree.Expr,
) {
	// Build the right side of the left outer join. Use a different instance of
	// table metadata so that col IDs do not overlap.
	//
	// NOTE: Include mutation columns, but be careful to never use them for any
	//       reason other than as "fetch columns". See buildScan comment.
	// TODO(andyk): Why does execution engine need mutation columns for Insert?
	mb.fetchScope = mb.b.buildScan(
		mb.b.addTable(mb.tab, &mb.alias),
		tableOrdinals(mb.tab, columnKinds{
			includeMutations: true,
			includeSystem:    true,
			includeInverted:  false,
		}),
		nil, /* indexFlags */
		noRowLocking,
		inScope,
	)
	// Set fetchColIDs to reference the columns created for the fetch values.
	mb.setFetchColIDs(mb.fetchScope.cols)

	// If the index is a unique partial index, then rows that are not in the
	// partial index cannot conflict with insert rows. Therefore, a Select wraps
	// the scan on the right side of the left outer join with the partial index
	// predicate expression as the filter.
	if pred != nil {
		texpr := mb.fetchScope.resolveAndRequireType(pred, types.Bool)
		predScalar := mb.b.buildScalar(texpr, mb.fetchScope, nil, nil, nil)
		mb.fetchScope.expr = mb.b.factory.ConstructSelect(
			mb.fetchScope.expr,
			memo.FiltersExpr{mb.b.factory.ConstructFiltersItem(predScalar)},
		)
	}

	// Build the join condition by creating a conjunction of equality conditions
	// that test each conflict column:
	//
	//   ON ins.x = scan.a AND ins.y = scan.b
	//
	var on memo.FiltersExpr
	for i := range mb.fetchScope.cols {
		// Include fetch columns with ordinal positions in conflictOrds.
		if conflictOrds.Contains(i) {
			condition := mb.b.factory.ConstructEq(
				mb.b.factory.ConstructVariable(mb.insertColIDs[i]),
				mb.b.factory.ConstructVariable(mb.fetchScope.cols[i].id),
			)
			on = append(on, mb.b.factory.ConstructFiltersItem(condition))
		}
	}

	// If the index is a unique partial index, then insert rows that do not
	// satisfy the partial index predicate cannot conflict with existing rows in
	// the unique partial index. Therefore, the partial index predicate
	// expression is added to the ON filters.
	if pred != nil {
		texpr := mb.outScope.resolveAndRequireType(pred, types.Bool)
		predScalar := mb.b.buildScalar(texpr, mb.outScope, nil, nil, nil)
		on = append(on, mb.b.factory.ConstructFiltersItem(predScalar))
	}

	// Add the fetch columns to the current scope. It's OK to modify the current
	// scope because it contains only INSERT columns that were added by the
	// mutationBuilder, and which are no longer needed for any other purpose.
	mb.outScope.appendColumnsFromScope(mb.fetchScope)

	// Construct the left join.
	mb.outScope.expr = mb.b.factory.ConstructLeftJoin(
		mb.outScope.expr,
		mb.fetchScope.expr,
		on,
		memo.EmptyJoinPrivate,
	)
}

// buildDistinctOnForDoNothingArbiter adds an UpsertDistinctOn operator for a
// single arbiter index or constraint.
//
//   - insertColScope contains the columns of the insert rows.
//   - conflictOrds is the set of table column ordinals that the arbiter
//     guarantees uniqueness of.
//   - partialArbiterDistinctCol is a column that allows the UpsertDistinctOn to
//     only de-duplicate insert rows that satisfy the partial index or
//     constraint predicate. If the arbiter is not a partial index or
//     constraint, partialArbiterDistinctCol is nil.
//   - errorOnDup indicates whether multiple rows in the same distinct group
//     should trigger an error. If empty, no error is triggered.
//
func (mb *mutationBuilder) buildDistinctOnForArbiter(
	insertColScope *scope,
	conflictOrds util.FastIntSet,
	partialArbiterDistinctCol *scopeColumn,
	errorOnDup string,
) {
	// Add an UpsertDistinctOn operator to ensure there are no duplicate input
	// rows for this arbiter. Duplicate rows can trigger conflict errors at
	// runtime, which DO NOTHING is not supposed to do. See issue #37880.
	var conflictCols opt.ColSet
	for i, ok := conflictOrds.Next(0); ok; i, ok = conflictOrds.Next(i + 1) {
		conflictCols.Add(mb.insertColIDs[i])
	}
	if partialArbiterDistinctCol != nil {
		conflictCols.Add(partialArbiterDistinctCol.id)
	}

	// Treat NULL values as distinct from one another. And if duplicates are
	// detected, remove them rather than raising an error.
	mb.outScope = mb.b.buildDistinctOn(
		conflictCols, mb.outScope, true /* nullsAreDistinct */, errorOnDup,
	)

	// Remove the partialArbiterDistinctCol from the output.
	if partialArbiterDistinctCol != nil {
		projectionScope := mb.outScope.replace()
		projectionScope.appendColumnsFromScope(insertColScope)
		mb.b.constructProjectForScope(mb.outScope, projectionScope)
		mb.outScope = projectionScope
	}
}

// projectPartialArbiterDistinctColumn projects a column to facilitate
// de-duplicating insert rows for UPSERT/INSERT ON CONFLICT when the arbiter
// index or constraint is partial. Only those insert rows that satisfy the
// partial index or unique constraint predicate should be de-duplicated. For
// example:
//
//   CREATE TABLE t (a INT, b INT, UNIQUE INDEX (a) WHERE b > 0)
//   INSERT INTO t VALUES (1, 1), (1, 2), (1, -1), (1, -10) ON CONFLICT DO NOTHING
//
// The rows (1, 1), (1, -1), and (1, -10) should be inserted. (1, -1)
// and (1, -10) should not be removed from the input set. Even though
// their values for a conflict with the other input rows, their values
// for b are less than 0, so they will not conflict given the unique
// partial index predicate.
//
// In order to avoid de-duplicating all input rows, we project a new
// column to group by. This column is true if the predicate is satisfied
// and NULL otherwise. For the example above, the projected column would
// be (b > 0) OR NULL. The values of the projected rows would be:
//
//   (1, 1)   -> (1, 1, true)
//   (1, 2)   -> (1, 2, true)
//   (1, -1)  -> (1, -1, NULL)
//   (1, -10) -> (1, -10, NULL)
//
// The set of conflict columns to be used for de-duplication includes a and the
// newly projected column. The UpsertDistinctOn considers NULL values as unique,
// so the rows remaining would be (1, 1, true), (1, -1, NULL), and (1, -10,
// NULL).
//
// The newly project scopeColumn is returned.
func (mb *mutationBuilder) projectPartialArbiterDistinctColumn(
	insertScope *scope, pred tree.Expr, arbiterName string,
) *scopeColumn {
	projectionScope := mb.outScope.replace()
	projectionScope.appendColumnsFromScope(insertScope)

	expr := &tree.OrExpr{
		Left:  pred,
		Right: tree.DNull,
	}
	texpr := insertScope.resolveAndRequireType(expr, types.Bool)

	// Use an anonymous name because the column cannot be referenced
	// in other expressions.
	colName := scopeColName("").WithMetadataName(fmt.Sprintf("arbiter_%s_distinct", arbiterName))
	scopeCol := projectionScope.addColumn(colName, texpr)
	mb.b.buildScalar(texpr, mb.outScope, projectionScope, scopeCol, nil)

	mb.b.constructProjectForScope(mb.outScope, projectionScope)
	mb.outScope = projectionScope

	return scopeCol
}

// arbiterPredicateHelper is used to determine if a partial index or constraint
// can be used as an arbiter.
type arbiterPredicateHelper struct {
	mb               *mutationBuilder
	tabMeta          *opt.TableMeta
	im               partialidx.Implicator
	arbiterPredicate tree.Expr

	// tableScopeLazy is a lazily initialized scope including all the columns of
	// the mutation table and a scan on the table as an expression. Do NOT
	// access it directly, use the tableScope method instead.
	tableScopeLazy *scope

	// arbiterFiltersLazy is a lazily initialized scalar expression that is the
	// result of building arbiterPredicate. Do NOT access it directly, use the
	// arbiterFilters method instead.
	arbiterFiltersLazy memo.FiltersExpr

	// invalidArbiterPredicate is true if the arbiterPredicate contains
	// non-immutable operators. Such a predicate cannot imply any filters.
	invalidArbiterPredicate bool
}

// init initializes the helper for the given arbiter predicate.
func (h *arbiterPredicateHelper) init(mb *mutationBuilder, arbiterPredicate tree.Expr) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*h = arbiterPredicateHelper{
		mb:               mb,
		tabMeta:          mb.md.TableMeta(mb.tabID),
		arbiterPredicate: arbiterPredicate,
	}
	h.im.Init(mb.b.factory, mb.md, mb.b.evalCtx)
}

// tableScope returns a scope that can be used to build predicate expressions.
// The scope contains a Scan expression. The logical properties of the Scan are
// used to fully normalize predicate expressions.
func (h *arbiterPredicateHelper) tableScope() *scope {
	if h.tableScopeLazy == nil {
		h.tableScopeLazy = h.mb.b.buildScan(
			h.tabMeta, tableOrdinals(h.tabMeta.Table, columnKinds{
				includeMutations: false,
				includeSystem:    false,
				includeInverted:  false,
			}),
			nil, /* indexFlags */
			noRowLocking,
			h.mb.b.allocScope(),
		)
	}
	return h.tableScopeLazy
}

// partialIndexPredicate returns the partial index predicate of the given index.
// Rather than build the predicate scalar expression from the tree.Expr in the
// catalog, it fetches the predicates from the table metadata. These predicates
// are populated when the Scan expression is built for the tableScope. This
// eliminates unnecessarily rebuilding partial index predicate expressions.
func (h *arbiterPredicateHelper) partialIndexPredicate(idx cat.IndexOrdinal) memo.FiltersExpr {
	// Call tableScope to ensure that buildScan has been called to populate
	// tabMeta with partial index predicates.
	h.tableScope()

	pred, _ := h.tabMeta.PartialIndexPredicate(idx)
	return *pred.(*memo.FiltersExpr)
}

// partialUniqueConstraintPredicate returns the predicate of the given unique
// constraint.
func (h *arbiterPredicateHelper) partialUniqueConstraintPredicate(
	idx cat.UniqueOrdinal,
) memo.FiltersExpr {
	// Build and normalize the unique constraint predicate expression.
	pred, err := h.mb.b.buildPartialIndexPredicate(
		h.tabMeta, h.tableScope(), h.mb.parseUniqueConstraintPredicateExpr(idx), "unique constraint predicate",
	)
	if err != nil {
		panic(err)
	}
	return pred
}

// arbiterFilters returns a scalar expression representing the arbiter
// predicate. If the arbiter predicate contains non-immutable operators,
// ok=false is returned.
func (h *arbiterPredicateHelper) arbiterFilters() (_ memo.FiltersExpr, ok bool) {
	// The filters have been initialized if they are non-nil or
	// invalidArbiterPredicate has been set to true.
	arbiterFiltersInitialized := h.arbiterFiltersLazy != nil || h.invalidArbiterPredicate

	if !arbiterFiltersInitialized {
		filters, err := h.mb.b.buildPartialIndexPredicate(
			h.tabMeta, h.tableScope(), h.arbiterPredicate, "arbiter predicate",
		)
		if err == nil {
			h.arbiterFiltersLazy = filters
		} else {
			// The error is due to a non-immutable operator in the arbiter
			// predicate. Such a predicate cannot imply any filters, so we mark
			// it as invalid.
			h.invalidArbiterPredicate = true
		}
	}
	return h.arbiterFiltersLazy, !h.invalidArbiterPredicate
}

// predicateIsImpliedByArbiterPredicate returns true if the arbiterPredicate
// implies pred. If there is no arbiterPredicate (it is nil), or it contains
// non-immutable operators, it cannot imply any predicate, so false is returned.
func (h *arbiterPredicateHelper) predicateIsImpliedByArbiterPredicate(pred memo.FiltersExpr) bool {
	if h.arbiterPredicate == nil {
		return false
	}

	arbiterFilters, ok := h.arbiterFilters()
	if !ok {
		// The arbiterPredicate contains non-immutable operators and cannot
		// imply any predicate.
		return false
	}

	_, ok = h.im.FiltersImplyPredicate(arbiterFilters, pred)
	return ok
}
