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

// arbiterIndexesAndConstraints returns sets of index ordinals and unique
// constraint ordinals to be used as arbiter constraints for an INSERT ON
// CONFLICT statement. This function panics if no arbiter indexes or constraints
// are found.
//
// Arbiter constraints ensure that the columns designated by conflictOrds
// reference at most one target row of a UNIQUE index or constraint. Using ANTI
// JOINs and LEFT OUTER JOINs to detect conflicts relies upon this being true
// (otherwise result cardinality could increase). This is also a Postgres
// requirement.
//
// An arbiter index:
//
//   1. Must have lax key columns that match the columns in conflictOrds.
//   2. If it is a partial index, its predicate must be implied by the
//      arbiterPredicate supplied by the user.
//
// An arbiter constraint must have columns that match the columns in
// conflictOrds. Unique constraints without an index cannot be "partial",
// so they have no predicate to check for implication with arbiterPredicate.
// TODO(rytaft): revisit this for the case of implicitly partitioned partial
//  unique indexes (see #59195).
//
// If conflictOrds is empty then all unique indexes and unique without index
// constraints are returned as arbiters. This is required to support a
// DO NOTHING with no ON CONFLICT columns. In this case, all unique indexes
// and constraints are used to check for conflicts.
//
// If a non-partial or pseudo-partial arbiter index is found, the return set
// contains only that index. No other arbiter is necessary because a non-partial
// or pseudo-partial index guarantees uniqueness of its columns across all
// rows.
func (mb *mutationBuilder) arbiterIndexesAndConstraints(
	conflictOrds util.FastIntSet, arbiterPredicate tree.Expr,
) (indexes util.FastIntSet, uniqueConstraints util.FastIntSet) {
	// If conflictOrds is empty, then all unique indexes and unique without index
	// constraints are arbiters.
	if conflictOrds.Empty() {
		for idx, idxCount := 0, mb.tab.IndexCount(); idx < idxCount; idx++ {
			if mb.tab.Index(idx).IsUnique() {
				indexes.Add(idx)
			}
		}
		for uc, ucCount := 0, mb.tab.UniqueCount(); uc < ucCount; uc++ {
			if mb.tab.Unique(uc).WithoutIndex() {
				uniqueConstraints.Add(uc)
			}
		}
		return indexes, uniqueConstraints
	}

	tabMeta := mb.md.TableMeta(mb.tabID)
	var tableScope *scope
	var im *partialidx.Implicator
	var arbiterFilters memo.FiltersExpr
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

		_, isPartial := index.Predicate()

		// If the index is not a partial index, it can always be an arbiter.
		// Furthermore, it is the only arbiter needed because it guarantees
		// uniqueness of its columns across all rows.
		if !isPartial {
			return util.MakeFastIntSet(idx), util.FastIntSet{}
		}

		// Initialize tableScope once and only if needed. We need to build a scan
		// so we can use the logical properties of the scan to fully normalize the
		// index predicates.
		if tableScope == nil {
			tableScope = mb.b.buildScan(
				tabMeta, tableOrdinals(tabMeta.Table, columnKinds{
					includeMutations:       false,
					includeSystem:          false,
					includeVirtualInverted: false,
					includeVirtualComputed: true,
				}),
				nil, /* indexFlags */
				noRowLocking,
				mb.b.allocScope(),
			)
		}

		// Fetch the partial index predicate which was added to tabMeta in the
		// call to buildScan above.
		p, _ := tabMeta.PartialIndexPredicate(idx)
		predFilter := *p.(*memo.FiltersExpr)

		// If the index is a pseudo-partial index, it can always be an arbiter.
		// Furthermore, it is the only arbiter needed because it guarantees
		// uniqueness of its columns across all rows.
		if predFilter.IsTrue() {
			return util.MakeFastIntSet(idx), util.FastIntSet{}
		}

		// If the index is a partial index, then it can only be an arbiter if
		// the arbiterPredicate implies it.
		if arbiterPredicate != nil {

			// Initialize the Implicator once.
			if im == nil {
				im = &partialidx.Implicator{}
				im.Init(mb.b.factory, mb.md, mb.b.evalCtx)
			}

			// Build the arbiter filters once.
			if arbiterFilters == nil {
				var err error
				arbiterFilters, err = mb.b.buildPartialIndexPredicate(
					tabMeta, tableScope, arbiterPredicate, "arbiter predicate",
				)
				if err != nil {
					// The error is due to a non-immutable operator in the arbiter
					// predicate. Continue on to see if a matching non-partial or
					// pseudo-partial index exists.
					continue
				}
			}

			if _, ok := im.FiltersImplyPredicate(arbiterFilters, predFilter); ok {
				indexes.Add(idx)
			}
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

		if uniqueConstraint.ColumnCount() != conflictOrds.Len() {
			continue
		}

		// Determine whether the conflict columns match the columns in the unique
		// constraint. If not, the constraint cannot be an arbiter.
		ucOrds := getUniqueConstraintOrdinals(mb.tab, uniqueConstraint)
		if ucOrds.Equals(conflictOrds) {
			return util.FastIntSet{}, util.MakeFastIntSet(uc)
		}
	}

	// There are no full indexes or constraints, so return any partial indexes
	// that were found.
	if !indexes.Empty() {
		return indexes, util.FastIntSet{}
	}

	panic(pgerror.Newf(pgcode.InvalidColumnReference,
		"there is no unique or exclusion constraint matching the ON CONFLICT specification"))
}

// buildAntiJoinForDoNothingArbiter builds an anti-join for a single arbiter index
// or constraint for an INSERT ON CONFLICT DO NOTHING mutation. The anti-join
// wraps the current mb.outScope.expr and removes rows that would conflict with
// existing rows.
//
// 	 - columnOrds is the set of table column ordinals that the arbiter
//     guarantees uniqueness of.
// 	 - predExpr is the partial index predicate. If the arbiter is not a partial
//     index, predExpr is nil.
//
func (mb *mutationBuilder) buildAntiJoinForDoNothingArbiter(
	inScope *scope, columnOrds util.FastIntSet, predExpr tree.Expr,
) {
	// Build the right side of the anti-join. Use a new metadata instance
	// of the mutation table so that a different set of column IDs are used for
	// the two tables in the self-join.
	fetchScope := mb.b.buildScan(
		mb.b.addTable(mb.tab, &mb.alias),
		tableOrdinals(mb.tab, columnKinds{
			includeMutations:       false,
			includeSystem:          false,
			includeVirtualInverted: false,
			includeVirtualComputed: true,
		}),
		nil, /* indexFlags */
		noRowLocking,
		inScope,
	)

	// If the index is a unique partial index, then rows that are not in the
	// partial index cannot conflict with insert rows. Therefore, a Select
	// wraps the scan on the right side of the anti-join with the partial
	// index predicate expression as the filter.
	if predExpr != nil {
		texpr := fetchScope.resolveAndRequireType(predExpr, types.Bool)
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
	for i, ok := columnOrds.Next(0); ok; i, ok = columnOrds.Next(i + 1) {
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
	if predExpr != nil {
		texpr := mb.outScope.resolveAndRequireType(predExpr, types.Bool)
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

// buildDistinctOnForDoNothingArbiter adds an UpsertDistinctOn operator for a
// single arbiter index or constraint.
//
// 	 - columnOrds is the set of table column ordinals that the arbiter
//     guarantees uniqueness of.
//   - partialIndexDistinctCol is a column that allows the UpsertDistinctOn to
//     only de-duplicate insert rows that satisfy the partial index predicate.
//     If the arbiter is not a partial index, partialIndexDistinctCol is nil.
//
func (mb *mutationBuilder) buildDistinctOnForDoNothingArbiter(
	insertColScope *scope, colOrds util.FastIntSet, partialIndexDistinctCol *scopeColumn,
) {
	// Add an UpsertDistinctOn operator to ensure there are no duplicate input
	// rows for this arbiter. Duplicate rows can trigger conflict errors at
	// runtime, which DO NOTHING is not supposed to do. See issue #37880.
	var conflictCols opt.ColSet
	for i, ok := colOrds.Next(0); ok; i, ok = colOrds.Next(i + 1) {
		conflictCols.Add(mb.insertColIDs[i])
	}
	if partialIndexDistinctCol != nil {
		conflictCols.Add(partialIndexDistinctCol.id)
	}

	// Treat NULL values as distinct from one another. And if duplicates are
	// detected, remove them rather than raising an error.
	mb.outScope = mb.b.buildDistinctOn(
		conflictCols, mb.outScope, true /* nullsAreDistinct */, "" /* errorOnDup */)

	// Remove the partialIndexDistinctCol from the output.
	if partialIndexDistinctCol != nil {
		projectionScope := mb.outScope.replace()
		projectionScope.appendColumnsFromScope(insertColScope)
		mb.b.constructProjectForScope(mb.outScope, projectionScope)
		mb.outScope = projectionScope
	}
}

// projectPartialIndexDistinctColumn projects a column to facilitate
// de-duplicating insert rows for UPSERT/INSERT ON CONFLICT when the arbiter
// index is a partial index. Only those insert rows that satisfy the partial
// index predicate should be de-duplicated. For example:
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
func (mb *mutationBuilder) projectPartialIndexDistinctColumn(
	insertScope *scope, idx cat.IndexOrdinal,
) *scopeColumn {
	projectionScope := mb.outScope.replace()
	projectionScope.appendColumnsFromScope(insertScope)

	predExpr := mb.parsePartialIndexPredicateExpr(idx)
	expr := &tree.OrExpr{
		Left:  predExpr,
		Right: tree.DNull,
	}
	texpr := insertScope.resolveAndRequireType(expr, types.Bool)

	alias := fmt.Sprintf("upsert_partial_index_distinct%d", idx)
	scopeCol := projectionScope.addColumn(alias, texpr)
	mb.b.buildScalar(texpr, mb.outScope, projectionScope, scopeCol, nil)

	mb.b.constructProjectForScope(mb.outScope, projectionScope)
	mb.outScope = projectionScope

	return scopeCol
}
