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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// addPartialIndexPredicatesForTable finds all partial indexes in the table and
// adds their predicates to the table metadata (see
// TableMeta.partialIndexPredicates). The predicates are converted from strings
// to ScalarExprs here.
//
// The predicates are used as "known truths" about table data. Any predicates
// containing non-immutable operators are omitted.
//
// scan is an optional argument that is a Scan expression on the table. If scan
// outputs all the ordinary columns in the table, we avoid constructing a new
// scan. A scan and its logical properties are required in order to fully
// normalize the partial index predicates.
func (b *Builder) addPartialIndexPredicatesForTable(tabMeta *opt.TableMeta, scan memo.RelExpr) {
	// We do not want to track view deps here, otherwise a view depending
	// on a table with a partial index predicate using an UDT will result in a
	// type dependency being added between the view and the UDT.
	if b.trackViewDeps {
		b.trackViewDeps = false
		defer func() {
			b.trackViewDeps = true
		}()
	}
	tab := tabMeta.Table
	numIndexes := tab.DeletableIndexCount()

	// Find the first partial index.
	indexOrd := 0
	for ; indexOrd < numIndexes; indexOrd++ {
		if _, ok := tab.Index(indexOrd).Predicate(); ok {
			break
		}
	}

	// Return early if there are no partial indexes. Only partial indexes have
	// predicates.
	if indexOrd == numIndexes {
		return
	}

	// Construct a scan as the tableScope expr so that logical properties of the
	// scan can be used to fully normalize the index predicate.
	tableScope := b.allocScope()
	tableScope.appendOrdinaryColumnsFromTable(tabMeta, &tabMeta.Alias)

	// If the optional scan argument was provided and it outputs all of the
	// ordinary table columns, we use it as tableScope.expr. Otherwise, we must
	// construct a new scan. Attaching a scan to tableScope.expr is required to
	// fully normalize the partial index predicates with logical properties of
	// the scan.
	if scan != nil && tableScope.colSet().SubsetOf(scan.Relational().OutputCols) {
		tableScope.expr = scan
	} else {
		// TODO(mgartner): This is a sketchy because computed columns won't
		// exist in the scan table's metadata. Currently, the scan argument is
		// nil when building partial index predicates for UPDATEs and UPSERTs
		// table metadata (which are built in order to prune fetch columns). As
		// a result, virtual columns referenced in partial index predicates
		// might not be pruned in all cases that they can be pruned in theory.
		tableScope.expr = b.factory.ConstructScan(&memo.ScanPrivate{
			Table: tabMeta.MetaID,
			Cols:  tableScope.colSet(),
		})
	}

	// Skip to the first partial index we found above.
	for ; indexOrd < numIndexes; indexOrd++ {
		index := tab.Index(indexOrd)
		pred, ok := index.Predicate()

		// If the index is not a partial index, do nothing.
		if !ok {
			continue
		}

		expr, err := parser.ParseExpr(pred)
		if err != nil {
			panic(err)
		}

		// Build the partial index predicate as a memo.FiltersExpr and add it
		// to the table metadata.
		predExpr, err := b.buildPartialIndexPredicate(tabMeta, tableScope, expr, "index predicate")
		if err != nil {
			panic(err)
		}
		tabMeta.AddPartialIndexPredicate(indexOrd, &predExpr)
	}
}

// buildPartialIndexPredicate builds a memo.FiltersExpr from the given
// tree.Expr. Virtual computed columns are inlined as their expressions in the
// resulting filter. Returns an error if any non-immutable operators are found.
//
// Note: This function should only be used to build partial index or arbiter
// predicate expressions that have only a table's ordinary columns in scope and
// that are not part of the relational expression tree. For example, this is
// used to populate the partial index predicates map in TableMeta and for
// determining arbiter indexes in UPSERT and INSERT ON CONFLICT mutations. But
// it is not used for building synthesized mutation columns that determine
// whether to issue PUT or DEL operations on a partial index for a mutated row;
// these synthesized columns are projected as part of the opt expression tree
// and they can reference columns not part of a table's ordinary columns.
func (b *Builder) buildPartialIndexPredicate(
	tabMeta *opt.TableMeta, tableScope *scope, expr tree.Expr, context string,
) (memo.FiltersExpr, error) {
	texpr := resolvePartialIndexPredicate(tableScope, expr)

	var scalar opt.ScalarExpr
	b.factory.FoldingControl().TemporarilyDisallowStableFolds(func() {
		scalar = b.buildScalar(texpr, tableScope, nil, nil, nil)
	})

	// Inline virtual computed column expressions. This is required for
	// partial index predicate implication with virtual columns. A virtual
	// computed column is built as a Project on top of a Scan. The
	// PushSelectIntoInlinableProject normalization rule will push a filter
	// on a virtual computed column below the Project by inlining the
	// virtual column expression. The pushed-down filter will only imply a
	// partial index predicate if the virtual column expression is also
	// inlined in the predicate.
	//
	// Stored computed column expressions do not need to be inlined because
	// they are produced directly from a Scan, not a Project.
	var replace norm.ReplaceFunc
	replace = func(e opt.Expr) opt.Expr {
		switch t := e.(type) {
		case *memo.VariableExpr:
			ord := tabMeta.MetaID.ColumnOrdinal(t.Col)
			col := tabMeta.Table.Column(ord)
			if col.IsVirtualComputed() {
				if expr, ok := tabMeta.ComputedCols[t.Col]; ok {
					return expr
				}
			}
		}
		return b.factory.Replace(e, replace)
	}
	scalar = replace(scalar).(opt.ScalarExpr)

	// Wrap the scalar in a FiltersItem.
	filter := b.factory.ConstructFiltersItem(scalar)

	// Expressions with non-immutable operators are not supported as partial
	// index or arbiter predicates.
	if filter.ScalarProps().VolatilitySet.HasStable() || filter.ScalarProps().VolatilitySet.HasVolatile() {
		return nil, tree.NewContextDependentOpsNotAllowedError(context)
	}

	// Wrap the expression in a FiltersExpr and normalize it by constructing a
	// Select expression with a FakeRel as input. The FakeRel has the same
	// logical properties as the tableScope's expression to aid in
	// normalization.
	filters := memo.FiltersExpr{filter}
	selExpr := b.factory.ConstructSelect(
		b.factory.ConstructFakeRel(
			&memo.FakeRelPrivate{Props: tableScope.expr.Relational()},
		),
		filters,
	)

	switch t := selExpr.(type) {
	case *memo.SelectExpr:
		// If the expression remains a Select, return the normalized filters.
		return t.Filters, nil
	case *memo.FakeRelExpr:
		// If the expression has been normalized to a FakeRelExpr, then the
		// filters were normalized to true and the Select was eliminated.
		// So, return a true filter.
		return memo.TrueFilter, nil
	case *memo.ValuesExpr:
		// If the expression has been normalized to a Values expression, then
		// the filters were normalized to false and the Select and FakeRel were
		// eliminated. So, return a false filter.
		if !t.Relational().Cardinality.IsZero() {
			panic(errors.AssertionFailedf("values expression should have a cardinality of zero"))
		}
		return memo.FiltersExpr{b.factory.ConstructFiltersItem(memo.FalseSingleton)}, nil
	default:
		// Otherwise, normalization resulted in an unexpected expression type.
		// Panic rather than return an incorrect predicate.
		panic(errors.AssertionFailedf("unexpected expression during partial index normalization: %T", t))
	}
}

// resolvePartialIndexPredicate attempts to resolve the type of expr as a
// boolean and return a tree.TypedExpr if successful. It asserts that no errors
// occur during resolution because the predicate should always be valid within
// this context. If an error occurs, it is likely due to a bug in the optimizer.
func resolvePartialIndexPredicate(tableScope *scope, expr tree.Expr) tree.TypedExpr {
	defer func() {
		if r := recover(); r != nil {
			panic(errors.AssertionFailedf("unexpected error during partial index predicate type resolution: %v", r))
		}
	}()
	return tableScope.resolveAndRequireType(expr, types.Bool)
}
