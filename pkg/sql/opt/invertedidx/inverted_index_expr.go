// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package invertedidx

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/trigram"
	"github.com/cockroachdb/errors"
)

// NewDatumsToInvertedExpr returns a new DatumsToInvertedExpr.
func NewDatumsToInvertedExpr(
	ctx context.Context,
	evalCtx *eval.Context,
	colTypes []*types.T,
	expr tree.TypedExpr,
	geoConfig geopb.Config,
) (invertedexpr.DatumsToInvertedExpr, error) {
	if !geoConfig.IsEmpty() {
		return NewGeoDatumsToInvertedExpr(ctx, evalCtx, colTypes, expr, geoConfig)
	}

	return NewJSONOrArrayDatumsToInvertedExpr(ctx, evalCtx, colTypes, expr)
}

// NewBoundPreFilterer returns a PreFilterer for the given expr where the type
// of the bound param is specified by typ. Unlike the use of PreFilterer in an
// inverted join, where each left value is bound, this function is for the
// invertedFilterer where the param to be bound is already specified as a
// constant in the expr. The callee will bind this parameter and return the
// opaque pre-filtering state for that binding (the interface{}) in the return
// values).
func NewBoundPreFilterer(typ *types.T, expr tree.TypedExpr) (*PreFilterer, interface{}, error) {
	if !typ.Equivalent(types.Geometry) && !typ.Equivalent(types.Geography) {
		return nil, nil, fmt.Errorf("pre-filtering not supported for type %s", typ)
	}
	return newGeoBoundPreFilterer(typ, expr)
}

// TryFilterInvertedIndex tries to derive an inverted filter condition for
// the given inverted index from the specified filters. If an inverted filter
// condition is derived, it is returned with ok=true. If no condition can be
// derived, then TryFilterInvertedIndex returns ok=false.
//
// In addition to the inverted filter condition (spanExpr), returns:
//   - a constraint of the prefix columns if there are any,
//   - remaining filters that must be applied if the span expression is not tight,
//     and
//   - pre-filterer state that can be used by the invertedFilterer operator to
//     reduce the number of false positives returned by the span expression.
func TryFilterInvertedIndex(
	ctx context.Context,
	evalCtx *eval.Context,
	factory *norm.Factory,
	filters memo.FiltersExpr,
	optionalFilters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
	computedColumns map[opt.ColumnID]opt.ScalarExpr,
	checkCancellation func(),
) (
	spanExpr *inverted.SpanExpression,
	constraint *constraint.Constraint,
	remainingFilters memo.FiltersExpr,
	preFiltererState *invertedexpr.PreFiltererStateForInvertedFilterer,
	ok bool,
) {
	// Attempt to constrain the prefix columns, if there are any. If they cannot
	// be constrained to single values, the index cannot be used.
	columns, notNullCols := prefixCols(tabID, index)
	if len(columns) > 0 {
		constraint, filters, ok = constrainNonInvertedCols(
			ctx, evalCtx, factory, columns, notNullCols, filters,
			optionalFilters, tabID, index, checkCancellation,
		)
		if !ok {
			return nil, nil, nil, nil, false
		}
	}

	config := index.GeoConfig()
	var typ *types.T
	var filterPlanner invertedFilterPlanner
	if config.IsGeography() {
		filterPlanner = &geoFilterPlanner{
			factory:     factory,
			tabID:       tabID,
			index:       index,
			getSpanExpr: getSpanExprForGeographyIndex,
		}
		typ = types.Geography
	} else if config.IsGeometry() {
		filterPlanner = &geoFilterPlanner{
			factory:     factory,
			tabID:       tabID,
			index:       index,
			getSpanExpr: getSpanExprForGeometryIndex,
		}
		typ = types.Geometry
	} else {
		col := index.InvertedColumn().InvertedSourceColumnOrdinal()
		typ = factory.Metadata().Table(tabID).Column(col).DatumType()
		switch typ.Family() {
		case types.StringFamily:
			filterPlanner = &trigramFilterPlanner{
				tabID:           tabID,
				index:           index,
				computedColumns: computedColumns,
			}
		case types.TSVectorFamily:
			filterPlanner = &tsqueryFilterPlanner{
				tabID:           tabID,
				index:           index,
				computedColumns: computedColumns,
			}
		case types.JsonFamily, types.ArrayFamily:
			filterPlanner = &jsonOrArrayFilterPlanner{
				tabID:           tabID,
				index:           index,
				computedColumns: computedColumns,
			}
		default:
			return nil, nil, nil, nil, false
		}
	}

	var invertedExpr inverted.Expression
	var pfState *invertedexpr.PreFiltererStateForInvertedFilterer
	for i := range filters {
		invertedExprLocal, remFiltersLocal, pfStateLocal := extractInvertedFilterCondition(
			ctx, evalCtx, factory, filters[i].Condition, filterPlanner,
		)
		if invertedExpr == nil {
			invertedExpr = invertedExprLocal
			pfState = pfStateLocal
		} else {
			invertedExpr = inverted.And(invertedExpr, invertedExprLocal)
			// Do pre-filtering using the first of the conjuncts that provided
			// non-nil pre-filtering state.
			if pfState == nil {
				pfState = pfStateLocal
			}
		}
		if remFiltersLocal != nil {
			remainingFilters = append(remainingFilters, factory.ConstructFiltersItem(remFiltersLocal))
		}
	}

	if invertedExpr == nil {
		return nil, nil, nil, nil, false
	}

	spanExpr, ok = invertedExpr.(*inverted.SpanExpression)
	if !ok {
		return nil, nil, nil, nil, false
	}
	if pfState != nil {
		pfState.Typ = typ
	}

	return spanExpr, constraint, remainingFilters, pfState, true
}

// TryFilterInvertedIndexBySimilarity attempts to constrain an inverted trigram
// index using a similarity filter. It returns the constraint and the set of
// remaining filters which are not "tight" in the constraint. If no constraint
// can be generated, then ok=false is returned.
//
// The returned constraint includes spans over the minimum number of trigrams in
// a and b that must match in order to satisfy the similarity filter. This
// optimization allows us to avoid scanning over some trigrams of the constant
// string. See similarityTrigramsToScan for more details.
func TryFilterInvertedIndexBySimilarity(
	ctx context.Context,
	evalCtx *eval.Context,
	f *norm.Factory,
	filters memo.FiltersExpr,
	optionalFilters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
	computedColumns map[opt.ColumnID]opt.ScalarExpr,
	checkCancellation func(),
) (_ *constraint.Constraint, remainingFilters memo.FiltersExpr, ok bool) {
	md := f.Metadata()
	columnCount := index.ExplicitColumnCount()
	prefixColumnCount := index.PrefixColumnCount()

	// The indexed column must be of a string-like type.
	srcColOrd := index.InvertedColumn().InvertedSourceColumnOrdinal()
	if md.Table(tabID).Column(srcColOrd).DatumType().Family() != types.StringFamily {
		return nil, nil, false
	}

	cols := make([]opt.OrderingColumn, columnCount)
	var notNullCols opt.ColSet
	for i := range cols {
		col := index.Column(i)
		colID := tabID.ColumnID(col.Ordinal())
		cols[i] = opt.MakeOrderingColumn(colID, col.Descending)
		if !col.IsNullable() {
			notNullCols.Add(colID)
		}
	}

	// First, we attempt to build a constraint from a similarity filter on the
	// inverted column. We search for expressions of the form `s % 'foo'` or
	// `'foo' % s`, where s is the indexed column.
	//
	// TODO(mgartner): Currently we only look for the first similarity filter.
	// We could improve query plans in some cases by looking for multiple
	// similarity filters and picking the one that requires the fewest trigrams
	// to be scanned, or by building a constrained scan for each similarity
	// filter and letting the optimizer determine the lowest cost trigrams to
	// scan.
	var con *constraint.Constraint
	for i := range filters {
		sim, isSim := filters[i].Condition.(*memo.ModExpr)
		if !isSim {
			continue
		}

		var constStr opt.ScalarExpr
		switch {
		case isIndexColumn(tabID, index, sim.Left, computedColumns):
			constStr = sim.Right
		case isIndexColumn(tabID, index, sim.Right, computedColumns):
			constStr = sim.Left
		default:
			continue
		}

		s, isConstStr := extractConstStringDatum(constStr)
		if !isConstStr {
			continue
		}

		// Generate trigrams to scan.
		trgms := similarityTrigramsToScan(s, evalCtx.SessionData().TrigramSimilarityThreshold)
		if len(trgms) == 0 {
			continue
		}

		keyCtx := constraint.KeyContext{Ctx: ctx, EvalCtx: evalCtx}
		keyCtx.Columns.Init(cols[prefixColumnCount:])

		var spans constraint.Spans
		spans.Alloc(len(trgms))
		for j := range trgms {
			// Create a key for the trigram. The trigram is encoded so that it
			// can be correctly compared to histogram upper bounds, which are
			// also encoded. The byte slice is pre-sized to hold the trigram
			// plus three extra bytes for the prefix, escape, and terminator.
			k := make([]byte, 0, len(trgms[j])+3)
			k = encoding.EncodeStringAscending(k, trgms[j])
			key := constraint.MakeKey(tree.NewDEncodedKey(tree.DEncodedKey(k)))

			var span constraint.Span
			span.Init(key, constraint.IncludeBoundary, key, constraint.IncludeBoundary)
			spans.Append(&span)
		}

		con = &constraint.Constraint{}
		con.Init(&keyCtx, &spans)
		break
	}

	if con == nil {
		return nil, nil, false
	}

	// If the index is a single-column index, then we are done.
	if columnCount == 1 {
		return con, filters, true
	}

	// If the index is a multi-column index, then we need to constrain the
	// prefix columns.
	var prefixConstraint *constraint.Constraint
	prefixConstraint, remainingFilters, ok = constrainNonInvertedCols(
		ctx, evalCtx, f, cols, notNullCols, filters,
		optionalFilters, tabID, index, checkCancellation,
	)
	if !ok {
		return nil, nil, false
	}
	prefixConstraint.Combine(ctx, evalCtx, con, checkCancellation)
	return prefixConstraint, remainingFilters, true
}

func extractConstStringDatum(expr opt.ScalarExpr) (string, bool) {
	if !memo.CanExtractConstDatum(expr) {
		return "", false
	}
	d := tree.UnwrapDOidWrapper(memo.ExtractConstDatum(expr))
	if ds, ok := d.(*tree.DString); ok {
		return string(*ds), ok
	}
	return "", false
}

// similarityTrigramsToScan returns a minimum set of trigrams that must be
// scanned in an inverted index to find all rows where `a % s` is true, where
// `a` is the indexed column. The returned trigrams are sorted.
//
// A similarity filter `a % b` returns true if the ratio between the
// cardinalities of the intersection and the union of trigrams of a and b is
// greater than or equal to pg_trgm.similarity_threshold. Expressed as a formula
// where T(a) and T(b) are the sets of trigrams of a and b, respectively:
//
// |T(a) ∩ T(b)|
// -------------- >= pg_trgm.similarity_threshold
// |T(a) ∪ T(b)|
//
// Observe that the denominator on the LHS is greater than or equal |T(b)|.
// Therefore, the numerator, or the number of matching trigrams of a and b, must
// be at least ⌈pg_trgm.similarity_threshold * |T(b)|⌉ for the expression to be
// true.
//
// This realization allows us to reduce the number of trigrams scanned while
// still guaranteeing that we scan at least one trigram for each row where the
// similarity filter is true. The minimum number of trigrams to scan is:
//
// |T(b)| - (⌈pg_trgm.similarity_threshold * |T(b)|⌉ - 1)
//
// As a concrete example, consider the filter `a % 'xyz'` and
// pg_trgm.similarity_threshold set to its default value of 0.3. The four
// trigrams of "xyz" are "  x", " xy", "xyz", and "yz ". The minimum number of
// matching trigrams of a and "xyz" required to satisfy the filter is 2=⌈0.3*4⌉.
// If we scan 3=4-(2-1) trigrams of "xyz", then we are guaranteed to find
// all rows that have at least 2 matching trigrams.
//
// Any of the trigrams can be discarded, as long as we include at least this
// minimum number to scan. We prefer to discard trigrams with spaces because
// they should always be more common than trigrams without spaces, e.g., all
// words that start with "a" share the trigram "  a".
func similarityTrigramsToScan(s string, similarityThreshold float64) []string {
	if similarityThreshold == 0 {
		// If the similarity threshold is 0, then all strings are similar, so
		// all trigrams would need to be scanned. Return nil to avoid planning a
		// constrained scan over the inverted index, since a full-table scan
		// would be preferable.
		return nil
	}
	if similarityThreshold < 0 || similarityThreshold > 1 {
		panic(errors.AssertionFailedf(
			"similarity threshold %f must be in the range [0, 1]", similarityThreshold,
		))
	}

	trgms := trigram.MakeTrigrams(s, true /* pad */)
	if len(trgms) == 0 {
		// If there are no trigrams then the inverted index cannot be
		// constrained, so return nil.
		return nil
	}

	// Determine the minimum number of trigrams of s that need to match the
	// trigrams of an arbitrary string in order to satisfy the similarity
	// threshold.
	minMatchingTrigrams := int(math.Ceil(similarityThreshold * float64(len(trgms))))
	if minMatchingTrigrams < 1 {
		// Ensure that minMatchingTrigrams is at least one.
		minMatchingTrigrams = 1
	}
	if minMatchingTrigrams > len(trgms) {
		// Ensure that minMatchingTrigrams is no more than the original number
		// of trigrams.
		minMatchingTrigrams = len(trgms)
	}

	// The minimum number of trigrams to scan is:
	//
	//   len(trgms) - (minMatchingTrigrams - 1)
	//
	// So we can remove:
	//
	//   len(trgms) - [len(trgms) - (minMatchingTrigrams - 1)]
	//   => minMatchingTrigrams - 1
	//
	toRemove := minMatchingTrigrams - 1
	switch toRemove {
	case 0, 1, 2:
		// Remove up to the first two trigrams which should always have leading
		// spaces.
		return trgms[toRemove:]

	default:
		// Remove the first two trigrams which should always have leading
		// spaces.
		trgms = trgms[2:]
		toRemove -= 2

		// Remove other trigrams containing spaces.
		for i := 0; i < len(trgms) && toRemove > 0; {
			if strings.ContainsRune(trgms[i], ' ') {
				trgms[i] = trgms[len(trgms)-1]
				trgms = trgms[:len(trgms)-1]
				toRemove--
				continue
			}
			i++
		}

		// If there are still trigrams to remove, remove trigrams at the end of
		// the slice.
		if toRemove > 0 {
			trgms = trgms[:len(trgms)-toRemove]
		}

		// Sort the trigrams because they may have been re-ordered when trigrams
		// with spaces were removed.
		sort.Strings(trgms)

		return trgms
	}
}

// TryJoinInvertedIndex tries to create an inverted join with the given input
// and inverted index from the specified filters. If a join is created, the
// inverted join condition is returned. If no join can be created, then
// TryJoinInvertedIndex returns nil.
func TryJoinInvertedIndex(
	ctx context.Context,
	factory *norm.Factory,
	filters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
	inputCols opt.ColSet,
) opt.ScalarExpr {
	if index.Type() != idxtype.INVERTED {
		return nil
	}

	config := index.GeoConfig()
	var joinPlanner invertedJoinPlanner
	if config.IsGeography() {
		joinPlanner = &geoJoinPlanner{
			factory:     factory,
			tabID:       tabID,
			index:       index,
			inputCols:   inputCols,
			getSpanExpr: getSpanExprForGeographyIndex,
		}
	} else if config.IsGeometry() {
		joinPlanner = &geoJoinPlanner{
			factory:     factory,
			tabID:       tabID,
			index:       index,
			inputCols:   inputCols,
			getSpanExpr: getSpanExprForGeometryIndex,
		}
	} else {
		joinPlanner = &jsonOrArrayJoinPlanner{
			factory:   factory,
			tabID:     tabID,
			index:     index,
			inputCols: inputCols,
		}
	}

	var invertedExpr opt.ScalarExpr
	for i := range filters {
		invertedExprLocal := extractInvertedJoinCondition(
			ctx, factory, filters[i].Condition, joinPlanner,
		)
		if invertedExprLocal == nil {
			continue
		}
		if invertedExpr == nil {
			invertedExpr = invertedExprLocal
		} else {
			invertedExpr = factory.ConstructAnd(invertedExpr, invertedExprLocal)
		}
	}

	if invertedExpr == nil {
		return nil
	}

	// The resulting expression must contain at least one column from the input.
	var p props.Shared
	memo.BuildSharedProps(invertedExpr, &p, factory.EvalContext())
	if !p.OuterCols.Intersects(inputCols) {
		return nil
	}

	return invertedExpr
}

type invertedJoinPlanner interface {
	// extractInvertedJoinConditionFromLeaf extracts a join condition from the
	// given expression, which represents a leaf of an expression tree in which
	// the internal nodes are And and/or Or expressions. Returns nil if no join
	// condition could be extracted.
	extractInvertedJoinConditionFromLeaf(ctx context.Context, expr opt.ScalarExpr) opt.ScalarExpr
}

// extractInvertedJoinCondition extracts a scalar expression from the given
// filter condition, where the scalar expression represents a join condition
// between the input columns and inverted index. Returns nil if no join
// condition could be extracted.
//
// The filter condition should be an expression tree of And, Or, and leaf
// expressions. Extraction of the join condition from the leaves is delegated
// to the given invertedJoinPlanner.
func extractInvertedJoinCondition(
	ctx context.Context,
	factory *norm.Factory,
	filterCond opt.ScalarExpr,
	joinPlanner invertedJoinPlanner,
) opt.ScalarExpr {
	switch t := filterCond.(type) {
	case *memo.AndExpr:
		leftExpr := extractInvertedJoinCondition(ctx, factory, t.Left, joinPlanner)
		rightExpr := extractInvertedJoinCondition(ctx, factory, t.Right, joinPlanner)
		if leftExpr == nil {
			return rightExpr
		}
		if rightExpr == nil {
			return leftExpr
		}
		return factory.ConstructAnd(leftExpr, rightExpr)

	case *memo.OrExpr:
		leftExpr := extractInvertedJoinCondition(ctx, factory, t.Left, joinPlanner)
		rightExpr := extractInvertedJoinCondition(ctx, factory, t.Right, joinPlanner)
		if leftExpr == nil || rightExpr == nil {
			return nil
		}
		return factory.ConstructOr(leftExpr, rightExpr)

	default:
		return joinPlanner.extractInvertedJoinConditionFromLeaf(ctx, filterCond)
	}
}

// getInvertedExpr takes a TypedExpr tree consisting of And, Or and leaf
// expressions, and constructs a new TypedExpr tree in which the leaves are
// replaced by the given getInvertedExprLeaf function.
func getInvertedExpr(
	expr tree.TypedExpr, getInvertedExprLeaf func(expr tree.TypedExpr) (tree.TypedExpr, error),
) (tree.TypedExpr, error) {
	switch t := expr.(type) {
	case *tree.AndExpr:
		leftExpr, err := getInvertedExpr(t.TypedLeft(), getInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		rightExpr, err := getInvertedExpr(t.TypedRight(), getInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		return tree.NewTypedAndExpr(leftExpr, rightExpr), nil

	case *tree.OrExpr:
		leftExpr, err := getInvertedExpr(t.TypedLeft(), getInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		rightExpr, err := getInvertedExpr(t.TypedRight(), getInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		return tree.NewTypedOrExpr(leftExpr, rightExpr), nil

	default:
		return getInvertedExprLeaf(expr)
	}
}

// evalInvertedExpr evaluates a TypedExpr tree consisting of And, Or and leaf
// expressions, and returns the resulting inverted.Expression. Delegates
// evaluation of leaf expressions to the given evalInvertedExprLeaf function.
func evalInvertedExpr(
	expr tree.TypedExpr, evalInvertedExprLeaf func(expr tree.TypedExpr) (inverted.Expression, error),
) (inverted.Expression, error) {
	switch t := expr.(type) {
	case *tree.AndExpr:
		leftExpr, err := evalInvertedExpr(t.TypedLeft(), evalInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		rightExpr, err := evalInvertedExpr(t.TypedRight(), evalInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		if leftExpr == nil || rightExpr == nil {
			return nil, nil
		}
		return inverted.And(leftExpr, rightExpr), nil

	case *tree.OrExpr:
		leftExpr, err := evalInvertedExpr(t.TypedLeft(), evalInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		rightExpr, err := evalInvertedExpr(t.TypedRight(), evalInvertedExprLeaf)
		if err != nil {
			return nil, err
		}
		if leftExpr == nil {
			return rightExpr, nil
		}
		if rightExpr == nil {
			return leftExpr, nil
		}
		return inverted.Or(leftExpr, rightExpr), nil

	default:
		return evalInvertedExprLeaf(expr)
	}
}

// prefixCols returns a slice of ordering columns for each of the non-inverted
// prefix of the index. It also returns a set of those columns that are NOT
// NULL. If the index is a single-column inverted index, the function returns
// nil ordering columns.
func prefixCols(
	tabID opt.TableID, index cat.Index,
) (_ []opt.OrderingColumn, notNullCols opt.ColSet) {
	prefixColumnCount := index.PrefixColumnCount()

	// If this is a single-column inverted index, there are no prefix columns.
	// constrain.
	if prefixColumnCount == 0 {
		return nil, opt.ColSet{}
	}

	prefixColumns := make([]opt.OrderingColumn, prefixColumnCount)
	for i := range prefixColumns {
		col := index.Column(i)
		colID := tabID.ColumnID(col.Ordinal())
		prefixColumns[i] = opt.MakeOrderingColumn(colID, col.Descending)
		if !col.IsNullable() {
			notNullCols.Add(colID)
		}
	}
	return prefixColumns, notNullCols
}

// constrainNonInvertedCols attempts to build a constraint for the non-inverted
// prefix columns of the given index. If a constraint is successfully built, it
// is returned along with remaining filters and ok=true. The function is only
// successful if it can generate a constraint where all spans have the same
// start and end keys for all non-inverted prefix columns. This is required for
// building spans for scanning multi-column inverted indexes (see
// span.Builder.SpansFromInvertedSpans).
func constrainNonInvertedCols(
	ctx context.Context,
	evalCtx *eval.Context,
	factory *norm.Factory,
	columns []opt.OrderingColumn,
	notNullCols opt.ColSet,
	filters memo.FiltersExpr,
	optionalFilters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
	checkCancellation func(),
) (_ *constraint.Constraint, remainingFilters memo.FiltersExpr, ok bool) {
	tabMeta := factory.Metadata().TableMeta(tabID)
	prefixColumnCount := index.PrefixColumnCount()
	ps := tabMeta.IndexPartitionLocality(index.Ordinal())

	// Consolidation of a constraint converts contiguous spans into a single
	// span. By definition, the consolidated span would have different start and
	// end keys and could not be used for multi-column inverted index scans.
	// Therefore, we only generate and check the unconsolidated constraint,
	// allowing the optimizer to plan multi-column inverted index scans in more
	// cases.
	//
	// For example, the consolidated constraint for (x IN (1, 2, 3)) is:
	//
	//   /x: [/1 - /3]
	//   Prefix: 0
	//
	// The unconsolidated constraint for the same expression is:
	//
	//   /x: [/1 - /1] [/2 - /2] [/3 - /3]
	//   Prefix: 1
	//
	var ic idxconstraint.Instance
	ic.Init(
		ctx, filters, optionalFilters,
		columns, notNullCols, tabMeta.ComputedCols,
		tabMeta.ColsInComputedColsExpressions,
		false, /* consolidate */
		evalCtx, factory, ps, checkCancellation,
	)
	var c constraint.Constraint
	ic.UnconsolidatedConstraint(&c)
	if c.Prefix(ctx, evalCtx) != prefixColumnCount {
		// The prefix columns must be constrained to single values.
		return nil, nil, false
	}

	return &c, ic.RemainingFilters(), true
}

type invertedFilterPlanner interface {
	// extractInvertedFilterConditionFromLeaf extracts an inverted filter
	// condition from the given expression, which represents a leaf of an
	// expression tree in which the internal nodes are And and/or Or expressions.
	// Returns an empty inverted.Expression if no inverted filter condition could
	// be extracted.
	//
	// Additionally, returns:
	// - remaining filters that must be applied if the inverted expression is not
	//   tight, and
	// - pre-filterer state that can be used to reduce false positives.
	extractInvertedFilterConditionFromLeaf(ctx context.Context, evalCtx *eval.Context, expr opt.ScalarExpr) (
		invertedExpr inverted.Expression,
		remainingFilters opt.ScalarExpr,
		_ *invertedexpr.PreFiltererStateForInvertedFilterer,
	)
}

// extractInvertedFilterCondition extracts an inverted.Expression from the given
// filter condition, where the inverted.Expression represents an inverted filter
// over the given inverted index. Returns an empty inverted.Expression if no
// inverted filter condition could be extracted.
//
// The filter condition should be an expression tree of And, Or, and leaf
// expressions. Extraction of the inverted.Expression from the leaves is
// delegated to the given invertedFilterPlanner.
//
// In addition to the inverted.Expression, returns:
//   - remaining filters that must be applied if the inverted expression is not
//     tight, and
//   - pre-filterer state that can be used to reduce false positives. This is
//     only non-nil if filterCond is a leaf condition (i.e., has no ANDs or ORs).
func extractInvertedFilterCondition(
	ctx context.Context,
	evalCtx *eval.Context,
	factory *norm.Factory,
	filterCond opt.ScalarExpr,
	filterPlanner invertedFilterPlanner,
) (
	invertedExpr inverted.Expression,
	remainingFilters opt.ScalarExpr,
	_ *invertedexpr.PreFiltererStateForInvertedFilterer,
) {
	switch t := filterCond.(type) {
	case *memo.AndExpr:
		l, remLeft, _ := extractInvertedFilterCondition(ctx, evalCtx, factory, t.Left, filterPlanner)
		r, remRight, _ := extractInvertedFilterCondition(ctx, evalCtx, factory, t.Right, filterPlanner)
		if remLeft == nil {
			remainingFilters = remRight
		} else if remRight == nil {
			remainingFilters = remLeft
		} else {
			remainingFilters = factory.ConstructAnd(remLeft, remRight)
		}
		return inverted.And(l, r), remainingFilters, nil

	case *memo.OrExpr:
		l, remLeft, _ := extractInvertedFilterCondition(ctx, evalCtx, factory, t.Left, filterPlanner)
		r, remRight, _ := extractInvertedFilterCondition(ctx, evalCtx, factory, t.Right, filterPlanner)
		if remLeft != nil || remRight != nil {
			// If either child has remaining filters, we must return the original
			// condition as the remaining filter. It would be incorrect to return
			// only part of the original condition.
			remainingFilters = filterCond
		}
		return inverted.Or(l, r), remainingFilters, nil

	default:
		return filterPlanner.extractInvertedFilterConditionFromLeaf(ctx, evalCtx, filterCond)
	}
}

// isIndexColumn returns true if e is an expression that corresponds to an
// inverted index column. The expression can be either:
//   - a variable on the index column, or
//   - an expression that matches the computed column expression (if the index
//     column is computed).
func isIndexColumn(
	tabID opt.TableID, index cat.Index, e opt.Expr, computedColumns map[opt.ColumnID]opt.ScalarExpr,
) bool {
	invertedSourceCol := tabID.ColumnID(index.InvertedColumn().InvertedSourceColumnOrdinal())
	if v, ok := e.(*memo.VariableExpr); ok && v.Col == invertedSourceCol {
		return true
	}
	if computedColumns != nil && e == computedColumns[invertedSourceCol] {
		return true
	}
	return false
}
