// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// joinReaderSpanGenerator is used by the joinReader to generate spans for
// looking up into the index.
type joinReaderSpanGenerator interface {
	// setResizeMemoryAccountFunc provides the generator with a
	// resizeMemoryAccountFunc. It must be called once before the generator is
	// used.
	setResizeMemoryAccountFunc(resizeMemoryAccountFunc)

	// generateSpans generates spans for the given batch of input rows. The spans
	// are returned in rows order, but there are no duplicates (i.e. if a 2nd row
	// results in the same spans as a previous row, the results don't include them
	// a second time).
	//
	// The returned spans are not accounted for, so it is the caller's
	// responsibility to register the spans memory usage with our memory
	// accounting system.
	generateSpans(ctx context.Context, rows []rowenc.EncDatumRow) (roachpb.Spans, []int, error)

	// getMatchingRowIndices returns the indices of the input rows that are
	// associated with the given span ID (i.e., the indices of the rows passed
	// to generateSpans that resulted in the spanID'th generated span).
	getMatchingRowIndices(spanID int) []int

	// close releases any resources associated with the joinReaderSpanGenerator.
	close(context.Context)
}

var _ joinReaderSpanGenerator = &defaultSpanGenerator{}
var _ joinReaderSpanGenerator = &multiSpanGenerator{}
var _ joinReaderSpanGenerator = &localityOptimizedSpanGenerator{}

type resizeMemoryAccountFunc func(_ *mon.BoundAccount, oldSz, newSz int64) error

// spanIDHelper helps joinReaderSpanGenerators in maintaining a mapping between
// spans returned by generateSpans() and input rows that need the result of each
// span. This is needed in order to join the looked up rows with the input rows
// later.
type spanIDHelper struct {
	// spanKeyToSpanID maps a lookup span key to the span ID. This is used for
	// de-duping spans for lookup joins.
	//
	// Index joins already have unique rows that generate unique spans to fetch,
	// so they don't need this map.
	//
	// TODO(drewk): using a map instead of a sort-merge strategy is inefficient
	//  for inequality spans, which tend to overlap significantly without being
	//  exactly the same. For now, we can just limit when inequality-only lookup
	//  joins are planned to avoid regression.
	spanKeyToSpanID map[string]int
	// spanIDToInputRowIndices maps a span ID to the input row indices that
	// desire the lookup of the span corresponding to that span ID.
	//
	// Index joins simply output the fetched rows, so they don't need this
	// mapping.
	spanIDToInputRowIndices [][]int

	scratchSpanIDs []int
}

// reset sets up the helper for reuse.
func (h *spanIDHelper) reset() {
	// This loop gets optimized to a runtime.mapclear call.
	for k := range h.spanKeyToSpanID {
		delete(h.spanKeyToSpanID, k)
	}
	h.spanIDToInputRowIndices = h.spanIDToInputRowIndices[:0]
	h.scratchSpanIDs = h.scratchSpanIDs[:0]
}

// addInputRowIdxForSpan adds the given input row index to the list of indices
// corresponding to the given span (identified by the span key). Span ID of the
// span as well as a boolean indicating whether this span key is seen for the
// first time are returned.
func (h *spanIDHelper) addInputRowIdxForSpan(
	span *roachpb.Span, inputRowIdx int,
) (spanID int, newSpanKey bool) {
	// Derive a unique key for the span. This pattern for constructing the string
	// is more efficient than using fmt.Sprintf().
	spanKey := strconv.Itoa(len(span.Key)) + "/" + string(span.Key) + "/" + string(span.EndKey)
	spanID, ok := h.spanKeyToSpanID[spanKey]
	if !ok {
		spanID = len(h.spanKeyToSpanID)
		h.spanKeyToSpanID[spanKey] = spanID
		if cap(h.spanIDToInputRowIndices) > spanID {
			h.spanIDToInputRowIndices = h.spanIDToInputRowIndices[:spanID+1]
			h.spanIDToInputRowIndices[spanID] = h.spanIDToInputRowIndices[spanID][:0]
		} else {
			h.spanIDToInputRowIndices = append(h.spanIDToInputRowIndices, nil)
		}
	}
	h.spanIDToInputRowIndices[spanID] = append(h.spanIDToInputRowIndices[spanID], inputRowIdx)
	return spanID, !ok
}

// addedSpans notifies the helper that 'count' number of spans were created that
// correspond to the given spanID.
func (h *spanIDHelper) addedSpans(spanID int, count int) {
	for i := 0; i < count; i++ {
		h.scratchSpanIDs = append(h.scratchSpanIDs, spanID)
	}
}

func (h *spanIDHelper) getMatchingRowIndices(spanID int) []int {
	return h.spanIDToInputRowIndices[spanID]
}

// memUsage returns the size of the data structures in the spanIDHelper for
// memory accounting purposes.
func (h *spanIDHelper) memUsage() int64 {
	var size int64
	// Account for spanKeyToSpanID map.
	size += int64(len(h.spanKeyToSpanID)) * (memsize.MapEntryOverhead + memsize.Int)
	// Account for each key in the map since it can be arbitrarily large.
	for k := range h.spanKeyToSpanID {
		size += memsize.String + int64(len(k))
	}
	// Account for the two-dimensional spanIDToInputRowIndices.
	size += int64(cap(h.spanIDToInputRowIndices)) * memsize.IntSliceOverhead
	// Account for each one-dimensional int slice in spanIDToInputRowIndices.
	for _, slice := range h.spanIDToInputRowIndices[:cap(h.spanIDToInputRowIndices)] {
		size += int64(cap(slice)) * memsize.Int64
	}
	// Account for scratchSpanIDs.
	size += int64(cap(h.scratchSpanIDs)) * memsize.Int64
	return size
}

type defaultSpanGenerator struct {
	spanBuilder  span.Builder
	spanSplitter span.Splitter
	lookupCols   []uint32

	indexKeyRow rowenc.EncDatumRow

	spanIDHelper

	scratchSpans roachpb.Spans

	// memAcc is owned by this span generator and is closed when the generator
	// is closed. All memory reservations should be done via
	// resizeMemoryAccount.
	memAcc              *mon.BoundAccount
	resizeMemoryAccount resizeMemoryAccountFunc
}

func (g *defaultSpanGenerator) init(
	evalCtx *eval.Context,
	codec keys.SQLCodec,
	fetchSpec *fetchpb.IndexFetchSpec,
	splitFamilyIDs []descpb.FamilyID,
	uniqueRows bool,
	lookupCols []uint32,
	memAcc *mon.BoundAccount,
) error {
	g.spanBuilder.InitWithFetchSpec(evalCtx, codec, fetchSpec)
	g.spanSplitter = span.MakeSplitterWithFamilyIDs(len(fetchSpec.KeyFullColumns()), splitFamilyIDs)
	g.lookupCols = lookupCols
	if len(lookupCols) > len(fetchSpec.KeyAndSuffixColumns) {
		return errors.AssertionFailedf(
			"%d lookup columns specified, expecting at most %d", len(lookupCols), len(fetchSpec.KeyAndSuffixColumns),
		)
	}
	g.indexKeyRow = nil
	if !uniqueRows {
		g.spanIDHelper.spanKeyToSpanID = make(map[string]int)
	}
	g.memAcc = memAcc
	return nil
}

func (g *defaultSpanGenerator) setResizeMemoryAccountFunc(f resizeMemoryAccountFunc) {
	g.resizeMemoryAccount = f
}

// Generate spans for a given row.
// If lookup columns are specified will use those to collect the relevant
// columns. Otherwise the first rows are assumed to correspond with the index.
// It additionally returns whether the row contains null, which is needed to
// decide whether or not to split the generated span into separate family
// specific spans.
func (g *defaultSpanGenerator) generateSpan(
	row rowenc.EncDatumRow,
) (_ roachpb.Span, containsNull bool, _ error) {
	g.indexKeyRow = g.indexKeyRow[:0]
	for _, id := range g.lookupCols {
		g.indexKeyRow = append(g.indexKeyRow, row[id])
	}
	return g.spanBuilder.SpanFromEncDatums(g.indexKeyRow)
}

func (g *defaultSpanGenerator) hasNullLookupColumn(row rowenc.EncDatumRow) bool {
	for _, colIdx := range g.lookupCols {
		if row[colIdx].IsNull() {
			return true
		}
	}
	return false
}

// generateSpans is part of the joinReaderSpanGenerator interface.
func (g *defaultSpanGenerator) generateSpans(
	ctx context.Context, rows []rowenc.EncDatumRow,
) (roachpb.Spans, []int, error) {
	isIndexJoin := g.spanKeyToSpanID == nil
	if !isIndexJoin {
		g.spanIDHelper.reset()
	}
	g.scratchSpans = g.scratchSpans[:0]
	for i, inputRow := range rows {
		if g.hasNullLookupColumn(inputRow) {
			continue
		}
		generatedSpan, containsNull, err := g.generateSpan(inputRow)
		if err != nil {
			return nil, nil, err
		}
		if isIndexJoin {
			g.scratchSpans = g.spanSplitter.MaybeSplitSpanIntoSeparateFamilies(
				g.scratchSpans, generatedSpan, len(g.lookupCols), containsNull,
			)
		} else {
			spanID, newSpanKey := g.addInputRowIdxForSpan(&generatedSpan, i)
			if newSpanKey {
				numOldSpans := len(g.scratchSpans)
				g.scratchSpans = g.spanSplitter.MaybeSplitSpanIntoSeparateFamilies(
					g.scratchSpans, generatedSpan, len(g.lookupCols), containsNull,
				)
				g.addedSpans(spanID, len(g.scratchSpans)-numOldSpans)
			}
		}
	}

	// Memory accounting.
	//
	// NOTE: this does not account for scratchSpans because the joinReader
	// passes the ownership of spans to the fetcher which will account for it
	// accordingly.
	if err := g.resizeMemoryAccount(g.memAcc, g.memAcc.Used(), g.memUsage()); err != nil {
		return nil, nil, err
	}

	return g.scratchSpans, g.scratchSpanIDs, nil
}

func (g *defaultSpanGenerator) close(ctx context.Context) {
	g.memAcc.Close(ctx)
	*g = defaultSpanGenerator{}
}

// multiSpanGenerator is the joinReaderSpanGenerator used when each lookup will
// scan multiple spans in the index. This is the case when some of the index
// columns can take on multiple constant values. For example, the
// multiSpanGenerator would be used for a left lookup join in the following
// case:
//   - The index has key columns (region, id)
//   - The input columns are (a, b, c)
//   - The join condition is region IN ('east', 'west') AND id = a
//
// In this case, the multiSpanGenerator would generate two spans for each input
// row: [/'east'/<val_a> - /'east'/<val_a>] [/'west'/<val_a> - /'west'/<val_a>].
type multiSpanGenerator struct {
	spanBuilder  span.Builder
	spanSplitter span.Splitter

	// indexColInfos stores info about the values that each index column can
	// take on in the spans produced by the multiSpanGenerator. See the comment
	// above multiSpanGeneratorColInfo for more details.
	indexColInfos []multiSpanGeneratorColInfo

	// indexKeyRows and indexKeySpans are used to generate the spans for a single
	// input row. They are allocated once in init(), and then reused for every row.
	indexKeyRows  []rowenc.EncDatumRow
	indexKeySpans roachpb.Spans

	// inequalityInfo holds the information needed to generate spans when the last
	// lookup column is constrained by an inequality condition.
	inequalityInfo struct {
		// colIdx is the index of the lookup column that is constrained to a range.
		// It is set to -1 if no lookup column is constrained by an inequality.
		colIdx                       int
		colTyp                       *types.T
		startInclusive, endInclusive bool
		start, end                   tree.TypedExpr
	}

	spanIDHelper

	// spansCount is the number of spans generated for each input row.
	spansCount int

	// fetchedOrdToIndexKeyOrd maps the ordinals of fetched (right-hand side)
	// columns to ordinals in the index key columns.
	fetchedOrdToIndexKeyOrd util.FastIntMap

	// numInputCols is the number of columns in the input to the joinReader.
	numInputCols int

	scratchSpans roachpb.Spans

	// memAcc is owned by this span generator and is closed when the generator
	// is closed. All memory reservations should be done via
	// resizeMemoryAccount.
	memAcc              *mon.BoundAccount
	resizeMemoryAccount resizeMemoryAccountFunc
}

// multiSpanGeneratorColInfo contains info about the values that a specific
// index column can take on in the spans produced by the multiSpanGenerator. The
// column ordinal is not contained in this struct, but depends on the location
// of this struct in the indexColInfos slice; the position in the slice
// corresponds to the position in the index.
type multiSpanGeneratorColInfo interface {
	String() string
}

// multiSpanGeneratorValuesColInfo is used to represent a column constrained
// by a set of constants (i.e. '=' or 'in' expressions).
type multiSpanGeneratorValuesColInfo struct {
	constVals tree.Datums
}

func (i multiSpanGeneratorValuesColInfo) String() string {
	return fmt.Sprintf("[constVals: %s]", i.constVals.String())
}

// multiSpanGeneratorIndexVarColInfo represents a column that matches a column
// in the input row. inputRowIdx corresponds to an index into the input row.
// This is the case for join filters such as c = a, where c is a column in the
// index and a is a column in the input.
type multiSpanGeneratorIndexVarColInfo struct {
	inputRowIdx int
}

func (i multiSpanGeneratorIndexVarColInfo) String() string {
	return fmt.Sprintf("[inputRowIdx: %d]", i.inputRowIdx)
}

var _ multiSpanGeneratorColInfo = &multiSpanGeneratorValuesColInfo{}
var _ multiSpanGeneratorColInfo = &multiSpanGeneratorIndexVarColInfo{}

// init must be called before the multiSpanGenerator can be used to generate
// spans.
// - spansCanOverlap indicates whether it is possible for the same spans to be
// used more than once for a given input batch. This is only the case when an
// inequality on an input column is used, since in that case we don't fully
// de-duplicate.
func (g *multiSpanGenerator) init(
	evalCtx *eval.Context,
	codec keys.SQLCodec,
	fetchSpec *fetchpb.IndexFetchSpec,
	splitFamilyIDs []descpb.FamilyID,
	numInputCols int,
	expr tree.TypedExpr,
	fetchedOrdToIndexKeyOrd util.FastIntMap,
	memAcc *mon.BoundAccount,
) (spansCanOverlap bool, _ error) {
	g.spanBuilder.InitWithFetchSpec(evalCtx, codec, fetchSpec)
	g.spanSplitter = span.MakeSplitterWithFamilyIDs(len(fetchSpec.KeyFullColumns()), splitFamilyIDs)
	g.numInputCols = numInputCols
	g.spanKeyToSpanID = make(map[string]int)
	g.fetchedOrdToIndexKeyOrd = fetchedOrdToIndexKeyOrd
	g.inequalityInfo.colIdx = -1
	g.memAcc = memAcc

	// Initialize the spansCount to 1, since we'll always have at least one span.
	// This number may increase when we call fillInIndexColInfos() below.
	g.spansCount = 1

	// Process the given expression to fill in g.indexColInfos with info from the
	// join conditions. This info will be used later to generate the spans.
	g.indexColInfos = make([]multiSpanGeneratorColInfo, 0, len(fetchSpec.KeyAndSuffixColumns))
	if err := g.fillInIndexColInfos(expr); err != nil {
		return false, err
	}

	// Check that the results of fillInIndexColInfos can be used to generate valid
	// spans.
	lookupColsCount := len(g.indexColInfos)
	if g.inequalityInfo.colIdx != -1 {
		lookupColsCount++
	}
	if lookupColsCount > len(fetchSpec.KeyAndSuffixColumns) {
		return false, errors.AssertionFailedf(
			"%d lookup columns specified, expecting at most %d",
			lookupColsCount, len(fetchSpec.KeyAndSuffixColumns),
		)
	}

	// Fill in g.indexKeyRows with the cartesian product of the constant values
	// collected above. This reduces the amount of work that is needed for each
	// input row, since only columns depending on the input values will need
	// to be filled in later.
	//
	// For example, suppose that we have index columns on
	// (region, tenant, category, id), and the join conditions are:
	//
	//    region IN ('east', 'west')
	//    AND tenant = input.tenant
	//    AND category IN (1, 2)
	//    AND id = input.id
	//
	// The following code would create the following rows, leaving spaces for
	// tenant and id to be filled in later:
	//
	//   [ 'east'  -  1  - ]
	//   [ 'east'  -  2  - ]
	//   [ 'west'  -  1  - ]
	//   [ 'west'  -  2  - ]
	//

	// First fill the structure with empty EncDatum values.
	g.indexKeyRows = make([]rowenc.EncDatumRow, g.spansCount)
	for rowIdx := range g.indexKeyRows {
		g.indexKeyRows[rowIdx] = make(rowenc.EncDatumRow, lookupColsCount)
	}

	// Next replace the constant values according to the cartesian product, as in
	// the example above.
	productSize := len(g.indexKeyRows)
	for colIdx, info := range g.indexColInfos {
		if valuesInfo, ok := info.(multiSpanGeneratorValuesColInfo); ok {
			productSize /= len(valuesInfo.constVals)
			for rowIdx := range g.indexKeyRows {
				valueIdx := (rowIdx / productSize) % len(valuesInfo.constVals)
				g.indexKeyRows[rowIdx][colIdx] = rowenc.EncDatum{Datum: valuesInfo.constVals[valueIdx]}
			}
		}
	}

	g.indexKeySpans = make(roachpb.Spans, 0, g.spansCount)
	return lookupExprHasVarInequality(expr), nil
}

// lookupExprHasVarInequality returns true if the given lookup expression
// contains an inequality that references an input column.
func lookupExprHasVarInequality(lookupExpr tree.TypedExpr) bool {
	switch t := lookupExpr.(type) {
	case *tree.AndExpr:
		return lookupExprHasVarInequality(t.Left.(tree.TypedExpr)) ||
			lookupExprHasVarInequality(t.Right.(tree.TypedExpr))
	case *tree.ComparisonExpr:
		switch t.Operator.Symbol {
		case treecmp.LT, treecmp.LE, treecmp.GT, treecmp.GE:
			_, leftIsVar := t.Left.(*tree.IndexedVar)
			_, rightIsVar := t.Right.(*tree.IndexedVar)
			return leftIsVar && rightIsVar
		}
	}
	return false
}

// fillInIndexColInfos recursively walks the expression tree to collect join
// conditions that are AND-ed together. It fills in g.indexColInfos with info
// from the join conditions.
//
// The only acceptable join conditions are:
//  1. Equalities between input columns and index columns, such as c1 = c2.
//  2. Equalities or IN conditions between index columns and constants, such
//     as c = 5 or c IN ('a', 'b', 'c').
//  3. Inequalities from (possibly AND'd) <,>,<=,>= exprs.
//
// The optimizer should have ensured that all conditions fall into one of
// these categories. Any other expression types will return an error.
// TODO(treilly): We should probably be doing this at compile time, see #65773
func (g *multiSpanGenerator) fillInIndexColInfos(expr tree.TypedExpr) error {
	switch t := expr.(type) {
	case *tree.AndExpr:
		if err := g.fillInIndexColInfos(t.Left.(tree.TypedExpr)); err != nil {
			return err
		}
		return g.fillInIndexColInfos(t.Right.(tree.TypedExpr))

	case *tree.ComparisonExpr:
		var inequality bool
		switch t.Operator.Symbol {
		case treecmp.EQ, treecmp.In:
			inequality = false
		case treecmp.GE, treecmp.LE, treecmp.GT, treecmp.LT:
			inequality = true
		default:
			// This should never happen because of enforcement at opt time.
			return errors.AssertionFailedf("comparison operator not supported. Found %s", t.Operator)
		}

		tabOrd := -1

		var info multiSpanGeneratorColInfo

		// For EQ and In, we just need to check the types of the arguments in order
		// to extract the info. For inequalities we return the expressions that
		// will form the span boundaries.
		getInfo := func(typedExpr tree.TypedExpr) (tree.TypedExpr, error) {
			switch t := typedExpr.(type) {
			case *tree.IndexedVar:
				if t.Idx >= g.numInputCols {
					// The IndexedVar is from the index. shift it over to find the
					// corresponding ordinal in the base table.
					tabOrd = t.Idx - g.numInputCols
					return nil, nil
				}
				// The IndexedVar is from the input. It will be used to generate spans.
				if inequality {
					// We will use the IndexVar expression as an inequality bound.
					return t, nil
				}
				info = multiSpanGeneratorIndexVarColInfo{inputRowIdx: t.Idx}

			case tree.Datum:
				if inequality {
					// We will use the Datum as an inequality bound.
					return t, nil
				}
				var values tree.Datums
				switch t.ResolvedType().Family() {
				case types.TupleFamily:
					values = t.(*tree.DTuple).D
				default:
					values = tree.Datums{t}
				}
				// Every time there are multiple possible values, we multiply the
				// spansCount by the number of possibilities. We will need to create
				// spans representing the cartesian product of possible values for
				// each column.
				info = multiSpanGeneratorValuesColInfo{constVals: values}
				g.spansCount *= len(values)

			default:
				return nil, errors.AssertionFailedf("unhandled comparison argument type %T", t)
			}
			return nil, nil
		}

		// NB: we make no attempt to deal with column direction here, that is sorted
		// out later in the span builder.

		lval, err := getInfo(t.Left.(tree.TypedExpr))
		if err != nil {
			return err
		}

		rval, err := getInfo(t.Right.(tree.TypedExpr))
		if err != nil {
			return err
		}

		idxOrd, ok := g.fetchedOrdToIndexKeyOrd.Get(tabOrd)
		if !ok {
			return errors.AssertionFailedf("table column %d not found in index", tabOrd)
		}

		if !inequality {
			// Make sure slice has room for new entry.
			if len(g.indexColInfos) <= idxOrd {
				g.indexColInfos = g.indexColInfos[:idxOrd+1]
			}
			g.indexColInfos[idxOrd] = info
			return nil
		}

		if g.inequalityInfo.colIdx != -1 && g.inequalityInfo.colIdx != idxOrd {
			return errors.AssertionFailedf("two inequality columns found: %d and %d",
				g.inequalityInfo.colIdx, idxOrd)
		}
		g.inequalityInfo.colIdx = idxOrd

		if lval != nil {
			g.inequalityInfo.colTyp = lval.ResolvedType()
			if t.Operator.Symbol == treecmp.LT || t.Operator.Symbol == treecmp.LE {
				g.inequalityInfo.start = lval
				g.inequalityInfo.startInclusive = t.Operator.Symbol == treecmp.LE
			} else {
				g.inequalityInfo.end = lval
				g.inequalityInfo.endInclusive = t.Operator.Symbol == treecmp.GE
			}
		}

		if rval != nil {
			g.inequalityInfo.colTyp = rval.ResolvedType()
			if t.Operator.Symbol == treecmp.LT || t.Operator.Symbol == treecmp.LE {
				g.inequalityInfo.end = rval
				g.inequalityInfo.endInclusive = t.Operator.Symbol == treecmp.LE
			} else {
				g.inequalityInfo.start = rval
				g.inequalityInfo.startInclusive = t.Operator.Symbol == treecmp.GE
			}
		}

	default:
		return errors.AssertionFailedf("unhandled expression type %T", t)
	}
	return nil
}

func (g *multiSpanGenerator) setResizeMemoryAccountFunc(f resizeMemoryAccountFunc) {
	g.resizeMemoryAccount = f
}

// generateNonNullSpans generates spans for a given row. It does not include
// null values, since those values would not match the lookup condition anyway.
func (g *multiSpanGenerator) generateNonNullSpans(
	ctx context.Context, row rowenc.EncDatumRow,
) (roachpb.Spans, error) {
	// Fill in the holes in g.indexKeyRows that correspond to input row values.
	for i := 0; i < len(g.indexKeyRows); i++ {
		for j, info := range g.indexColInfos {
			if inf, ok := info.(multiSpanGeneratorIndexVarColInfo); ok {
				g.indexKeyRows[i][j] = row[inf.inputRowIdx]
			}
		}
	}

	// Convert the index key rows to spans.
	g.indexKeySpans = g.indexKeySpans[:0]

	// Hoist inequality lookup out of loop if we have one.
	var startBound, endBound *rowenc.EncDatum
	if g.inequalityInfo.colIdx != -1 {
		startBound, endBound = g.getInequalityBounds(row)
	}

	// Build spans for each row.
	for _, indexKeyRow := range g.indexKeyRows {
		var s roachpb.Span
		var err error
		var containsNull, filterRow bool
		if g.inequalityInfo.colIdx == -1 {
			s, containsNull, err = g.spanBuilder.SpanFromEncDatums(indexKeyRow[:len(g.indexColInfos)])
		} else {
			s, containsNull, filterRow, err = g.spanBuilder.SpanFromEncDatumsWithRange(
				ctx, indexKeyRow, len(g.indexColInfos), startBound, endBound,
				g.inequalityInfo.startInclusive, g.inequalityInfo.endInclusive, g.inequalityInfo.colTyp)
		}

		if err != nil {
			return roachpb.Spans{}, err
		}

		if filterRow {
			// The row has been filtered by the range conditions.
			return roachpb.Spans{}, nil
		}

		if !containsNull {
			g.indexKeySpans = append(g.indexKeySpans, s)
		}
	}

	return g.indexKeySpans, nil
}

// generateSpans is part of the joinReaderSpanGenerator interface.
func (g *multiSpanGenerator) generateSpans(
	ctx context.Context, rows []rowenc.EncDatumRow,
) (roachpb.Spans, []int, error) {
	g.spanIDHelper.reset()
	g.scratchSpans = g.scratchSpans[:0]
	for i, inputRow := range rows {
		generatedSpans, err := g.generateNonNullSpans(ctx, inputRow)
		if err != nil {
			return nil, nil, err
		}
		for j := range generatedSpans {
			generatedSpan := &generatedSpans[j]
			spanID, newSpanKey := g.spanIDHelper.addInputRowIdxForSpan(generatedSpan, i)
			if newSpanKey {
				numOldSpans := len(g.scratchSpans)
				// MaybeSplitSpanIntoSeparateFamilies is an optimization for doing more
				// efficient point lookups when the span hits multiple column families.
				// It doesn't work with inequality ranges because they aren't point lookups.
				if g.inequalityInfo.colIdx != -1 {
					g.scratchSpans = append(g.scratchSpans, *generatedSpan)
				} else {
					g.scratchSpans = g.spanSplitter.MaybeSplitSpanIntoSeparateFamilies(
						g.scratchSpans, *generatedSpan, len(g.indexColInfos), false, /* containsNull */
					)
				}
				g.addedSpans(spanID, len(g.scratchSpans)-numOldSpans)
			}
		}
	}

	// Memory accounting.
	//
	// NOTE: this does not account for scratchSpans because the joinReader
	// passes the ownership of spans to the fetcher which will account for it
	// accordingly.
	if err := g.memAcc.ResizeTo(ctx, g.memUsage()); err != nil {
		return nil, nil, addWorkmemHint(err)
	}

	return g.scratchSpans, g.scratchSpanIDs, nil
}

// getInequalityBounds returns the start and end bounds for the index column
// that is constrained by a range expression. If either the start or end is
// unconstrained, the corresponding bound is nil.
func (g *multiSpanGenerator) getInequalityBounds(
	row []rowenc.EncDatum,
) (start, end *rowenc.EncDatum) {
	getBound := func(expr tree.TypedExpr) *rowenc.EncDatum {
		switch t := expr.(type) {
		case tree.Datum:
			return &rowenc.EncDatum{Datum: t}
		case *tree.IndexedVar:
			return &row[t.Idx]
		}
		return nil
	}
	start = getBound(g.inequalityInfo.start)
	end = getBound(g.inequalityInfo.end)
	return start, end
}

func (g *multiSpanGenerator) close(ctx context.Context) {
	g.memAcc.Close(ctx)
	*g = multiSpanGenerator{}
}

// localityOptimizedSpanGenerator is the span generator for locality optimized
// lookup joins. The localSpanGen is used to generate spans targeting local
// nodes, and the remoteSpanGen is used to generate spans targeting remote
// nodes.
type localityOptimizedSpanGenerator struct {
	localSpanGen  multiSpanGenerator
	remoteSpanGen multiSpanGenerator
	// localSpanGenUsedLast is true if generateSpans() was called more recently
	// than generateRemoteSpans().
	localSpanGenUsedLast bool
}

// init must be called before the localityOptimizedSpanGenerator can be used to
// generate spans. Note that we use two different span builders so that both
// local and remote span generators could release their own when they are
// close()d.
func (g *localityOptimizedSpanGenerator) init(
	evalCtx *eval.Context,
	codec keys.SQLCodec,
	fetchSpec *fetchpb.IndexFetchSpec,
	splitFamilyIDs []descpb.FamilyID,
	numInputCols int,
	localExpr tree.TypedExpr,
	remoteExpr tree.TypedExpr,
	fetchedOrdToIndexKeyOrd util.FastIntMap,
	localSpanGenMemAcc *mon.BoundAccount,
	remoteSpanGenMemAcc *mon.BoundAccount,
) (spansCanOverlap bool, err error) {
	var localSpansCanOverlap, remoteSpansCanOverlap bool
	if localSpansCanOverlap, err = g.localSpanGen.init(
		evalCtx, codec, fetchSpec, splitFamilyIDs,
		numInputCols, localExpr, fetchedOrdToIndexKeyOrd, localSpanGenMemAcc,
	); err != nil {
		return false, err
	}
	if remoteSpansCanOverlap, err = g.remoteSpanGen.init(
		evalCtx, codec, fetchSpec, splitFamilyIDs,
		numInputCols, remoteExpr, fetchedOrdToIndexKeyOrd, remoteSpanGenMemAcc,
	); err != nil {
		return false, err
	}
	// Check that the resulting span generators have the same lookup columns.
	localLookupCols := len(g.localSpanGen.indexColInfos)
	remoteLookupCols := len(g.remoteSpanGen.indexColInfos)
	if localLookupCols != remoteLookupCols {
		return false, errors.AssertionFailedf(
			"local lookup cols (%d) != remote lookup cols (%d)", localLookupCols, remoteLookupCols,
		)
	}
	return localSpansCanOverlap || remoteSpansCanOverlap, nil
}

func (g *localityOptimizedSpanGenerator) setResizeMemoryAccountFunc(f resizeMemoryAccountFunc) {
	g.localSpanGen.setResizeMemoryAccountFunc(f)
	g.remoteSpanGen.setResizeMemoryAccountFunc(f)
}

// generateSpans is part of the joinReaderSpanGenerator interface.
func (g *localityOptimizedSpanGenerator) generateSpans(
	ctx context.Context, rows []rowenc.EncDatumRow,
) (roachpb.Spans, []int, error) {
	g.localSpanGenUsedLast = true
	return g.localSpanGen.generateSpans(ctx, rows)
}

// generateRemoteSpans generates spans targeting remote nodes for the given
// batch of input rows.
func (g *localityOptimizedSpanGenerator) generateRemoteSpans(
	ctx context.Context, rows []rowenc.EncDatumRow,
) (roachpb.Spans, []int, error) {
	g.localSpanGenUsedLast = false
	return g.remoteSpanGen.generateSpans(ctx, rows)
}

// getMatchingRowIndices is part of the joinReaderSpanGenerator interface.
func (g *localityOptimizedSpanGenerator) getMatchingRowIndices(spanID int) []int {
	if g.localSpanGenUsedLast {
		return g.localSpanGen.getMatchingRowIndices(spanID)
	}
	return g.remoteSpanGen.getMatchingRowIndices(spanID)
}

func (g *localityOptimizedSpanGenerator) close(ctx context.Context) {
	g.localSpanGen.close(ctx)
	g.remoteSpanGen.close(ctx)
	*g = localityOptimizedSpanGenerator{}
}
