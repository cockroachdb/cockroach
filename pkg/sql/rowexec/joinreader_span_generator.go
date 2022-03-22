// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"fmt"
	"sort"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
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
	// generateSpans generates spans for the given batch of input rows. The spans
	// are returned in rows order, but there are no duplicates (i.e. if a 2nd row
	// results in the same spans as a previous row, the results don't include them
	// a second time).
	//
	// The returned spans are not accounted for, so it is the caller's
	// responsibility to register the spans memory usage with our memory
	// accounting system.
	generateSpans(ctx context.Context, rows []rowenc.EncDatumRow) (roachpb.Spans, error)

	// getMatchingRowIndices returns the indices of the input rows that desire
	// the given key (i.e., the indices of the rows passed to generateSpans that
	// generated spans containing the given key).
	getMatchingRowIndices(key roachpb.Key) []int

	// maxLookupCols returns the maximum number of index columns used as the key
	// for each lookup.
	maxLookupCols() int

	// close releases any resources associated with the joinReaderSpanGenerator.
	close(context.Context)
}

var _ joinReaderSpanGenerator = &defaultSpanGenerator{}
var _ joinReaderSpanGenerator = &multiSpanGenerator{}
var _ joinReaderSpanGenerator = &localityOptimizedSpanGenerator{}

type defaultSpanGenerator struct {
	spanBuilder  span.Builder
	spanSplitter span.Splitter
	lookupCols   []uint32

	indexKeyRow rowenc.EncDatumRow
	// keyToInputRowIndices maps a lookup span key to the input row indices that
	// desire that span. This is used for joins other than index joins, for
	// de-duping spans, and to map the fetched rows to the input rows that need
	// to join with them. Index joins already have unique rows in the input that
	// generate unique spans for fetch, and simply output the fetched rows, do
	// do not use this map.
	keyToInputRowIndices map[string][]int

	scratchSpans roachpb.Spans

	// memAcc is owned by this span generator and is closed when the generator
	// is closed.
	memAcc *mon.BoundAccount
}

func (g *defaultSpanGenerator) init(
	evalCtx *tree.EvalContext,
	codec keys.SQLCodec,
	fetchSpec *descpb.IndexFetchSpec,
	splitFamilyIDs []descpb.FamilyID,
	keyToInputRowIndices map[string][]int,
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
	g.keyToInputRowIndices = keyToInputRowIndices
	g.scratchSpans = nil
	g.memAcc = memAcc
	return nil
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
) (roachpb.Spans, error) {
	// This loop gets optimized to a runtime.mapclear call.
	for k := range g.keyToInputRowIndices {
		delete(g.keyToInputRowIndices, k)
	}

	// We maintain a map from index key to the corresponding input rows so we can
	// join the index results to the inputs.
	g.scratchSpans = g.scratchSpans[:0]
	for i, inputRow := range rows {
		if g.hasNullLookupColumn(inputRow) {
			continue
		}
		generatedSpan, containsNull, err := g.generateSpan(inputRow)
		if err != nil {
			return nil, err
		}
		if g.keyToInputRowIndices == nil {
			// Index join.
			g.scratchSpans = g.spanSplitter.MaybeSplitSpanIntoSeparateFamilies(
				g.scratchSpans, generatedSpan, len(g.lookupCols), containsNull)
		} else {
			inputRowIndices := g.keyToInputRowIndices[string(generatedSpan.Key)]
			if inputRowIndices == nil {
				g.scratchSpans = g.spanSplitter.MaybeSplitSpanIntoSeparateFamilies(
					g.scratchSpans, generatedSpan, len(g.lookupCols), containsNull)
			}
			g.keyToInputRowIndices[string(generatedSpan.Key)] = append(inputRowIndices, i)
		}
	}

	// Memory accounting.
	if err := g.memAcc.ResizeTo(ctx, g.memUsage()); err != nil {
		return nil, err
	}

	return g.scratchSpans, nil
}

// getMatchingRowIndices is part of the joinReaderSpanGenerator interface.
func (g *defaultSpanGenerator) getMatchingRowIndices(key roachpb.Key) []int {
	return g.keyToInputRowIndices[string(key)]
}

// maxLookupCols is part of the joinReaderSpanGenerator interface.
func (g *defaultSpanGenerator) maxLookupCols() int {
	return len(g.lookupCols)
}

// memUsage returns the size of the data structures in the defaultSpanGenerator
// for memory accounting purposes.
// NOTE: this does not account for scratchSpans because the joinReader passes
// the ownership of spans to the fetcher which will account for it accordingly.
func (g *defaultSpanGenerator) memUsage() int64 {
	// Account for keyToInputRowIndices.
	var size int64
	for k, v := range g.keyToInputRowIndices {
		size += memsize.MapEntryOverhead
		size += memsize.String + int64(len(k))
		size += memsize.IntSliceOverhead + memsize.Int*int64(cap(v))
	}
	return size
}

func (g *defaultSpanGenerator) close(ctx context.Context) {
	g.memAcc.Close(ctx)
	*g = defaultSpanGenerator{}
}

type spanRowIndex struct {
	span       roachpb.Span
	rowIndices []int
}

type spanRowIndices []spanRowIndex

func (s spanRowIndices) Len() int           { return len(s) }
func (s spanRowIndices) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s spanRowIndices) Less(i, j int) bool { return s[i].span.Key.Compare(s[j].span.Key) < 0 }

var _ sort.Interface = &spanRowIndices{}

// memUsage returns the size of the spanRowIndices for memory accounting
// purposes.
func (s spanRowIndices) memUsage() int64 {
	// Slice the full capacity of s so we can account for the memory
	// used past the length of s.
	sCap := s[:cap(s)]
	size := int64(unsafe.Sizeof(spanRowIndices{}))
	for i := range sCap {
		size += sCap[i].span.MemUsage()
		size += memsize.IntSliceOverhead + memsize.Int*int64(cap(sCap[i].rowIndices))
	}
	return size
}

// multiSpanGenerator is the joinReaderSpanGenerator used when each lookup will
// scan multiple spans in the index. This is the case when some of the index
// columns can take on multiple constant values. For example, the
// multiSpanGenerator would be used for a left lookup join in the following
// case:
//  - The index has key columns (region, id)
//  - The input columns are (a, b, c)
//  - The join condition is region IN ('east', 'west') AND id = a
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

	// keyToInputRowIndices maps a lookup span key to the input row indices that
	// desire that span. This is used for de-duping spans, and to map the fetched
	// rows to the input rows that need to join with them. If we have inequality
	// exprs we can't use this from getMatchingRowIndices because the spans are
	// ranges and not point spans so we build this map using the start keys and
	// then convert it into a spanToInputRowIndices.
	keyToInputRowIndices map[string][]int

	// spanToInputRowIndices maps a lookup span to the input row indices that
	// desire that span. This is a range based equivalent of the
	// keyToInputRowIndices that is only used when there are range based, i.e.
	// inequality conditions. This is a sorted set we do binary searches on.
	spanToInputRowIndices spanRowIndices

	// spansCount is the number of spans generated for each input row.
	spansCount int

	// indexOrds contains the ordinals (i.e., the positions in the index) of the
	// index columns that are constrained by the spans produced by this
	// multiSpanGenerator. indexOrds must be a prefix of the index columns.
	indexOrds util.FastIntSet

	// fetchedOrdToIndexKeyOrd maps the ordinals of fetched (right-hand side)
	// columns to ordinals in the index key columns.
	fetchedOrdToIndexKeyOrd util.FastIntMap

	// numInputCols is the number of columns in the input to the joinReader.
	numInputCols int

	// inequalityColIdx is the index of inequality colinfo (there can be only one),
	// -1 otherwise.
	inequalityColIdx int

	scratchSpans roachpb.Spans

	// memAcc is owned by this span generator and is closed when the generator
	// is closed.
	memAcc *mon.BoundAccount
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

// multiSpanGeneratorInequalityColInfo represents a column that is bound by a
// range expression. If there are <,>, >= or <= inequalities we distill them
// into a start and end datum.
type multiSpanGeneratorInequalityColInfo struct {
	start          tree.Datum
	startInclusive bool
	end            tree.Datum
	endInclusive   bool
}

func (i multiSpanGeneratorInequalityColInfo) String() string {
	var startBoundary byte
	if i.startInclusive {
		startBoundary = '['
	} else {
		startBoundary = '('
	}
	var endBoundary rune
	if i.endInclusive {
		endBoundary = ']'
	} else {
		endBoundary = ')'
	}
	return fmt.Sprintf("%c%v - %v%c", startBoundary, i.start, i.end, endBoundary)
}

var _ multiSpanGeneratorColInfo = &multiSpanGeneratorValuesColInfo{}
var _ multiSpanGeneratorColInfo = &multiSpanGeneratorIndexVarColInfo{}
var _ multiSpanGeneratorColInfo = &multiSpanGeneratorInequalityColInfo{}

// maxLookupCols is part of the joinReaderSpanGenerator interface.
func (g *multiSpanGenerator) maxLookupCols() int {
	return len(g.indexColInfos)
}

// init must be called before the multiSpanGenerator can be used to generate
// spans.
func (g *multiSpanGenerator) init(
	evalCtx *tree.EvalContext,
	codec keys.SQLCodec,
	fetchSpec *descpb.IndexFetchSpec,
	splitFamilyIDs []descpb.FamilyID,
	numInputCols int,
	exprHelper *execinfrapb.ExprHelper,
	fetchedOrdToIndexKeyOrd util.FastIntMap,
	memAcc *mon.BoundAccount,
) error {
	g.spanBuilder.InitWithFetchSpec(evalCtx, codec, fetchSpec)
	g.spanSplitter = span.MakeSplitterWithFamilyIDs(len(fetchSpec.KeyFullColumns()), splitFamilyIDs)
	g.numInputCols = numInputCols
	g.keyToInputRowIndices = make(map[string][]int)
	g.fetchedOrdToIndexKeyOrd = fetchedOrdToIndexKeyOrd
	g.inequalityColIdx = -1
	g.memAcc = memAcc

	// Initialize the spansCount to 1, since we'll always have at least one span.
	// This number may increase when we call fillInIndexColInfos() below.
	g.spansCount = 1

	// Process the given expression to fill in g.indexColInfos with info from the
	// join conditions. This info will be used later to generate the spans.
	g.indexColInfos = make([]multiSpanGeneratorColInfo, 0, len(fetchSpec.KeyAndSuffixColumns))
	if err := g.fillInIndexColInfos(exprHelper.Expr); err != nil {
		return err
	}

	// Check that the results of fillInIndexColInfos can be used to generate valid
	// spans.
	lookupColsCount := len(g.indexColInfos)
	if lookupColsCount != g.indexOrds.Len() {
		return errors.AssertionFailedf(
			"columns in the join condition do not form a prefix on the index",
		)
	}
	if lookupColsCount > len(fetchSpec.KeyAndSuffixColumns) {
		return errors.AssertionFailedf(
			"%d lookup columns specified, expecting at most %d", lookupColsCount, len(fetchSpec.KeyAndSuffixColumns),
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
	//   [ 'west'  -  1  - ]
	//   [ 'east'  -  2  - ]
	//   [ 'west'  -  2  - ]
	//

	// Make first pass flushing out the structure with const values.
	g.indexKeyRows = make([]rowenc.EncDatumRow, 1, g.spansCount)
	g.indexKeyRows[0] = make(rowenc.EncDatumRow, 0, lookupColsCount)
	for _, info := range g.indexColInfos {
		if valuesInfo, ok := info.(multiSpanGeneratorValuesColInfo); ok {
			for i, n := 0, len(g.indexKeyRows); i < n; i++ {
				indexKeyRow := g.indexKeyRows[i]
				for j := 1; j < len(valuesInfo.constVals); j++ {
					newIndexKeyRow := make(rowenc.EncDatumRow, len(indexKeyRow), lookupColsCount)
					copy(newIndexKeyRow, indexKeyRow)
					newIndexKeyRow = append(newIndexKeyRow, rowenc.EncDatum{Datum: valuesInfo.constVals[j]})
					g.indexKeyRows = append(g.indexKeyRows, newIndexKeyRow)
				}
				g.indexKeyRows[i] = append(indexKeyRow, rowenc.EncDatum{Datum: valuesInfo.constVals[0]})
			}
		} else {
			for i := 0; i < len(g.indexKeyRows); i++ {
				// Just fill in an empty EncDatum for now -- this will be replaced
				// inside generateNonNullSpans when we process each row.
				g.indexKeyRows[i] = append(g.indexKeyRows[i], rowenc.EncDatum{})
			}
		}
	}

	g.indexKeySpans = make(roachpb.Spans, 0, g.spansCount)
	return nil
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
		setOfVals := false
		inequality := false
		switch t.Operator.Symbol {
		case treecmp.EQ, treecmp.In:
			setOfVals = true
		case treecmp.GE, treecmp.LE, treecmp.GT, treecmp.LT:
			inequality = true
		default:
			// This should never happen because of enforcement at opt time.
			return errors.AssertionFailedf("comparison operator not supported. Found %s", t.Operator)
		}

		tabOrd := -1

		var info multiSpanGeneratorColInfo

		// For EQ and In, we just need to check the types of the arguments in order
		// to extract the info. For inequalities we return the const datums that
		// will form the span boundaries.
		getInfo := func(typedExpr tree.TypedExpr) (tree.Datum, error) {
			switch t := typedExpr.(type) {
			case *tree.IndexedVar:
				// IndexedVars can either be from the input or the index. If the
				// IndexedVar is from the index, shift it over by numInputCols to
				// find the corresponding ordinal in the base table.
				if t.Idx >= g.numInputCols {
					tabOrd = t.Idx - g.numInputCols
				} else {
					info = multiSpanGeneratorIndexVarColInfo{inputRowIdx: t.Idx}
				}

			case tree.Datum:
				if setOfVals {
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
				} else {
					return t, nil
				}

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

		// Make sure slice has room for new entry.
		if len(g.indexColInfos) <= idxOrd {
			g.indexColInfos = g.indexColInfos[:idxOrd+1]
		}

		if inequality {
			// If we have two inequalities we might already have an info, ie if we
			// have a < 10 and a > 0 we'll have two invocations of fillInIndexColInfo
			// for each comparison and they need to update the same info.
			colInfo := g.indexColInfos[idxOrd]
			var inequalityInfo multiSpanGeneratorInequalityColInfo
			if colInfo != nil {
				inequalityInfo, ok = colInfo.(multiSpanGeneratorInequalityColInfo)
				if !ok {
					return errors.AssertionFailedf("unexpected colinfo type (%d): %T", idxOrd, colInfo)
				}
			}

			if lval != nil {
				if t.Operator.Symbol == treecmp.LT || t.Operator.Symbol == treecmp.LE {
					inequalityInfo.start = lval
					inequalityInfo.startInclusive = t.Operator.Symbol == treecmp.LE
				} else {
					inequalityInfo.end = lval
					inequalityInfo.endInclusive = t.Operator.Symbol == treecmp.GE
				}
			}

			if rval != nil {
				if t.Operator.Symbol == treecmp.LT || t.Operator.Symbol == treecmp.LE {
					inequalityInfo.end = rval
					inequalityInfo.endInclusive = t.Operator.Symbol == treecmp.LE
				} else {
					inequalityInfo.start = rval
					inequalityInfo.startInclusive = t.Operator.Symbol == treecmp.GE
				}
			}
			info = inequalityInfo
			g.inequalityColIdx = idxOrd
		}

		g.indexColInfos[idxOrd] = info
		g.indexOrds.Add(idxOrd)

	default:
		return errors.AssertionFailedf("unhandled expression type %T", t)
	}
	return nil
}

// generateNonNullSpans generates spans for a given row. It does not include
// null values, since those values would not match the lookup condition anyway.
func (g *multiSpanGenerator) generateNonNullSpans(row rowenc.EncDatumRow) (roachpb.Spans, error) {
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
	var inequalityInfo multiSpanGeneratorInequalityColInfo
	if g.inequalityColIdx != -1 {
		inequalityInfo = g.indexColInfos[g.inequalityColIdx].(multiSpanGeneratorInequalityColInfo)
	}

	// Build spans for each row.
	for _, indexKeyRow := range g.indexKeyRows {
		var s roachpb.Span
		var err error
		var containsNull bool
		if g.inequalityColIdx == -1 {
			s, containsNull, err = g.spanBuilder.SpanFromEncDatums(indexKeyRow[:len(g.indexColInfos)])
		} else {
			s, containsNull, err = g.spanBuilder.SpanFromEncDatumsWithRange(indexKeyRow, len(g.indexColInfos),
				inequalityInfo.start, inequalityInfo.startInclusive, inequalityInfo.end, inequalityInfo.endInclusive)
		}

		if err != nil {
			return roachpb.Spans{}, err
		}

		if !containsNull {
			g.indexKeySpans = append(g.indexKeySpans, s)
		}
	}

	return g.indexKeySpans, nil
}

// findInputRowIndicesByKey does a binary search to find the span that contains
// the given key.
func (s *spanRowIndices) findInputRowIndicesByKey(key roachpb.Key) []int {
	i, j := 0, s.Len()
	for i < j {
		h := (i + j) >> 1
		sp := (*s)[h]
		switch sp.span.CompareKey(key) {
		case 0:
			return sp.rowIndices
		case -1:
			j = h
		case 1:
			i = h + 1
		}
	}

	return nil
}

// generateSpans is part of the joinReaderSpanGenerator interface.
func (g *multiSpanGenerator) generateSpans(
	ctx context.Context, rows []rowenc.EncDatumRow,
) (roachpb.Spans, error) {
	// This loop gets optimized to a runtime.mapclear call.
	for k := range g.keyToInputRowIndices {
		delete(g.keyToInputRowIndices, k)
	}
	g.spanToInputRowIndices = g.spanToInputRowIndices[:0]

	// We maintain a map from index key to the corresponding input rows so we can
	// join the index results to the inputs.
	g.scratchSpans = g.scratchSpans[:0]
	for i, inputRow := range rows {
		generatedSpans, err := g.generateNonNullSpans(inputRow)
		if err != nil {
			return nil, err
		}
		for j := range generatedSpans {
			generatedSpan := &generatedSpans[j]
			inputRowIndices := g.keyToInputRowIndices[string(generatedSpan.Key)]
			if inputRowIndices == nil {
				// MaybeSplitSpanIntoSeparateFamilies is an optimization for doing more
				// efficient point lookups when the span hits multiple column families.
				// It doesn't work with inequality ranges because they aren't point lookups.
				if g.inequalityColIdx != -1 {
					g.scratchSpans = append(g.scratchSpans, *generatedSpan)
				} else {
					g.scratchSpans = g.spanSplitter.MaybeSplitSpanIntoSeparateFamilies(
						g.scratchSpans, *generatedSpan, len(g.indexColInfos), false /* containsNull */)
				}
			}

			g.keyToInputRowIndices[string(generatedSpan.Key)] = append(inputRowIndices, i)
		}
	}

	// If we need to map against range spans instead of point spans convert the
	// map into a sorted set of spans we can binary search against.
	if g.inequalityColIdx != -1 {
		for _, s := range g.scratchSpans {
			g.spanToInputRowIndices = append(g.spanToInputRowIndices, spanRowIndex{span: s, rowIndices: g.keyToInputRowIndices[string(s.Key)]})
		}
		sort.Sort(g.spanToInputRowIndices)
		// We don't need this anymore.
		for k := range g.keyToInputRowIndices {
			delete(g.keyToInputRowIndices, k)
		}
	}

	// Memory accounting.
	if err := g.memAcc.ResizeTo(ctx, g.memUsage()); err != nil {
		return nil, err
	}

	return g.scratchSpans, nil
}

// getMatchingRowIndices is part of the joinReaderSpanGenerator interface.
func (g *multiSpanGenerator) getMatchingRowIndices(key roachpb.Key) []int {
	if g.inequalityColIdx != -1 {
		return g.spanToInputRowIndices.findInputRowIndicesByKey(key)
	}
	return g.keyToInputRowIndices[string(key)]
}

// memUsage returns the size of the data structures in the multiSpanGenerator
// for memory accounting purposes.
// NOTE: this does not account for scratchSpans because the joinReader passes
// the ownership of spans to the fetcher which will account for it accordingly.
func (g *multiSpanGenerator) memUsage() int64 {
	// Account for keyToInputRowIndices.
	var size int64
	for k, v := range g.keyToInputRowIndices {
		size += memsize.MapEntryOverhead
		size += memsize.String + int64(len(k))
		size += memsize.IntSliceOverhead + memsize.Int*int64(cap(v))
	}

	// Account for spanToInputRowIndices.
	size += g.spanToInputRowIndices.memUsage()
	return size
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
}

// init must be called before the localityOptimizedSpanGenerator can be used to
// generate spans. Note that we use two different span builders so that both
// local and remote span generators could release their own when they are
// close()d.
func (g *localityOptimizedSpanGenerator) init(
	evalCtx *tree.EvalContext,
	codec keys.SQLCodec,
	fetchSpec *descpb.IndexFetchSpec,
	splitFamilyIDs []descpb.FamilyID,
	numInputCols int,
	localExprHelper *execinfrapb.ExprHelper,
	remoteExprHelper *execinfrapb.ExprHelper,
	fetchedOrdToIndexKeyOrd util.FastIntMap,
	localSpanGenMemAcc *mon.BoundAccount,
	remoteSpanGenMemAcc *mon.BoundAccount,
) error {
	if err := g.localSpanGen.init(
		evalCtx, codec, fetchSpec, splitFamilyIDs,
		numInputCols, localExprHelper, fetchedOrdToIndexKeyOrd, localSpanGenMemAcc,
	); err != nil {
		return err
	}
	if err := g.remoteSpanGen.init(
		evalCtx, codec, fetchSpec, splitFamilyIDs,
		numInputCols, remoteExprHelper, fetchedOrdToIndexKeyOrd, remoteSpanGenMemAcc,
	); err != nil {
		return err
	}
	// Check that the resulting span generators have the same lookup columns.
	localLookupCols := g.localSpanGen.maxLookupCols()
	remoteLookupCols := g.remoteSpanGen.maxLookupCols()
	if localLookupCols != remoteLookupCols {
		return errors.AssertionFailedf(
			"local lookup cols (%d) != remote lookup cols (%d)", localLookupCols, remoteLookupCols,
		)
	}
	return nil
}

// maxLookupCols is part of the joinReaderSpanGenerator interface.
func (g *localityOptimizedSpanGenerator) maxLookupCols() int {
	// We already asserted in init that maxLookupCols is the same for both the
	// local and remote span generators.
	return g.localSpanGen.maxLookupCols()
}

// generateSpans is part of the joinReaderSpanGenerator interface.
func (g *localityOptimizedSpanGenerator) generateSpans(
	ctx context.Context, rows []rowenc.EncDatumRow,
) (roachpb.Spans, error) {
	return g.localSpanGen.generateSpans(ctx, rows)
}

// generateRemoteSpans generates spans targeting remote nodes for the given
// batch of input rows.
func (g *localityOptimizedSpanGenerator) generateRemoteSpans(
	ctx context.Context, rows []rowenc.EncDatumRow,
) (roachpb.Spans, error) {
	return g.remoteSpanGen.generateSpans(ctx, rows)
}

// getMatchingRowIndices is part of the joinReaderSpanGenerator interface.
func (g *localityOptimizedSpanGenerator) getMatchingRowIndices(key roachpb.Key) []int {
	if res := g.localSpanGen.getMatchingRowIndices(key); len(res) > 0 {
		return res
	}
	return g.remoteSpanGen.getMatchingRowIndices(key)
}

func (g *localityOptimizedSpanGenerator) close(ctx context.Context) {
	g.localSpanGen.close(ctx)
	g.remoteSpanGen.close(ctx)
	*g = localityOptimizedSpanGenerator{}
}
