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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// joinReaderSpanGenerator is used by the joinReader to generate spans for
// looking up into the index.
type joinReaderSpanGenerator interface {
	// generateSpans generates spans for the given batch of input rows. The spans
	// are returned in rows order, but there are no duplicates (i.e. if a 2nd row
	// results in the same spans as a previous row, the results don't include them
	// a second time).
	generateSpans(rows []rowenc.EncDatumRow) (roachpb.Spans, error)

	// getMatchingRowIndices returns the indices of the input rows that desire
	// the given key (i.e., the indices of the rows passed to generateSpans that
	// generated spans containing the given key).
	getMatchingRowIndices(key roachpb.Key) []int

	// maxLookupCols returns the maximum number of index columns used as the key
	// for each lookup.
	maxLookupCols() int
}

var _ joinReaderSpanGenerator = &defaultSpanGenerator{}
var _ joinReaderSpanGenerator = &multiSpanGenerator{}
var _ joinReaderSpanGenerator = &localityOptimizedSpanGenerator{}

type defaultSpanGenerator struct {
	spanBuilder *span.Builder
	numKeyCols  int
	lookupCols  []uint32

	indexKeyRow rowenc.EncDatumRow
	// keyToInputRowIndices maps a lookup span key to the input row indices that
	// desire that span. This is used for joins other than index joins, for
	// de-duping spans, and to map the fetched rows to the input rows that need
	// to join with them. Index joins already have unique rows in the input that
	// generate unique spans for fetch, and simply output the fetched rows, do
	// do not use this map.
	keyToInputRowIndices map[string][]int

	scratchSpans roachpb.Spans
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
	numLookupCols := len(g.lookupCols)
	if numLookupCols > g.numKeyCols {
		return roachpb.Span{}, false, errors.Errorf(
			"%d lookup columns specified, expecting at most %d", numLookupCols, g.numKeyCols)
	}

	g.indexKeyRow = g.indexKeyRow[:0]
	for _, id := range g.lookupCols {
		g.indexKeyRow = append(g.indexKeyRow, row[id])
	}
	return g.spanBuilder.SpanFromEncDatums(g.indexKeyRow, numLookupCols)
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
func (g *defaultSpanGenerator) generateSpans(rows []rowenc.EncDatumRow) (roachpb.Spans, error) {
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
			g.scratchSpans = g.spanBuilder.MaybeSplitSpanIntoSeparateFamilies(
				g.scratchSpans, generatedSpan, len(g.lookupCols), containsNull)
		} else {
			inputRowIndices := g.keyToInputRowIndices[string(generatedSpan.Key)]
			if inputRowIndices == nil {
				g.scratchSpans = g.spanBuilder.MaybeSplitSpanIntoSeparateFamilies(
					g.scratchSpans, generatedSpan, len(g.lookupCols), containsNull)
			}
			g.keyToInputRowIndices[string(generatedSpan.Key)] = append(inputRowIndices, i)
		}
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
	spanBuilder *span.Builder

	// indexColInfos stores info about the values that each index column can
	// take on in the spans produced by the multiSpanGenerator. See the comment
	// above multiSpanGeneratorIndexColInfo for more details.
	indexColInfos []multiSpanGeneratorIndexColInfo

	// indexKeyRows and indexKeySpans are used to generate the spans for a single
	// input row. They are allocated once in init(), and then reused for every row.
	indexKeyRows  []rowenc.EncDatumRow
	indexKeySpans roachpb.Spans

	// keyToInputRowIndices maps a lookup span key to the input row indices that
	// desire that span. This is used for de-duping spans, and to map the fetched
	// rows to the input rows that need to join with them.
	keyToInputRowIndices map[string][]int

	// spansCount is the number of spans generated for each input row.
	spansCount int

	// indexOrds contains the ordinals (i.e., the positions in the index) of the
	// index columns that are constrained by the spans produced by this
	// multiSpanGenerator. indexOrds must be a prefix of the index columns.
	indexOrds util.FastIntSet

	// tableOrdToIndexOrd maps the ordinals of columns in the table to their
	// ordinals in the index.
	tableOrdToIndexOrd util.FastIntMap

	// numInputCols is the number of columns in the input to the joinReader.
	numInputCols int

	scratchSpans roachpb.Spans
}

// multiSpanGeneratorIndexColInfo contains info about the values that a specific
// index column can take on in the spans produced by the multiSpanGenerator. The
// column ordinal is not contained in this struct, but depends on the location
// of this struct in the indexColInfos slice; the position in the slice
// corresponds to the position in the index.
// - If len(constVals) > 0, the index column can equal any of the given
//   constant values. This is the case when there is a join filter such as
//   c IN ('a', 'b', 'c'), where c is a key column in the index.
// - If constVals is empty, then inputRowIdx corresponds to an index into the
//   input row. This is the case for join filters such as c = a, where c is a
//   column in the index and a is a column in the input.
type multiSpanGeneratorIndexColInfo struct {
	constVals   tree.Datums
	inputRowIdx int
}

func (i multiSpanGeneratorIndexColInfo) String() string {
	if len(i.constVals) > 0 {
		return fmt.Sprintf("[constVals: %s]", i.constVals.String())
	}
	return fmt.Sprintf("[inputRowIdx: %d]", i.inputRowIdx)
}

// maxLookupCols is part of the joinReaderSpanGenerator interface.
func (g *multiSpanGenerator) maxLookupCols() int {
	return len(g.indexColInfos)
}

// init must be called before the multiSpanGenerator can be used to generate
// spans.
func (g *multiSpanGenerator) init(
	spanBuilder *span.Builder,
	numKeyCols int,
	numInputCols int,
	keyToInputRowIndices map[string][]int,
	exprHelper *execinfrapb.ExprHelper,
	tableOrdToIndexOrd util.FastIntMap,
) error {
	g.spanBuilder = spanBuilder
	g.numInputCols = numInputCols
	g.keyToInputRowIndices = keyToInputRowIndices
	g.tableOrdToIndexOrd = tableOrdToIndexOrd

	// Initialize the spansCount to 1, since we'll always have at least one span.
	// This number may increase when we call fillInIndexColInfos() below.
	g.spansCount = 1

	// Process the given expression to fill in g.indexColInfos with info from the
	// join conditions. This info will be used later to generate the spans.
	g.indexColInfos = make([]multiSpanGeneratorIndexColInfo, 0, numKeyCols)
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
	if lookupColsCount > numKeyCols {
		return errors.AssertionFailedf(
			"%d lookup columns specified, expecting at most %d", lookupColsCount, numKeyCols,
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
	g.indexKeyRows = make([]rowenc.EncDatumRow, 1, g.spansCount)
	g.indexKeyRows[0] = make(rowenc.EncDatumRow, 0, lookupColsCount)
	for _, info := range g.indexColInfos {
		if len(info.constVals) > 0 {
			for i, n := 0, len(g.indexKeyRows); i < n; i++ {
				indexKeyRow := g.indexKeyRows[i]
				for j := 1; j < len(info.constVals); j++ {
					newIndexKeyRow := make(rowenc.EncDatumRow, len(indexKeyRow), lookupColsCount)
					copy(newIndexKeyRow, indexKeyRow)
					newIndexKeyRow = append(newIndexKeyRow, rowenc.EncDatum{Datum: info.constVals[j]})
					g.indexKeyRows = append(g.indexKeyRows, newIndexKeyRow)
				}
				g.indexKeyRows[i] = append(indexKeyRow, rowenc.EncDatum{Datum: info.constVals[0]})
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
// The optimizer should have ensured that all conditions fall into one of
// these two categories. Any other expression types will return an error.
func (g *multiSpanGenerator) fillInIndexColInfos(expr tree.TypedExpr) error {
	switch t := expr.(type) {
	case *tree.AndExpr:
		if err := g.fillInIndexColInfos(t.Left.(tree.TypedExpr)); err != nil {
			return err
		}
		return g.fillInIndexColInfos(t.Right.(tree.TypedExpr))

	case *tree.ComparisonExpr:
		if t.Operator.Symbol != tree.EQ && t.Operator.Symbol != tree.In {
			return errors.AssertionFailedf("comparison operator must be EQ or In. Found %s", t.Operator)
		}

		tabOrd := -1
		info := multiSpanGeneratorIndexColInfo{inputRowIdx: -1}

		// Since we only support EQ and In, we don't need to check anything other
		// than the types of the arguments in order to extract the info.
		getInfo := func(typedExpr tree.TypedExpr) error {
			switch t := typedExpr.(type) {
			case *tree.IndexedVar:
				// IndexedVars can either be from the input or the index. If the
				// IndexedVar is from the index, shift it over by numInputCols to
				// find the corresponding ordinal in the base table.
				if t.Idx >= g.numInputCols {
					tabOrd = t.Idx - g.numInputCols
				} else {
					info.inputRowIdx = t.Idx
				}

			case tree.Datum:
				switch t.ResolvedType().Family() {
				case types.TupleFamily:
					info.constVals = t.(*tree.DTuple).D
				default:
					info.constVals = tree.Datums{t}
				}
				// Every time there are multiple possible values, we multiply the
				// spansCount by the number of possibilities. We will need to create
				// spans representing the cartesian product of possible values for
				// each column.
				g.spansCount *= len(info.constVals)

			default:
				return errors.AssertionFailedf("unhandled comparison argument type %T", t)
			}
			return nil
		}
		if err := getInfo(t.Left.(tree.TypedExpr)); err != nil {
			return err
		}
		if err := getInfo(t.Right.(tree.TypedExpr)); err != nil {
			return err
		}

		idxOrd, ok := g.tableOrdToIndexOrd.Get(tabOrd)
		if !ok {
			return errors.AssertionFailedf("table column %d not found in index", tabOrd)
		}
		if len(g.indexColInfos) <= idxOrd {
			g.indexColInfos = g.indexColInfos[:idxOrd+1]
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
	// Fill in the holes in g.indexKeyRows that correspond to input row
	// values.
	for j, info := range g.indexColInfos {
		if len(info.constVals) == 0 {
			for i := 0; i < len(g.indexKeyRows); i++ {
				g.indexKeyRows[i][j] = row[info.inputRowIdx]
			}
		}
	}

	// Convert the index key rows to spans.
	g.indexKeySpans = g.indexKeySpans[:0]
	for _, indexKeyRow := range g.indexKeyRows {
		span, containsNull, err := g.spanBuilder.SpanFromEncDatums(indexKeyRow, len(g.indexColInfos))
		if err != nil {
			return roachpb.Spans{}, err
		}
		if !containsNull {
			g.indexKeySpans = append(g.indexKeySpans, span)
		}
	}
	return g.indexKeySpans, nil
}

// generateSpans is part of the joinReaderSpanGenerator interface.
func (g *multiSpanGenerator) generateSpans(rows []rowenc.EncDatumRow) (roachpb.Spans, error) {
	// This loop gets optimized to a runtime.mapclear call.
	for k := range g.keyToInputRowIndices {
		delete(g.keyToInputRowIndices, k)
	}
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
				g.scratchSpans = g.spanBuilder.MaybeSplitSpanIntoSeparateFamilies(
					g.scratchSpans, *generatedSpan, len(g.indexColInfos), false /* containsNull */)
			}
			g.keyToInputRowIndices[string(generatedSpan.Key)] = append(inputRowIndices, i)
		}
	}

	return g.scratchSpans, nil
}

// getMatchingRowIndices is part of the joinReaderSpanGenerator interface.
func (g *multiSpanGenerator) getMatchingRowIndices(key roachpb.Key) []int {
	return g.keyToInputRowIndices[string(key)]
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
// generate spans.
func (g *localityOptimizedSpanGenerator) init(
	spanBuilder *span.Builder,
	numKeyCols int,
	numInputCols int,
	keyToInputRowIndices map[string][]int,
	localExprHelper *execinfrapb.ExprHelper,
	remoteExprHelper *execinfrapb.ExprHelper,
	tableOrdToIndexOrd util.FastIntMap,
) error {
	if err := g.localSpanGen.init(
		spanBuilder, numKeyCols, numInputCols, keyToInputRowIndices, localExprHelper, tableOrdToIndexOrd,
	); err != nil {
		return err
	}
	if err := g.remoteSpanGen.init(
		spanBuilder, numKeyCols, numInputCols, keyToInputRowIndices, remoteExprHelper, tableOrdToIndexOrd,
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
	rows []rowenc.EncDatumRow,
) (roachpb.Spans, error) {
	return g.localSpanGen.generateSpans(rows)
}

// generateRemoteSpans generates spans targeting remote nodes for the given
// batch of input rows.
func (g *localityOptimizedSpanGenerator) generateRemoteSpans(
	rows []rowenc.EncDatumRow,
) (roachpb.Spans, error) {
	return g.remoteSpanGen.generateSpans(rows)
}

// getMatchingRowIndices is part of the joinReaderSpanGenerator interface.
func (g *localityOptimizedSpanGenerator) getMatchingRowIndices(key roachpb.Key) []int {
	if res := g.localSpanGen.getMatchingRowIndices(key); len(res) > 0 {
		return res
	}
	return g.remoteSpanGen.getMatchingRowIndices(key)
}
