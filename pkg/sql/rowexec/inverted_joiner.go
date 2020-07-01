// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// TODO(sumeer): adjust this batch size dynamically to balance between the
// higher scan throughput of larger batches and the cost of spilling the
// scanned rows to disk. The spilling cost will probably be dominated by
// the de-duping cost, since it incurs a read.
const invertedJoinerBatchSize = 10

// invertedJoinerState represents the state of the processor.
type invertedJoinerState int

const (
	ijStateUnknown invertedJoinerState = iota
	// ijReadingInput means that a batch of rows is being read from the input.
	ijReadingInput
	// ijPerformingIndexScan means it is performing an inverted index scan
	// for the current input row batch.
	ijPerformingIndexScan
	// ijEmittingRows means it is emitting the results of the inverted join.
	ijEmittingRows
)

type invertedJoiner struct {
	execinfra.ProcessorBase

	runningState invertedJoinerState
	diskMonitor  *mon.BytesMonitor
	desc         sqlbase.TableDescriptor
	// The map from ColumnIDs in the table to the column position.
	colIdxMap map[sqlbase.ColumnID]int
	index     *sqlbase.IndexDescriptor
	// The ColumnID of the inverted column. Confusingly, this is also the id of
	// the table column that was indexed.
	invertedColID sqlbase.ColumnID

	onExprHelper execinfra.ExprHelper
	combinedRow  sqlbase.EncDatumRow

	joinType sqlbase.JoinType

	// fetcher wraps the row.Fetcher used to perform scans. This enables the
	// invertedJoiner to wrap the fetcher with a stat collector when necessary.
	fetcher  rowFetcher
	alloc    sqlbase.DatumAlloc
	rowAlloc sqlbase.EncDatumRowAlloc

	// The row retrieved from the index represents the columns of the table
	// with the datums corresponding to the columns in the index populated.
	// The inverted column is in the position colIdxMap[invertedColID] and
	// the []byte stored there is used as the first parameter in
	// batchedExprEvaluator.addIndexRow(enc, keyIndex).
	//
	// The remaining columns in the index represent the primary key of the
	// table. They are at positions described by the keys in the
	// tableRowToKeyRowMap. The map is used to transform the retrieved table row
	// to the keyRow, and add to the row container, which de-duplicates the
	// primary keys. The index assigned by the container is the keyIndex in the
	// addIndexRow() call mentioned earlier.
	keyRow              sqlbase.EncDatumRow
	keyTypes            []*types.T
	tableRowToKeyRowMap map[int]int
	// The reverse transformation, from a key row to a table row, is done
	// before evaluating the onExpr.
	tableRow            sqlbase.EncDatumRow
	keyRowToTableRowMap []int

	// The input being joined using the index.
	input               execinfra.RowSource
	inputTypes          []*types.T
	lookupColumnIdx     uint32
	datumToInvertedExpr invertedexpr.DatumToInvertedExpr
	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSize int

	// State variables for each batch of input rows.
	inputRows       sqlbase.EncDatumRows
	batchedExprEval batchedInvertedExprEvaluator
	// The row indexes that are the result of the inverted expression evaluation
	// of the join. These will be further filtered using the onExpr.
	joinedRowIdx [][]KeyIndex

	// The container for the primary key rows retrieved from the index. For
	// evaluating each inverted expression, which involved set unions and
	// intersections, it is necessary to de-duplicate the primary key rows
	// retrieved from the inverted index. Instead of doing such de-duplication
	// for each expression in the batch of expressions, it is done once when
	// adding to keyRows -- this is more efficient since multiple expressions
	// may be using the same spans from the index.
	keyRows *rowcontainer.DiskBackedNumberedRowContainer

	// emitCursor contains information about where the next row to emit is within
	// joinedRowIdx.
	emitCursor struct {
		// inputRowIdx corresponds to joinedRowIdx[inputRowIdx].
		inputRowIdx int
		// outputRowIdx corresponds to joinedRowIdx[inputRowIdx][outputRowIdx].
		outputRowIdx int
		// seenMatch is true if there was a match at the current inputRowIdx.
		seenMatch bool
	}

	spanBuilder *span.Builder
	// A row with one element, corresponding to an encoded inverted column
	// value. Used to construct the span of the index for that value.
	invertedColRow sqlbase.EncDatumRow
}

var _ execinfra.Processor = &invertedJoiner{}
var _ execinfra.RowSource = &invertedJoiner{}
var _ execinfrapb.MetadataSource = &invertedJoiner{}
var _ execinfra.OpNode = &invertedJoiner{}

const invertedJoinerProcName = "inverted joiner"

// newInvertedJoiner constructs an invertedJoiner. The datumToInvertedExpr
// argument is non-nil only for tests. When nil, the invertedJoiner uses
// the spec to construct an implementation of DatumToInvertedExpr.
func newInvertedJoiner(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.InvertedJoinerSpec,
	datumToInvertedExpr invertedexpr.DatumToInvertedExpr,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.RowSourcedProcessor, error) {
	ij := &invertedJoiner{
		desc:                spec.Table,
		colIdxMap:           spec.Table.ColumnIdxMap(),
		input:               input,
		inputTypes:          input.OutputTypes(),
		lookupColumnIdx:     spec.LookupColumn,
		datumToInvertedExpr: datumToInvertedExpr,
		joinType:            spec.Type,
		batchSize:           invertedJoinerBatchSize,
	}

	var err error
	ij.index, _, err = ij.desc.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		return nil, err
	}
	ij.invertedColID = ij.index.ColumnIDs[0]

	indexColumnIDs, _ := ij.index.FullColumnIDs()
	// Inverted joins are not used for mutations.
	tableColumns := ij.desc.ColumnsWithMutations(false /* mutations */)
	ij.keyRow = make(sqlbase.EncDatumRow, len(indexColumnIDs)-1)
	ij.keyTypes = make([]*types.T, len(ij.keyRow))
	ij.tableRow = make(sqlbase.EncDatumRow, len(tableColumns))
	ij.tableRowToKeyRowMap = make(map[int]int)
	ij.keyRowToTableRowMap = make([]int, len(indexColumnIDs)-1)
	for i := 1; i < len(indexColumnIDs); i++ {
		keyRowIdx := i - 1
		tableRowIdx := ij.colIdxMap[indexColumnIDs[i]]
		ij.tableRowToKeyRowMap[tableRowIdx] = keyRowIdx
		ij.keyRowToTableRowMap[keyRowIdx] = tableRowIdx
		ij.keyTypes[keyRowIdx] = ij.desc.Columns[tableRowIdx].Type
	}

	outputColCount := len(ij.inputTypes)
	// Inverted joins are not used for mutations.
	rightColTypes := ij.desc.ColumnTypesWithMutations(false /* mutations */)
	var includeRightCols bool
	if ij.joinType == sqlbase.InnerJoin || ij.joinType == sqlbase.LeftOuterJoin {
		outputColCount += len(rightColTypes)
		includeRightCols = true
	}
	outputColTypes := make([]*types.T, 0, outputColCount)
	outputColTypes = append(outputColTypes, ij.inputTypes...)
	if includeRightCols {
		outputColTypes = append(outputColTypes, rightColTypes...)
	}
	if err := ij.ProcessorBase.Init(
		ij, post, outputColTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{ij.input},
			TrailingMetaCallback: func(ctx context.Context) []execinfrapb.ProducerMetadata {
				ij.close()
				return ij.generateMeta(ctx)
			},
		},
	); err != nil {
		return nil, err
	}

	onExprColTypes := make([]*types.T, 0, len(ij.inputTypes)+len(rightColTypes))
	onExprColTypes = append(onExprColTypes, ij.inputTypes...)
	onExprColTypes = append(onExprColTypes, rightColTypes...)
	if err := ij.onExprHelper.Init(spec.OnExpr, onExprColTypes, ij.EvalCtx); err != nil {
		return nil, err
	}
	ij.combinedRow = make(sqlbase.EncDatumRow, 0, len(onExprColTypes))

	if ij.datumToInvertedExpr == nil {
		var invertedExprHelper execinfra.ExprHelper
		if err := invertedExprHelper.Init(spec.InvertedExpr, onExprColTypes, ij.EvalCtx); err != nil {
			return nil, err
		}
		ij.datumToInvertedExpr, err = xform.NewDatumToInvertedExpr(invertedExprHelper.Expr, ij.index)
		if err != nil {
			return nil, err
		}
	}

	var fetcher row.Fetcher
	// In general we need all the columns in the index to compute the set
	// expression. There may be InvertedJoinerSpec.InvertedExpr that are known
	// to generate only set union expressions, which together with LEFT_SEMI and
	// LEFT_ANTI, and knowledge of the columns needed by
	// InvertedJoinerSpec.OnExpr, could be used to prune the columns needed
	// here. For now, we do the simple thing, since we have no idea whether
	// such workloads actually occur in practice.
	allIndexCols := util.MakeFastIntSet()
	for _, colID := range indexColumnIDs {
		allIndexCols.Add(ij.colIdxMap[colID])
	}
	// We use ScanVisibilityPublic since inverted joins are not used for mutations,
	// and so do not need to see in-progress schema changes.
	_, _, err = initRowFetcher(
		flowCtx, &fetcher, &ij.desc, int(spec.IndexIdx), ij.colIdxMap, false, /* reverse */
		allIndexCols, false /* isCheck */, &ij.alloc, execinfra.ScanVisibilityPublic,
		sqlbase.ScanLockingStrength_FOR_NONE,
	)
	if err != nil {
		return nil, err
	}

	collectingStats := false
	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		collectingStats = true
	}
	if collectingStats {
		ij.input = newInputStatCollector(ij.input)
		ij.fetcher = newRowFetcherStatCollector(&fetcher)
		ij.FinishTrace = ij.outputStatsToTrace
	} else {
		ij.fetcher = &fetcher
	}

	ij.spanBuilder = span.MakeBuilder(flowCtx.Codec(), &ij.desc, ij.index)
	ij.spanBuilder.SetNeededColumns(allIndexCols)

	// Initialize memory monitors and row container for key rows.
	ctx := flowCtx.EvalCtx.Ctx()
	ij.MemMonitor = execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, "invertedjoiner-limited")
	ij.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "invertedjoiner-disk")
	ij.keyRows = rowcontainer.NewDiskBackedNumberedRowContainer(
		true, /* deDup */
		ij.keyTypes,
		ij.EvalCtx,
		ij.FlowCtx.Cfg.TempStorage,
		ij.MemMonitor,
		ij.diskMonitor,
		0, /* rowCapacity */
	)

	return ij, nil
}

// SetBatchSize sets the desired batch size. It should only be used in tests.
func (ij *invertedJoiner) SetBatchSize(batchSize int) {
	ij.batchSize = batchSize
}

func (ij *invertedJoiner) generateSpan(enc []byte) (roachpb.Span, error) {
	// Pretend that the encoded inverted val is an EncDatum. This isn't always
	// true, since JSON inverted columns use a custom encoding. But since we
	// are providing an already encoded Datum, the following will eventually
	// fall through to EncDatum.Encode() which will reuse the encoded bytes.
	encDatum := sqlbase.EncDatumFromEncoded(sqlbase.DatumEncoding_ASCENDING_KEY, enc)
	ij.invertedColRow = append(ij.invertedColRow[:0], encDatum)
	span, _, err := ij.spanBuilder.SpanFromEncDatums(ij.invertedColRow, 1 /* prefixLen */)
	return span, err
}

func (ij *invertedJoiner) generateSpans(invertedSpans []invertedSpan) (roachpb.Spans, error) {
	var spans []roachpb.Span
	for _, span := range invertedSpans {
		startSpan, err := ij.generateSpan(span.Start)
		if err != nil {
			return nil, err
		}

		endSpan, err := ij.generateSpan(span.End)
		if err != nil {
			return nil, err
		}
		startSpan.EndKey = endSpan.Key
		spans = append(spans, startSpan)
	}
	return spans, nil
}

// Next is part of the RowSource interface.
func (ij *invertedJoiner) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The join is implemented as follows:
	// - Read the input rows in batches.
	// - For each batch, map the rows to SpanExpressionProtos and initialize
	//   a batchedInvertedExprEvaluator. Use that evaluator to generate spans
	//   to read from the inverted index.
	// - Retrieve the index rows and add the primary keys in these rows to the
	//   row container, that de-duplicates, and pass the de-duplicated keys to
	//   the batch evaluator.
	// - Retrieve the results from the batch evaluator and buffer in joinedRowIdx,
	//   and use the emitCursor to emit rows.
	for ij.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch ij.runningState {
		case ijReadingInput:
			ij.runningState, meta = ij.readInput()
		case ijPerformingIndexScan:
			ij.runningState, meta = ij.performScan()
		case ijEmittingRows:
			ij.runningState, row, meta = ij.emitRow()
		default:
			log.Fatalf(ij.Ctx, "unsupported state: %d", ij.runningState)
		}
		if row == nil && meta == nil {
			continue
		}
		if meta != nil {
			return nil, meta
		}
		if outRow := ij.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, ij.DrainHelper()
}

// readInput reads the next batch of input rows and starts an index scan.
func (ij *invertedJoiner) readInput() (invertedJoinerState, *execinfrapb.ProducerMetadata) {
	// Read the next batch of input rows.
	for len(ij.inputRows) < ij.batchSize {
		row, meta := ij.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ij.MoveToDraining(nil /* err */)
				return ijStateUnknown, meta
			}
			return ijReadingInput, meta
		}
		if row == nil {
			break
		}
		if row[ij.lookupColumnIdx].IsNull() &&
			(ij.joinType != sqlbase.LeftOuterJoin && ij.joinType != sqlbase.LeftAntiJoin) {
			// There is no expression to evaluate, so the evaluation will be the
			// empty set. And the join type will emit no row, so don't bother
			// copying the input row.
			ij.inputRows = append(ij.inputRows, nil)
		} else {
			ij.inputRows = append(ij.inputRows, ij.rowAlloc.CopyRow(row))
		}
		if row[ij.lookupColumnIdx].IsNull() {
			// No expression to evaluate. The nil serves as a marker that will
			// result in an empty set as the evaluation result.
			ij.batchedExprEval.exprs = append(ij.batchedExprEval.exprs, nil)
		} else {
			expr, err := ij.datumToInvertedExpr.Convert(ij.Ctx, row[ij.lookupColumnIdx])
			if err != nil {
				ij.MoveToDraining(err)
				return ijStateUnknown, ij.DrainHelper()
			}
			ij.batchedExprEval.exprs = append(ij.batchedExprEval.exprs, expr)
		}
	}

	if len(ij.inputRows) == 0 {
		log.VEventf(ij.Ctx, 1, "no more input rows")
		// We're done.
		ij.MoveToDraining(nil)
		return ijStateUnknown, ij.DrainHelper()
	}
	log.VEventf(ij.Ctx, 1, "read %d input rows", len(ij.inputRows))

	spans := ij.batchedExprEval.init()
	if len(spans) == 0 {
		// Nothing to scan. For each input row, place a nil slice in the joined
		// rows, for emitRow() to process.
		ij.joinedRowIdx = ij.joinedRowIdx[:0]
		for range ij.inputRows {
			ij.joinedRowIdx = append(ij.joinedRowIdx, nil)
		}
		return ijEmittingRows, nil
	}
	indexSpans, err := ij.generateSpans(spans)
	if err != nil {
		ij.MoveToDraining(err)
		return ijStateUnknown, ij.DrainHelper()
	}

	// Sort the spans for locality of reads.
	sort.Sort(indexSpans)
	log.VEventf(ij.Ctx, 1, "scanning %d spans", len(indexSpans))
	if err = ij.fetcher.StartScan(
		ij.Ctx, ij.FlowCtx.Txn, indexSpans, false /* limitBatches */, 0, /* limitHint */
		ij.FlowCtx.TraceKV); err != nil {
		ij.MoveToDraining(err)
		return ijStateUnknown, ij.DrainHelper()
	}

	return ijPerformingIndexScan, nil
}

func (ij *invertedJoiner) performScan() (invertedJoinerState, *execinfrapb.ProducerMetadata) {
	log.VEventf(ij.Ctx, 1, "joining rows")
	// Read the entire set of rows that are part of the scan.
	for {
		// Fetch the next row and copy it into the row container.
		scannedRow, _, _, err := ij.fetcher.NextRow(ij.Ctx)
		if err != nil {
			ij.MoveToDraining(scrub.UnwrapScrubError(err))
			return ijStateUnknown, ij.DrainHelper()
		}
		if scannedRow == nil {
			// Done with this input batch.
			break
		}
		encInvertedVal := scannedRow[ij.colIdxMap[ij.invertedColID]].EncodedBytes()
		ij.transformToKeyRow(scannedRow)
		rowIdx, err := ij.keyRows.AddRow(ij.Ctx, ij.keyRow)
		if err != nil {
			ij.MoveToDraining(err)
			return ijStateUnknown, ij.DrainHelper()
		}
		ij.batchedExprEval.addIndexRow(encInvertedVal, rowIdx)
	}
	ij.joinedRowIdx = ij.batchedExprEval.evaluate()
	ij.keyRows.SetupForRead(ij.Ctx, ij.joinedRowIdx)
	log.VEventf(ij.Ctx, 1, "done evaluating expressions")

	return ijEmittingRows, nil
}

// emitRow returns the next row from ij.emitCursor, if present. Otherwise it
// prepares for another input batch.
func (ij *invertedJoiner) emitRow() (
	invertedJoinerState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	// Finished processing the batch.
	if ij.emitCursor.inputRowIdx >= len(ij.joinedRowIdx) {
		log.VEventf(ij.Ctx, 1, "done emitting rows")
		// Ready for another input batch. Reset state.
		ij.inputRows = ij.inputRows[:0]
		ij.batchedExprEval.reset()
		ij.joinedRowIdx = nil
		ij.emitCursor.outputRowIdx = 0
		ij.emitCursor.inputRowIdx = 0
		ij.emitCursor.seenMatch = false
		if err := ij.keyRows.UnsafeReset(ij.Ctx); err != nil {
			ij.MoveToDraining(err)
			return ijStateUnknown, nil, ij.DrainHelper()
		}
		return ijReadingInput, nil, nil
	}

	// Reached the end of the matches for an input row. May need to emit for
	// LeftOuterJoin and LeftAntiJoin.
	if ij.emitCursor.outputRowIdx >= len(ij.joinedRowIdx[ij.emitCursor.inputRowIdx]) {
		inputRowIdx := ij.emitCursor.inputRowIdx
		seenMatch := ij.emitCursor.seenMatch
		ij.emitCursor.inputRowIdx++
		ij.emitCursor.outputRowIdx = 0
		ij.emitCursor.seenMatch = false

		if !seenMatch {
			switch ij.joinType {
			case sqlbase.LeftOuterJoin:
				return ijEmittingRows, ij.renderUnmatchedRow(ij.inputRows[inputRowIdx]), nil
			case sqlbase.LeftAntiJoin:
				return ijEmittingRows, ij.inputRows[inputRowIdx], nil
			}
		}
		return ijEmittingRows, nil, nil
	}

	inputRow := ij.inputRows[ij.emitCursor.inputRowIdx]
	joinedRowIdx := ij.joinedRowIdx[ij.emitCursor.inputRowIdx][ij.emitCursor.outputRowIdx]
	indexedRow, err := ij.keyRows.GetRow(ij.Ctx, joinedRowIdx, false /* skip */)
	if err != nil {
		ij.MoveToDraining(err)
		return ijStateUnknown, nil, ij.DrainHelper()
	}
	ij.emitCursor.outputRowIdx++
	ij.transformToTableRow(indexedRow)
	renderedRow, err := ij.render(inputRow, ij.tableRow)
	if err != nil {
		ij.MoveToDraining(err)
		return ijStateUnknown, nil, ij.DrainHelper()
	}
	skipRemaining := func() error {
		for ; ij.emitCursor.outputRowIdx < len(ij.joinedRowIdx[ij.emitCursor.inputRowIdx]); ij.emitCursor.outputRowIdx++ {
			idx := ij.joinedRowIdx[ij.emitCursor.inputRowIdx][ij.emitCursor.outputRowIdx]
			if _, err := ij.keyRows.GetRow(ij.Ctx, idx, true /* skip */); err != nil {
				return err
			}
		}
		return nil
	}
	if renderedRow != nil {
		ij.emitCursor.seenMatch = true
		switch ij.joinType {
		case sqlbase.InnerJoin, sqlbase.LeftOuterJoin:
			return ijEmittingRows, renderedRow, nil
		case sqlbase.LeftSemiJoin:
			// Skip the rest of the joined rows.
			if err := skipRemaining(); err != nil {
				ij.MoveToDraining(err)
				return ijStateUnknown, nil, ij.DrainHelper()
			}
			return ijEmittingRows, inputRow, nil
		case sqlbase.LeftAntiJoin:
			// Skip the rest of the joined rows.
			if err := skipRemaining(); err != nil {
				ij.MoveToDraining(err)
				return ijStateUnknown, nil, ij.DrainHelper()
			}
			ij.emitCursor.outputRowIdx = len(ij.joinedRowIdx[ij.emitCursor.inputRowIdx])
		}
	}
	return ijEmittingRows, nil, nil
}

// render constructs a row with columns from both sides. The ON condition is
// evaluated; if it fails, returns nil.
func (ij *invertedJoiner) render(lrow, rrow sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	ij.combinedRow = append(ij.combinedRow[:0], lrow...)
	ij.combinedRow = append(ij.combinedRow, rrow...)
	if ij.onExprHelper.Expr != nil {
		res, err := ij.onExprHelper.EvalFilter(ij.combinedRow)
		if !res || err != nil {
			return nil, err
		}
	}
	return ij.combinedRow, nil
}

// renderUnmatchedRow creates a result row given an unmatched row.
func (ij *invertedJoiner) renderUnmatchedRow(row sqlbase.EncDatumRow) sqlbase.EncDatumRow {
	ij.combinedRow = append(ij.combinedRow[:0], row...)
	ij.combinedRow = ij.combinedRow[:cap(ij.combinedRow)]
	for i := len(row); i < len(ij.combinedRow); i++ {
		ij.combinedRow[i].Datum = tree.DNull
	}
	return ij.combinedRow
}

func (ij *invertedJoiner) transformToKeyRow(row sqlbase.EncDatumRow) {
	for i, rowIdx := range ij.keyRowToTableRowMap {
		ij.keyRow[i] = row[rowIdx]
	}
}

func (ij *invertedJoiner) transformToTableRow(keyRow sqlbase.EncDatumRow) {
	for r, k := range ij.tableRowToKeyRowMap {
		ij.tableRow[r] = keyRow[k]
	}
}

// Start is part of the RowSource interface.
func (ij *invertedJoiner) Start(ctx context.Context) context.Context {
	ij.input.Start(ctx)
	ctx = ij.StartInternal(ctx, invertedJoinerProcName)
	ij.runningState = ijReadingInput
	return ctx
}

// ConsumerClosed is part of the RowSource interface.
func (ij *invertedJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ij.close()
}

func (ij *invertedJoiner) close() {
	if ij.InternalClose() {
		if ij.keyRows != nil {
			ij.keyRows.Close(ij.Ctx)
		}
		ij.MemMonitor.Stop(ij.Ctx)
		if ij.diskMonitor != nil {
			ij.diskMonitor.Stop(ij.Ctx)
		}
	}
}

var _ execinfrapb.DistSQLSpanStats = &InvertedJoinerStats{}

const invertedJoinerTagPrefix = "invertedjoiner."

// Stats implements the SpanStats interface.
func (ijs *InvertedJoinerStats) Stats() map[string]string {
	statsMap := ijs.InputStats.Stats(invertedJoinerTagPrefix)
	toMerge := ijs.IndexScanStats.Stats(invertedJoinerTagPrefix + "index.")
	for k, v := range toMerge {
		statsMap[k] = v
	}
	statsMap[invertedJoinerTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(ijs.MaxAllocatedMem)
	statsMap[invertedJoinerTagPrefix+MaxDiskTagSuffix] = humanizeutil.IBytes(ijs.MaxAllocatedDisk)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ijs *InvertedJoinerStats) StatsForQueryPlan() []string {
	stats := append(
		ijs.InputStats.StatsForQueryPlan(""),
		ijs.IndexScanStats.StatsForQueryPlan("index ")...,
	)
	if ijs.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(ijs.MaxAllocatedMem)))
	}
	if ijs.MaxAllocatedDisk != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxDiskQueryPlanSuffix, humanizeutil.IBytes(ijs.MaxAllocatedDisk)))
	}
	return stats
}

// outputStatsToTrace outputs the collected stats to the trace. Will
// fail silently if the invertedJoiner is not collecting stats.
func (ij *invertedJoiner) outputStatsToTrace() {
	is, ok := getInputStats(ij.FlowCtx, ij.input)
	if !ok {
		return
	}
	fis, ok := getFetcherInputStats(ij.FlowCtx, ij.fetcher)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(ij.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp,
			&InvertedJoinerStats{
				InputStats:       is,
				IndexScanStats:   fis,
				MaxAllocatedMem:  ij.MemMonitor.MaximumBytes(),
				MaxAllocatedDisk: ij.diskMonitor.MaximumBytes(),
			})
	}
}

func (ij *invertedJoiner) generateMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	if tfs := execinfra.GetLeafTxnFinalState(ctx, ij.FlowCtx.Txn); tfs != nil {
		return []execinfrapb.ProducerMetadata{{LeafTxnFinalState: tfs}}
	}
	return nil
}

// DrainMeta is part of the MetadataSource interface.
func (ij *invertedJoiner) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	return ij.generateMeta(ctx)
}

// ChildCount is part of the execinfra.OpNode interface.
func (ij *invertedJoiner) ChildCount(verbose bool) int {
	if _, ok := ij.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (ij *invertedJoiner) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := ij.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to invertedJoiner is not an execinfra.OpNode")
	}
	panic(fmt.Sprintf("invalid index %d", nth))
}
