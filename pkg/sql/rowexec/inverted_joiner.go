// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedidx"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/errors"
)

// TODO(sumeer): adjust this batch size dynamically to balance between the
// higher scan throughput of larger batches and the cost of spilling the
// scanned rows to disk. The spilling cost will probably be dominated by
// the de-duping cost, since it incurs a read.
var invertedJoinerBatchSize = metamorphic.ConstantWithTestValue(
	"inverted-joiner-batch-size",
	100, /* defaultValue */
	1,   /* metamorphicValue */
)

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

	fetchSpec fetchpb.IndexFetchSpec

	runningState        invertedJoinerState
	unlimitedMemMonitor *mon.BytesMonitor
	diskMonitor         *mon.BytesMonitor

	// prefixEqualityCols are the ordinals of the columns from the join input
	// that represent join values for the non-inverted prefix columns of
	// multi-column inverted indexes. The length is equal to the number of
	// non-inverted prefix columns of the index.
	prefixEqualityCols []uint32

	// invertedFetchedColOrdinal is the ordinal of the inverted key column among
	// the fetched columns.
	invertedFetchedColOrdinal int

	// prefixColOrdinals contains the ordinals of any prefix key columns among the
	// fetched columns (same length with prefixEqualityCols).
	prefixFetchedColOrdinals []int

	onExprHelper execinfrapb.ExprHelper
	combinedRow  rowenc.EncDatumRow

	joinType descpb.JoinType

	// fetcher wraps the row.Fetcher used to perform scans. This enables the
	// invertedJoiner to wrap the fetcher with a stat collector when necessary.
	fetcher rowFetcher

	// rowsRead is the total number of rows that the fetcher read from disk.
	rowsRead                  int64
	contentionEventsListener  execstats.ContentionEventsListener
	scanStatsListener         execstats.ScanStatsListener
	tenantConsumptionListener execstats.TenantConsumptionListener
	alloc                     tree.DatumAlloc
	rowAlloc                  rowenc.EncDatumRowAlloc

	// prefixKey is used to avoid reallocating a prefix key (when we have a
	// non-inverted prefix).
	prefixKey roachpb.Key

	// The input being joined using the index.
	input                execinfra.RowSource
	inputTypes           []*types.T
	datumsToInvertedExpr invertedexpr.DatumsToInvertedExpr
	canPreFilter         bool
	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSize int

	// State variables for each batch of input rows.
	inputRows       rowenc.EncDatumRows
	batchedExprEval batchedInvertedExprEvaluator
	// The row indexes that are the result of the inverted expression evaluation
	// of the join. These will be further filtered using the onExpr.
	joinedRowIdx [][]KeyIndex

	// The container for the index rows retrieved from the index. For evaluating
	// each inverted expression, which involved set unions and intersections, it
	// is necessary to de-duplicate the primary key rows retrieved from the
	// inverted index. Instead of doing such de-duplication for each expression
	// in the batch of expressions, it is done once when adding to indexRows --
	// this is more efficient since multiple expressions may be using the same
	// spans from the index. Note that De-duplicating by the entire index row,
	// which includes non-inverted prefix columns for multi-column inverted
	// indexes, is equivalent to de-duplicating by the PK because the all table
	// columns are functionally dependent on the PK.
	indexRows *rowcontainer.DiskBackedNumberedRowContainer

	// indexSpans are the roachpb.Spans generated based on the inverted spans.
	// The slice is reused between different input batches.
	// NB: the row fetcher takes ownership of the slice and deeply resets each
	// element of the slice once the fetcher is done with it, so we don't need
	// to do ourselves.
	indexSpans roachpb.Spans

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

	spanBuilder           span.Builder
	outputContinuationCol bool
}

var _ execinfra.Processor = &invertedJoiner{}
var _ execinfra.RowSource = &invertedJoiner{}
var _ execopnode.OpNode = &invertedJoiner{}

const invertedJoinerProcName = "inverted joiner"

// newInvertedJoiner constructs an invertedJoiner. The datumsToInvertedExpr
// argument is non-nil only for tests. When nil, the invertedJoiner uses
// the spec to construct an implementation of DatumsToInvertedExpr.
func newInvertedJoiner(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.InvertedJoinerSpec,
	datumsToInvertedExpr invertedexpr.DatumsToInvertedExpr,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (execinfra.RowSourcedProcessor, error) {
	switch spec.Type {
	case descpb.InnerJoin, descpb.LeftOuterJoin, descpb.LeftSemiJoin, descpb.LeftAntiJoin:
	default:
		return nil, errors.AssertionFailedf("unexpected inverted join type %s", spec.Type)
	}
	ij := &invertedJoiner{
		fetchSpec:            spec.FetchSpec,
		input:                input,
		inputTypes:           input.OutputTypes(),
		prefixEqualityCols:   spec.PrefixEqualityColumns,
		datumsToInvertedExpr: datumsToInvertedExpr,
		joinType:             spec.Type,
		batchSize:            invertedJoinerBatchSize,
	}

	var err error
	ij.invertedFetchedColOrdinal, err = findInvertedFetchedColOrdinal(&spec.FetchSpec)
	if err != nil {
		return nil, err
	}
	ij.prefixFetchedColOrdinals = make([]int, len(ij.prefixEqualityCols))
	for i := range ij.prefixFetchedColOrdinals {
		id := spec.FetchSpec.KeyAndSuffixColumns[i].ColumnID
		ij.prefixFetchedColOrdinals[i], err = findFetchedColOrdinal(&spec.FetchSpec, id)
		if err != nil {
			return nil, err
		}
	}

	outputColCount := len(ij.inputTypes)

	rightColTypes := spec.FetchSpec.FetchedColumnTypes()

	// Inverted joins are not used for mutations.
	var includeRightCols bool
	if ij.joinType == descpb.InnerJoin || ij.joinType == descpb.LeftOuterJoin {
		outputColCount += len(rightColTypes)
		includeRightCols = true
		if spec.OutputGroupContinuationForLeftRow {
			outputColCount++
		}
	}
	outputColTypes := make([]*types.T, 0, outputColCount)
	outputColTypes = append(outputColTypes, ij.inputTypes...)
	if includeRightCols {
		outputColTypes = append(outputColTypes, rightColTypes...)
		if spec.OutputGroupContinuationForLeftRow {
			outputColTypes = append(outputColTypes, types.Bool)
		}
	}

	// Make sure the key column types are hydrated. The fetched column types
	// will be hydrated in ProcessorBase.Init below.
	resolver := flowCtx.NewTypeResolver(flowCtx.Txn)
	for i := range spec.FetchSpec.KeyAndSuffixColumns {
		if err := typedesc.EnsureTypeIsHydrated(
			ctx, spec.FetchSpec.KeyAndSuffixColumns[i].Type, &resolver,
		); err != nil {
			return nil, err
		}
	}

	// Always make a copy of the eval context since it might be mutated later.
	evalCtx := flowCtx.NewEvalCtx()
	if err := ij.ProcessorBase.InitWithEvalCtx(
		ctx, ij, post, outputColTypes, flowCtx, evalCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{ij.input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				// We need to generate metadata before closing the processor
				// because InternalClose() updates ij.Ctx to the "original"
				// context.
				trailingMeta := ij.generateMeta()
				ij.close()
				return trailingMeta
			},
		},
	); err != nil {
		return nil, err
	}

	semaCtx := flowCtx.NewSemaContext(flowCtx.Txn)
	onExprColTypes := make([]*types.T, 0, len(ij.inputTypes)+len(rightColTypes))
	onExprColTypes = append(onExprColTypes, ij.inputTypes...)
	onExprColTypes = append(onExprColTypes, rightColTypes...)

	// The ON expression refers to the inverted key column but expects its type to
	// be the original column type (e.g. JSON), not EncodedKey.
	// TODO(radu): this is sketchy. The ON expression should use internal
	// functions that are properly typed. See the corresponding comment in
	// execbuilder.Builder.buildInvertedJoin.
	onExprColTypes[len(ij.inputTypes)+ij.invertedFetchedColOrdinal] = spec.InvertedColumnOriginalType

	if err := ij.onExprHelper.Init(ctx, spec.OnExpr, onExprColTypes, semaCtx, evalCtx); err != nil {
		return nil, err
	}
	combinedRowLen := len(onExprColTypes)
	if spec.OutputGroupContinuationForLeftRow {
		combinedRowLen++
	}
	ij.combinedRow = make(rowenc.EncDatumRow, 0, combinedRowLen)

	if ij.datumsToInvertedExpr == nil {
		var invertedExprHelper execinfrapb.ExprHelper
		if err := invertedExprHelper.Init(ctx, spec.InvertedExpr, onExprColTypes, semaCtx, evalCtx); err != nil {
			return nil, err
		}
		ij.datumsToInvertedExpr, err = invertedidx.NewDatumsToInvertedExpr(
			ctx, evalCtx, onExprColTypes, invertedExprHelper.Expr(), ij.fetchSpec.GeoConfig,
		)
		if err != nil {
			return nil, err
		}
	}
	ij.canPreFilter = ij.datumsToInvertedExpr.CanPreFilter()
	if ij.canPreFilter {
		ij.batchedExprEval.filterer = ij.datumsToInvertedExpr
	}

	var fetcher row.Fetcher
	if err := fetcher.Init(
		ctx,
		row.FetcherInitArgs{
			Txn:                        flowCtx.Txn,
			LockStrength:               spec.LockingStrength,
			LockWaitPolicy:             spec.LockingWaitPolicy,
			LockDurability:             spec.LockingDurability,
			LockTimeout:                flowCtx.EvalCtx.SessionData().LockTimeout,
			DeadlockTimeout:            flowCtx.EvalCtx.SessionData().DeadlockTimeout,
			Alloc:                      &ij.alloc,
			MemMonitor:                 flowCtx.Mon,
			Spec:                       &spec.FetchSpec,
			TraceKV:                    flowCtx.TraceKV,
			ForceProductionKVBatchSize: flowCtx.EvalCtx.TestingKnobs.ForceProductionValues,
		},
	); err != nil {
		return nil, err
	}

	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
		ij.input = newInputStatCollector(ij.input)
		ij.fetcher = newRowFetcherStatCollector(&fetcher)
		ij.ExecStatsForTrace = ij.execStatsForTrace
	} else {
		ij.fetcher = &fetcher
	}

	ij.spanBuilder.InitWithFetchSpec(flowCtx.EvalCtx, flowCtx.Codec(), &ij.fetchSpec)

	// Initialize memory monitors and row container for index rows.
	ij.MemMonitor = execinfra.NewLimitedMonitor(ctx, flowCtx.Mon, flowCtx, "invertedjoiner-limited")
	ij.unlimitedMemMonitor = execinfra.NewMonitor(ctx, flowCtx.Mon, "invertedjoiner-unlimited")
	ij.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.DiskMonitor, "invertedjoiner-disk")
	ij.indexRows = rowcontainer.NewDiskBackedNumberedRowContainer(
		true, /* deDup */
		rightColTypes,
		ij.FlowCtx.EvalCtx,
		ij.FlowCtx.Cfg.TempStorage,
		ij.MemMonitor,
		ij.unlimitedMemMonitor,
		ij.diskMonitor,
	)

	ij.outputContinuationCol = spec.OutputGroupContinuationForLeftRow

	return ij, nil
}

// findFetchedColOrdinal finds the ordinal into fetchSpec.FetchedColumns for the
// column with the given ID.
func findFetchedColOrdinal(
	fetchSpec *fetchpb.IndexFetchSpec, id descpb.ColumnID,
) (ordinal int, _ error) {
	for i := range fetchSpec.FetchedColumns {
		if fetchSpec.FetchedColumns[i].ColumnID == id {
			return i, nil
		}
	}
	return -1, errors.AssertionFailedf("inverted joiner fetched columns must contain column %d", id)
}

// findInvertedFetchedColOrdinal finds the ordinal into fetchSpec.FetchedColumns for the
// inverted key column.
func findInvertedFetchedColOrdinal(fetchSpec *fetchpb.IndexFetchSpec) (ordinal int, _ error) {
	for i := range fetchSpec.KeyAndSuffixColumns {
		if c := &fetchSpec.KeyAndSuffixColumns[i]; c.IsInverted {
			return findFetchedColOrdinal(fetchSpec, c.ColumnID)
		}
	}
	return -1, errors.AssertionFailedf("no inverted key column")
}

// SetBatchSize sets the desired batch size. It should only be used in tests.
func (ij *invertedJoiner) SetBatchSize(batchSize int) {
	ij.batchSize = batchSize
}

// Next is part of the RowSource interface.
func (ij *invertedJoiner) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
		var row rowenc.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch ij.runningState {
		case ijReadingInput:
			ij.runningState, meta = ij.readInput()
		case ijPerformingIndexScan:
			ij.runningState, meta = ij.performScan()
		case ijEmittingRows:
			ij.runningState, row, meta = ij.emitRow()
		default:
			log.Fatalf(ij.Ctx(), "unsupported state: %d", ij.runningState)
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

		expr, preFilterState, err := ij.datumsToInvertedExpr.Convert(ij.Ctx(), row)
		if err != nil {
			ij.MoveToDraining(err)
			return ijStateUnknown, ij.DrainHelper()
		}
		if expr == nil &&
			(ij.joinType != descpb.LeftOuterJoin && ij.joinType != descpb.LeftAntiJoin) {
			// One of the input columns was NULL, resulting in a nil expression.
			// The join type will emit no row since the evaluation result will be
			// an empty set, so don't bother copying the input row.
			ij.inputRows = append(ij.inputRows, nil)
		} else {
			ij.inputRows = append(ij.inputRows, ij.rowAlloc.CopyRow(row))
		}
		if expr == nil {
			// One of the input columns was NULL, resulting in a nil expression.
			// The nil serves as a marker that will result in an empty set as the
			// evaluation result.
			ij.batchedExprEval.exprs = append(ij.batchedExprEval.exprs, nil)
			if ij.canPreFilter {
				ij.batchedExprEval.preFilterState = append(ij.batchedExprEval.preFilterState, nil)
			}
		} else {
			ij.batchedExprEval.exprs = append(ij.batchedExprEval.exprs, expr)
			if ij.canPreFilter {
				ij.batchedExprEval.preFilterState = append(ij.batchedExprEval.preFilterState, preFilterState)
			}
		}
		if len(ij.prefixEqualityCols) > 0 {
			if expr == nil {
				// One of the input columns was NULL, resulting in a nil expression.
				// The join type will emit no row since the evaluation result will be
				// an empty set, so don't bother creating a prefix key span.
				ij.batchedExprEval.nonInvertedPrefixes = append(ij.batchedExprEval.nonInvertedPrefixes, roachpb.Key{})
			} else {
				// Encode the prefix key; we reuse a buffer to avoid extra allocations
				// when appending values.
				ij.prefixKey = ij.prefixKey[:0]
				for i, inputOrd := range ij.prefixEqualityCols {
					if err := ij.appendPrefixColumn(&ij.fetchSpec.KeyAndSuffixColumns[i], row[inputOrd]); err != nil {
						ij.MoveToDraining(err)
						return ijStateUnknown, ij.DrainHelper()
					}
				}
				ij.batchedExprEval.appendNonInvertedPrefix(ij.prefixKey)
			}
		}
	}

	if len(ij.inputRows) == 0 {
		log.VEventf(ij.Ctx(), 1, "no more input rows")
		// We're done.
		ij.MoveToDraining(nil)
		return ijStateUnknown, ij.DrainHelper()
	}
	log.VEventf(ij.Ctx(), 1, "read %d input rows", len(ij.inputRows))

	spans, err := ij.batchedExprEval.init()
	if err != nil {
		ij.MoveToDraining(err)
		return ijStateUnknown, ij.DrainHelper()
	}
	if len(spans) == 0 {
		// Nothing to scan. For each input row, place a nil slice in the joined
		// rows, for emitRow() to process.
		ij.joinedRowIdx = ij.joinedRowIdx[:0]
		for range ij.inputRows {
			ij.joinedRowIdx = append(ij.joinedRowIdx, nil)
		}
		return ijEmittingRows, nil
	}
	// NB: spans is already sorted, and that sorting is preserved when
	// generating ij.indexSpans.
	ij.indexSpans, err = ij.spanBuilder.SpansFromInvertedSpans(ij.Ctx(), spans, nil /* constraint */, ij.indexSpans)
	if err != nil {
		ij.MoveToDraining(err)
		return ijStateUnknown, ij.DrainHelper()
	}

	log.VEventf(ij.Ctx(), 1, "scanning %d spans", len(ij.indexSpans))
	if err = ij.fetcher.StartScan(
		ij.Ctx(), ij.indexSpans, nil, /* spanIDs */
		rowinfra.NoBytesLimit, rowinfra.NoRowLimit,
	); err != nil {
		ij.MoveToDraining(err)
		return ijStateUnknown, ij.DrainHelper()
	}

	return ijPerformingIndexScan, nil
}

func (ij *invertedJoiner) performScan() (invertedJoinerState, *execinfrapb.ProducerMetadata) {
	log.VEventf(ij.Ctx(), 1, "joining rows")
	// Read the entire set of rows that are part of the scan.
	for {
		// Fetch the next row and copy it into the row container.
		fetchedRow, _, err := ij.fetcher.NextRow(ij.Ctx())
		if err != nil {
			ij.MoveToDraining(scrub.UnwrapScrubError(err))
			return ijStateUnknown, ij.DrainHelper()
		}
		if fetchedRow == nil {
			// Done with this input batch.
			break
		}
		ij.rowsRead++

		// Inverted columns are custom encoded in a manner that does not correspond
		// to usual Datum encoding, and as such the encoded data is passed around
		// as-is (using the special EncodedKey type). We could use EnsureDecoded()
		// and then use the bytes from the DEncodedKey datum, but it is more
		// efficient to use the EncodedBytes() directly (we know the encoded bytes
		// are available because the row was produced by the fetcher).
		encInvertedVal := fetchedRow[ij.invertedFetchedColOrdinal].EncodedBytes()
		// The indexRows container de-duplicates along all columns. We do not want
		// to de-duplicate along the inverted column, which can take multiple values
		// for a single table row. We will not need the value anymore, so we can set
		// it to NULL here.
		// TODO(radu): we should only need to de-dup on the PK columns.
		fetchedRow[ij.invertedFetchedColOrdinal] = rowenc.EncDatum{Datum: tree.DNull}
		var encFullVal []byte
		if len(ij.prefixEqualityCols) > 0 {
			ij.prefixKey = ij.prefixKey[:0]
			for i, ord := range ij.prefixFetchedColOrdinals {
				if err := ij.appendPrefixColumn(&ij.fetchSpec.KeyAndSuffixColumns[i], fetchedRow[ord]); err != nil {
					ij.MoveToDraining(err)
					return ijStateUnknown, ij.DrainHelper()
				}
			}
			// We append an encoded inverted value/datum to the key prefix
			// representing the non-inverted prefix columns, to generate the key
			// for the inverted index. This is similar to the internals of
			// rowenc.appendEncDatumsToKey.
			encFullVal = append(ij.prefixKey, encInvertedVal...)
		}
		shouldAdd, err := ij.batchedExprEval.prepareAddIndexRow(encInvertedVal, encFullVal)
		if err != nil {
			ij.MoveToDraining(err)
			return ijStateUnknown, ij.DrainHelper()
		}
		if shouldAdd {
			rowIdx, err := ij.indexRows.AddRow(ij.Ctx(), fetchedRow)
			if err != nil {
				ij.MoveToDraining(err)
				return ijStateUnknown, ij.DrainHelper()
			}
			if err = ij.batchedExprEval.addIndexRow(rowIdx); err != nil {
				ij.MoveToDraining(err)
				return ijStateUnknown, ij.DrainHelper()
			}
		}
	}
	ij.joinedRowIdx = ij.batchedExprEval.evaluate()
	ij.indexRows.SetupForRead(ij.Ctx(), ij.joinedRowIdx)
	log.VEventf(ij.Ctx(), 1, "done evaluating expressions")

	return ijEmittingRows, nil
}

var trueEncDatum = rowenc.DatumToEncDatum(types.Bool, tree.DBoolTrue)
var falseEncDatum = rowenc.DatumToEncDatum(types.Bool, tree.DBoolFalse)

// emitRow returns the next row from ij.emitCursor, if present. Otherwise it
// prepares for another input batch.
func (ij *invertedJoiner) emitRow() (
	invertedJoinerState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	// Finished processing the batch.
	if ij.emitCursor.inputRowIdx >= len(ij.joinedRowIdx) {
		log.VEventf(ij.Ctx(), 1, "done emitting rows")
		// Ready for another input batch. Reset state.
		ij.inputRows = ij.inputRows[:0]
		ij.batchedExprEval.reset()
		ij.joinedRowIdx = nil
		ij.emitCursor.outputRowIdx = 0
		ij.emitCursor.inputRowIdx = 0
		ij.emitCursor.seenMatch = false
		if err := ij.indexRows.UnsafeReset(ij.Ctx()); err != nil {
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
			case descpb.LeftOuterJoin:
				ij.renderUnmatchedRow(ij.inputRows[inputRowIdx])
				return ijEmittingRows, ij.combinedRow, nil
			case descpb.LeftAntiJoin:
				return ijEmittingRows, ij.inputRows[inputRowIdx], nil
			}
		}
		return ijEmittingRows, nil, nil
	}

	inputRow := ij.inputRows[ij.emitCursor.inputRowIdx]
	joinedRowIdx := ij.joinedRowIdx[ij.emitCursor.inputRowIdx][ij.emitCursor.outputRowIdx]
	indexedRow, err := ij.indexRows.GetRow(ij.Ctx(), joinedRowIdx, false /* skip */)
	if err != nil {
		ij.MoveToDraining(err)
		return ijStateUnknown, nil, ij.DrainHelper()
	}
	ij.emitCursor.outputRowIdx++
	renderedRow, err := ij.render(inputRow, indexedRow)
	if err != nil {
		ij.MoveToDraining(err)
		return ijStateUnknown, nil, ij.DrainHelper()
	}
	skipRemaining := func() error {
		for ; ij.emitCursor.outputRowIdx < len(ij.joinedRowIdx[ij.emitCursor.inputRowIdx]); ij.emitCursor.outputRowIdx++ {
			idx := ij.joinedRowIdx[ij.emitCursor.inputRowIdx][ij.emitCursor.outputRowIdx]
			if _, err := ij.indexRows.GetRow(ij.Ctx(), idx, true /* skip */); err != nil {
				return err
			}
		}
		return nil
	}
	if renderedRow != nil {
		seenMatch := ij.emitCursor.seenMatch
		ij.emitCursor.seenMatch = true
		switch ij.joinType {
		case descpb.InnerJoin, descpb.LeftOuterJoin:
			if ij.outputContinuationCol {
				if seenMatch {
					// This is not the first row output for this left row, so set the
					// group continuation to true.
					ij.combinedRow = append(ij.combinedRow, trueEncDatum)
				} else {
					// This is the first row output for this left row, so set the group
					// continuation to false.
					ij.combinedRow = append(ij.combinedRow, falseEncDatum)
				}
				renderedRow = ij.combinedRow
			}
			return ijEmittingRows, renderedRow, nil
		case descpb.LeftSemiJoin:
			// Skip the rest of the joined rows.
			if err := skipRemaining(); err != nil {
				ij.MoveToDraining(err)
				return ijStateUnknown, nil, ij.DrainHelper()
			}
			return ijEmittingRows, inputRow, nil
		case descpb.LeftAntiJoin:
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
// evaluated; if it fails, returns nil. When it returns a non-nil row, it is
// identical to ij.combinedRow.
func (ij *invertedJoiner) render(lrow, rrow rowenc.EncDatumRow) (rowenc.EncDatumRow, error) {
	ij.combinedRow = append(ij.combinedRow[:0], lrow...)
	ij.combinedRow = append(ij.combinedRow, rrow...)
	if ij.onExprHelper.Expr() != nil {
		res, err := ij.onExprHelper.EvalFilter(ij.Ctx(), ij.combinedRow)
		if !res || err != nil {
			return nil, err
		}
	}
	return ij.combinedRow, nil
}

// renderUnmatchedRow creates a result row given an unmatched row and
// stores it in ij.combinedRow.
func (ij *invertedJoiner) renderUnmatchedRow(row rowenc.EncDatumRow) {
	ij.combinedRow = ij.combinedRow[:cap(ij.combinedRow)]
	// Copy the left row.
	copy(ij.combinedRow, row)
	// Set the remaining columns to NULL.
	for i := len(row); i < len(ij.combinedRow); i++ {
		ij.combinedRow[i].Datum = tree.DNull
	}
	if ij.outputContinuationCol {
		// The last column is the continuation column, so set it to false since
		// this is the only output row for this group.
		ij.combinedRow[len(ij.combinedRow)-1] = falseEncDatum
	}
}

// appendPrefixColumn encodes a datum corresponding to an index prefix column
// and appends it to ij.prefixKey.
func (ij *invertedJoiner) appendPrefixColumn(
	keyCol *fetchpb.IndexFetchSpec_KeyColumn, encDatum rowenc.EncDatum,
) error {
	var err error
	ij.prefixKey, err = encDatum.Encode(keyCol.Type, &ij.alloc, keyCol.DatumEncoding(), ij.prefixKey)
	return err
}

// Start is part of the RowSource interface.
func (ij *invertedJoiner) Start(ctx context.Context) {
	ctx = ij.StartInternal(
		ctx, invertedJoinerProcName, &ij.contentionEventsListener,
		&ij.scanStatsListener, &ij.tenantConsumptionListener,
	)
	ij.input.Start(ctx)
	ij.runningState = ijReadingInput
}

// ConsumerClosed is part of the RowSource interface.
func (ij *invertedJoiner) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ij.close()
}

func (ij *invertedJoiner) close() {
	if ij.InternalClose() {
		if ij.fetcher != nil {
			ij.fetcher.Close(ij.Ctx())
		}
		if ij.indexRows != nil {
			ij.indexRows.Close(ij.Ctx())
		}
		ij.MemMonitor.Stop(ij.Ctx())
		if ij.unlimitedMemMonitor != nil {
			ij.unlimitedMemMonitor.Stop(ij.Ctx())
		}
		if ij.diskMonitor != nil {
			ij.diskMonitor.Stop(ij.Ctx())
		}
	}
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (ij *invertedJoiner) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(ij.input)
	if !ok {
		return nil
	}
	fis, ok := getFetcherInputStats(ij.fetcher)
	if !ok {
		return nil
	}
	ret := execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		KV: execinfrapb.KVStats{
			BytesRead:           optional.MakeUint(uint64(ij.fetcher.GetBytesRead())),
			KVPairsRead:         optional.MakeUint(uint64(ij.fetcher.GetKVPairsRead())),
			TuplesRead:          fis.NumTuples,
			KVTime:              fis.WaitTime,
			ContentionTime:      optional.MakeTimeValue(ij.contentionEventsListener.GetContentionTime()),
			BatchRequestsIssued: optional.MakeUint(uint64(ij.fetcher.GetBatchRequestsIssued())),
			KVCPUTime:           optional.MakeTimeValue(fis.kvCPUTime),
		},
		Exec: execinfrapb.ExecStats{
			MaxAllocatedMem:  optional.MakeUint(uint64(ij.MemMonitor.MaximumBytes() + ij.unlimitedMemMonitor.MaximumBytes())),
			MaxAllocatedDisk: optional.MakeUint(uint64(ij.diskMonitor.MaximumBytes())),
		},
		Output: ij.OutputHelper.Stats(),
	}
	ret.Exec.ConsumedRU = optional.MakeUint(ij.tenantConsumptionListener.GetConsumedRU())
	scanStats := ij.scanStatsListener.GetScanStats()
	execstats.PopulateKVMVCCStats(&ret.KV, &scanStats)
	return &ret
}

func (ij *invertedJoiner) generateMeta() []execinfrapb.ProducerMetadata {
	trailingMeta := make([]execinfrapb.ProducerMetadata, 1, 2)
	meta := &trailingMeta[0]
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.BytesRead = ij.fetcher.GetBytesRead()
	meta.Metrics.RowsRead = ij.rowsRead
	if tfs := execinfra.GetLeafTxnFinalState(ij.Ctx(), ij.FlowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	return trailingMeta
}

// ChildCount is part of the execopnode.OpNode interface.
func (ij *invertedJoiner) ChildCount(verbose bool) int {
	if _, ok := ij.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (ij *invertedJoiner) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := ij.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to invertedJoiner is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
