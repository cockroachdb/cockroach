// Copyright 2016 The Cockroach Authors.
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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// joinReaderState represents the state of the processor.
type joinReaderState int

const (
	jrStateUnknown joinReaderState = iota
	// jrReadingInput means that a batch of rows is being read from the input.
	jrReadingInput
	// jrPerformingLookup means we are performing an index lookup for the current
	// input row batch.
	jrPerformingLookup
	// jrEmittingRows means we are emitting the results of the index lookup.
	jrEmittingRows
)

// joinReaderType represents the type of join being used.
type joinReaderType int

const (
	// lookupJoinReaderType means we are performing a lookup join.
	lookupJoinReaderType joinReaderType = iota
	// indexJoinReaderType means we are performing an index join.
	indexJoinReaderType
)

// joinReader performs a lookup join between `input` and the specified `index`.
// `lookupCols` specifies the input columns which will be used for the index
// lookup.
type joinReader struct {
	joinerBase
	strategy joinReaderStrategy

	// runningState represents the state of the joinReader. This is in addition to
	// ProcessorBase.State - the runningState is only relevant when
	// ProcessorBase.State == StateRunning.
	runningState joinReaderState

	diskMonitor *mon.BytesMonitor

	desc             tabledesc.Immutable
	index            *descpb.IndexDescriptor
	colIdxMap        map[descpb.ColumnID]int
	maintainOrdering bool

	// fetcher wraps the row.Fetcher used to perform lookups. This enables the
	// joinReader to wrap the fetcher with a stat collector when necessary.
	fetcher            rowFetcher
	alloc              rowenc.DatumAlloc
	rowAlloc           rowenc.EncDatumRowAlloc
	shouldLimitBatches bool
	readerType         joinReaderType

	input      execinfra.RowSource
	inputTypes []*types.T
	// Column indexes in the input stream specifying the columns which match with
	// the index columns. These are the equality columns of the join.
	lookupCols []uint32

	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSizeBytes    int64
	curBatchSizeBytes int64

	// rowsRead is the total number of rows that this fetcher read from
	// disk.
	rowsRead int64

	// State variables for each batch of input rows.
	scratchInputRows rowenc.EncDatumRows
}

var _ execinfra.Processor = &joinReader{}
var _ execinfra.RowSource = &joinReader{}
var _ execinfrapb.MetadataSource = &joinReader{}
var _ execinfra.OpNode = &joinReader{}
var _ execinfra.IOReader = &joinReader{}

const joinReaderProcName = "join reader"

// newJoinReader returns a new joinReader.
func newJoinReader(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.JoinReaderSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	readerType joinReaderType,
) (execinfra.RowSourcedProcessor, error) {
	if spec.IndexIdx != 0 && readerType == indexJoinReaderType {
		return nil, errors.AssertionFailedf("index join must be against primary index")
	}

	var lookupCols []uint32
	switch readerType {
	case indexJoinReaderType:
		pkIDs := spec.Table.PrimaryIndex.ColumnIDs
		lookupCols = make([]uint32, len(pkIDs))
		for i := range pkIDs {
			lookupCols[i] = uint32(i)
		}
	case lookupJoinReaderType:
		lookupCols = spec.LookupColumns
	default:
		return nil, errors.Errorf("unsupported joinReaderType")
	}
	jr := &joinReader{
		desc:             tabledesc.MakeImmutable(spec.Table),
		maintainOrdering: spec.MaintainOrdering,
		input:            input,
		inputTypes:       input.OutputTypes(),
		lookupCols:       lookupCols,
	}

	var err error
	var isSecondary bool
	jr.index, isSecondary, err = jr.desc.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		return nil, err
	}
	returnMutations := spec.Visibility == execinfra.ScanVisibilityPublicAndNotPublic
	jr.colIdxMap = jr.desc.ColumnIdxMapWithMutations(returnMutations)

	columnIDs, _ := jr.index.FullColumnIDs()
	indexCols := make([]uint32, len(columnIDs))
	columnTypes := jr.desc.ColumnTypesWithMutations(returnMutations)
	for i, columnID := range columnIDs {
		indexCols[i] = uint32(columnID)
	}

	// If the lookup columns form a key, there is only one result per lookup, so the fetcher
	// should parallelize the key lookups it performs.
	jr.shouldLimitBatches = !spec.LookupColumnsAreKey && readerType == lookupJoinReaderType
	jr.readerType = readerType

	// Add all requested system columns to the output.
	var sysColDescs []descpb.ColumnDescriptor
	if spec.HasSystemColumns {
		sysColDescs = colinfo.AllSystemColumnDescs
	}
	for i := range sysColDescs {
		columnTypes = append(columnTypes, sysColDescs[i].Type)
		jr.colIdxMap[sysColDescs[i].ID] = len(jr.colIdxMap)
	}

	var leftTypes []*types.T
	var leftEqCols []uint32
	switch readerType {
	case indexJoinReaderType:
		// Index join performs a join between a secondary index, the `input`,
		// and the primary index of the same table, `desc`, to retrieve columns
		// which are not stored in the secondary index. It outputs the looked
		// up rows as is (meaning that the output rows before post-processing
		// will contain all columns from the table) whereas the columns that
		// came from the secondary index (input rows) are ignored. As a result,
		// we leave leftTypes as empty.
		leftEqCols = indexCols
	case lookupJoinReaderType:
		leftTypes = input.OutputTypes()
		leftEqCols = jr.lookupCols
	default:
		return nil, errors.Errorf("unsupported joinReaderType")
	}

	if err := jr.joinerBase.init(
		jr,
		flowCtx,
		processorID,
		leftTypes,
		columnTypes,
		spec.Type,
		spec.OnExpr,
		leftEqCols,
		indexCols,
		0, /* numMergedColumns */
		post,
		output,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{jr.input},
			TrailingMetaCallback: func(ctx context.Context) []execinfrapb.ProducerMetadata {
				jr.close()
				return jr.generateMeta(ctx)
			},
		},
	); err != nil {
		return nil, err
	}

	collectingStats := false
	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		collectingStats = true
	}

	neededRightCols := jr.neededRightCols()
	if isSecondary && !neededRightCols.SubsetOf(getIndexColSet(jr.index, jr.colIdxMap)) {
		return nil, errors.Errorf("joinreader index does not cover all columns")
	}

	var fetcher row.Fetcher
	var rightCols util.FastIntSet
	switch readerType {
	case indexJoinReaderType:
		rightCols = jr.Out.NeededColumns()
	case lookupJoinReaderType:
		rightCols = neededRightCols
	default:
		return nil, errors.Errorf("unsupported joinReaderType")
	}

	_, _, err = initRowFetcher(
		flowCtx, &fetcher, &jr.desc, int(spec.IndexIdx), jr.colIdxMap, false, /* reverse */
		rightCols, false /* isCheck */, jr.EvalCtx.Mon, &jr.alloc, spec.Visibility, spec.LockingStrength,
		spec.LockingWaitPolicy, sysColDescs,
	)

	if err != nil {
		return nil, err
	}
	if collectingStats {
		jr.input = newInputStatCollector(jr.input)
		jr.fetcher = newRowFetcherStatCollector(&fetcher)
		jr.FinishTrace = jr.outputStatsToTrace
	} else {
		jr.fetcher = &fetcher
	}

	jr.initJoinReaderStrategy(flowCtx, columnTypes, len(columnIDs), rightCols, readerType)
	jr.batchSizeBytes = jr.strategy.getLookupRowsBatchSizeHint()

	// TODO(radu): verify the input types match the index key types
	return jr, nil
}

func (jr *joinReader) initJoinReaderStrategy(
	flowCtx *execinfra.FlowCtx,
	typs []*types.T,
	numKeyCols int,
	neededRightCols util.FastIntSet,
	readerType joinReaderType,
) {
	spanBuilder := span.MakeBuilder(flowCtx.Codec(), &jr.desc, jr.index)
	spanBuilder.SetNeededColumns(neededRightCols)

	var keyToInputRowIndices map[string][]int
	if readerType != indexJoinReaderType {
		keyToInputRowIndices = make(map[string][]int)
	}
	// Else: see the comment in defaultSpanGenerator on why we don't need
	// this map for index joins.
	spanGenerator := defaultSpanGenerator{
		spanBuilder:          spanBuilder,
		keyToInputRowIndices: keyToInputRowIndices,
		numKeyCols:           numKeyCols,
		lookupCols:           jr.lookupCols,
	}
	if readerType == indexJoinReaderType {
		jr.strategy = &joinReaderIndexJoinStrategy{
			joinerBase:           &jr.joinerBase,
			defaultSpanGenerator: spanGenerator,
		}
		return
	}

	if !jr.maintainOrdering {
		jr.strategy = &joinReaderNoOrderingStrategy{
			joinerBase:           &jr.joinerBase,
			defaultSpanGenerator: spanGenerator,
			isPartialJoin:        jr.joinType == descpb.LeftSemiJoin || jr.joinType == descpb.LeftAntiJoin,
		}
		return
	}

	ctx := flowCtx.EvalCtx.Ctx()
	// Limit the memory use by creating a child monitor with a hard limit.
	// joinReader will overflow to disk if this limit is not enough.
	limit := execinfra.GetWorkMemLimit(flowCtx.Cfg)
	if flowCtx.Cfg.TestingKnobs.ForceDiskSpill {
		limit = 1
	}
	// Initialize memory monitors and row container for looked up rows.
	jr.MemMonitor = execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, "joinreader-limited")
	jr.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "joinreader-disk")
	drc := rowcontainer.NewDiskBackedNumberedRowContainer(
		false, /* deDup */
		typs,
		jr.EvalCtx,
		jr.FlowCtx.Cfg.TempStorage,
		jr.MemMonitor,
		jr.diskMonitor,
	)
	if limit < mon.DefaultPoolAllocationSize {
		// The memory limit is too low for caching, most likely to force disk
		// spilling for testing.
		drc.DisableCache = true
	}
	jr.strategy = &joinReaderOrderingStrategy{
		joinerBase:           &jr.joinerBase,
		defaultSpanGenerator: spanGenerator,
		isPartialJoin:        jr.joinType == descpb.LeftSemiJoin || jr.joinType == descpb.LeftAntiJoin,
		lookedUpRows:         drc,
	}
}

// getIndexColSet returns a set of all column indices for the given index.
func getIndexColSet(
	index *descpb.IndexDescriptor, colIdxMap map[descpb.ColumnID]int,
) util.FastIntSet {
	cols := util.MakeFastIntSet()
	err := index.RunOverAllColumns(func(id descpb.ColumnID) error {
		cols.Add(colIdxMap[id])
		return nil
	})
	if err != nil {
		// This path should never be hit since the column function never returns an
		// error.
		panic(err)
	}
	return cols
}

// SetBatchSizeBytes sets the desired batch size. It should only be used in tests.
func (jr *joinReader) SetBatchSizeBytes(batchSize int64) {
	jr.batchSizeBytes = batchSize
}

// Spilled returns whether the joinReader spilled to disk.
func (jr *joinReader) Spilled() bool {
	return jr.strategy.spilled()
}

// neededRightCols returns the set of column indices which need to be fetched
// from the right side of the join (jr.desc).
func (jr *joinReader) neededRightCols() util.FastIntSet {
	neededCols := jr.Out.NeededColumns()

	// Get the columns from the right side of the join and shift them over by
	// the size of the left side so the right side starts at 0.
	neededRightCols := util.MakeFastIntSet()
	for i, ok := neededCols.Next(len(jr.inputTypes)); ok; i, ok = neededCols.Next(i + 1) {
		neededRightCols.Add(i - len(jr.inputTypes))
	}

	// Add columns needed by OnExpr.
	for _, v := range jr.onCond.Vars.GetIndexedVars() {
		rightIdx := v.Idx - len(jr.inputTypes)
		if rightIdx >= 0 {
			neededRightCols.Add(rightIdx)
		}
	}

	return neededRightCols
}

// Next is part of the RowSource interface.
func (jr *joinReader) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The lookup join is implemented as follows:
	// - Read the input rows in batches.
	// - For each batch, map the rows onto index keys and perform an index
	//   lookup for those keys. Note that multiple rows may map to the same key.
	// - Retrieve the index lookup results in batches, since the index scan may
	//   return more rows than the input batch size.
	// - Join the index rows with the corresponding input rows and buffer the
	//   results in jr.toEmit.
	for jr.State == execinfra.StateRunning {
		var row rowenc.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch jr.runningState {
		case jrReadingInput:
			jr.runningState, meta = jr.readInput()
		case jrPerformingLookup:
			jr.runningState, meta = jr.performLookup()
		case jrEmittingRows:
			jr.runningState, row, meta = jr.emitRow()
		default:
			log.Fatalf(jr.Ctx, "unsupported state: %d", jr.runningState)
		}
		if row == nil && meta == nil {
			continue
		}
		if meta != nil {
			return nil, meta
		}
		if outRow := jr.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, jr.DrainHelper()
}

// readInput reads the next batch of input rows and starts an index scan.
func (jr *joinReader) readInput() (joinReaderState, *execinfrapb.ProducerMetadata) {
	// Read the next batch of input rows.
	for jr.curBatchSizeBytes < jr.batchSizeBytes {
		row, meta := jr.input.Next()
		if meta != nil {
			if meta.Err != nil {
				jr.MoveToDraining(nil /* err */)
				return jrStateUnknown, meta
			}
			return jrReadingInput, meta
		}
		if row == nil {
			break
		}
		jr.curBatchSizeBytes += int64(row.Size())
		jr.scratchInputRows = append(jr.scratchInputRows, jr.rowAlloc.CopyRow(row))
	}

	if len(jr.scratchInputRows) == 0 {
		log.VEventf(jr.Ctx, 1, "no more input rows")
		// We're done.
		jr.MoveToDraining(nil)
		return jrStateUnknown, jr.DrainHelper()
	}
	log.VEventf(jr.Ctx, 1, "read %d input rows", len(jr.scratchInputRows))

	spans, err := jr.strategy.processLookupRows(jr.scratchInputRows)
	if err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, jr.DrainHelper()
	}
	jr.scratchInputRows = jr.scratchInputRows[:0]
	jr.curBatchSizeBytes = 0
	if len(spans) == 0 {
		// All of the input rows were filtered out. Skip the index lookup.
		return jrEmittingRows, nil
	}

	// Sort the spans for the following cases:
	// - For lookupJoinReaderType: this is so that we can rely upon the fetcher
	//   to limit the number of results per batch. It's safe to reorder the
	//   spans here because we already restore the original order of the output
	//   during the output collection phase.
	// - For indexJoinReaderType when !maintainOrdering: this allows lower
	//   layers to optimize iteration over the data. Note that the looked up
	//   rows are output unchanged, in the retrieval order, so it is not safe to
	//   do this when maintainOrdering is true (the ordering to be maintained
	//   may be different than the ordering in the index).
	if jr.readerType == lookupJoinReaderType ||
		(jr.readerType == indexJoinReaderType && !jr.maintainOrdering) {
		sort.Sort(spans)
	}

	log.VEventf(jr.Ctx, 1, "scanning %d spans", len(spans))
	if err := jr.fetcher.StartScan(
		jr.Ctx, jr.FlowCtx.Txn, spans, jr.shouldLimitBatches, 0, /* limitHint */
		jr.FlowCtx.TraceKV); err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, jr.DrainHelper()
	}

	return jrPerformingLookup, nil
}

// performLookup reads the next batch of index rows.
func (jr *joinReader) performLookup() (joinReaderState, *execinfrapb.ProducerMetadata) {
	nCols := len(jr.lookupCols)

	for {
		// Construct a "partial key" of nCols, so we can match the key format that
		// was stored in our keyToInputRowIndices map. This matches the format that
		// is output in jr.generateSpan.
		var key roachpb.Key
		// Index joins do not look at this key parameter so don't bother populating
		// it, since it is not cheap for long keys.
		if jr.readerType != indexJoinReaderType {
			var err error
			key, err = jr.fetcher.PartialKey(nCols)
			if err != nil {
				jr.MoveToDraining(err)
				return jrStateUnknown, jr.DrainHelper()
			}
		}

		// Fetch the next row and copy it into the row container.
		lookedUpRow, _, _, err := jr.fetcher.NextRow(jr.Ctx)
		if err != nil {
			jr.MoveToDraining(scrub.UnwrapScrubError(err))
			return jrStateUnknown, jr.DrainHelper()
		}
		if lookedUpRow == nil {
			// Done with this input batch.
			break
		}
		jr.rowsRead++

		if nextState, err := jr.strategy.processLookedUpRow(jr.Ctx, lookedUpRow, key); err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, jr.DrainHelper()
		} else if nextState != jrPerformingLookup {
			return nextState, nil
		}
	}
	log.VEvent(jr.Ctx, 1, "done joining rows")
	jr.strategy.prepareToEmit(jr.Ctx)

	return jrEmittingRows, nil
}

// emitRow returns the next row from jr.toEmit, if present. Otherwise it
// prepares for another input batch.
func (jr *joinReader) emitRow() (
	joinReaderState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	rowToEmit, nextState, err := jr.strategy.nextRowToEmit(jr.Ctx)
	if err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}
	return nextState, rowToEmit, nil
}

// Start is part of the RowSource interface.
func (jr *joinReader) Start(ctx context.Context) context.Context {
	jr.input.Start(ctx)
	ctx = jr.StartInternal(ctx, joinReaderProcName)
	jr.runningState = jrReadingInput
	return ctx
}

// ConsumerClosed is part of the RowSource interface.
func (jr *joinReader) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	jr.close()
}

func (jr *joinReader) close() {
	if jr.InternalClose() {
		if jr.fetcher != nil {
			jr.fetcher.Close(jr.Ctx)
		}
		jr.strategy.close(jr.Ctx)
		if jr.MemMonitor != nil {
			jr.MemMonitor.Stop(jr.Ctx)
		}
		if jr.diskMonitor != nil {
			jr.diskMonitor.Stop(jr.Ctx)
		}
	}
}

var _ execinfrapb.DistSQLSpanStats = &JoinReaderStats{}

const joinReaderTagPrefix = "joinreader."

// Stats implements the SpanStats interface.
func (jrs *JoinReaderStats) Stats() map[string]string {
	statsMap := jrs.InputStats.Stats(joinReaderTagPrefix)
	toMerge := jrs.IndexLookupStats.Stats(joinReaderTagPrefix + "index.")
	for k, v := range toMerge {
		statsMap[k] = v
	}
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (jrs *JoinReaderStats) StatsForQueryPlan() []string {
	is := append(
		jrs.InputStats.StatsForQueryPlan(""),
		jrs.IndexLookupStats.StatsForQueryPlan("index ")...,
	)
	return is
}

// outputStatsToTrace outputs the collected joinReader stats to the trace. Will
// fail silently if the joinReader is not collecting stats.
func (jr *joinReader) outputStatsToTrace() {
	is, ok := getInputStats(jr.FlowCtx, jr.input)
	if !ok {
		return
	}
	ils, ok := getFetcherInputStats(jr.FlowCtx, jr.fetcher)
	if !ok {
		return
	}

	// TODO(asubiotto): Add memory and disk usage to EXPLAIN ANALYZE.
	jrs := &JoinReaderStats{
		InputStats:       is,
		IndexLookupStats: ils,
	}
	if sp := opentracing.SpanFromContext(jr.Ctx); sp != nil {
		tracing.SetSpanStats(sp, jrs)
	}
}

// GetBytesRead is part of the execinfra.IOReader interface.
func (jr *joinReader) GetBytesRead() int64 {
	return jr.fetcher.GetBytesRead()
}

// GetRowsRead is part of the execinfra.IOReader interface.
func (jr *joinReader) GetRowsRead() int64 {
	return jr.rowsRead
}

func (jr *joinReader) generateMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	trailingMeta := make([]execinfrapb.ProducerMetadata, 1)
	meta := &trailingMeta[0]
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.RowsRead = jr.GetRowsRead()
	meta.Metrics.BytesRead = jr.GetBytesRead()
	if tfs := execinfra.GetLeafTxnFinalState(ctx, jr.FlowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta,
			execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs},
		)
	}
	return trailingMeta
}

// DrainMeta is part of the MetadataSource interface.
func (jr *joinReader) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	return jr.generateMeta(ctx)
}

// ChildCount is part of the execinfra.OpNode interface.
func (jr *joinReader) ChildCount(verbose bool) int {
	if _, ok := jr.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (jr *joinReader) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := jr.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to joinReader is not an execinfra.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
