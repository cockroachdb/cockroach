// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colfetcher

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// TODO(yuzefovich): reading the data through a pair of ColBatchScan and
// materializer turns out to be more efficient than through a table reader (at
// the moment, the exception is the case of reading very small number of rows
// because we still pre-allocate batches of 1024 size). Once we can control the
// initial size of pre-allocated batches (probably via a batch allocator), we
// should get rid off table readers entirely. We will have to be careful about
// propagating the metadata though.

// ColBatchScan is the exec.Operator implementation of TableReader. It reads a table
// from kv, presenting it as coldata.Batches via the exec.Operator interface.
type ColBatchScan struct {
	colexecbase.ZeroInputNode
	spans       roachpb.Spans
	flowCtx     *execinfra.FlowCtx
	rf          *cFetcher
	limitHint   int64
	parallelize bool
	ctx         context.Context
	// rowsRead contains the number of total rows this ColBatchScan has returned
	// so far.
	rowsRead int64
	// init is true after Init() has been called.
	init bool
	// ResultTypes is the slice of resulting column types from this operator.
	// It should be used rather than the slice of column types from the scanned
	// table because the scan might synthesize additional implicit system columns.
	ResultTypes []*types.T
}

var _ execinfra.KVReader = &ColBatchScan{}
var _ execinfra.Releasable = &ColBatchScan{}

// Init initializes a ColBatchScan.
func (s *ColBatchScan) Init() {
	s.init = true
	limitBatches := !s.parallelize
	if err := s.rf.StartScan(
		s.ctx, s.flowCtx.Txn, s.spans,
		limitBatches, s.limitHint, s.flowCtx.TraceKV,
	); err != nil {
		colexecerror.InternalError(err)
	}
}

// Next is part of the Operator interface.
func (s *ColBatchScan) Next(ctx context.Context) coldata.Batch {
	bat, err := s.rf.NextBatch(ctx)
	if err != nil {
		colexecerror.InternalError(err)
	}
	if bat.Selection() != nil {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly a selection vector is set on the batch coming from CFetcher"))
	}
	s.rowsRead += int64(bat.Length())
	return bat
}

// DrainMeta is part of the MetadataSource interface.
func (s *ColBatchScan) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	if !s.init {
		// In some pathological queries like `SELECT 1 FROM t HAVING true`, Init()
		// and Next() may never get called. Return early to avoid using an
		// uninitialized fetcher.
		return nil
	}
	var trailingMeta []execinfrapb.ProducerMetadata
	if !s.flowCtx.Local {
		nodeID, ok := s.flowCtx.NodeID.OptionalNodeID()
		if ok {
			ranges := execinfra.MisplannedRanges(ctx, s.spans, nodeID, s.flowCtx.Cfg.RangeCache)
			if ranges != nil {
				trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: ranges})
			}
		}
	}
	if tfs := execinfra.GetLeafTxnFinalState(ctx, s.flowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	meta := execinfrapb.GetProducerMeta()
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.BytesRead = s.GetBytesRead()
	meta.Metrics.RowsRead = s.GetRowsRead()
	trailingMeta = append(trailingMeta, *meta)

	if contentionEvents := s.rf.fetcher.GetContentionEvents(); len(contentionEvents) != 0 {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{ContentionEvents: contentionEvents})
	}
	return trailingMeta
}

// GetBytesRead is part of the execinfra.KVReader interface.
func (s *ColBatchScan) GetBytesRead() int64 {
	return s.rf.fetcher.GetBytesRead()
}

// GetRowsRead is part of the execinfra.KVReader interface.
func (s *ColBatchScan) GetRowsRead() int64 {
	return s.rowsRead
}

// GetCumulativeContentionTime is part of the execinfra.KVReader interface.
func (s *ColBatchScan) GetCumulativeContentionTime() time.Duration {
	var totalContentionTime time.Duration
	for _, e := range s.rf.fetcher.GetContentionEvents() {
		totalContentionTime += e.Duration
	}
	return totalContentionTime
}

var colBatchScanPool = sync.Pool{
	New: func() interface{} {
		return &ColBatchScan{}
	},
}

// NewColBatchScan creates a new ColBatchScan operator.
func NewColBatchScan(
	ctx context.Context,
	allocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	evalCtx *tree.EvalContext,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
) (*ColBatchScan, error) {
	// NB: we hit this with a zero NodeID (but !ok) with multi-tenancy.
	if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); nodeID == 0 && ok {
		return nil, errors.Errorf("attempting to create a ColBatchScan with uninitialized NodeID")
	}
	if spec.IsCheck {
		// cFetchers don't support these checks.
		return nil, errors.AssertionFailedf("attempting to create a cFetcher with the IsCheck flag set")
	}

	limitHint := execinfra.LimitHint(spec.LimitHint, post)

	returnMutations := spec.Visibility == execinfra.ScanVisibilityPublicAndNotPublic
	// TODO(ajwerner): The need to construct an Immutable here
	// indicates that we're probably doing this wrong. Instead we should be
	// just setting the ID and Version in the spec or something like that and
	// retrieving the hydrated Immutable from cache.
	table := tabledesc.NewImmutable(spec.Table)
	typs := table.ColumnTypesWithMutationsAndVirtualCol(returnMutations, spec.VirtualColumn)
	columnIdxMap := table.ColumnIdxMapWithMutations(returnMutations)

	// Add all requested system columns to the output.
	var sysColDescs []descpb.ColumnDescriptor
	if spec.HasSystemColumns {
		sysColDescs = colinfo.AllSystemColumnDescs
		for i := range sysColDescs {
			typs = append(typs, sysColDescs[i].Type)
			columnIdxMap.Set(sysColDescs[i].ID, columnIdxMap.Len())
		}
	}

	// Before we can safely use types from the table descriptor, we need to
	// make sure they are hydrated. In row execution engine it is done during
	// the processor initialization, but neither ColBatchScan nor cFetcher are
	// processors, so we need to do the hydration ourselves.
	resolver := flowCtx.TypeResolverFactory.NewTypeResolver(evalCtx.Txn)
	if err := resolver.HydrateTypeSlice(ctx, typs); err != nil {
		return nil, err
	}

	var neededColumns util.FastIntSet
	for _, neededColumn := range spec.NeededColumns {
		neededColumns.Add(int(neededColumn))
	}

	fetcher := cFetcherPool.Get().(*cFetcher)
	if _, _, err := initCRowFetcher(
		flowCtx.Codec(), allocator, fetcher, table, columnIdxMap, neededColumns, spec, sysColDescs,
	); err != nil {
		return nil, err
	}

	if flowCtx.Cfg.TestingKnobs.GenerateMockContentionEvents {
		fetcher.testingGenerateMockContentionEvents = true
	}

	s := colBatchScanPool.Get().(*ColBatchScan)
	spans := s.spans[:0]
	specSpans := spec.Spans
	for i := range specSpans {
		//gcassert:bce
		spans = append(spans, specSpans[i].Span)
	}
	*s = ColBatchScan{
		ctx:       ctx,
		spans:     spans,
		flowCtx:   flowCtx,
		rf:        fetcher,
		limitHint: limitHint,
		// Parallelize shouldn't be set when there's a limit hint, but double-check
		// just in case.
		parallelize: spec.Parallelize && limitHint == 0,
		ResultTypes: typs,
	}
	return s, nil
}

// initCRowFetcher initializes a row.cFetcher. See initRowFetcher.
func initCRowFetcher(
	codec keys.SQLCodec,
	allocator *colmem.Allocator,
	fetcher *cFetcher,
	desc *tabledesc.Immutable,
	colIdxMap catalog.TableColMap,
	valNeededForCol util.FastIntSet,
	spec *execinfrapb.TableReaderSpec,
	systemColumnDescs []descpb.ColumnDescriptor,
) (index *descpb.IndexDescriptor, isSecondaryIndex bool, err error) {
	index, isSecondaryIndex, err = desc.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		return nil, false, err
	}

	tableArgs := row.FetcherTableArgs{
		Desc:             desc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		ValNeededForCol:  valNeededForCol,
	}
	tableArgs.InitCols(desc, spec.Visibility, systemColumnDescs, spec.VirtualColumn)

	if err := fetcher.Init(
		codec, allocator, spec.Reverse, spec.LockingStrength, spec.LockingWaitPolicy, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}

// Release implements the execinfra.Releasable interface.
func (s *ColBatchScan) Release() {
	s.rf.Release()
	*s = ColBatchScan{
		spans: s.spans[:0],
	}
	colBatchScanPool.Put(s)
}
