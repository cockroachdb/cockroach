// Copyright 2022 The Cockroach Authors.
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

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

type ColBatchDirectScan struct {
	*colBatchScan
	fetcher row.KVBatchFetcher

	allocator   *colmem.Allocator
	spec        *fetchpb.IndexFetchSpec
	resultTypes []*types.T
	hasDatumVec bool

	// Only used when coldata.Batches are sent across the wire.
	data      []*array.Data
	batch     coldata.Batch
	converter *colserde.ArrowBatchConverter
	deser     *colserde.RecordBatchSerializer
}

var _ ScanOperator = &ColBatchDirectScan{}

func (s *ColBatchDirectScan) Init(ctx context.Context) {
	if !s.InitHelper.Init(ctx) {
		return
	}
	// If tracing is enabled, we need to start a child span so that the only
	// contention events present in the recording would be because of this
	// fetcher. Note that ProcessorSpan method itself will check whether tracing
	// is enabled.
	s.Ctx, s.tracingSpan = execinfra.ProcessorSpan(s.Ctx, "colbatchdirectscan")
	var err error
	s.deser, err = colserde.NewRecordBatchSerializer(s.resultTypes)
	if err != nil {
		colexecerror.InternalError(err)
	}
	s.converter, err = colserde.NewArrowBatchConverter(s.resultTypes)
	if err != nil {
		colexecerror.InternalError(err)
	}
	firstBatchLimit := cFetcherFirstBatchLimit(s.limitHint, s.spec.MaxKeysPerRow)
	err = s.fetcher.SetupNextFetch(
		ctx, s.Spans, nil /* spanIDs */, s.batchBytesLimit, firstBatchLimit,
	)
	if err != nil {
		colexecerror.InternalError(err)
	}
}

func (s *ColBatchDirectScan) Next() (ret coldata.Batch) {
	defer func() {
		if ret != nil {
			s.mu.Lock()
			s.mu.rowsRead += int64(ret.Length())
			s.mu.Unlock()
		}
	}()
	var res row.KVBatchFetcherResponse
	var err error
	for {
		res, err = s.fetcher.NextBatch(s.Ctx)
		if err != nil {
			colexecerror.InternalError(convertFetchError(s.spec, err))
		}
		if !res.MoreKVs {
			return coldata.ZeroBatch
		}
		if res.KVs != nil || res.BatchResponse != nil {
			colexecerror.InternalError(errors.AssertionFailedf("unexpectedly encountered KVs or BatchResponse in a direct scan"))
		}
		// TODO: make sure that someone is accounting for the memory footprint of
		// this batch.
		if res.ColBatch != nil {
			if s.hasDatumVec {
				for _, vec := range res.ColBatch.ColVecs() {
					if vec.CanonicalTypeFamily() == typeconv.DatumVecCanonicalTypeFamily {
						vec.Datum().SetEvalCtx(s.flowCtx.EvalCtx)
					}
				}
			}
			return res.ColBatch
		}
		if res.SerializedColBatch != nil {
			break
		}
		// If both ColBatch and SerializedColBatch are nil, then it was an empty
		// response for a ScanRequest, and we need to proceed further.
	}
	// Update the allocator since we're holding onto the serialized bytes for
	// now.
	s.allocator.AdjustMemoryUsageAfterAllocation(int64(len(res.SerializedColBatch)))
	s.data = s.data[:0]
	batchLength, err := s.deser.Deserialize(&s.data, res.SerializedColBatch)
	if err != nil {
		colexecerror.InternalError(err)
	}
	// We rely on the cFetcherWrapper to produce reasonably sized batches.
	s.batch, _ = s.allocator.ResetMaybeReallocateNoMemLimit(s.resultTypes, s.batch, batchLength)
	s.allocator.PerformOperation(s.batch.ColVecs(), func() {
		if err = s.converter.ArrowToBatch(s.data, batchLength, s.batch); err != nil {
			colexecerror.InternalError(err)
		}
	})
	// At this point, we have lost all references to the serialized bytes
	// (because ArrowToBatch nils out elements in s.data once processed), so we
	// update the allocator accordingly.
	s.allocator.AdjustMemoryUsage(-int64(len(res.SerializedColBatch)))
	return s.batch
}

// DrainMeta is part of the colexecop.MetadataSource interface.
func (s *ColBatchDirectScan) DrainMeta() []execinfrapb.ProducerMetadata {
	trailingMeta := s.colBatchScan.DrainMeta()
	meta := execinfrapb.GetProducerMeta()
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.BytesRead = s.GetBytesRead()
	meta.Metrics.RowsRead = s.GetRowsRead()
	trailingMeta = append(trailingMeta, *meta)
	return trailingMeta
}

func (s *ColBatchDirectScan) GetBytesRead() int64 {
	//TODO implement me
	return 0
}

func (s *ColBatchDirectScan) GetBatchRequestsIssued() int64 {
	//TODO implement me
	return 0
}

// Release implements the execreleasable.Releasable interface.
func (s *ColBatchDirectScan) Release() {
	s.colBatchScan.Release()
}

// Close implements the colexecop.Closer interface.
func (s *ColBatchDirectScan) Close(context.Context) error {
	// Note that we're using the context of the ColBatchDirectScan rather than
	// the argument of Close() because the ColBatchDirectScan derives its own
	// tracing span.
	ctx := s.EnsureCtx()
	s.fetcher.Close(ctx)
	return s.colBatchScan.Close(ctx)
}

// NewColBatchDirectScan creates a new ColBatchDirectScan operator.
// TODO(yuzefovich): use estimated row count.
func NewColBatchDirectScan(
	ctx context.Context,
	allocator *colmem.Allocator,
	kvFetcherMemAcc *mon.BoundAccount,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	typeResolver *descs.DistSQLTypeResolver,
) (*ColBatchDirectScan, []*types.T, error) {
	scan, bsHeader, tableArgs, err := newColBatchScan(
		ctx, allocator, flowCtx, spec, post, typeResolver,
	)
	if err != nil {
		return nil, nil, err
	}
	fetcher := row.NewDirectKVBatchFetcher(
		flowCtx.Txn,
		bsHeader,
		&spec.FetchSpec,
		spec.Reverse,
		spec.LockingStrength,
		spec.LockingWaitPolicy,
		flowCtx.EvalCtx.SessionData().LockTimeout,
		kvFetcherMemAcc,
		flowCtx.EvalCtx.TestingKnobs.ForceProductionValues,
	)

	var hasDatumVec bool
	for _, t := range tableArgs.typs {
		if typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) == typeconv.DatumVecCanonicalTypeFamily {
			hasDatumVec = true
			break
		}
	}

	return &ColBatchDirectScan{
		colBatchScan: scan,
		allocator:    allocator,
		fetcher:      fetcher,
		spec:         &spec.FetchSpec,
		resultTypes:  tableArgs.typs,
		hasDatumVec:  hasDatumVec,
		data:         make([]*array.Data, len(tableArgs.typs)),
	}, tableArgs.typs, nil
}
