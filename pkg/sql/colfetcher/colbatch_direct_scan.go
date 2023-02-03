// Copyright 2023 The Cockroach Authors.
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
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
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

// ColBatchDirectScan is a colexecop.Operator that performs a scan of the given
// key spans using the COL_BATCH_RESPONSE scan format.
type ColBatchDirectScan struct {
	*colBatchScanBase
	fetcher row.KVBatchFetcher

	allocator   *colmem.Allocator
	spec        *fetchpb.IndexFetchSpec
	resultTypes []*types.T

	data      []array.Data
	batch     coldata.Batch
	converter *colserde.ArrowBatchConverter
	deser     *colserde.RecordBatchSerializer
}

var _ ScanOperator = &ColBatchDirectScan{}

// Init implements the colexecop.Operator interface.
func (s *ColBatchDirectScan) Init(ctx context.Context) {
	if !s.InitHelper.Init(ctx) {
		return
	}
	s.Ctx, s.tracingSpan = execinfra.ProcessorSpan(s.Ctx, s.flowCtx, "colbatchdirectscan")
	var err error
	s.deser, err = colserde.NewRecordBatchSerializer(s.resultTypes)
	if err != nil {
		colexecerror.InternalError(err)
	}
	s.converter, err = colserde.NewArrowBatchConverter(s.resultTypes, colserde.ArrowToBatchOnly, nil /* acc */)
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

// Next implements the colexecop.Operator interface.
func (s *ColBatchDirectScan) Next() (ret coldata.Batch) {
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
		if res.KVs != nil {
			colexecerror.InternalError(errors.AssertionFailedf("unexpectedly encountered KVs in a direct scan"))
		}
		if res.BatchResponse != nil {
			break
		}
		// If BatchResponse is nil, then it was an empty response for a
		// ScanRequest, and we need to proceed further.
	}
	s.data = s.data[:0]
	batchLength, err := s.deser.Deserialize(&s.data, res.BatchResponse)
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
	s.mu.Lock()
	s.mu.rowsRead += int64(batchLength)
	s.mu.Unlock()
	return s.batch
}

// DrainMeta is part of the colexecop.MetadataSource interface.
func (s *ColBatchDirectScan) DrainMeta() []execinfrapb.ProducerMetadata {
	trailingMeta := s.colBatchScanBase.drainMeta()
	meta := execinfrapb.GetProducerMeta()
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.BytesRead = s.GetBytesRead()
	meta.Metrics.RowsRead = s.GetRowsRead()
	trailingMeta = append(trailingMeta, *meta)
	return trailingMeta
}

// GetBytesRead is part of the colexecop.KVReader interface.
func (s *ColBatchDirectScan) GetBytesRead() int64 {
	return s.fetcher.GetBytesRead()
}

// GetBatchRequestsIssued is part of the colexecop.KVReader interface.
func (s *ColBatchDirectScan) GetBatchRequestsIssued() int64 {
	return s.fetcher.GetBatchRequestsIssued()
}

// GetKVCPUTime is part of the colexecop.KVReader interface.
func (s *ColBatchDirectScan) GetKVCPUTime() time.Duration {
	// TODO(yuzefovich, 23.1): implement this.
	return 0
}

// Release implements the execreleasable.Releasable interface.
func (s *ColBatchDirectScan) Release() {
	s.colBatchScanBase.Release()
	*s = ColBatchDirectScan{}
}

// Close implements the colexecop.Closer interface.
func (s *ColBatchDirectScan) Close(context.Context) error {
	// Note that we're using the context of the ColBatchDirectScan rather than
	// the argument of Close() because the ColBatchDirectScan derives its own
	// tracing span.
	ctx := s.EnsureCtx()
	s.fetcher.Close(ctx)
	s.converter.Release(ctx)
	return s.colBatchScanBase.close()
}

// NewColBatchDirectScan creates a new ColBatchDirectScan operator.
func NewColBatchDirectScan(
	ctx context.Context,
	allocator *colmem.Allocator,
	kvFetcherMemAcc *mon.BoundAccount,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	typeResolver *descs.DistSQLTypeResolver,
) (*ColBatchDirectScan, []*types.T, error) {
	base, bsHeader, tableArgs, err := newColBatchScanBase(
		ctx, kvFetcherMemAcc, flowCtx, spec, post, typeResolver,
	)
	if err != nil {
		return nil, nil, err
	}
	// Make a copy of the fetchpb.IndexFetchSpec and use the reference to that
	// copy when creating the fetcher and the ColBatchDirectScan below.
	//
	// This is needed to avoid a "data race" on the TableReaderSpec being put
	// back into the pool (which resets the fetch spec) - which is done when
	// cleaning up the flow - and the fetch spec being marshaled as part of the
	// BatchRequest. The "data race" is in quotes because it's a false positive
	// from kvcoord.GRPCTransportFactory from transport_race.go. In particular,
	// at the moment, we're not allowed to modify the BatchRequest after it was
	// issued and even after it was responded to. In theory, we (the client)
	// should be able to modify the BatchRequest, but alas.
	fetchSpec := spec.FetchSpec
	fetcher := row.NewDirectKVBatchFetcher(
		flowCtx.Txn,
		bsHeader,
		&fetchSpec,
		spec.Reverse,
		spec.LockingStrength,
		spec.LockingWaitPolicy,
		flowCtx.EvalCtx.SessionData().LockTimeout,
		kvFetcherMemAcc,
		flowCtx.EvalCtx.TestingKnobs.ForceProductionValues,
	)
	return &ColBatchDirectScan{
		colBatchScanBase: base,
		fetcher:          fetcher,
		allocator:        allocator,
		spec:             &fetchSpec,
		resultTypes:      tableArgs.typs,
		data:             make([]array.Data, len(tableArgs.typs)),
	}, tableArgs.typs, nil
}
