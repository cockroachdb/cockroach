// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colfetcher

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	hasDatumVec bool

	// cpuStopWatch tracks the CPU time spent by this ColBatchDirectScan while
	// fulfilling KV requests *in the current goroutine*.
	cpuStopWatch *timeutil.CPUStopWatch

	deserializer            colexecutils.Deserializer
	deserializerInitialized bool
}

var _ ScanOperator = &ColBatchDirectScan{}

// Init implements the colexecop.Operator interface.
func (s *ColBatchDirectScan) Init(ctx context.Context) {
	if !s.InitHelper.Init(ctx) {
		return
	}
	s.Ctx, s.tracingSpan = execinfra.ProcessorSpan(
		s.Ctx, s.flowCtx, "colbatchdirectscan", s.processorID,
		&s.ContentionEventsListener, &s.ScanStatsListener, &s.TenantConsumptionListener,
	)
	firstBatchLimit := cFetcherFirstBatchLimit(s.limitHint, s.spec.MaxKeysPerRow)
	err := s.fetcher.SetupNextFetch(
		ctx, s.Spans, nil /* spanIDs */, s.batchBytesLimit, firstBatchLimit, false, /* spansCanOverlap */
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
		s.cpuStopWatch.Start()
		res, err = s.fetcher.NextBatch(s.Ctx)
		s.cpuStopWatch.Stop()
		if err != nil {
			colexecerror.InternalError(convertFetchError(s.spec, err))
		}
		if !res.MoreKVs {
			return coldata.ZeroBatch
		}
		if res.KVs != nil {
			colexecerror.InternalError(errors.AssertionFailedf("unexpectedly encountered KVs in a direct scan"))
		}
		if res.ColBatch != nil {
			// If there are any datum-backed vectors in this batch, then they
			// are "incomplete", and we have to properly initialize them here.
			if s.hasDatumVec {
				for _, vec := range res.ColBatch.ColVecs() {
					if vec.CanonicalTypeFamily() == typeconv.DatumVecCanonicalTypeFamily {
						vec.Datum().SetEvalCtx(s.flowCtx.EvalCtx)
					}
				}
			}
			s.mu.Lock()
			s.mu.rowsRead += int64(res.ColBatch.Length())
			s.mu.Unlock()
			// Note that this batch has already been accounted for by the
			// KVBatchFetcher, so we don't need to do that.
			return res.ColBatch
		}
		if res.BatchResponse != nil {
			break
		}
		// If BatchResponse is nil, then it was an empty response for a
		// ScanRequest, and we need to proceed further.
	}
	if !s.deserializerInitialized {
		if err = s.deserializer.Init(
			s.allocator, s.resultTypes, false, /* alwaysReallocate */
		); err != nil {
			colexecerror.InternalError(err)
		}
		s.deserializerInitialized = true
	}
	batch := s.deserializer.Deserialize(res.BatchResponse)
	s.mu.Lock()
	s.mu.rowsRead += int64(batch.Length())
	s.mu.Unlock()
	return batch
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

// GetKVPairsRead is part of the colexecop.KVReader interface.
func (s *ColBatchDirectScan) GetKVPairsRead() int64 {
	return s.fetcher.GetKVPairsRead()
}

// GetBatchRequestsIssued is part of the colexecop.KVReader interface.
func (s *ColBatchDirectScan) GetBatchRequestsIssued() int64 {
	return s.fetcher.GetBatchRequestsIssued()
}

// TODO(yuzefovich): check whether GetScanStats and GetConsumedRU should be
// reimplemented.

// GetKVCPUTime is part of the colexecop.KVReader interface.
//
// Note that this KV CPU time, unlike for the ColBatchScan, includes the
// decoding time done by the cFetcherWrapper.
func (s *ColBatchDirectScan) GetKVCPUTime() time.Duration {
	return s.cpuStopWatch.Elapsed()
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
	s.deserializer.Close(ctx)
	return s.colBatchScanBase.close()
}

// NewColBatchDirectScan creates a new ColBatchDirectScan operator.
func NewColBatchDirectScan(
	ctx context.Context,
	allocator *colmem.Allocator,
	kvFetcherMemAcc *mon.BoundAccount,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
	typeResolver *descs.DistSQLTypeResolver,
) (*ColBatchDirectScan, []*types.T, error) {
	base, bsHeader, tableArgs, err := newColBatchScanBase(
		ctx, kvFetcherMemAcc, flowCtx, processorID, spec, post, typeResolver,
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
		tableArgs.RequiresRawMVCCValues(),
		spec.LockingStrength,
		spec.LockingWaitPolicy,
		spec.LockingDurability,
		flowCtx.EvalCtx.SessionData().LockTimeout,
		flowCtx.EvalCtx.SessionData().DeadlockTimeout,
		kvFetcherMemAcc,
		flowCtx.EvalCtx.TestingKnobs.ForceProductionValues,
		spec.FetchSpec.External,
	)
	var hasDatumVec bool
	for _, t := range tableArgs.typs {
		if typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) == typeconv.DatumVecCanonicalTypeFamily {
			hasDatumVec = true
			break
		}
	}
	var cpuStopWatch *timeutil.CPUStopWatch
	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
		cpuStopWatch = timeutil.NewCPUStopWatch()
	}
	return &ColBatchDirectScan{
		colBatchScanBase: base,
		fetcher:          fetcher,
		allocator:        allocator,
		spec:             &fetchSpec,
		resultTypes:      tableArgs.typs,
		hasDatumVec:      hasDatumVec,
		cpuStopWatch:     cpuStopWatch,
	}, tableArgs.typs, nil
}
