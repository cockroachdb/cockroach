// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type indexScan struct {
	execinfra.ProcessorBase
	spec         execinfrapb.IndexReaderSpec
	spanIdx      int
	rowIdx       int
	scanResponse *roachpb.ScanResponse
	ctx          context.Context
}

func (is *indexScan) OutputTypes() []*types.T {
	return []*types.T{types.Bytes, types.Bytes}
}

func (is *indexScan) MustBeStreaming() bool {
	return false
}

func (is *indexScan) Start(ctx context.Context) {
	ctx = is.StartInternal(ctx, "index-scan")
	// Setup the first batch
	ba := is.EvalCtx.Txn.NewBatch()
	scan := roachpb.NewScan(is.spec.Spans[is.spanIdx].Key,
		is.spec.Spans[is.spanIdx].EndKey,
		false)
	ba.AddRawRequest(scan)
	ba.Header.MaxSpanRequestKeys = 1000
	err := is.EvalCtx.Txn.Run(ctx, ba)
	if err != nil {
		panic(err)
	}
	is.scanResponse = ba.RawResponse().Responses[0].GetScan()
	is.spanIdx++
}

func (is *indexScan) getNextScan(ctx context.Context) (bool, error) {
	nextSpan := is.scanResponse.ResumeSpan
	if nextSpan == nil && is.spanIdx < len(is.spec.Spans) {
		nextSpan = &is.spec.Spans[is.spanIdx]
		is.spanIdx++
	}
	if nextSpan != nil {
		ba := is.EvalCtx.Txn.NewBatch()
		scan := roachpb.NewScan(nextSpan.Key,
			nextSpan.EndKey,
			false)
		ba.AddRawRequest(scan)
		ba.Header.MaxSpanRequestKeys = 1000
		err := is.EvalCtx.Txn.Run(ctx, ba)
		if err != nil {
			return false, err
		}
		is.scanResponse = ba.RawResponse().Responses[0].GetScan()
		is.rowIdx = 0
		return true, nil
	}

	return false, nil
}

func (is *indexScan) Release(ctx context.Context) {
}

func (is *indexScan) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for is.rowIdx >= len(is.scanResponse.Rows) {
		moreRows, err := is.getNextScan(is.ctx)
		if !moreRows || err != nil {
			is.MoveToDraining(err)
			return nil, is.DrainHelper()
		}
	}

	result := rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(is.scanResponse.Rows[is.rowIdx].Key))),
		rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(is.scanResponse.Rows[is.rowIdx].Value.RawBytes))),
	}
	is.rowIdx++
	return is.ProcessRowHelper(result), nil
}

// ConsumerClosed is part of the RowSource interface.
func (is *indexScan) ConsumerClosed() {
}

func newIndexScan(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.IndexReaderSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*indexScan, error) {
	tmpScan := &indexScan{spec: spec, ctx: ctx}
	err := tmpScan.Init(ctx, tmpScan, post, []*types.T{types.Bytes, types.Bytes}, flowCtx, processorID, output, nil, execinfra.ProcStateOpts{})
	return tmpScan, err
}
