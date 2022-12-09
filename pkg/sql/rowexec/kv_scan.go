// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// kvScan is used read raw KV values in a computation flow for purposes,
// of mainly implementing crdb_internal.scan. This execinfra.RowSource
// only returns a row format of key, value, and ts.
type kvScan struct {
	execinfra.ProcessorBase
	spans           []roachpb.Span
	rowIdx          int
	bytesRead       int64
	rowsRead        int64
	scanResponses   []kvpb.ResponseUnion
	currentResponse int
	row             rowenc.EncDatumRow
}

// Start is part of the execinfra.RowSource interface.
func (ks *kvScan) Start(ctx context.Context) {
	ctx = ks.StartInternal(ctx, "index-scan")
	// Set up the first batch.
	ba := ks.EvalCtx.Txn.NewBatch()
	for spanIdx := range ks.spans {
		scan := kvpb.NewScan(ks.spans[spanIdx].Key,
			ks.spans[spanIdx].EndKey,
			false)
		ba.AddRawRequest(scan)
	}
	ba.Header.MaxSpanRequestKeys = int64(rowinfra.ProductionKVBatchSize)
	ba.Header.TargetBytes = int64(rowinfra.GetDefaultBatchBytesLimit(false))
	err := ks.EvalCtx.Txn.Run(ctx, ba)
	if err != nil {
		ks.MoveToDraining(err)
	}
	ks.scanResponses = ba.RawResponse().Responses
	// Set up the results for the rows.
	ks.row = make(rowenc.EncDatumRow, 3)
}

// getNextScan will issue any additional scans to read additional data,
// using any response spans.
func (ks *kvScan) getNextScan(ctx context.Context) error {
	// Generate a new batch with all of the resume spans
	// from our first request.
	ba := ks.EvalCtx.Txn.NewBatch()
	for i := ks.currentResponse; i < len(ks.scanResponses); i++ {
		scan := ks.scanResponses[i].GetScan()
		nextSpan := scan.ResumeSpan
		scanRequest := kvpb.NewScan(nextSpan.Key,
			nextSpan.EndKey,
			false /*forUpdate*/)
		ba.AddRawRequest(scanRequest)
	}
	ba.Header.MaxSpanRequestKeys = int64(rowinfra.ProductionKVBatchSize)
	err := ks.EvalCtx.Txn.Run(ctx, ba)
	if err != nil {
		return err
	}
	ks.scanResponses = ba.RawResponse().Responses
	ks.currentResponse = 0
	ks.rowIdx = 0
	return nil
}

// Next is part of the execinfra.RowSource interface.
func (ks *kvScan) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if ks.State != execinfra.StateRunning {
		return nil, ks.DrainHelper()
	}
	moreRows := true
	var err error
	for moreRows {
		currentScan := ks.scanResponses[ks.currentResponse].GetScan()
		// The current scan is out of data, so check if we have more
		// data.
		if ks.rowIdx >= len(currentScan.Rows) {
			// If there is a resume span, then we need to
			// make another request to continue.
			if currentScan.ResumeSpan != nil {
				err = ks.getNextScan(ks.Ctx())
				if !moreRows || err != nil {
					ks.MoveToDraining(err)
					return nil, ks.DrainHelper()
				}
				continue
			}
			// Otherwise, check if the next response has data
			ks.currentResponse++
			ks.rowIdx = 0

			// No more data left, so we are finished.
			if ks.currentResponse == len(ks.scanResponses) {
				ks.MoveToDraining(err)
				return nil, ks.DrainHelper()
			}
			continue
		}
		// Encode the row from the raw scan response.
		ks.row[0] = rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(currentScan.Rows[ks.rowIdx].Key)))
		ks.row[1] = rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(currentScan.Rows[ks.rowIdx].Value.RawBytes)))
		ks.row[2] = rowenc.DatumToEncDatum(types.String, tree.NewDString(currentScan.Rows[ks.rowIdx].Value.Timestamp.String()))

		ks.bytesRead += int64(currentScan.Rows[ks.rowIdx].Size())
		ks.rowsRead += 1
		ks.rowIdx++
		if row := ks.ProcessRowHelper(ks.row); row != nil {
			return row, nil
		}
	}
	return nil, nil
}

func (ks *kvScan) generateMeta() []execinfrapb.ProducerMetadata {
	var trailingMeta []execinfrapb.ProducerMetadata
	nodeID, ok := ks.FlowCtx.NodeID.OptionalNodeID()
	if ok {
		ranges := execinfra.MisplannedRanges(ks.Ctx(), ks.spans, nodeID, ks.FlowCtx.Cfg.RangeCache)
		if ranges != nil {
			trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: ranges})
		}
	}
	if tfs := execinfra.GetLeafTxnFinalState(ks.Ctx(), ks.FlowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}

	meta := execinfrapb.GetProducerMeta()
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.BytesRead = ks.bytesRead
	meta.Metrics.RowsRead = ks.rowsRead
	return append(trailingMeta, *meta)
}

func (ks *kvScan) generateTrailingMeta() []execinfrapb.ProducerMetadata {
	// We need to generate metadata before closing the processor because
	// InternalClose() updates tr.Ctx to the "original" context.
	trailingMeta := ks.generateMeta()
	ks.InternalClose()
	return trailingMeta
}

// newKvScan creates a kvScan.
func newKvScan(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.KVScanReaderSpec,
	post *execinfrapb.PostProcessSpec,
) (*kvScan, error) {
	tmpScan := &kvScan{spans: spec.Spans}
	err := tmpScan.Init(ctx, tmpScan, post, []*types.T{types.Bytes, types.Bytes, types.String}, flowCtx, processorID, nil, execinfra.ProcStateOpts{
		TrailingMetaCallback: tmpScan.generateTrailingMeta,
	})
	return tmpScan, err
}
