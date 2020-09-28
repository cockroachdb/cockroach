// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

// Progress is streamed to the coordinator through metadata.
var restoreDataOutputTypes = []*types.T{}

type restoreDataProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.RestoreDataSpec
	input   execinfra.RowSource
	output  execinfra.RowReceiver

	alloc rowenc.DatumAlloc
	kr    *storageccl.KeyRewriter
}

var _ execinfra.Processor = &restoreDataProcessor{}
var _ execinfra.RowSource = &restoreDataProcessor{}

const restoreDataProcName = "restoreDataProcessor"

func newRestoreDataProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.RestoreDataSpec,
	post *execinfrapb.PostProcessSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	rd := &restoreDataProcessor{
		flowCtx: flowCtx,
		input:   input,
		spec:    spec,
		output:  output,
	}

	var err error
	rd.kr, err = storageccl.MakeKeyRewriterFromRekeys(rd.spec.Rekeys)
	if err != nil {
		return nil, err
	}

	if err := rd.Init(rd, post, restoreDataOutputTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{input},
		}); err != nil {
		return nil, err
	}
	return rd, nil
}

// Start is part of the RowSource interface.
func (rd *restoreDataProcessor) Start(ctx context.Context) context.Context {
	rd.input.Start(ctx)
	return rd.StartInternal(ctx, restoreDataProcName)
}

// Next is part of the RowSource interface.
func (rd *restoreDataProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if rd.State != execinfra.StateRunning {
		return nil, rd.DrainHelper()
	}
	// We read rows from the SplitAndScatter processor. We expect each row to
	// contain 2 columns. The first is used to route the row to this processor,
	// and the second contains the RestoreSpanEntry that we're interested in.
	row, meta := rd.input.Next()
	if meta != nil {
		if meta.Err != nil {
			rd.MoveToDraining(nil /* err */)
		}
		return nil, meta
	}
	if row == nil {
		rd.MoveToDraining(nil /* err */)
		return nil, rd.DrainHelper()
	}

	if len(row) != 2 {
		rd.MoveToDraining(errors.New("expected input rows to have exactly 2 columns"))
		return nil, rd.DrainHelper()
	}
	if err := row[1].EnsureDecoded(types.Bytes, &rd.alloc); err != nil {
		rd.MoveToDraining(err)
		return nil, rd.DrainHelper()
	}
	datum := row[1].Datum
	entryDatumBytes, ok := datum.(*tree.DBytes)
	if !ok {
		rd.MoveToDraining(errors.AssertionFailedf(`unexpected datum type %T: %+v`, datum, row))
		return nil, rd.DrainHelper()
	}

	var entry execinfrapb.RestoreSpanEntry
	if err := protoutil.Unmarshal([]byte(*entryDatumBytes), &entry); err != nil {
		rd.MoveToDraining(errors.Wrap(err, "un-marshaling restore span entry"))
		return nil, rd.DrainHelper()
	}

	newSpanKey, err := rewriteBackupSpanKey(rd.kr, entry.Span.Key)
	if err != nil {
		rd.MoveToDraining(errors.Wrap(err, "re-writing span key to import"))
		return nil, rd.DrainHelper()
	}

	log.VEventf(rd.Ctx, 1 /* level */, "importing span %v", entry.Span)
	importRequest := &roachpb.ImportRequest{
		// Import is a point request because we don't want DistSender to split
		// it. Assume (but don't require) the entire post-rewrite span is on the
		// same range.
		RequestHeader: roachpb.RequestHeader{Key: newSpanKey},
		DataSpan:      entry.Span,
		Files:         entry.Files,
		EndTime:       rd.spec.RestoreTime,
		Rekeys:        rd.spec.Rekeys,
		Encryption:    rd.spec.Encryption,
	}

	importRes, pErr := kv.SendWrapped(rd.Ctx, rd.flowCtx.Cfg.DB.NonTransactionalSender(), importRequest)
	if pErr != nil {
		rd.MoveToDraining(errors.Wrapf(pErr.GoError(), "importing span %v", importRequest.DataSpan))
		return nil, rd.DrainHelper()
	}

	var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	progDetails := RestoreProgress{}
	progDetails.Summary = countRows(importRes.(*roachpb.ImportResponse).Imported, rd.spec.PKIDs)
	progDetails.ProgressIdx = entry.ProgressIdx
	progDetails.DataSpan = entry.Span
	details, err := gogotypes.MarshalAny(&progDetails)
	if err != nil {
		rd.MoveToDraining(err)
		return nil, rd.DrainHelper()
	}
	prog.ProgressDetails = *details
	return nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &prog}
}

// ConsumerClosed is part of the RowSource interface.
func (rd *restoreDataProcessor) ConsumerClosed() {
	rd.InternalClose()
}

func init() {
	rowexec.NewRestoreDataProcessor = newRestoreDataProcessor
}
