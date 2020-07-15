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
	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	sqlbase "github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

// Progress is streamed to the coordinator through metadata.
var restoreDataOutputTypes = []*types.T{}

type restoreDataProcessor struct {
	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.RestoreDataSpec
	input   execinfra.RowSource
	output  execinfra.RowReceiver
}

var _ execinfra.Processor = &restoreDataProcessor{}

// OutputTypes implements the execinfra.Processor interface.
func (rd *restoreDataProcessor) OutputTypes() []*types.T {
	return restoreDataOutputTypes
}

func newRestoreDataProcessor(
	flowCtx *execinfra.FlowCtx,
	_ int32,
	spec execinfrapb.RestoreDataSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	rd := &restoreDataProcessor{
		flowCtx: flowCtx,
		input:   input,
		spec:    spec,
		output:  output,
	}
	return rd, nil
}

// Run implements the execinfra.Processor interface.
func (rd *restoreDataProcessor) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "restoreDataProcessor")
	defer tracing.FinishSpan(span)
	defer rd.output.ProducerDone()

	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)

	// We don't have to worry about this go routine leaking because next we loop over progCh
	// which is closed only after the goroutine returns.
	var err error
	go func() {
		defer close(progCh)
		err = runRestoreData(ctx, rd.flowCtx, &rd.spec, rd.input, progCh)
	}()

	for prog := range progCh {
		// Take a copy so that we can send the progress address to the output processor.
		p := prog
		rd.output.Push(nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p})
	}

	if err != nil {
		rd.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
		return
	}
}

func runRestoreData(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.RestoreDataSpec,
	input execinfra.RowSource,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	input.Start(ctx)
	kr, err := storageccl.MakeKeyRewriterFromRekeys(spec.Rekeys)
	if err != nil {
		return err
	}

	alloc := &sqlbase.DatumAlloc{}
	for {
		// We read rows from the SplitAndScatter processor. We expect each row to
		// contain 2 columns. The first is used to route the row to this processor,
		// and the second contains the RestoreSpanEntry that we're interested in.
		row, meta := input.Next()
		if meta != nil {
			return errors.Newf("unexpected metadata %+v", meta)
		}
		if row == nil {
			// Done.
			break
		}

		if len(row) != 2 {
			return errors.New("expected input rows to have exactly 2 columns")
		}
		if err := row[1].EnsureDecoded(types.Bytes, alloc); err != nil {
			return err
		}
		datum := row[1].Datum
		entryDatumBytes, ok := datum.(*tree.DBytes)
		if !ok {
			return errors.AssertionFailedf(`unexpected datum type %T: %+v`, datum, row)
		}

		var entry execinfrapb.RestoreSpanEntry
		if err := protoutil.Unmarshal([]byte(*entryDatumBytes), &entry); err != nil {
			return errors.Wrap(err, "un-marshaling restore span entry")
		}

		newSpanKey, err := rewriteBackupSpanKey(kr, entry.Span.Key)
		if err != nil {
			return errors.Wrap(err, "re-writing span key to import")
		}

		log.VEventf(ctx, 1 /* level */, "importing span %v", entry.Span)
		importRequest := &roachpb.ImportRequest{
			// Import is a point request because we don't want DistSender to split
			// it. Assume (but don't require) the entire post-rewrite span is on the
			// same range.
			RequestHeader: roachpb.RequestHeader{Key: newSpanKey},
			DataSpan:      entry.Span,
			Files:         entry.Files,
			EndTime:       spec.RestoreTime,
			Rekeys:        spec.Rekeys,
			Encryption:    spec.Encryption,
		}

		importRes, pErr := kv.SendWrapped(ctx, flowCtx.Cfg.DB.NonTransactionalSender(), importRequest)
		if pErr != nil {
			return errors.Wrapf(pErr.GoError(), "importing span %v", importRequest.DataSpan)
		}

		var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
		progDetails := RestoreProgress{}
		progDetails.Summary = countRows(importRes.(*roachpb.ImportResponse).Imported, spec.PKIDs)
		progDetails.ProgressIdx = entry.ProgressIdx
		details, err := gogotypes.MarshalAny(&progDetails)
		if err != nil {
			return err
		}
		prog.ProgressDetails = *details
		progCh <- prog
		log.VEventf(ctx, 1 /* level */, "imported span %v", entry.Span)
	}

	return nil
}

func init() {
	rowexec.NewRestoreDataProcessor = newRestoreDataProcessor
}
