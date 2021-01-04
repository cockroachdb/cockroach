// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const streamIngestionFrontierProcName = `ingestfntr`

type streamIngestionFrontier struct {
	execinfra.ProcessorBase
	execinfra.StreamingProcessor

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.StreamIngestionFrontierSpec
	a       rowenc.DatumAlloc

	// input returns rows from one or more streamIngestion processors
	input execinfra.RowSource
	// jobProgressedFn, if non-nil, is called to checkpoint the stream ingestion
	// job's progress in the corresponding system job entry.
	jobProgressedFn func(context.Context, jobs.HighWaterProgressedFn) error
	// highWaterAtStart is the job high-water. It's used in an assertion that we
	// never regress the job high-water.
	highWaterAtStart hlc.Timestamp

	// frontier contains the current resolved timestamp high-water for the tracked
	// span set.
	frontier *span.Frontier
}

var _ execinfra.Processor = &streamIngestionFrontier{}
var _ execinfra.RowSource = &streamIngestionFrontier{}

func newStreamIngestionFrontierProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.StreamIngestionFrontierSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	sf := &streamIngestionFrontier{
		flowCtx:  flowCtx,
		spec:     spec,
		input:    input,
		frontier: span.MakeFrontier(spec.TrackedSpans...),
	}
	if err := sf.Init(
		sf,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{sf.input},
		},
	); err != nil {
		return nil, err
	}
	return sf, nil
}

// Start is part of the RowSource interface.
func (sf *streamIngestionFrontier) Start(ctx context.Context) context.Context {
	sf.input.Start(ctx)

	ctx = sf.StartInternal(ctx, streamIngestionFrontierProcName)

	sf.highWaterAtStart = hlc.Timestamp{}
	if sf.spec.JobID != 0 {
		job, err := sf.flowCtx.Cfg.JobRegistry.LoadJob(ctx, sf.spec.JobID)
		if err != nil {
			sf.MoveToDraining(err)
			return ctx
		}
		sf.jobProgressedFn = job.HighWaterProgressed

		p := job.Progress()
		if ts := p.GetHighWater(); ts != nil {
			sf.highWaterAtStart.Forward(*ts)
		}
	}
	return ctx
}

// Next is part of the RowSource interface.
func (sf *streamIngestionFrontier) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for sf.State == execinfra.StateRunning {
		row, meta := sf.input.Next()
		if meta != nil {
			if meta.Err != nil {
				sf.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			sf.MoveToDraining(nil /* err */)
			break
		}

		if err := sf.noteResolvedTimestamp(row[0]); err != nil {
			sf.MoveToDraining(err)
			break
		}
	}

	return nil, sf.DrainHelper()
}

func (sf *streamIngestionFrontier) noteResolvedTimestamp(d rowenc.EncDatum) error {
	if err := d.EnsureDecoded(streamIngestionResultTypes[0], &sf.a); err != nil {
		return err
	}
	raw, ok := d.Datum.(*tree.DBytes)
	if !ok {
		return errors.AssertionFailedf(`unexpected datum type %T: %s`, d.Datum, d.Datum)
	}
	var resolved jobspb.ResolvedSpan
	if err := protoutil.Unmarshal([]byte(*raw), &resolved); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err,
			`unmarshalling resolved timestamp: %x`, raw)
	}

	// Inserting a timestamp less than the one the ingestion flow started at could
	// potentially regress the job progress. This is not expected and thus we
	// assert to catch such unexpected behavior.
	if !resolved.Timestamp.IsEmpty() && resolved.Timestamp.Less(sf.highWaterAtStart) {
		logcrash.ReportOrPanic(sf.Ctx, &sf.flowCtx.Cfg.Settings.SV,
			`got a resolved timestamp %s that is less than the initial high-water %s`,
			redact.Safe(resolved.Timestamp), redact.Safe(sf.highWaterAtStart))
		return nil
	}

	frontierChanged := sf.maybeMoveFrontier(resolved.Span, resolved.Timestamp)
	if frontierChanged {
		if err := sf.handleFrontierChanged(); err != nil {
			return err
		}
	}
	return nil
}

// maybeMoveFrontier updates the resolved ts for the provided span, and returns
// true if the update causes the frontier to move to higher resolved ts.
func (sf *streamIngestionFrontier) maybeMoveFrontier(
	span roachpb.Span, resolved hlc.Timestamp,
) bool {
	prevResolved := sf.frontier.Frontier()
	sf.frontier.Forward(span, resolved)
	return prevResolved.Less(sf.frontier.Frontier())
}

func (sf *streamIngestionFrontier) handleFrontierChanged() error {
	newResolved := sf.frontier.Frontier()
	return sf.checkpointResolvedTimestamp(newResolved)
}

// checkpointResolvedTimestamp checkpoints an ingestion-level resolved timestamp
// to the jobs record. It is only called if the new resolved timestamp is later
// than the current one.
//
// TODO(adityamaru): We could consider changing to a per partition progress
// model, rather than a global resolved timestamp watermark. This might make for
// more efficient resumption semantics.
func (sf *streamIngestionFrontier) checkpointResolvedTimestamp(resolved hlc.Timestamp) error {
	return sf.jobProgressedFn(sf.Ctx, func(ctx context.Context, txn *kv.Txn,
		details jobspb.ProgressDetails) (hlc.Timestamp, error) {
		return resolved, nil
	})
}

// ConsumerClosed is part of the RowSource interface.
func (sf *streamIngestionFrontier) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	sf.InternalClose()
}
