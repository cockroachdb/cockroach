// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const streamIngestionFrontierProcName = `ingestfntr`

type streamIngestionFrontier struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.StreamIngestionFrontierSpec
	alloc   rowenc.DatumAlloc

	// input returns rows from one or more streamIngestion processors.
	input execinfra.RowSource
	// highWaterAtStart is the job high-water. It's used in an assertion that we
	// never regress the job high-water.
	highWaterAtStart hlc.Timestamp

	// frontier contains the current resolved timestamp high-water for the tracked
	// span set.
	frontier *span.Frontier
}

var _ execinfra.Processor = &streamIngestionFrontier{}
var _ execinfra.RowSource = &streamIngestionFrontier{}

func init() {
	rowexec.NewStreamIngestionFrontierProcessor = newStreamIngestionFrontierProcessor
}

func newStreamIngestionFrontierProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.StreamIngestionFrontierSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	frontier, err := span.MakeFrontier(spec.TrackedSpans...)
	if err != nil {
		return nil, err
	}
	sf := &streamIngestionFrontier{
		flowCtx:          flowCtx,
		spec:             spec,
		input:            input,
		highWaterAtStart: spec.HighWaterAtStart,
		frontier:         frontier,
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

// MustBeStreaming implements the execinfra.Processor interface.
func (sf *streamIngestionFrontier) MustBeStreaming() bool {
	return true
}

// Start is part of the RowSource interface.
func (sf *streamIngestionFrontier) Start(ctx context.Context) {
	ctx = sf.StartInternal(ctx, streamIngestionFrontierProcName)
	sf.input.Start(ctx)
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

		var frontierChanged bool
		var err error
		if frontierChanged, err = sf.noteResolvedTimestamps(row[0]); err != nil {
			sf.MoveToDraining(err)
			break
		}
		if frontierChanged {
			// Send back a row to the job so that it can update the progress.
			newResolvedTS := sf.frontier.Frontier()
			progressBytes, err := protoutil.Marshal(&newResolvedTS)
			if err != nil {
				sf.MoveToDraining(err)
				break
			}
			pushRow := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(progressBytes))),
			}
			if outRow := sf.ProcessRowHelper(pushRow); outRow != nil {
				return outRow, nil
			}
		}
	}
	return nil, sf.DrainHelper()
}

// noteResolvedTimestamps processes a batch of resolved timestamp events, and
// returns whether the frontier has moved forward after processing the batch.
func (sf *streamIngestionFrontier) noteResolvedTimestamps(d rowenc.EncDatum) (bool, error) {
	var frontierChanged bool
	if err := d.EnsureDecoded(streamIngestionResultTypes[0], &sf.alloc); err != nil {
		return frontierChanged, err
	}
	raw, ok := d.Datum.(*tree.DBytes)
	if !ok {
		return frontierChanged, errors.AssertionFailedf(`unexpected datum type %T: %s`, d.Datum,
			d.Datum)
	}
	var resolvedSpans jobspb.ResolvedSpans
	if err := protoutil.Unmarshal([]byte(*raw), &resolvedSpans); err != nil {
		return frontierChanged, errors.NewAssertionErrorWithWrappedErrf(err,
			`unmarshalling resolved timestamp: %x`, raw)
	}

	for _, resolved := range resolvedSpans.ResolvedSpans {
		// Inserting a timestamp less than the one the ingestion flow started at could
		// potentially regress the job progress. This is not expected and thus we
		// assert to catch such unexpected behavior.
		if !resolved.Timestamp.IsEmpty() && resolved.Timestamp.Less(sf.highWaterAtStart) {
			return frontierChanged, errors.AssertionFailedf(
				`got a resolved timestamp %s that is less than the frontier processor start time %s`,
				redact.Safe(resolved.Timestamp), redact.Safe(sf.highWaterAtStart))
		}

		if changed, err := sf.frontier.Forward(resolved.Span, resolved.Timestamp); err == nil {
			frontierChanged = frontierChanged || changed
		} else {
			return false, err
		}
	}

	return frontierChanged, nil
}
