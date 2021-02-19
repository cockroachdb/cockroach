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
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const streamIngestionPollingProcName = `ingestpolling`

type streamIngestionPollingProcessor struct {
	execinfra.ProcessorBase
	execinfra.StreamingProcessor

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.StreamIngestionPollingSpec
	output  execinfra.RowReceiver

	jobID      int64
	registry   *jobs.Registry
	stopPoller chan struct{}
}

var _ execinfra.Processor = &streamIngestionPollingProcessor{}
var _ execinfra.RowSource = &streamIngestionPollingProcessor{}

func init() {
	rowexec.NewStreamIngestionPollingProcessor = newStreamIngestionPollingProcessor
}

func newStreamIngestionPollingProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.StreamIngestionPollingSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	sp := &streamIngestionPollingProcessor{
		flowCtx:    flowCtx,
		spec:       spec,
		output:     output,
		jobID:      spec.JobID,
		registry:   flowCtx.Cfg.JobRegistry,
		stopPoller: make(chan struct{}),
	}
	if err := sp.Init(
		sp,
		post,
		streamIngestionResultTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{},
			TrailingMetaCallback: func(ctx context.Context) []execinfrapb.ProducerMetadata {
				sp.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return sp, nil
}

func (sp *streamIngestionPollingProcessor) close() {
	if sp.InternalClose() {
		close(sp.stopPoller)
	}
}

// Start is part of the RowSource interface.
func (sp *streamIngestionPollingProcessor) Start(ctx context.Context) context.Context {
	return sp.StartInternal(ctx, streamIngestionPollingProcName)
}

// checkForCutoverSignal periodically loads the job progress to check for the
// sentinel value that signals the ingestion job to complete.
func (sp *streamIngestionPollingProcessor) checkForCutoverSignal(
	ctx context.Context,
) (rowenc.EncDatumRow, error) {
	tick := time.NewTicker(time.Second * 10)
	defer tick.Stop()
	for {
		select {
		case _, ok := <-sp.stopPoller:
			if !ok {
				return nil, nil
			}
			// Shouldn't come here.
			return nil, nil
		case <-tick.C:
			j, err := sp.registry.LoadJob(ctx, sp.jobID)
			if err != nil {
				return nil, err
			}
			progress := j.Progress()
			var prog *jobspb.Progress_StreamIngest
			var ok bool
			if prog, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
				return nil, errors.Newf("unknown progress type %T in stream ingestion job %d",
					j.Progress().Progress, sp.jobID)
			}
			// Job has been signaled to complete.
			if cutoverTime := prog.StreamIngest.CutoverTime; !cutoverTime.IsEmpty() {
				streamIngestBytes, err := protoutil.Marshal(prog.StreamIngest)
				if err != nil {
					return nil, err
				}
				row := rowenc.EncDatumRow{
					rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(streamIngestBytes))),
				}
				return row, nil
			}
		}
	}
}

// Next is part of the RowSource interface.
func (sp *streamIngestionPollingProcessor) Next() (
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if sp.State != execinfra.StateRunning {
		return nil, sp.DrainHelper()
	}

	row, err := sp.checkForCutoverSignal(sp.flowCtx.EvalCtx.Context)
	if err != nil {
		sp.MoveToDraining(errors.Wrap(err, "error when polling for cutover signal"))
		return nil, sp.DrainHelper()
	}

	if row != nil {
		return row, nil
	}

	sp.MoveToDraining(nil /* error */)
	return nil, sp.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (sp *streamIngestionPollingProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	sp.close()
}
