// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// cutoverProcessor is a local processor that polls a stream ingestion
// job and sends the current cutover time to all downstream
// processors.
type cutoverProcessor struct {
	execinfra.ProcessorBase

	settings     *cluster.Settings
	registry     *jobs.Registry
	jobID        jobspb.JobID
	testingKnobs *sql.StreamingTestingKnobs

	doneCh        chan struct{}
	cutoverTimeCh chan hlc.Timestamp
	wg            ctxgroup.Group

	mu struct {
		syncutil.Mutex

		pollError error
	}
}

// InputWithOutput implements LocalProcessor.
func (cm *cutoverProcessor) InitWithOutput(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) error {
	return cm.Init(
		ctx,
		cm,
		post,
		streamIngestionCutoverProcessorTypes,
		flowCtx,
		0, /* processorID */
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				cm.close()
				return nil
			},
		},
	)

}

// SetInput implmenents LocalProcessor.
func (cm *cutoverProcessor) SetInput(ctx context.Context, input execinfra.RowSource) error {
	return nil
}

var _ execinfra.LocalProcessor = (*cutoverProcessor)(nil)

// MustBeStreaming implements the execinfra.Processor interface.
func (cm *cutoverProcessor) MustBeStreaming() bool { return true }

// Start is part of the RowSource interface.
func (cm *cutoverProcessor) Start(ctx context.Context) {
	ctx = cm.StartInternal(ctx, "stream-ingestion-cutover-processor")

	cm.wg = ctxgroup.WithContext(ctx)
	cm.doneCh = make(chan struct{})
	cm.cutoverTimeCh = make(chan hlc.Timestamp)

	cm.wg.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(cutoverSignalPollInterval.Get(&cm.settings.SV))
		defer tick.Stop()
		defer close(cm.cutoverTimeCh)

		var (
			failureCount        = 0
			maxRepeatedFailures = 10
		)
		for {
			select {
			case <-cm.doneCh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-tick.C:
				ts, err := cm.loadCutoverTime(ctx)
				if err != nil {
					if failureCount >= maxRepeatedFailures {
						cm.setPollError(err)
						return err
					}
					failureCount++
					log.Warningf(ctx, "cutover-time-poller: failed to load cutover time from job: %s (attempt %d/%d)",
						err.Error(), failureCount, maxRepeatedFailures)
					continue
				}
				failureCount = 0
				// NOTE: We purposefully always send
				// the cutover time here, even if it
				// is empty or the same as a previous
				// timestamp. Without sending it
				// peridically, we may never learn
				// that our consumers are closing.
				cm.cutoverTimeCh <- ts
			}
		}
	})
}

// Next is part of the RowSource interface.
func (cm *cutoverProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if cm.State != execinfra.StateRunning {
		return nil, cm.DrainHelper()
	}

	select {
	case cutoverTime, ok := <-cm.cutoverTimeCh:
		if ok {
			row, err := encodeCutoverTimeRow(cutoverTime)
			if err != nil {
				cm.MoveToDraining(err)
				return nil, cm.DrainHelper()
			}
			return row, nil
		}
	case <-cm.Ctx().Done():
	case <-cm.doneCh:
		cm.MoveToDraining(nil)
		return nil, cm.DrainHelper()
	}

	cm.MoveToDraining(cm.getPollError())
	return nil, cm.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (cm *cutoverProcessor) ConsumerClosed() {
	cm.close()
}

func (cm *cutoverProcessor) close() {
	if cm.Closed {
		return
	}
	if cm.doneCh != nil {
		close(cm.doneCh)
	}
	_ = cm.wg.Wait()
	cm.InternalClose()
}

func (cm *cutoverProcessor) setPollError(err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.mu.pollError = err
}

func (cm *cutoverProcessor) getPollError() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.mu.pollError
}

func (cm *cutoverProcessor) loadCutoverTime(ctx context.Context) (hlc.Timestamp, error) {
	if cm.testingKnobs != nil && cm.testingKnobs.BeforePollCutoverTime != nil {
		if err := cm.testingKnobs.BeforePollCutoverTime(); err != nil {
			return hlc.Timestamp{}, err
		}
	}

	j, err := cm.registry.LoadJob(ctx, cm.jobID)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	progress := j.Progress()
	var sp *jobspb.Progress_StreamIngest
	var ok bool
	if sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
		return hlc.Timestamp{}, errors.Newf("unknown progress type %T in stream ingestion job %d",
			j.Progress().Progress, cm.jobID)
	}
	return sp.StreamIngest.CutoverTime, nil
}

var streamIngestionCutoverProcessorTypes = []*types.T{
	types.Bytes, // hlc.Timestamp
}

func encodeCutoverTimeRow(ts hlc.Timestamp) (rowenc.EncDatumRow, error) {
	cutoverTimeBytes, err := protoutil.Marshal(&ts)
	if err != nil {
		return nil, err
	}

	return rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(cutoverTimeBytes))),
	}, nil
}

func decodeCutoverTimeRow(row rowenc.EncDatumRow, alloc *tree.DatumAlloc) (hlc.Timestamp, error) {
	if len(row) != 1 {
		return hlc.Timestamp{}, errors.AssertionFailedf("unexpected number of datums: %d", len(row))
	}

	if err := row[0].EnsureDecoded(types.Bytes, alloc); err != nil {
		return hlc.Timestamp{}, err
	}

	datum := row[0].Datum
	cutoverDatumBytes, ok := datum.(*tree.DBytes)
	if !ok {
		return hlc.Timestamp{}, errors.AssertionFailedf("unexpected datum type %T: %+v", datum, row)
	}

	var cutoverTime hlc.Timestamp
	if err := protoutil.Unmarshal([]byte(*cutoverDatumBytes), &cutoverTime); err != nil {
		return hlc.Timestamp{}, err
	}
	return cutoverTime, nil
}
