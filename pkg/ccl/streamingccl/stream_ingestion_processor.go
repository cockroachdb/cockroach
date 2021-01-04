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
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var streamIngestionResultTypes = []*types.T{
	types.String, // Processor address
	types.TimeTZ, // Checkpoint timestamp
}

type mvccKeyValues []storage.MVCCKeyValue

func (s mvccKeyValues) Len() int {
	return len(s)
}
func (s mvccKeyValues) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s mvccKeyValues) Less(i, j int) bool {
	return s[i].Key.Less(s[j].Key)
}

type progressUpdate struct {
	partition         streamclient.PartitionAddress
	resolvedTimestamp time.Time
}

type streamIngestionProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.StreamIngestionDataSpec
	output  execinfra.RowReceiver

	// TODO: This doesn't yet use a buffering adder since the current
	// implementation is specific to ingesting KV pairs without timestamps rather
	// than MVCCKeys.
	curBatch mvccKeyValues
	batcher  *bulk.SSTBatcher
	client   streamclient.StreamClient

	// ingestionErr stores any error that is returned from the worker goroutine so
	// that it can be fowarded through the DistSQL flow.
	ingestionErr error

	progressCh chan progressUpdate
}

// partitionEvent augments a normal event with the partition it came from.
type partitionEvent struct {
	streamclient.Event

	partition streamclient.PartitionAddress
}

var _ execinfra.Processor = &streamIngestionProcessor{}
var _ execinfra.RowSource = &streamIngestionProcessor{}

const streamIngestionProcessorName = "stream-ingestion-processor"

func newStreamIngestionDataProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.StreamIngestionDataSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	sip := &streamIngestionProcessor{
		flowCtx: flowCtx,
		spec:    spec,
		output:  output,
	}

	var err error
	evalCtx := sip.flowCtx.EvalCtx
	db := sip.flowCtx.Cfg.DB
	sip.batcher, err = bulk.MakeStreamSSTBatcher(sip.Ctx, db, evalCtx.Settings,
		func() int64 { return storageccl.MaxImportBatchSize(evalCtx.Settings) })
	if err != nil {
		return sip, err
	}

	sip.curBatch = make([]storage.MVCCKeyValue, 0)
	sip.client = streamclient.NewMockStreamClient()

	// TODO: This channel size was chosen arbitrarily.
	sip.progressCh = make(chan progressUpdate, 10)

	if err := sip.Init(sip, post, streamIngestionResultTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{}); err != nil {
		return nil, err
	}
	return sip, nil
}

// Start is part of the RowSource interface.
func (sip *streamIngestionProcessor) Start(ctx context.Context) context.Context {
	go func() {
		defer close(sip.progressCh)

		startTime := timeutil.Unix(0 /* sec */, sip.spec.StartTime.WallTime)
		eventChs := make(map[streamclient.PartitionAddress]chan streamclient.Event)
		for _, partitionAddress := range sip.spec.PartitionAddress {
			eventCh, err := sip.client.ConsumePartition(partitionAddress, startTime)
			if err != nil {
				sip.ingestionErr = err
				return
			}
			eventChs[partitionAddress] = eventCh
		}
		eventCh := merge(eventChs)

		if err := sip.startIngestion(eventCh); err != nil {
			sip.ingestionErr = err
			return
		}
	}()
	return sip.StartInternal(ctx, streamIngestionProcessorName)
}

// merge takes events from all the streams and merges them into 1 channel.
func merge(
	partitionStreams map[streamclient.PartitionAddress]chan streamclient.Event,
) chan partitionEvent {
	merged := make(chan partitionEvent)

	var wg sync.WaitGroup
	wg.Add(len(partitionStreams))

	for partition, eventCh := range partitionStreams {
		go func(partition streamclient.PartitionAddress, eventCh <-chan streamclient.Event) {
			for event := range eventCh {
				merged <- partitionEvent{
					Event:     event,
					partition: partition,
				}
			}
			wg.Done()
		}(partition, eventCh)
	}
	go func() {
		wg.Wait()
		close(merged)
	}()

	return merged
}

// Next is part of the RowSource interface.
func (sip *streamIngestionProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if sip.State != execinfra.StateRunning {
		return nil, sip.DrainHelper()
	}

	for progressUpdate := range sip.progressCh {
		row := rowenc.EncDatumRow{
			rowenc.DatumToEncDatum(types.String, tree.NewDString(string(progressUpdate.partition))),
			rowenc.DatumToEncDatum(types.TimeTZ, tree.NewDTimeTZFromTime(progressUpdate.resolvedTimestamp)),
		}
		return row, nil
	}

	if sip.ingestionErr != nil {
		sip.MoveToDraining(sip.ingestionErr)
		return nil, sip.DrainHelper()
	}

	sip.MoveToDraining(nil /* error */)
	return nil, sip.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (sip *streamIngestionProcessor) ConsumerClosed() {
	if sip.batcher != nil {
		sip.batcher.Close()
	}

	sip.InternalClose()
}

func (sip *streamIngestionProcessor) startIngestion(eventCh chan partitionEvent) error {
	for event := range eventCh {
		switch event.Type() {
		case streamclient.KVEvent:
			kv := event.GetKV()
			mvccKey := storage.MVCCKey{
				Key:       kv.Key,
				Timestamp: kv.Value.Timestamp,
			}
			sip.curBatch = append(sip.curBatch, storage.MVCCKeyValue{Key: mvccKey, Value: kv.Value.RawBytes})
		case streamclient.CheckpointEvent:
			// TODO: In addition to flushing when receiving a checkpoint event, we
			// should also flush when we've buffered sufficient KVs, or perhaps
			// periodically.
			resolvedTimePtr := event.GetResolved()
			if resolvedTimePtr == nil {
				return errors.New("checkpoint event was expected to have a resolved timestamp")
			}
			resolvedTime := *resolvedTimePtr
			if err := sip.flush(); err != nil {
				return err
			}
			sip.progressCh <- progressUpdate{
				partition:         event.partition,
				resolvedTimestamp: resolvedTime,
			}
		default:
			return errors.Newf("unknown streaming event type %v", event.Type())
		}
	}

	return nil
}

func (sip *streamIngestionProcessor) flush() error {
	// Ensure that the current batch is sorted.
	sort.Sort(sip.curBatch)

	for _, kv := range sip.curBatch {
		if err := sip.batcher.AddMVCCKey(sip.Ctx, kv.Key, kv.Value); err != nil {
			return errors.Wrapf(err, "adding key %+v", kv)
		}
	}

	if err := sip.batcher.Flush(sip.Ctx); err != nil {
		return errors.Wrap(err, "flushing")
	}

	// Reset the current batch.
	sip.curBatch = nil
	return sip.batcher.Reset(sip.Ctx)
}

func init() {
	rowexec.NewStreamIngestionDataProcessor = newStreamIngestionDataProcessor
}
