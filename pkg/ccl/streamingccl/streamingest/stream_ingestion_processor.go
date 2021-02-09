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
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var minimumFlushInterval = settings.RegisterDurationSetting(
	"bulkio.stream_ingestion.minimum_flush_interval",
	"the minimum timestamp between flushes; unless the internal memory buffer is full",
	10*time.Second,
).WithPublic()

var streamIngestionResultTypes = []*types.T{
	types.Bytes, // jobspb.ResolvedSpan
}

type mvccKeyValues []storage.MVCCKeyValue

func (s mvccKeyValues) Len() int           { return len(s) }
func (s mvccKeyValues) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s mvccKeyValues) Less(i, j int) bool { return s[i].Key.Less(s[j].Key) }

type streamIngestionProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.StreamIngestionDataSpec
	output  execinfra.RowReceiver

	// curBatch temporarily batches MVCC Keys so they can be
	// sorted before ingestion.
	// TODO: This doesn't yet use a buffering adder since the current
	// implementation is specific to ingesting KV pairs without timestamps rather
	// than MVCCKeys.
	curBatch mvccKeyValues
	// batcher is used to flush SSTs to the storage layer.
	batcher *bulk.SSTBatcher

	// client is a streaming client which provides a stream of events from a given
	// address.
	client streamclient.Client

	// ingestionErr stores any error that is returned from the worker goroutine so
	// that it can be forwarded through the DistSQL flow.
	ingestionErr error

	flushedCheckpoints  chan *jobspb.ResolvedSpan
	lastFlushTime       time.Time
	bufferedCheckpoints map[streamingccl.PartitionAddress]hlc.Timestamp
}

// partitionEvent augments a normal event with the partition it came from.
type partitionEvent struct {
	streamingccl.Event

	partition streamingccl.PartitionAddress
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
	streamClient, err := streamclient.NewStreamClient(spec.StreamAddress)
	if err != nil {
		return nil, err
	}

	sip := &streamIngestionProcessor{
		flowCtx:             flowCtx,
		spec:                spec,
		output:              output,
		curBatch:            make([]storage.MVCCKeyValue, 0),
		client:              streamClient,
		flushedCheckpoints:  make(chan *jobspb.ResolvedSpan, len(spec.PartitionAddresses)),
		bufferedCheckpoints: make(map[streamingccl.PartitionAddress]hlc.Timestamp),
	}

	evalCtx := flowCtx.EvalCtx
	db := flowCtx.Cfg.DB
	sip.batcher, err = bulk.MakeStreamSSTBatcher(sip.Ctx, db, evalCtx.Settings,
		func() int64 { return storageccl.MaxImportBatchSize(evalCtx.Settings) })
	if err != nil {
		return nil, errors.Wrap(err, "making sst batcher")
	}

	if err := sip.Init(sip, post, streamIngestionResultTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{},
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				sip.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	return sip, nil
}

// Start is part of the RowSource interface.
func (sip *streamIngestionProcessor) Start(ctx context.Context) context.Context {
	ctx = sip.StartInternal(ctx, streamIngestionProcessorName)

	go func() {
		defer close(sip.flushedCheckpoints)

		startTime := timeutil.Unix(0 /* sec */, sip.spec.StartTime.WallTime)
		eventChs := make(map[streamingccl.PartitionAddress]chan streamingccl.Event)
		for _, partitionAddress := range sip.spec.PartitionAddresses {
			eventCh, err := sip.client.ConsumePartition(ctx, partitionAddress, startTime)
			if err != nil {
				sip.ingestionErr = errors.Wrapf(err, "consuming partition %v", partitionAddress)
				return
			}
			eventChs[partitionAddress] = eventCh
		}
		mergedEvents := merge(ctx, eventChs)

		sip.ingestionErr = sip.consumeEvents(mergedEvents)
	}()

	return ctx
}

// Next is part of the RowSource interface.
func (sip *streamIngestionProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if sip.State != execinfra.StateRunning {
		return nil, sip.DrainHelper()
	}

	// read from buffered resolved events.
	progressUpdate, ok := <-sip.flushedCheckpoints
	if ok && progressUpdate != nil {
		progressBytes, err := protoutil.Marshal(progressUpdate)
		if err != nil {
			sip.MoveToDraining(err)
			return nil, sip.DrainHelper()
		}
		row := rowenc.EncDatumRow{
			rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(progressBytes))),
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
	sip.close()
}

func (sip *streamIngestionProcessor) close() {
	if sip.InternalClose() {
		if sip.batcher != nil {
			// FIXME: We should close this, but there might be a race here?
			// sip.batcher.Close()
		}
	}
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

	log.Infof(sip.Ctx, "testing: flushing %d keys!", len(sip.curBatch))

	// Go through buffered checkpoint events, and put them on the channel to be
	// emitted to the downstream frontier processor.
	for partition, timestamp := range sip.bufferedCheckpoints {
		// Each partition is represented by a span defined by the
		// partition address.
		spanStartKey := roachpb.Key(partition)
		sip.flushedCheckpoints <- &jobspb.ResolvedSpan{
			Span:      roachpb.Span{Key: spanStartKey, EndKey: spanStartKey.Next()},
			Timestamp: timestamp,
		}
	}

	// Reset the current batch.
	sip.curBatch = nil
	sip.lastFlushTime = timeutil.Now()
	sip.bufferedCheckpoints = make(map[streamingccl.PartitionAddress]hlc.Timestamp)

	return sip.batcher.Reset(sip.Ctx)
}

// merge takes events from all the streams and merges them into a single
// channel.
func merge(
	ctx context.Context, partitionStreams map[streamingccl.PartitionAddress]chan streamingccl.Event,
) chan partitionEvent {
	merged := make(chan partitionEvent)

	var wg sync.WaitGroup
	wg.Add(len(partitionStreams))

	for partition, eventCh := range partitionStreams {
		go func(partition streamingccl.PartitionAddress, eventCh <-chan streamingccl.Event) {
			defer wg.Done()
			for event := range eventCh {
				pe := partitionEvent{
					Event:     event,
					partition: partition,
				}

				select {
				case merged <- pe:
				case <-ctx.Done():
					// TODO: Add ctx.Err() to an error channel once ConsumePartition
					// supports an error ch.
					return
				}
			}
		}(partition, eventCh)
	}
	go func() {
		wg.Wait()
		close(merged)
	}()

	return merged
}

// consumeEvents continues to consume events until it flushes a checkpoint
// event. It may not flush every checkpoint event due to the minimum flush
// interval.
//
// It returns all partitions that are flushed.
func (sip *streamIngestionProcessor) consumeEvents(events chan partitionEvent) error {
	var timer timeutil.Timer
	defer timer.Stop()

	for {
		select {
		case event, ok := <-events:
			if !ok {
				// Done consuming events
				return sip.flush()
			}

			switch event.Type() {
			case streamingccl.KVEvent:
				if err := sip.bufferKV(event); err != nil {
					return err
				}
			case streamingccl.CheckpointEvent:
				if err := sip.bufferCheckpoint(event); err != nil {
					return err
				}

				minFlushInterval := 5 * time.Millisecond
				if timeutil.Since(sip.lastFlushTime) < minFlushInterval {
					// Not enough time has passed since the last flush. Let's set a timer
					// that will trigger a flush eventually.
					// TODO: This resets the timer every checkpoint event, but we only
					// need to reset it once.
					timer.Reset(time.Until(sip.lastFlushTime.Add(minFlushInterval)))
					continue
				}

				if err := sip.flush(); err != nil {
					return err
				}
			default:
				return errors.Newf("unknown streaming event type %v", event.Type())
			}
		case <-timer.C:
			timer.Read = true
			if err := sip.flush(); err != nil {
				return err
			}
		}
	}
}

func (sip *streamIngestionProcessor) bufferKV(event partitionEvent) error {
	// TODO: In addition to flushing when receiving a checkpoint event, we
	// should also flush when we've buffered sufficient KVs. A buffering adder
	// would save us here.

	kv := event.GetKV()
	if kv == nil {
		return errors.New("kv event expected to have kv")
	}
	mvccKey := storage.MVCCKey{
		Key:       kv.Key,
		Timestamp: kv.Value.Timestamp,
	}
	sip.curBatch = append(sip.curBatch, storage.MVCCKeyValue{Key: mvccKey, Value: kv.Value.RawBytes})
	return nil
}

func (sip *streamIngestionProcessor) bufferCheckpoint(event partitionEvent) error {
	resolvedTimePtr := event.GetResolved()
	if resolvedTimePtr == nil {
		return errors.New("checkpoint event expected to have a resolved timestamp")
	}
	resolvedTime := *resolvedTimePtr

	// Buffer the checkpoint.
	if lastTimestamp, ok := sip.bufferedCheckpoints[event.partition]; !ok || lastTimestamp.Less(resolvedTime) {
		sip.bufferedCheckpoints[event.partition] = resolvedTime
	}
	return nil
}

func init() {
	rowexec.NewStreamIngestionDataProcessor = newStreamIngestionDataProcessor
}
