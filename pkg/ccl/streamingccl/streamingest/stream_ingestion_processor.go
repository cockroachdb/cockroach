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

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

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

	// eventCh is the merged event channel of all of the partition event streams.
	eventCh chan partitionEvent
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

	// Check if there are any interceptor methods that need to be registered with
	// the stream client.
	// These methods are invoked on every emitted Event.
	if knobs, ok := flowCtx.Cfg.TestingKnobs.StreamIngestionTestingKnobs.(*sql.
		StreamIngestionTestingKnobs); ok {
		if knobs.Interceptors != nil {
			if interceptable, ok := streamClient.(streamclient.InterceptableStreamClient); ok {
				for _, interceptor := range knobs.Interceptors {
					interceptable.RegisterInterception(interceptor)
				}
			}
		}
	}

	sip := &streamIngestionProcessor{
		flowCtx:  flowCtx,
		spec:     spec,
		output:   output,
		curBatch: make([]storage.MVCCKeyValue, 0),
		client:   streamClient,
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

	startTime := timeutil.Unix(0 /* sec */, sip.spec.StartTime.WallTime)
	eventChs := make(map[streamingccl.PartitionAddress]chan streamingccl.Event)
	for _, partitionAddress := range sip.spec.PartitionAddresses {
		eventCh, err := sip.client.ConsumePartition(ctx, partitionAddress, startTime)
		if err != nil {
			sip.ingestionErr = errors.Wrapf(err, "consuming partition %v", partitionAddress)
		}
		eventChs[partitionAddress] = eventCh
	}
	sip.eventCh = merge(ctx, eventChs)

	return ctx
}

// Next is part of the RowSource interface.
func (sip *streamIngestionProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if sip.State != execinfra.StateRunning {
		return nil, sip.DrainHelper()
	}

	progressUpdate, err := sip.consumeEvents()
	if err != nil {
		sip.MoveToDraining(err)
		return nil, sip.DrainHelper()
	}

	if progressUpdate != nil {
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
			sip.batcher.Close()
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

	// Reset the current batch.
	sip.curBatch = nil
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

// consumeEvents handles processing events on the merged event queue and returns
// once a checkpoint event has been emitted so that it can inform the downstream
// frontier processor to consider updating the frontier.
//
// It should only make a claim that about the resolved timestamp of a partition
// increasing after it has flushed all KV events previously received by that
// partition.
func (sip *streamIngestionProcessor) consumeEvents() (*jobspb.ResolvedSpan, error) {
	for event := range sip.eventCh {
		switch event.Type() {
		case streamingccl.KVEvent:
			kv := event.GetKV()
			if kv == nil {
				return nil, errors.New("kv event expected to have kv")
			}
			mvccKey := storage.MVCCKey{
				Key:       kv.Key,
				Timestamp: kv.Value.Timestamp,
			}
			sip.curBatch = append(sip.curBatch, storage.MVCCKeyValue{Key: mvccKey, Value: kv.Value.RawBytes})
		case streamingccl.CheckpointEvent:
			// TODO: In addition to flushing when receiving a checkpoint event, we
			// should also flush when we've buffered sufficient KVs. A buffering adder
			// would save us here.
			//
			// TODO: Add a setting to control the max flush-rate. This would be a time
			// interval to allow us to limit the number of flushes we do on
			// checkpoints.
			resolvedTimePtr := event.GetResolved()
			if resolvedTimePtr == nil {
				return nil, errors.New("checkpoint event expected to have a resolved timestamp")
			}
			resolvedTime := *resolvedTimePtr
			if err := sip.flush(); err != nil {
				return nil, errors.Wrap(err, "flushing")
			}

			// Each partition is represented by a span defined by the
			// partition address.
			spanStartKey := roachpb.Key(event.partition)
			return &jobspb.ResolvedSpan{
				Span:      roachpb.Span{Key: spanStartKey, EndKey: spanStartKey.Next()},
				Timestamp: resolvedTime,
			}, nil
		default:
			return nil, errors.Newf("unknown streaming event type %v", event.Type())
		}
	}

	return nil, nil
}

func init() {
	rowexec.NewStreamIngestionDataProcessor = newStreamIngestionDataProcessor
}
