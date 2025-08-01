// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"
	"runtime/pprof"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup"
	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// offlineInitialScanProcessor consumes a cross-cluster replication stream
// by decoding kvs in it to logical changes and applying them by executing DMLs.
type offlineInitialScanProcessor struct {
	execinfra.ProcessorBase
	processorID int32

	spec execinfrapb.LogicalReplicationOfflineScanSpec

	streamPartitionClient streamclient.Client

	// workerGroup is a context group holding all goroutines
	// related to this processor.
	workerGroup ctxgroup.Group

	// lastFlushTime keeps track of the last time that we flushed due to a
	// checkpoint timestamp event.
	lastFlushTime time.Time

	subscription       streamclient.Subscription
	subscriptionCancel context.CancelFunc

	// TODO(msbutler): consider removing this.
	stopCh chan struct{}

	errCh chan error

	checkpointCh chan offlineCheckpoint

	rekey *backup.KeyRewriter

	batcher              *bulk.SSTBatcher
	lastKeyAdded         roachpb.Key
	initialScanCompleted bool
}

type offlineCheckpoint struct {
	resolvedSpans              []jobspb.ResolvedSpan
	afterInitialScanCompletion bool
}

var (
	_ execinfra.Processor = &offlineInitialScanProcessor{}
	_ execinfra.RowSource = &offlineInitialScanProcessor{}
)

const offlineInitialScanProcessorName = "logical-replication-offline-initial-scan"

func newNewOfflineInitialScanProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.LogicalReplicationOfflineScanSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {

	rekeyer, err := backup.MakeKeyRewriterFromRekeys(flowCtx.Codec(),
		spec.Rekey, nil, /* tenantRekeys */
		false /* restoreTenantFromStream */)
	if err != nil {
		return nil, err
	}

	lrw := &offlineInitialScanProcessor{
		spec:         spec,
		processorID:  processorID,
		stopCh:       make(chan struct{}),
		checkpointCh: make(chan offlineCheckpoint),
		errCh:        make(chan error, 1),
		rekey:        rekeyer,
		lastKeyAdded: roachpb.Key{},
	}

	if err := lrw.Init(ctx, lrw, post, logicalReplicationWriterResultType, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				lrw.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	return lrw, nil
}

// Start launches a set of goroutines that read from the spans
// assigned to this processor, and ingests them via AddSStable.
//
// Start implements the RowSource interface.
func (o *offlineInitialScanProcessor) setup(ctx context.Context) error {
	db := o.FlowCtx.Cfg.DB
	// TODO(msbutler): consider if we want to pre-split and scatter and disable on the fly split and scatter.
	var err error
	o.batcher, err = bulk.MakeSSTBatcher(
		ctx,
		"ldr init scan",
		db.KV(),
		o.FlowCtx.Cfg.Settings,
		hlc.Timestamp{}, /* disallowShadowingBelow */
		true,            /* writeAtBatchTs */
		true,            /* splitAndScatterRanges */
		o.FlowCtx.Cfg.BackupMonitor.MakeConcurrentBoundAccount(),
		o.FlowCtx.Cfg.BulkSenderLimiter)
	if err != nil {
		return err
	}

	// Start the subscription for our partition.
	partitionSpec := o.spec.PartitionSpec
	token := streamclient.SubscriptionToken(partitionSpec.SubscriptionToken)
	uri, err := streamclient.ParseClusterUri(partitionSpec.PartitionConnUri)
	if err != nil {
		return err
	}
	streamClient, err := streamclient.NewStreamClient(ctx, uri, db,
		streamclient.WithStreamID(streampb.StreamID(o.spec.StreamID)),
		streamclient.WithCompression(true),
	)
	if err != nil {
		return errors.Wrapf(err, "creating client for partition spec %q from %q", token, uri.Redacted())
	}
	o.streamPartitionClient = streamClient

	frontier, err := span.MakeFrontier(o.spec.PartitionSpec.Spans...)
	if err != nil {
		return err
	}
	defer frontier.Release()

	for _, resolvedSpan := range o.spec.Checkpoint.ResolvedSpans {
		if _, err := frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp); err != nil {
			return err
		}
	}
	sub, err := streamClient.Subscribe(ctx,
		streampb.StreamID(o.spec.StreamID),
		int32(o.FlowCtx.NodeID.SQLInstanceID()), o.ProcessorID,
		token,
		o.spec.InitialScanTimestamp, frontier)
	if err != nil {
		return err
	}
	o.subscription = sub
	return nil

}

func (o *offlineInitialScanProcessor) Start(ctx context.Context) {
	tags := &logtags.Buffer{}
	tags = tags.Add("job", o.spec.JobID)
	tags = tags.Add("src-node", o.spec.PartitionSpec.PartitionID)
	tags = tags.Add("proc", o.ProcessorID)
	ctx = logtags.AddTags(ctx, tags)

	ctx = o.StartInternal(ctx, offlineInitialScanProcessorName)

	defer o.FlowCtx.Cfg.JobRegistry.MarkAsIngesting(catpb.JobID(o.spec.JobID))()

	if err := o.setup(ctx); err != nil {
		o.MoveToDrainingAndLogError(err)
		return
	}

	log.Infof(ctx, "starting offline initial scan writer for partition %s", o.spec.PartitionSpec.PartitionID)

	// We use a different context for the subscription here so
	// that we can explicitly cancel it.
	var subscriptionCtx context.Context
	subscriptionCtx, o.subscriptionCancel = context.WithCancel(o.Ctx())
	o.workerGroup = ctxgroup.WithContext(o.Ctx())
	o.workerGroup.GoCtx(func(_ context.Context) error {
		if err := o.subscription.Subscribe(subscriptionCtx); err != nil {
			log.Infof(o.Ctx(), "subscription completed. Error: %s", err)
			o.sendError(errors.Wrap(err, "subscription"))
		}
		return nil
	})
	o.workerGroup.GoCtx(func(ctx context.Context) error {
		defer close(o.checkpointCh)
		pprof.Do(ctx, pprof.Labels("proc", fmt.Sprintf("%d", o.ProcessorID)), func(ctx context.Context) {
			for event := range o.subscription.Events() {
				if err := o.handleEvent(ctx, event); err != nil {
					log.Infof(o.Ctx(), "consumer completed. Error: %s", err)
					o.sendError(errors.Wrap(err, "consume events"))
				}
			}
			if err := o.subscription.Err(); err != nil {
				o.sendError(errors.Wrap(err, "subscription"))
			}
		})
		return nil
	})
}

// Next is part of the RowSource interface.
func (o *offlineInitialScanProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if o.State != execinfra.StateRunning {
		return nil, o.DrainHelper()
	}

	select {
	case checkpoint, ok := <-o.checkpointCh:
		switch {
		case !ok:
			select {
			case err := <-o.errCh:
				o.MoveToDrainingAndLogError(err)
				return nil, o.DrainHelper()
			case <-time.After(10 * time.Second):
				logcrash.ReportOrPanic(o.Ctx(), &o.FlowCtx.Cfg.Settings.SV,
					"event channel closed but no error found on err channel after 10 seconds")
				o.MoveToDrainingAndLogError(nil /* error */)
				return nil, o.DrainHelper()
			}
		case checkpoint.afterInitialScanCompletion:
			// The previous checkpoint completed the initial scan and was already
			// ingested by the coordinator, so we can gracefully shut down the
			// processor.
			o.MoveToDraining(nil)
			return nil, o.DrainHelper()
		default:
			progressUpdate := &jobspb.ResolvedSpans{ResolvedSpans: checkpoint.resolvedSpans}
			progressBytes, err := protoutil.Marshal(progressUpdate)
			if err != nil {
				o.MoveToDrainingAndLogError(err)
				return nil, o.DrainHelper()
			}
			row := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(progressBytes))),
			}
			return row, nil
		}
	case err := <-o.errCh:
		o.MoveToDrainingAndLogError(err)
		return nil, o.DrainHelper()
	}
}

func (o *offlineInitialScanProcessor) MoveToDrainingAndLogError(err error) {
	if err != nil {
		log.Infof(o.Ctx(), "gracefully draining with error: %s", err)
	}
	o.MoveToDraining(err)
}

// MustBeStreaming implements the Processor interface.
func (o *offlineInitialScanProcessor) MustBeStreaming() bool {
	return true
}

// ConsumerClosed is part of the RowSource interface.
func (o *offlineInitialScanProcessor) ConsumerClosed() {
	o.close()
}

func (o *offlineInitialScanProcessor) close() {
	if o.Closed {
		return
	}

	if o.streamPartitionClient != nil {
		_ = o.streamPartitionClient.Close(o.Ctx())
	}
	if o.stopCh != nil {
		close(o.stopCh)
	}
	if o.subscriptionCancel != nil {
		o.subscriptionCancel()
	}

	// We shouldn't need to explicitly cancel the context for members of the
	// worker group. The client close and stopCh close above should result
	// in exit signals being sent to all relevant goroutines.
	if err := o.workerGroup.Wait(); err != nil {
		log.Errorf(o.Ctx(), "error on close(): %s", err)
	}

	if o.batcher != nil {
		o.batcher.Close(o.Ctx())
	}

	o.InternalClose()
}

func (o *offlineInitialScanProcessor) sendError(err error) {
	if err == nil {
		return
	}
	select {
	case o.errCh <- err:
	default:
		log.VInfof(o.Ctx(), 2, "dropping additional error: %s", err)
	}
}

func (o *offlineInitialScanProcessor) handleEvent(
	ctx context.Context, event crosscluster.Event,
) error {
	switch event.Type() {
	case crosscluster.KVEvent:
		if err := o.handleStreamBuffer(ctx, event.GetKVs()); err != nil {
			return err
		}
	case crosscluster.CheckpointEvent:
		if err := o.checkpoint(ctx, event.GetCheckpoint().ResolvedSpans); err != nil {
			return err
		}
	case crosscluster.SSTableEvent, crosscluster.DeleteRangeEvent:
		return errors.AssertionFailedf("unexpected event for offline initial scan: %v", event)
	case crosscluster.SplitEvent:
		log.Infof(o.Ctx(), "SplitEvent received on logical replication stream")
	default:
		return errors.Newf("unknown streaming event type %v", event.Type())
	}
	return nil
}

func (o *offlineInitialScanProcessor) checkpoint(
	ctx context.Context, resolvedSpans []jobspb.ResolvedSpan,
) error {
	if resolvedSpans == nil {
		return errors.New("checkpoint event expected to have resolved spans")
	}

	completedInitialScan := true
	for i := range resolvedSpans {
		if resolvedSpans[i].Timestamp.IsEmpty() {
			completedInitialScan = false
		}
		if o.spec.InitialScanTimestamp.Less(resolvedSpans[i].Timestamp) {
			// Elide all checkpoints after initial scan timestamp.
			resolvedSpans[i].Timestamp = o.spec.InitialScanTimestamp
		}
	}

	if err := o.flushBatch(ctx); err != nil {
		return errors.Wrap(err, "flushing batcher on checkpoint")
	}
	o.batcher.Reset(ctx)

	select {
	case o.checkpointCh <- offlineCheckpoint{
		resolvedSpans:              resolvedSpans,
		afterInitialScanCompletion: o.initialScanCompleted,
	}:
	case <-ctx.Done():
		return ctx.Err()
	case <-o.stopCh:
		// we need to select on stopCh here because the reader
		// of checkpointCh is the caller of Next(). But there
		// might never be another Next() call since it may
		// have exited based on an error.
		return nil
	}
	o.lastFlushTime = timeutil.Now()
	if completedInitialScan {
		// Modify the processor state _after_ Next() has received the first
		// checkpoint that completes the initial scan. This ensures Next()'s
		// caller receives this checkpoint. Then, on the next checkpoint,
		// `afterInitialScanCompletion` will be set, causing Next to gracefully
		// shutdown the processor.
		o.initialScanCompleted = true
	}
	return nil
}

func (o *offlineInitialScanProcessor) flushBatch(ctx context.Context) error {
	if err := o.batcher.Flush(ctx); err != nil {
		return err
	}
	o.batcher.Reset(ctx)
	o.lastKeyAdded = roachpb.Key{}
	return nil
}

// handleStreamBuffer handles a buffer of KV events from the incoming stream.
func (o *offlineInitialScanProcessor) handleStreamBuffer(
	ctx context.Context, kvs []streampb.StreamEvent_KV,
) error {
	for _, ev := range kvs {
		kv := ev.KeyValue
		if o.spec.InitialScanTimestamp.Less(kv.Value.Timestamp) {
			// Elide all keys sent during rangefeed steady state.
			continue
		}
		var err error
		var ok bool
		kv.Key, ok, err = o.rekey.RewriteKey(kv.Key, 0)
		if err != nil {
			return err
		}
		if !ok {
			return errors.AssertionFailedf("key rewrite returned !ok")
		}
		kv.Value.ClearChecksum()
		kv.Value.InitChecksum(kv.Key)

		if kv.Key.Less(o.lastKeyAdded) {
			// Indicates we've begun ingesting a new span in the partition.
			//
			// TODO(msbutler): send all spans in reverse order to always hit this
			// check, which would guarantee all ssts ingest to l6.
			if err := o.flushBatch(ctx); err != nil {
				return errors.Wrap(err, "flushing batcher on new span")
			}
		}
		if err := o.batcher.AddMVCCKeyLDR(ctx, storage.MVCCKey{Key: kv.Key, Timestamp: kv.Value.Timestamp}, kv.Value.RawBytes); err != nil {
			return errors.Wrap(err, "adding key to batcher")
		}
		o.lastKeyAdded = kv.Key
	}

	return nil
}

func init() {
	rowexec.NewLogicalReplicationOfflineScanProcessor = newNewOfflineInitialScanProcessor
}
