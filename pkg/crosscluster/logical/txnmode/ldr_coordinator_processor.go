// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnapply"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnlock"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnscheduler"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

var (
	_ execinfra.Processor = &ldrCoordinatorProcessor{}
	_ execinfra.RowSource = &ldrCoordinatorProcessor{}
)

// coordinatorOutputTypes defines the row format emitted by the coordinator
// processor and consumed by applier processors.
// Column 0: routing key (encoded SQL instance ID for the BY_RANGE router).
// Column 1: serialized ApplierEvent (TODO: define serialization format).
var coordinatorOutputTypes = []*types.T{
	types.Bytes,
	types.Bytes,
}

// decodedBatch carries either a slice of decoded transactions or a checkpoint
// timestamp from the decode stage to the schedule stage. Exactly one of
// transactions or checkpoint is set per batch.
type decodedBatch struct {
	transactions    []ldrdecoder.Transaction
	rawTransactions [][]streampb.StreamEvent_KV
	checkpoint      hlc.Timestamp
}

type ldrCoordinatorProcessor struct {
	execinfra.ProcessorBase

	spec execinfrapb.TxnLDRCoordinatorSpec

	// All components initialized in Start().
	streamClients   []streamclient.Client     // one per partition
	feed            streamclient.Subscription // merged feed from all partitions
	txnDecoder      *ldrdecoder.TxnDecoder
	lockSynthesizer *txnlock.LockSynthesizer
	scheduler       *txnscheduler.Scheduler
	rangeCache      *rangecache.RangeCache

	applierIDs []ldrdecoder.ApplierID

	// Internal pipeline channels.
	batches      chan decodedBatch
	outputEvents chan rowenc.EncDatumRow

	// grp manages the lifecycle of the subscribe, decode, and schedule
	// goroutines. cancelGrp cancels the context used by grp, allowing
	// close() to unblock goroutines waiting on channel operations.
	grp       ctxgroup.Group
	cancelGrp context.CancelFunc
}

// Start implements execinfra.RowSource.
//
// The processor runs three pipeline stages as goroutines managed by a
// ctxgroup:
//
//	feed.Subscribe -> runDecode -> runScheduleAndRoute -> Next()
//
// All errors are reported to Next() via errCh, with the first error winning.
func (p *ldrCoordinatorProcessor) Start(ctx context.Context) {
	p.StartInternal(ctx, "ldrCoordinator")

	if err := p.setup(ctx); err != nil {
		p.MoveToDraining(err)
		return
	}

	p.batches = make(chan decodedBatch, 1)
	p.outputEvents = make(chan rowenc.EncDatumRow, 1)

	grpCtx, cancelGrp := context.WithCancel(ctx)
	p.cancelGrp = cancelGrp
	p.grp = ctxgroup.WithContext(grpCtx)

	// Start the subscribe stage.
	p.grp.GoCtx(func(ctx context.Context) error {
		return p.feed.Subscribe(ctx)
	})

	// Start the decode stage.
	p.grp.GoCtx(func(ctx context.Context) error {
		defer close(p.batches)
		return p.runDecode(ctx)
	})

	// Start the schedule and route stage.
	p.grp.GoCtx(func(ctx context.Context) error {
		defer close(p.outputEvents)
		return p.runScheduleAndRoute(ctx)
	})
}

// setup initializes all components required for the processor
// pipeline: decoder, lock synthesizer, scheduler, range cache, feed, and
// applier mappings.
func (p *ldrCoordinatorProcessor) setup(ctx context.Context) error {
	// Build table mappings from spec.
	tableMappings, err := buildTableMappings(ctx, p.spec.TableMetadataByDestID, p.spec.TypeDescriptors)
	if err != nil {
		return errors.Wrap(err, "building table mappings")
	}

	// Initialize decoder.
	p.txnDecoder, err = ldrdecoder.NewTxnDecoder(
		ctx,
		p.FlowCtx.Cfg.DB,
		p.FlowCtx.Cfg.Settings,
		tableMappings,
	)
	if err != nil {
		return errors.Wrap(err, "creating txn decoder")
	}

	// Initialize lock synthesizer.
	p.lockSynthesizer, err = txnlock.NewLockSynthesizer(
		ctx,
		p.FlowCtx.EvalCtx,
		p.FlowCtx.Cfg.LeaseManager.(*lease.Manager),
		p.FlowCtx.Cfg.DB.KV().Clock(),
		tableMappings,
	)
	if err != nil {
		return errors.Wrap(err, "creating lock synthesizer")
	}

	// Initialize scheduler.
	schedulerLockCount := int32(txnSchedulerLockCount.Get(&p.FlowCtx.Cfg.Settings.SV))
	p.scheduler = txnscheduler.NewScheduler(schedulerLockCount)

	// Initialize range cache.
	p.rangeCache = p.FlowCtx.Cfg.RangeCache

	// Create feed from partition specs (following LogicalReplicationWriterSpec pattern).
	p.feed, err = p.createTxnFeed(ctx)
	if err != nil {
		return errors.Wrap(err, "creating txn feed")
	}

	p.applierIDs = make([]ldrdecoder.ApplierID, len(p.spec.ApplierIds))
	copy(p.applierIDs, p.spec.ApplierIds)

	return nil
}

// Next implements execinfra.RowSource.
func (p *ldrCoordinatorProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if p.State == execinfra.StateRunning {
		row, ok := <-p.outputEvents
		if !ok {
			p.MoveToDraining(nil)
			return nil, p.DrainHelper()
		}
		return row, nil
	}
	return nil, p.DrainHelper()
}

func (p *ldrCoordinatorProcessor) ConsumerClosed() {
	p.close()
}

// close cancels background goroutines, waits for them to exit, and cleans up
// resources. It returns any non-cancellation error from the ctxgroup as
// trailing metadata so the TrailingMetaCallback can plumb it back to the
// distsql flow consumer.
func (p *ldrCoordinatorProcessor) close() []execinfrapb.ProducerMetadata {
	if p.Closed {
		return nil
	}

	// Cancel the goroutine context to unblock any goroutines waiting on
	// channel operations, then wait for them to exit.
	if p.cancelGrp != nil {
		p.cancelGrp()
	}
	var meta []execinfrapb.ProducerMetadata
	if err := p.grp.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		meta = append(meta, execinfrapb.ProducerMetadata{Err: err})
	}

	// Close all stream clients.
	for i, client := range p.streamClients {
		if err := client.Close(p.Ctx()); err != nil {
			log.Ops.Infof(p.Ctx(), "failed to close stream client %d: %v", i, err)
		}
	}

	p.InternalClose()
	return meta
}

// runDecode is the decode stage, reading from the feed and writing decoded
// transaction batches to the batches channel.
func (p *ldrCoordinatorProcessor) runDecode(ctx context.Context) error {
	sv := &p.FlowCtx.Cfg.Settings.SV
	maxInterval := txnBatchFlushInterval.Get(sv)
	batchRowCount := int(txnBatchSize.Get(sv))
	ticker := time.NewTicker(maxInterval)
	defer ticker.Stop()

	var transactions []ldrdecoder.Transaction
	var rawTransactions [][]streampb.StreamEvent_KV
	rows := 0

	flush := func() error {
		if len(transactions) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.batches <- decodedBatch{
			transactions:    transactions,
			rawTransactions: rawTransactions,
		}:
			transactions = nil
			rawTransactions = nil
			rows = 0
			ticker.Reset(maxInterval)
			return nil
		}
	}

	for {
		shouldFlush := false
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-p.feed.Events():
			if !ok {
				return flush()
			}
			switch ev.Type() {
			case crosscluster.KVEvent:
				for txnKVs := range txnfeed.Transactions(ev.GetKVs()) {
					decoded, err := p.txnDecoder.DecodeTxn(ctx, txnKVs)
					if err != nil {
						return errors.Wrap(err, "decoding transaction")
					}
					transactions = append(transactions, decoded)
					rawTransactions = append(rawTransactions, txnKVs)
					rows += len(decoded.WriteSet)
				}
				if batchRowCount <= rows {
					shouldFlush = true
				}
			case crosscluster.CheckpointEvent:
				if err := flush(); err != nil {
					return err
				}
				checkpoint := ev.GetCheckpoint().ResolvedSpans[0].Timestamp
				select {
				case <-ctx.Done():
					return ctx.Err()
				case p.batches <- decodedBatch{checkpoint: checkpoint}:
				}
			default:
				continue
			}
		case <-ticker.C:
			shouldFlush = true
		}

		if !shouldFlush {
			continue
		}
		if err := flush(); err != nil {
			return err
		}
	}
}

// runScheduleAndRoute reads decoded batches, schedules dependencies, hydrates
// ApplierIDs, and writes routed output rows to outputEvents.
func (p *ldrCoordinatorProcessor) runScheduleAndRoute(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-p.batches:
			if !ok {
				return nil
			}

			if batch.checkpoint.IsSet() {
				// Broadcast checkpoint to all appliers.
				//
				// TODO(msbutler): currently, we send rangefeed spawned checkpoints to
				// all appliers without thinking through the exact checkpointing
				// asumptions the applier makes. Specifically, the applier assumes it
				// will receive a checkpoint when the incoming transaction's
				// EventHorizon is greater than the previous checkpoint timestamp. I'm
				// pretty sure the periodic rangefeed checkpoints satisfy this, but we
				// should be more rigorous.
				for _, applierID := range p.applierIDs {
					row := p.encodeCheckpoint(batch.checkpoint, applierID)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case p.outputEvents <- row:
					}
				}
				continue
			}

			for i := range batch.transactions {
				applierID := p.hydrateApplierID(ctx, batch.transactions[i])
				batch.transactions[i].TxnID.ApplierID = applierID

				lockSet, err := p.lockSynthesizer.DeriveLocks(
					ctx, batch.transactions[i].WriteSet,
				)
				if err != nil {
					return errors.Wrap(err, "deriving locks")
				}
				batch.transactions[i].WriteSet = lockSet.SortedRows

				schedulerTxn := txnscheduler.Transaction{
					TxnID: batch.transactions[i].TxnID,
					Locks: lockSet.Locks,
				}
				dependencies, eventHorizon := p.scheduler.Schedule(
					schedulerTxn, nil,
				)

				scheduled := txnapply.ScheduledTransaction{
					Transaction:  batch.transactions[i],
					Dependencies: dependencies,
					EventHorizon: eventHorizon,
				}

				row := p.encodeScheduledTxn(scheduled, applierID, batch.rawTransactions[i])
				select {
				case <-ctx.Done():
					return ctx.Err()
				case p.outputEvents <- row:
				}
			}
		}
	}
}

// hydrateApplierID determines which applier should handle the given
// transaction by looking up the leaseholder of a key in the write set via the
// range cache. Falls back to a random applier if the leaseholder is not in the
// plan.
func (p *ldrCoordinatorProcessor) hydrateApplierID(
	ctx context.Context, txn ldrdecoder.Transaction,
) ldrdecoder.ApplierID {
	if len(p.applierIDs) == 1 {
		return p.applierIDs[0]
	}
	if len(txn.WriteSet) > 0 {
		row := txn.WriteSet[rand.Intn(len(txn.WriteSet))]
		rKey, err := destKeyFromRow(p.FlowCtx.Codec(), row)
		if err == nil {
			ri, err := p.rangeCache.Lookup(ctx, rKey)
			if err == nil {
				// TODO(msbutler): use a sql instance resolver instead of
				// casting the KV node ID directly to a sql instance ID.
				candidateID := ldrdecoder.ApplierID(ri.Lease.Replica.NodeID)
				for _, id := range p.applierIDs {
					if id == candidateID {
						return id
					}
				}
			}
		}
	}
	return p.applierIDs[rand.Intn(len(p.applierIDs))]
}

// destKeyFromRow extracts a destination key from a decoded row for range cache
// lookups. It returns the table prefix for the row's destination table.
func destKeyFromRow(codec keys.SQLCodec, row ldrdecoder.DecodedRow) (roachpb.RKey, error) {
	prefix := codec.TablePrefix(uint32(row.TableID))
	return keys.Addr(prefix)
}

// encodeScheduledTxn serializes a ScheduledTransaction into a DistSQL row
// routed to the given applier. The raw KV bytes are passed separately from the
// scheduled transaction because the coordinator only needs them for wire
// serialization, not for scheduling.
func (p *ldrCoordinatorProcessor) encodeScheduledTxn(
	scheduled txnapply.ScheduledTransaction,
	applierID ldrdecoder.ApplierID,
	rawKVs []streampb.StreamEvent_KV,
) rowenc.EncDatumRow {
	routingKey := physicalplan.RoutingKeyForSQLInstance(
		p.instanceIDForApplier(applierID),
	)

	encodedKVs := make([]streampb.StreamEvent_KV, len(rawKVs))
	copy(encodedKVs, rawKVs)

	evt := streampb.LDRApplierEvent{
		Event: &streampb.LDRApplierEvent_ScheduledTxn{
			ScheduledTxn: &streampb.EncodedScheduledTransaction{
				TxnID:        scheduled.TxnID,
				EncodedKVs:   encodedKVs,
				Dependencies: scheduled.Dependencies,
				EventHorizon: scheduled.EventHorizon,
			},
		},
	}

	payload, err := protoutil.Marshal(&evt)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(
			err, "marshaling scheduled txn"))
	}
	return rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(routingKey))},
		rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(payload))},
	}
}

// encodeCheckpoint creates a DistSQL row for a checkpoint timestamp routed to
// the given applier.
func (p *ldrCoordinatorProcessor) encodeCheckpoint(
	ts hlc.Timestamp, applierID ldrdecoder.ApplierID,
) rowenc.EncDatumRow {
	routingKey := physicalplan.RoutingKeyForSQLInstance(
		p.instanceIDForApplier(applierID),
	)

	evt := streampb.LDRApplierEvent{
		Event: &streampb.LDRApplierEvent_Checkpoint{
			Checkpoint: &streampb.EncodedCheckpoint{
				Timestamp: ts,
			},
		},
	}

	payload, err := protoutil.Marshal(&evt)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(
			err, "marshaling checkpoint"))
	}
	return rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(routingKey))},
		rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(payload))},
	}
}

func (p *ldrCoordinatorProcessor) instanceIDForApplier(
	applierID ldrdecoder.ApplierID,
) base.SQLInstanceID {
	return base.SQLInstanceID(applierID)
}

// createTxnFeed creates a merged feed from all partition specs in the coordinator spec.
// This follows the pattern from LogicalReplicationWriterSpec where each processor
// creates its own stream clients and subscriptions.
func (p *ldrCoordinatorProcessor) createTxnFeed(
	ctx context.Context,
) (streamclient.Subscription, error) {
	db := p.FlowCtx.Cfg.DB

	// Compute covering span for the merge feed.
	var coveringSpan roachpb.Span
	for i, partitionSpec := range p.spec.PartitionSpecs {
		if len(partitionSpec.Spans) == 0 {
			continue
		}
		partSpan := partitionSpec.Spans[0]
		for _, s := range partitionSpec.Spans[1:] {
			if s.Key.Compare(partSpan.Key) < 0 {
				partSpan.Key = s.Key
			}
			if partSpan.EndKey.Compare(s.EndKey) < 0 {
				partSpan.EndKey = s.EndKey
			}
		}
		if i == 0 {
			coveringSpan = partSpan
		} else {
			if partSpan.Key.Compare(coveringSpan.Key) < 0 {
				coveringSpan.Key = partSpan.Key
			}
			if coveringSpan.EndKey.Compare(partSpan.EndKey) < 0 {
				coveringSpan.EndKey = partSpan.EndKey
			}
		}
	}

	var orderedFeeds []streamclient.Subscription
	for i, partitionSpec := range p.spec.PartitionSpecs {
		// Parse partition URI.
		uri, err := streamclient.ParseClusterUri(partitionSpec.PartitionConnUri)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing partition %d URI", i)
		}

		// Create client for this partition.
		client, err := streamclient.NewStreamClient(ctx, uri, db,
			streamclient.WithStreamID(streampb.StreamID(p.spec.StreamID)),
			streamclient.WithCompression(true),
			streamclient.WithLogical(),
		)
		if err != nil {
			return nil, errors.Wrapf(err, "creating client for partition %d", i)
		}
		p.streamClients = append(p.streamClients, client)

		// Create partition frontier.
		partitionFrontier, err := span.MakeFrontierAt(p.spec.ReplicatedTime, partitionSpec.Spans...)
		if err != nil {
			return nil, errors.Wrapf(err, "creating frontier for partition %d", i)
		}

		// Subscribe to this partition.
		token := streamclient.SubscriptionToken(partitionSpec.SubscriptionToken)
		sub, err := client.Subscribe(
			ctx,
			streampb.StreamID(p.spec.StreamID),
			0,        // consumerNode - not used in logical replication
			int32(i), // consumerProc - use partition index
			token,
			p.spec.ReplicatedTime,
			partitionFrontier,
			streamclient.WithDiff(true),
			streamclient.WithMvccOrdering(true),
		)
		if err != nil {
			return nil, errors.Wrapf(err, "subscribing to partition %d", i)
		}
		orderedFeeds = append(orderedFeeds, sub)
	}

	// Create merged feed from all partitions.
	targetBatchKVs := int(txnBatchSize.Get(&p.FlowCtx.Cfg.Settings.SV))
	return txnfeed.NewMergeFeed(orderedFeeds, coveringSpan, targetBatchKVs), nil
}

// buildTableMappings reconstructs table mappings by hydrating type references
// in source descriptors. Used by both the coordinator and applier processors.
func buildTableMappings(
	ctx context.Context,
	tableMetadata map[int32]descpb.TableDescriptor,
	typeDescriptors []*descpb.TypeDescriptor,
) ([]ldrdecoder.TableMapping, error) {
	crossClusterResolver := crosscluster.MakeCrossClusterTypeResolver(typeDescriptors)

	tableMappings := make([]ldrdecoder.TableMapping, 0, len(tableMetadata))
	for destID, srcTableDesc := range tableMetadata {
		cpy := tabledesc.NewBuilder(&srcTableDesc).BuildCreatedMutableTable()

		if err := typedesc.HydrateTypesInDescriptor(ctx, cpy, crossClusterResolver); err != nil {
			return nil, errors.Wrapf(err, "hydrating types for dest table %d", destID)
		}

		tableMappings = append(tableMappings, ldrdecoder.TableMapping{
			SourceDescriptor: cpy,
			DestID:           descpb.ID(destID),
		})
	}
	return tableMappings, nil
}

func init() {
	rowexec.NewTxnLDRCoordinatorProcessor = func(
		ctx context.Context,
		flowCtx *execinfra.FlowCtx,
		processorID int32,
		spec execinfrapb.TxnLDRCoordinatorSpec,
		post *execinfrapb.PostProcessSpec,
	) (execinfra.Processor, error) {
		proc := &ldrCoordinatorProcessor{spec: spec}
		err := proc.Init(
			ctx, proc, post, coordinatorOutputTypes, flowCtx, processorID, nil,
			execinfra.ProcStateOpts{
				InputsToDrain: []execinfra.RowSource{},
				TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
					return proc.close()
				},
			},
		)
		if err != nil {
			return nil, err
		}
		return proc, nil
	}
}
