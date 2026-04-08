// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

// TODO(jeffswenson): move this to a txncoordinator package.

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnapply"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnlock"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnscheduler"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnwriter"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/besteffort"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var txnNumWriters = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.txn_num_writers",
	"the number of parallel writers for transactional logical replication",
	128,
	settings.PositiveInt,
)

var txnSchedulerLockCount = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.txn_scheduler_lock_count",
	"the maximum number of locks tracked by the transaction scheduler "+
		"before it starts evicting old transactions",
	1024*1024,
	settings.PositiveInt,
)

var txnBatchSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.txn_batch_size",
	"target number of rows or KVs per batch in the replication pipeline",
	1024,
	settings.PositiveInt,
)

var txnBatchFlushInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.txn_batch_flush_interval",
	"the maximum time to wait before flushing a partial batch of "+
		"decoded transactions",
	50*time.Millisecond,
)

// decodedBatch carries either a slice of decoded transactions or a checkpoint
// timestamp from the decode stage to the schedule stage. Exactly one of
// transactions or checkpoint is set per batch.
type decodedBatch struct {
	transactions []ldrdecoder.Transaction
	checkpoint   hlc.Timestamp
}

// TxnLdrCoordinator orchestrates transactional logical replication by running
// a pipeline of stages: subscribe, decode, schedule, apply, and checkpoint. It
// is created per replication attempt and owned by the job resumer. The caller
// retains ownership of the client and is responsible for closing it.
type TxnLdrCoordinator struct {
	execCtx sql.JobExecContext

	job               *jobs.Job
	payload           jobspb.LogicalReplicationDetails
	client            streamclient.Client
	heartbeatInterval func() time.Duration
}

func NewTxnLdrCoordinator(
	execCtx sql.JobExecContext,
	job *jobs.Job,
	client streamclient.Client,
	heartbeatInterval func() time.Duration,
) *TxnLdrCoordinator {
	payload := job.Details().(jobspb.LogicalReplicationDetails)
	return &TxnLdrCoordinator{
		job:               job,
		execCtx:           execCtx,
		payload:           payload,
		client:            client,
		heartbeatInterval: heartbeatInterval,
	}
}

func (p *TxnLdrCoordinator) Resume(ctx context.Context) error {
	group := ctxgroup.WithContext(ctx)

	plan, asOf, err := p.planReplication(ctx)
	if err != nil {
		return err
	}

	tables, err := p.buildTableMappings(ctx, plan)
	if err != nil {
		return errors.Wrap(err, "building table mappings")
	}

	feed, err := p.createTxnFeed(ctx, plan, asOf)
	if err != nil {
		return errors.Wrap(err, "creating txn feed")
	}

	heartbeatSender := streamclient.NewHeartbeatSender(
		ctx,
		p.client,
		streampb.StreamID(p.payload.StreamID),
		p.heartbeatInterval,
	)
	defer besteffort.Cleanup(ctx, "ldr-stop-heartbeat", heartbeatSender.Stop)

	// Create multiple writers for parallel application.
	sv := &p.execCtx.ExecCfg().Settings.SV
	numWriters := int(txnNumWriters.Get(sv))
	var writers []txnwriter.TransactionWriter
	for range numWriters {
		writer, err := txnwriter.NewTransactionWriter(ctx, p.execCtx.ExecCfg().InternalDB, p.execCtx.ExecCfg().LeaseManager, p.execCtx.ExecCfg().Settings)
		if err != nil {
			// Close any writers we already created.
			for _, w := range writers {
				w.Close(ctx)
			}
			return errors.Wrap(err, "creating txn writer")
		}
		writers = append(writers, writer)
	}

	// Create the applier which manages parallel transaction application.
	// NewApplier takes ownership of writers.
	const applierID ldrdecoder.ApplierID = 1
	allIDs := []ldrdecoder.ApplierID{applierID}
	applierEvents := make(chan txnapply.ApplierEvent)
	applier, err := txnapply.NewApplier(ctx, applierID, writers, txnapply.NewDependencyTracker(allIDs), allIDs)
	if err != nil {
		return errors.Wrap(err, "creating applier")
	}
	defer applier.Close(ctx)

	txnDecoder, err := ldrdecoder.NewTxnDecoder(
		ctx,
		p.execCtx.ExecCfg().DistSQLSrv.DB,
		p.execCtx.ExecCfg().Settings,
		tables,
	)
	if err != nil {
		return errors.Wrap(err, "creating txn decoder")
	}

	lockSynthesizer, err := txnlock.NewLockSynthesizer(
		ctx,
		&eval.Context{},
		p.execCtx.ExecCfg().LeaseManager,
		p.execCtx.ExecCfg().Clock,
		tables,
	)
	if err != nil {
		return errors.Wrap(err, "creating lock synthesizer")
	}

	// Create the scheduler to track lock dependencies between transactions.
	schedulerLockCount := int32(txnSchedulerLockCount.Get(sv))
	scheduler := txnscheduler.NewScheduler(schedulerLockCount)

	batches := make(chan decodedBatch, 1)

	group.GoCtx(func(ctx context.Context) error {
		heartbeatSender.Start(ctx, timeutil.DefaultTimeSource{})
		return heartbeatSender.Wait()
	})
	group.GoCtx(func(ctx context.Context) error {
		return p.stageSubscribe(ctx, feed)
	})
	group.GoCtx(func(ctx context.Context) error {
		return p.stageDecode(ctx, feed, txnDecoder, batches)
	})
	group.GoCtx(func(ctx context.Context) error {
		return p.stageSchedule(ctx, lockSynthesizer, scheduler, applierID, batches, applierEvents)
	})
	group.GoCtx(func(ctx context.Context) error {
		return p.stageApply(ctx, applier, applierEvents)
	})
	group.GoCtx(func(ctx context.Context) error {
		return p.stageCheckpoint(ctx, applier)
	})

	return group.Wait()
}

// stageSubscribe runs the rangefeed subscription that populates the event
// channel consumed by stageDecode.
func (p *TxnLdrCoordinator) stageSubscribe(
	ctx context.Context, feed streamclient.Subscription,
) error {
	return feed.Subscribe(ctx)
}

// stageDecode reads raw events from the feed, decodes them into transactions,
// and forwards decoded batches to the batches channel.
func (p *TxnLdrCoordinator) stageDecode(
	ctx context.Context,
	feed streamclient.Subscription,
	txnDecoder *ldrdecoder.TxnDecoder,
	batches chan decodedBatch,
) error {
	defer close(batches)
	sv := &p.execCtx.ExecCfg().Settings.SV
	maxInterval := txnBatchFlushInterval.Get(sv)
	batchRowCount := int(txnBatchSize.Get(sv))
	ticker := time.NewTicker(maxInterval)
	defer ticker.Stop()

	var transactions []ldrdecoder.Transaction
	rows := 0

	flush := func() error {
		if len(transactions) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batches <- decodedBatch{transactions: transactions}:
			transactions = nil
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
		case ev, ok := <-feed.Events():
			if !ok {
				return flush()
			}
			switch ev.Type() {
			case crosscluster.KVEvent:
				for txnKVs := range txnfeed.Transactions(ev.GetKVs()) {
					decoded, err := txnDecoder.DecodeTxn(ctx, txnKVs)
					if err != nil {
						return errors.Wrap(err, "decoding transaction")
					}
					transactions = append(transactions, decoded)
					rows += len(decoded.WriteSet)
				}
				if batchRowCount <= rows {
					shouldFlush = true
				}
			case crosscluster.CheckpointEvent:
				// Flush pending transactions before sending the checkpoint
				// so the checkpoint correctly indicates all prior
				// transactions have been forwarded.
				if err := flush(); err != nil {
					return err
				}
				// The merged feed always emits a single resolved span
				// covering the entire key space.
				checkpoint := ev.GetCheckpoint().ResolvedSpans[0].Timestamp
				select {
				case <-ctx.Done():
					return ctx.Err()
				case batches <- decodedBatch{checkpoint: checkpoint}:
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

// stageSchedule processes decoded transaction batches through the lock
// synthesizer and scheduler to compute dependencies, then forwards scheduled
// transactions to the applier.
func (p *TxnLdrCoordinator) stageSchedule(
	ctx context.Context,
	lockSynthesizer *txnlock.LockSynthesizer,
	scheduler *txnscheduler.Scheduler,
	applierID ldrdecoder.ApplierID,
	batches chan decodedBatch,
	applierEvents chan txnapply.ApplierEvent,
) error {
	defer close(applierEvents)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-batches:
			if !ok {
				return nil
			}

			// Forward checkpoint events to the applier.
			if batch.checkpoint.IsSet() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case applierEvents <- txnapply.Checkpoint{Timestamp: batch.checkpoint}:
				}
				continue
			}

			if len(batch.transactions) == 0 {
				continue
			}

			for i := range batch.transactions {
				batch.transactions[i].TxnID.ApplierID = applierID

				// Derive locks for the transaction.
				lockSet, err := lockSynthesizer.DeriveLocks(ctx, batch.transactions[i].WriteSet)
				if err != nil {
					return errors.Wrap(err, "deriving locks for transaction")
				}
				batch.transactions[i].WriteSet = lockSet.SortedRows

				// Schedule the transaction to compute dependencies and an
				// event horizon. The event horizon is non-zero when the
				// scheduler evicts old transactions to reclaim lock
				// table capacity; it ensures the applier waits for all
				// transactions up to that timestamp before applying.
				schedulerTxn := txnscheduler.Transaction{
					CommitTime: batch.transactions[i].TxnID.Timestamp,
					Locks:      lockSet.Locks,
				}
				dependencies, eventHorizon := scheduler.Schedule(schedulerTxn, nil)

				// Convert hlc.Timestamp dependencies to TxnIDs.
				txnDeps := make([]ldrdecoder.TxnID, len(dependencies))
				for j, ts := range dependencies {
					txnDeps[j] = ldrdecoder.TxnID{Timestamp: ts, ApplierID: applierID}
				}

				// Send the scheduled transaction to the applier.
				scheduled := txnapply.ScheduledTransaction{
					Transaction:  batch.transactions[i],
					Dependencies: txnDeps,
					EventHorizon: eventHorizon,
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case applierEvents <- scheduled:
				}
			}
		}
	}
}

// stageApply runs the applier which writes transactions to the destination
// cluster in parallel while respecting dependency ordering.
func (p *TxnLdrCoordinator) stageApply(
	ctx context.Context, applier *txnapply.Applier, applierEvents chan txnapply.ApplierEvent,
) error {
	return applier.Run(ctx, applierEvents)
}

// stageCheckpoint monitors the applier's frontier and persists replication
// progress to the job record.
func (p *TxnLdrCoordinator) stageCheckpoint(ctx context.Context, applier *txnapply.Applier) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frontier, ok := <-applier.Frontier():
			// TODO(167943): Improve frontier management.
			if !ok {
				return nil
			}
			log.Dev.Infof(ctx, "checkpointing frontier: %+v", frontier)
			if err := p.checkpoint(ctx, frontier); err != nil {
				return errors.Wrap(err, "checkpointing progress")
			}
		}
	}
}

func (p *TxnLdrCoordinator) checkpoint(ctx context.Context, frontier hlc.Timestamp) error {
	return p.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		progress := md.Progress
		prog := progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication

		if frontier.IsSet() {
			prog.ReplicatedTime = frontier
			progress.Progress = &jobspb.Progress_HighWater{
				HighWater: &frontier,
			}
		}
		ju.UpdateProgress(progress)
		return nil
	})
}

// planReplication computes the as-of timestamp from job progress and calls
// PlanLogicalReplication on the source cluster.
func (p *TxnLdrCoordinator) planReplication(
	ctx context.Context,
) (streamclient.LogicalReplicationPlan, hlc.Timestamp, error) {
	progress := p.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
	asOf := p.payload.ReplicationStartTime
	if !progress.ReplicatedTime.IsEmpty() {
		asOf = progress.ReplicatedTime
	}

	req := streampb.LogicalReplicationPlanRequest{
		StreamID: streampb.StreamID(p.payload.StreamID),
		PlanAsOf: asOf,
	}
	for _, pair := range p.payload.ReplicationPairs {
		req.TableIDs = append(req.TableIDs, pair.SrcDescriptorID)
	}

	plan, err := p.client.PlanLogicalReplication(ctx, req)
	if err != nil {
		return streamclient.LogicalReplicationPlan{}, hlc.Timestamp{},
			errors.Wrap(err, "planning logical replication")
	}
	return plan, asOf, nil
}

// buildTableMappings hydrates source table descriptors from the replication
// plan and pairs them with their destination table IDs.
func (p *TxnLdrCoordinator) buildTableMappings(
	ctx context.Context, plan streamclient.LogicalReplicationPlan,
) ([]ldrdecoder.TableMapping, error) {
	crossClusterResolver := crosscluster.MakeCrossClusterTypeResolver(plan.SourceTypes)
	tableMappings := make([]ldrdecoder.TableMapping, 0, len(p.payload.ReplicationPairs))
	for _, pair := range p.payload.ReplicationPairs {
		srcTableDesc := plan.DescriptorMap[pair.SrcDescriptorID]
		cpy := tabledesc.NewBuilder(&srcTableDesc).BuildCreatedMutableTable()
		if err := typedesc.HydrateTypesInDescriptor(ctx, cpy, crossClusterResolver); err != nil {
			return nil, err
		}
		tableMappings = append(tableMappings, ldrdecoder.TableMapping{
			SourceDescriptor: cpy,
			DestID:           descpb.ID(pair.DstDescriptorID),
		})
	}
	return tableMappings, nil
}

func (p *TxnLdrCoordinator) createTxnFeed(
	ctx context.Context, plan streamclient.LogicalReplicationPlan, asOf hlc.Timestamp,
) (streamclient.Subscription, error) {
	coveringSpan := plan.SourceSpans[0]
	for _, s := range plan.SourceSpans[1:] {
		if s.Key.Compare(coveringSpan.Key) < 0 {
			coveringSpan.Key = s.Key
		}
		if coveringSpan.EndKey.Compare(s.EndKey) < 0 {
			coveringSpan.EndKey = s.EndKey
		}
	}

	var orderedFeeds []streamclient.Subscription
	for i, partition := range plan.Topology.Partitions {
		// Create a frontier scoped to this partition's spans so that each
		// OrderedFeed has its own mutable frontier (no concurrent mutation).
		partitionFrontier, err := span.MakeFrontierAt(asOf, partition.Spans...)
		if err != nil {
			return nil, errors.Wrapf(err, "creating frontier for partition %d", i)
		}

		// NB: if Subscribe fails for partition N, subscriptions created for
		// partitions 0..N-1 are not explicitly closed here. The Subscription
		// interface has no Close method, so proactive cleanup isn't possible.
		// They remain registered in the client's activeSubscriptions and are
		// cleaned up when p.client is closed on job shutdown.
		sub, err := p.client.Subscribe(
			ctx,
			streampb.StreamID(p.payload.StreamID),
			0,        // consumerNode - not used in logical replication
			int32(i), // consumerProc - use partition index
			partition.SubscriptionToken,
			asOf,
			partitionFrontier,
			streamclient.WithDiff(true),
		)
		if err != nil {
			return nil, errors.Wrapf(err, "subscribing to partition %d", i)
		}
		ordered, err := txnfeed.NewOrderedFeed(sub, partitionFrontier)
		if err != nil {
			return nil, errors.Wrapf(err, "creating ordered feed for partition %d", i)
		}
		orderedFeeds = append(orderedFeeds, ordered)
	}

	sv := &p.execCtx.ExecCfg().Settings.SV
	targetBatchKVs := int(txnBatchSize.Get(sv))
	return txnfeed.NewMergeFeed(orderedFeeds, coveringSpan, targetBatchKVs), nil
}
