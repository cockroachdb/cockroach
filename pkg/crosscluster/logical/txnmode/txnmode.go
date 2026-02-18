// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

// TODO(jeffswenson): move this to a txncoordinator package.

import (
	"context"
	"slices"
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

type TxnLdrCoordinator struct {
	execCtx sql.JobExecContext

	job     *jobs.Job
	payload jobspb.LogicalReplicationDetails
	client  streamclient.Client
	tables  []ldrdecoder.TableMapping
}

func NewTxnLdrCoordinator(
	ctx context.Context,
	execCtx sql.JobExecContext,
	job *jobs.Job,
	client streamclient.Client,
	tables []ldrdecoder.TableMapping,
) *TxnLdrCoordinator {
	payload := job.Details().(jobspb.LogicalReplicationDetails)
	return &TxnLdrCoordinator{
		job:     job,
		execCtx: execCtx,
		payload: payload,
		client:  client,
		tables:  tables,
	}
}

func (p *TxnLdrCoordinator) Resume(ctx context.Context) error {
	group := ctxgroup.WithContext(ctx)

	// TODO(jeffswenson): restructure this so that every stage has the format
	// input channel -> output channel.
	// TODO(jeffswenson): stages should shut down when the context is cancelled
	// in order to ensure we don't get stuck clearing buffers on cancellation.
	// TODO(jeffswenson): stages should pass batches of transactions via channel
	// instead of channels pushing individual transactions.

	// The general API is:
	// NewStage(ctx, input, output) -> initialization and initial validation
	// Start(ctx, ctxgroup) -> start goroutines using the passed in context
	//   group, error passed to context group
	// Close(ctx) -> Frees resources associated with the stage
	// Alternatively: Each module has a Run(ctx) error function that is called
	// by a ctx group in the parent.
	//
	// But why not Stage(ctx, ctxgroup, args, input, output) error ?

	feed, err := p.createTxnFeed(ctx)
	if err != nil {
		return errors.Wrap(err, "creating txn feed")
	}

	// Create multiple writers for parallel application.
	const numWriters = 4
	var writers []txnwriter.TransactionWriter
	for i := 0; i < numWriters; i++ {
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

	txnDecoder, err := ldrdecoder.NewTxnDecoder(
		ctx,
		p.execCtx.ExecCfg().DistSQLSrv.DB,
		p.execCtx.ExecCfg().Settings,
		p.tables,
	)
	if err != nil {
		for _, w := range writers {
			w.Close(ctx)
		}
		return errors.Wrap(err, "creating txn decoder")
	}

	lockSynthesizer, err := txnlock.NewLockSynthesizer(
		ctx,
		p.execCtx.ExecCfg().LeaseManager,
		p.execCtx.ExecCfg().Clock,
		p.tables,
	)
	if err != nil {
		for _, w := range writers {
			w.Close(ctx)
		}
		return errors.Wrap(err, "creating lock synthesizer")
	}

	// Create the scheduler to track lock dependencies between transactions.
	// The lock count is the max number of locks the scheduler will track before
	// it starts evicting old transactions.
	const schedulerLockCount = 10000
	scheduler := txnscheduler.NewScheduler(schedulerLockCount)

	// Create the applier which manages parallel transaction application.
	scheduledTxns := make(chan txnapply.ScheduledTransaction)
	applier := txnapply.NewApplier(writers)
	defer applier.Close(ctx)

	// TODO(jeffswenson): we need to sometimes emit checkpoints even if there are
	// no transactions so that we can advance resolved time.
	writeSets := make(chan txnfeed.WriteSet)
	group.GoCtx(func(ctx context.Context) error {
		return feed.Run(ctx)
	})
	group.GoCtx(func(ctx context.Context) error {
		defer close(writeSets)
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			ev, err := feed.Next(ctx)
			if err != nil {
				return errors.Wrap(err, "reading from txn feed")
			}
			select {
			case writeSets <- ev:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	batches := make(chan []ldrdecoder.Transaction, 1)
	group.GoCtx(func(ctx context.Context) error {
		maxInterval := time.Second
		ticker := time.NewTicker(maxInterval)
		defer ticker.Stop()

		var transactions []ldrdecoder.Transaction
		rows := 0

		for {
			flush := false
			select {
			case <-ctx.Done():
				return ctx.Err()
			case writeSet, ok := <-writeSets:
				if !ok {
					return nil
				}
				decoded, err := txnDecoder.DecodeTxn(writeSet.Rows)
				if err != nil {
					return errors.Wrap(err, "decoding transaction")
				}
				transactions = append(transactions, decoded)
				rows += len(decoded.WriteSet)
				if 1000 <= rows {
					flush = true
				}
			case <-ticker.C:
				flush = true
			}

			if !flush {
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case batches <- transactions:
				transactions = nil
				rows = 0
				ticker.Reset(maxInterval)
			}
		}
	})

	// Scheduling goroutine: processes decoded transactions through the scheduler
	// and sends scheduled transactions to the applier.
	group.GoCtx(func(ctx context.Context) error {
		defer close(scheduledTxns)
		var scratch []hlc.Timestamp
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case transactions, ok := <-batches:
				if !ok {
					return nil
				}
				if len(transactions) == 0 {
					continue
				}

				for i := range transactions {
					// Derive locks for the transaction.
					lockSet, err := lockSynthesizer.DeriveLocks(transactions[i].WriteSet)
					if err != nil {
						return errors.Wrap(err, "deriving locks for transaction")
					}
					transactions[i].WriteSet = lockSet.SortedRows

					// Convert txnlock.Lock to txnscheduler.Lock format.
					schedulerLocks := make([]txnscheduler.Lock, len(lockSet.Locks))
					for j, lock := range lockSet.Locks {
						schedulerLocks[j] = txnscheduler.Lock{
							Hash:   txnscheduler.LockHash(lock.Hash),
							IsRead: lock.Read,
						}
					}

					// Schedule the transaction to compute dependencies.
					schedulerTxn := txnscheduler.Transaction{
						CommitTime: transactions[i].Timestamp,
						Locks:      schedulerLocks,
					}
					dependencies, _ := scheduler.Schedule(schedulerTxn, scratch)
					scratch = scratch[:0]

					dependencies = slices.Clone(dependencies)
					slices.SortFunc(dependencies, func(a, b hlc.Timestamp) int {
						return a.Compare(b)
					})
					dependencies = slices.Compact(dependencies)

					// Send the scheduled transaction to the applier.
					// Clone dependencies since the scheduler reuses the scratch slice.
					scheduled := txnapply.ScheduledTransaction{
						Transaction:  transactions[i],
						Dependencies: slices.Clone(dependencies),
					}

					log.Dev.Infof(ctx, "scheduled %v -> %v", scheduled.Timestamp, scheduled.Dependencies)

					select {
					case <-ctx.Done():
						return ctx.Err()
					case scheduledTxns <- scheduled:
					}
				}
			}
		}
	})

	// Run the applier.
	group.GoCtx(func(ctx context.Context) error {
		return applier.Run(ctx, scheduledTxns)
	})

	// Checkpoint goroutine: monitors the applier's frontier and checkpoints progress.
	group.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case frontier, ok := <-applier.Frontier():
				if !ok {
					return nil
				}
				log.Dev.Infof(ctx, "checkpointing frontier: %+v", frontier)
				if err := p.checkpoint(ctx, frontier); err != nil {
					return errors.Wrap(err, "checkpointing progress")
				}
			}
		}
	})

	return group.Wait()
}

func (t *TxnLdrCoordinator) checkpoint(ctx context.Context, frontier hlc.Timestamp) error {
	return t.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
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

func (t *TxnLdrCoordinator) createTxnFeed(ctx context.Context) (_ *txnfeed.OrderedFeed, err error) {
	// TODO(jeffswenson): heartbeat the stream

	// Use the checkpointed replicated time if resuming, otherwise use the start time.
	progress := t.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
	asOf := t.payload.ReplicationStartTime
	if !progress.ReplicatedTime.IsEmpty() {
		asOf = progress.ReplicatedTime
	}

	req := streampb.LogicalReplicationPlanRequest{
		StreamID: streampb.StreamID(t.payload.StreamID),
		PlanAsOf: asOf,
	}
	for _, pair := range t.payload.ReplicationPairs {
		req.TableIDs = append(req.TableIDs, pair.SrcDescriptorID)
	}

	plan, err := t.client.PlanLogicalReplication(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "planning txn logical replication")
	}

	frontier, err := t.makeInitialFrontier(plan.SourceSpans)
	if err != nil {
		return nil, errors.Wrap(err, "creating initial frontier")
	}

	var subscriptions []streamclient.Subscription
	for i, partition := range plan.Topology.Partitions {
		sub, err := t.client.Subscribe(
			ctx,
			streampb.StreamID(t.payload.StreamID),
			0,        // consumerNode - not used in logical replication
			int32(i), // consumerProc - use partition index
			partition.SubscriptionToken,
			asOf,
			frontier,
			streamclient.WithDiff(true),
		)
		if err != nil {
			return nil, errors.Wrapf(err, "subscribing to partition %d", i)
		}
		subscriptions = append(subscriptions, sub)
	}
	return txnfeed.NewOrderedFeed(txnfeed.NewMuxSubscription(subscriptions), frontier)
}

func (t *TxnLdrCoordinator) makeInitialFrontier(sourceSpans []roachpb.Span) (span.Frontier, error) {
	// NOTE: unlike row wise LDR, we can't actually checkpoint a fine grained
	// frontier. We can only advance resolved time in full steps.
	progress := t.job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication

	time := t.payload.ReplicationStartTime
	if !progress.ReplicatedTime.IsEmpty() {
		time = progress.ReplicatedTime
	}

	return span.MakeFrontierAt(time, sourceSpans...)
}
