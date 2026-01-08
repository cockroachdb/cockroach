// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

// TODO(jeffswenson): move this to a txncoordinator package.

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnlock"
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

	feed, err := p.createTxnFeed(ctx)
	if err != nil {
		return errors.Wrap(err, "creating txn feed")
	}

	writer, err := txnwriter.NewTransactionWriter(ctx, p.execCtx.ExecCfg().InternalDB, p.execCtx.ExecCfg().LeaseManager, p.execCtx.ExecCfg().Settings)
	if err != nil {
		return errors.Wrap(err, "creating txn writer")
	}
	defer writer.Close(ctx)

	txnDecoder, err := ldrdecoder.NewTxnDecoder(
		ctx,
		p.execCtx.ExecCfg().DistSQLSrv.DB,
		p.execCtx.ExecCfg().Settings,
		p.tables,
	)
	if err != nil {
		return errors.Wrap(err, "creating txn decoder")
	}

	lockSynthesizer, err := txnlock.NewLockSynthesizer(
		ctx,
		p.execCtx.ExecCfg().LeaseManager,
		p.execCtx.ExecCfg().Clock,
		p.tables,
	)
	if err != nil {
		return errors.Wrap(err, "creating lock synthesizer")
	}

	// TODO(jeffswenson): we need to sometimes emit checkpoints even if there are
	// no transactions so that we can advance resolved time.
	writeSets := make(chan txnfeed.WriteSet)
	group.Go(func() error {
		return feed.Run(ctx)
	})
	group.Go(func() error {
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
	group.Go(func() error {
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

	group.Go(func() error {
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

				// Run lock synthesis on each transaction to determine apply order
				for i := range transactions {
					lockSet, err := lockSynthesizer.DeriveLocks(transactions[i].WriteSet)
					if err != nil {
						return errors.Wrap(err, "deriving locks for transaction")
					}
					transactions[i].WriteSet = lockSet.SortedRows

					// TODO(jeffswenson): why can't I put multiple transactions touching the same
					// constraint in a batch?
					status, err := writer.ApplyBatch(ctx, transactions[i:i+1])
					if err != nil {
						// TODO(jeffswenson): ensure this gets logged somewhere higher up
						log.Dev.Errorf(ctx, "failed to apply transaction batch: %+v", err)
						return errors.Wrap(err, "applying transaction batch")
					}
					for _, s := range status {
						if s.DlqReason != nil {
							log.Dev.Errorf(ctx, "transaction batch sent to DLQ: %+v", s.DlqReason)
							// TODO(jeffswenson): DLQ failed transactions
							return errors.Wrap(s.DlqReason, "transaction batch failed with DLQ reason")
						}
					}
				}

				err = p.checkpoint(ctx, transactions[len(transactions)-1].Timestamp)
				if err != nil {
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
	req := streampb.LogicalReplicationPlanRequest{
		StreamID: streampb.StreamID(t.payload.StreamID),
		// TODO(jeffswenson): this should be the checkpointed time if resuming
		PlanAsOf: t.payload.ReplicationStartTime,
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
			t.payload.ReplicationStartTime,
			// TODO(jeffswenson): do we need to scope this down to the spans we care
			// about?
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
