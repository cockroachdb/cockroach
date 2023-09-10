// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// spanConfigIngestor listens for spanConfig updates relevant to the replicating
// tenant and writes them transactionally to the span configurations table.
//
// As updates come in, the spanConfigIngestor buffers updates and deletes with
// the same source side commit timestamp and writes them in a transaction  when
// it observes a new update with a newer timestamp.
//
// The spanConfigIngestor assumes each replicated update is unique and in
// timestamp order, which is enforced by the producer side
// SpanConfigEventStream. This assumption simplifies ingestion logic which must
// write updates and deletes with the same source side transaction commit
// timestamp at the same new timestamp on the destination side. This invariant
// ensures a span configuration's target (i.e. the span that a configuration
// applies to) never overlaps with any other span configuration target. Else,
// C2C would break the span config reconciliation system.
//
// During the rangefeed initial scan, the spanConfigIngestor buffers up all
// updates and writes them to the span config table in one transaction, along
// with a delete over the whole tenant key span. The span configuration
// ingestion does not create a PTS record for the source side span configuration
// table to avoid the possibility of an errant physical replication job from
// impacting a system table's GC. As a result, on resumption we must do a new
// initial scan, rebuilding the config from scratch, to avoid missing data that
// may have been GC'd.
type spanConfigIngestor struct {
	// State passed at creation.
	accessor                 spanconfig.KVAccessor
	session                  sqlliveness.Session
	stopperCh                chan struct{}
	settings                 *cluster.Settings
	client                   streamclient.SpanConfigClient
	rekeyer                  *backupccl.KeyRewriter
	destinationTenantKeySpan roachpb.Span
	db                       *kv.DB
	testingKnobs             *sql.StreamingTestingKnobs

	// Dynamic state maintained during ingestion.
	initialScanComplete         bool
	bufferedUpdates             []spanconfig.Record
	bufferedDeletes             []spanconfig.Target
	lastBufferedSourceTimestamp hlc.Timestamp
}

func makeSpanConfigIngestor(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	ingestionJob *jobs.Job,
	sourceTenantID roachpb.TenantID,
	stopperCh chan struct{},
) (*spanConfigIngestor, error) {

	streamAddreses := getStreamAddresses(ctx, ingestionJob)
	client, err := streamclient.GetFirstActiveSpanConfigClient(ctx, streamAddreses, execCfg.InternalDB)
	if err != nil {
		return nil, err
	}

	details := ingestionJob.Details().(jobspb.StreamIngestionDetails)

	rekeyCfg := execinfrapb.TenantRekey{
		OldID: sourceTenantID,
		NewID: details.DestinationTenantID,
	}
	rekeyer, err := backupccl.MakeKeyRewriterFromRekeys(keys.SystemSQLCodec,
		nil /* tableRekeys */, []execinfrapb.TenantRekey{rekeyCfg},
		true /* restoreTenantFromStream */)
	if err != nil {
		return nil, err
	}

	destTenantStartKey := keys.MakeTenantPrefix(details.DestinationTenantID)
	destTenantSpan := roachpb.Span{Key: destTenantStartKey, EndKey: destTenantStartKey.PrefixEnd()}
	log.Infof(ctx, "initialized span config ingestor")
	return &spanConfigIngestor{
		accessor:                 execCfg.SpanConfigKVAccessor,
		settings:                 execCfg.Settings,
		session:                  ingestionJob.Session(),
		client:                   client,
		rekeyer:                  rekeyer,
		stopperCh:                stopperCh,
		destinationTenantKeySpan: destTenantSpan,
		db:                       execCfg.DB,
		testingKnobs:             execCfg.StreamingTestingKnobs,
	}, nil
}

func (sc *spanConfigIngestor) ingestSpanConfigs(
	ctx context.Context, tenantName roachpb.TenantName,
) error {
	sub, err := sc.client.SetupSpanConfigsStream(tenantName)
	if err != nil {
		return err
	}

	group := ctxgroup.WithContext(ctx)
	group.GoCtx(sub.Subscribe)
	group.GoCtx(func(ctx context.Context) error {
		defer closeAndLog(ctx, sc.client)
		return sc.consumeSpanConfigs(ctx, sub)
	})
	return group.Wait()
}

func (sc *spanConfigIngestor) consumeSpanConfigs(
	ctx context.Context, subscription streamclient.Subscription,
) error {
	for {
		select {
		case event, ok := <-subscription.Events():
			if !ok {
				return nil
			}
			if err := sc.consumeEvent(ctx, event); err != nil {
				return err
			}
		case <-sc.stopperCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (sc *spanConfigIngestor) consumeEvent(ctx context.Context, event streamingccl.Event) error {
	switch event.Type() {
	case streamingccl.SpanConfigEvent:
		return sc.bufferRecord(ctx, event.GetSpanConfigEvent())
	case streamingccl.CheckpointEvent:
		return sc.maybeFlushOnCheckpoint(ctx)
	default:
		return errors.AssertionFailedf("received non span config update %s", event)
	}
}

func (sc *spanConfigIngestor) bufferRecord(
	ctx context.Context, update *streampb.StreamedSpanConfigEntry,
) error {
	sourceSpan := update.SpanConfig.Target.GetSpan()
	destStartKey, ok, err := sc.rekeyer.RewriteKey(sourceSpan.Key, 0)
	if err != nil {
		return err
	}
	if !ok {
		// No need to replicate the span cfgs for ephemeral tables in the app tenant
		return nil
	}
	destEndKey, ok, err := sc.rekeyer.RewriteKey(sourceSpan.EndKey, 0)
	if err != nil {
		return err
	}
	if !ok {
		log.Warningf(ctx, "could not rekey this span as part of an ephemeral table %s", sourceSpan)
		// No need to replicate the span cfgs for ephemeral tables in the app tenant
		//
		// TODO(msbutler): This error handling isn't ideal as the span for this span
		// cfg doesn't necessarily cover only the ephemeral tables. It would be
		// better to remove this logic out of the rekeyer.
		return nil
	}
	targetSpan := roachpb.Span{Key: destStartKey, EndKey: destEndKey}
	if err := sc.maybeFlushOnUpdate(ctx, update.Timestamp); err != nil {
		return err
	}
	target := spanconfig.MakeTargetFromSpan(targetSpan)
	if update.SpanConfig.Config.IsEmpty() {
		sc.bufferedDeletes = append(sc.bufferedDeletes, target)
	} else {
		record, err := spanconfig.MakeRecord(target, update.SpanConfig.Config)
		if err != nil {
			return err
		}
		sc.bufferedUpdates = append(sc.bufferedUpdates, record)
	}
	sc.lastBufferedSourceTimestamp = update.Timestamp
	return nil
}

func (sc *spanConfigIngestor) bufferIsEmpty() bool {
	return len(sc.bufferedUpdates) == 0 && len(sc.bufferedDeletes) == 0
}
func (sc *spanConfigIngestor) maybeFlushOnUpdate(
	ctx context.Context, updateTimestamp hlc.Timestamp,
) error {
	// If this event was originally written at a later timestamp and the initial scan has complete, flush the current buffer.
	if sc.initialScanComplete &&
		sc.lastBufferedSourceTimestamp.Less(updateTimestamp) &&
		!sc.bufferIsEmpty() {
		return sc.flushEvents(ctx)
	}
	return nil
}

func (sc *spanConfigIngestor) maybeFlushOnCheckpoint(ctx context.Context) error {
	if !sc.bufferIsEmpty() {
		return sc.flushEvents(ctx)
	} else if !sc.initialScanComplete {
		return errors.AssertionFailedf("a flush after the initial scan checkpoint must have data in it")
	}
	return nil
}

// flushEvents writes all buffered events to the system span configuration table
// in one transaction via kvAccesor.UpdateSpanConfigRecords.
func (sc *spanConfigIngestor) flushEvents(ctx context.Context) error {
	log.VEventf(ctx, 2, "flushing span config %d updates and %d deletes", len(sc.bufferedUpdates), len(sc.bufferedDeletes))
	retryOpts := retry.Options{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     5 * time.Second,
		MaxRetries:     5,
	}

	for retrier := retry.StartWithCtx(ctx, retryOpts); retrier.Next(); {
		sessionStart, sessionExpiration := sc.session.Start(), sc.session.Expiration()
		if sessionExpiration.IsEmpty() {
			return errors.Errorf("sqlliveness session has expired")
		}
		var err error
		if !sc.initialScanComplete {
			// The first flush will always contain all span configs found during the initial scan.
			err = sc.flushInitialScan(ctx, sessionStart, sessionExpiration)
		} else {
			err = sc.accessor.UpdateSpanConfigRecords(
				ctx, sc.bufferedDeletes, sc.bufferedUpdates, sessionStart, sessionExpiration,
			)
			if sc.testingKnobs != nil && sc.testingKnobs.RightAfterSpanConfigFlush != nil {
				sc.testingKnobs.RightAfterSpanConfigFlush(ctx, sc.bufferedUpdates, sc.bufferedDeletes)
			}
		}
		if err != nil {
			if spanconfig.IsCommitTimestampOutOfBoundsError(err) {
				// We expect the underlying sqlliveness session's expiration to be
				// extended automatically, which makes this retry loop effective in the
				// face of these retryable lease expired errors from the RPC.
				log.Infof(ctx, "lease expired while updating span config records, retrying..")
				continue
			}
			return err // not a retryable error, bubble up
		}
		break
	}

	sc.bufferedUpdates = sc.bufferedUpdates[:0]
	sc.bufferedDeletes = sc.bufferedDeletes[:0]
	return nil
}

// flushInitialScan flushes all contents from the source side rangefeed's
// initial scan. The function assumes the buffer contains only updates from the
// initial scan. To obey destination side span config invariants, the function
// deletes all existing span config records related to the replicating tenant in
// the same transaction that it writes all initial scan updates.
func (sc *spanConfigIngestor) flushInitialScan(
	ctx context.Context, sessionStart, sessionExpiration hlc.Timestamp,
) error {
	log.Infof(ctx, "flushing initial span configuration state (%d records)", len(sc.bufferedUpdates))

	if len(sc.bufferedDeletes) != 0 {
		return errors.AssertionFailedf("initial scan flush should not contain records to delete")
	}
	target := spanconfig.MakeTargetFromSpan(sc.destinationTenantKeySpan)
	return sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		accessor := sc.accessor.WithTxn(ctx, txn)
		existingRecords, err := accessor.GetSpanConfigRecords(ctx, []spanconfig.Target{target})
		if err != nil {
			return err
		}
		// Within the txn, we allocate a new buffer for deletes, instead of using
		// sc.BufferedDeletes, because if the transaction retries, we don't want to
		// worry about clearing the spanConfigIngestor's delete buffer.
		bufferedDeletes := make([]spanconfig.Target, 0, len(existingRecords))
		for _, record := range existingRecords {
			bufferedDeletes = append(bufferedDeletes, record.GetTarget())
		}
		if err := accessor.UpdateSpanConfigRecords(
			ctx, bufferedDeletes, sc.bufferedUpdates, sessionStart, sessionExpiration,
		); err != nil {
			return err
		}
		if sc.testingKnobs != nil && sc.testingKnobs.RightAfterSpanConfigFlush != nil {
			sc.testingKnobs.RightAfterSpanConfigFlush(ctx, sc.bufferedUpdates, bufferedDeletes)
		}
		sc.initialScanComplete = true
		return nil
	})
}
