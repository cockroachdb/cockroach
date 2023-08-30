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
// TODO(msbutler): on an initial scan, we need to buffer up all updates and
// write them to the span config table in one transaction, along with a delete
// over the whole tenant key span. Since C2C does not lay a PTS on the source
// side span config table, the initial scan on resumption may miss updates;
// therefore, the only way to cleanly update the destination side span config
// table is to write the latest state of the source table and delete all
// existing state, in one transaction.
type spanConfigIngestor struct {
	accessor                    spanconfig.KVAccessor
	bufferedUpdates             []spanconfig.Record
	bufferedDeletes             []spanconfig.Target
	lastBufferedSourceTimestamp hlc.Timestamp
	session                     sqlliveness.Session
	stopperCh                   chan struct{}
	settings                    *cluster.Settings
	client                      streamclient.Client
	rekeyer                     *backupccl.KeyRewriter
	testingKnobs                *sql.StreamingTestingKnobs
}

func makeSpanConfigIngestor(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	ingestionJob *jobs.Job,
	sourceTenantID roachpb.TenantID,
	stopperCh chan struct{},
) (*spanConfigIngestor, error) {

	client, err := connectToActiveClient(ctx, ingestionJob, execCfg.InternalDB, streamclient.ForSpanConfigs())
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
	log.Infof(ctx, "initialized span config ingestor")
	return &spanConfigIngestor{
		accessor:     execCfg.SpanConfigKVAccessor,
		settings:     execCfg.Settings,
		session:      ingestionJob.Session(),
		client:       client,
		rekeyer:      rekeyer,
		stopperCh:    stopperCh,
		testingKnobs: execCfg.StreamingTestingKnobs,
	}, nil
}

func (sc *spanConfigIngestor) ingestSpanConfigs(
	ctx context.Context, tenantName roachpb.TenantName,
) error {
	sub, err := sc.client.SetupSpanConfigsStream(ctx, tenantName)
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
		return sc.maybeFlushEvents(ctx)
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
	if sc.lastBufferedSourceTimestamp.Less(update.Timestamp) {
		// If this event was originally written at a later timestamp than what's in the buffer, flush the buffer.
		if err := sc.maybeFlushEvents(ctx); err != nil {
			return err
		}
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
func (sc *spanConfigIngestor) maybeFlushEvents(ctx context.Context) error {
	if len(sc.bufferedUpdates) != 0 || len(sc.bufferedDeletes) != 0 {
		return sc.flushEvents(ctx)
	}
	return nil
}

// flushEvents writes all buffered events to the system span configuration table
// in one transaction via kvAccesor.UpdateSpanConfigRecords.
func (sc *spanConfigIngestor) flushEvents(ctx context.Context) error {
	log.VEventf(ctx, 2, "flushing span config %d updates and %d deletes", len(sc.bufferedUpdates), len(sc.bufferedDeletes))
	if sc.testingKnobs != nil && sc.testingKnobs.BeforeIngestSpanConfigFlush != nil {
		sc.testingKnobs.BeforeIngestSpanConfigFlush(ctx, sc.bufferedUpdates, sc.bufferedDeletes)
	}

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
		err := sc.accessor.UpdateSpanConfigRecords(
			ctx, sc.bufferedDeletes, sc.bufferedUpdates, sessionStart, sessionExpiration,
		)
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
