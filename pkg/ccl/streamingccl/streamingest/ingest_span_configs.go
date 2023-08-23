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
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

type spanConfigIngestor struct {
	accessor                    *spanconfigkvaccessor.KVAccessor
	bufferedUpdates             []spanconfig.Record
	bufferedDeletes             []spanconfig.Target
	lastBufferedSourceTimestamp hlc.Timestamp
	session                     sqlliveness.Session
	cutoverSignal               cutoverProvider
	cutoverCh                   chan struct{}
	group                       ctxgroup.Group
	settings                    *cluster.Settings
	client                      streamclient.Client
	rekeyer                     *backupccl.KeyRewriter
}

func makeSpanConfigIngestor(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	ingestionJob *jobs.Job,
	sourceTenantID roachpb.TenantID,
) (*spanConfigIngestor, error) {

	accessor := spanconfigkvaccessor.New(
		execCfg.DB,
		execCfg.InternalDB.Executor(),
		execCfg.Settings,
		execCfg.Clock,
		systemschema.SpanConfigurationsTableName.FQString(),
		nil, /* knobs */
	)

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

	cutoverProvider := &cutoverFromJobProgress{
		jobID: ingestionJob.ID(),
		db:    execCfg.InternalDB,
	}
	log.Infof(ctx, "initialized span config ingestor")
	return &spanConfigIngestor{
		accessor:      accessor,
		settings:      execCfg.Settings,
		session:       ingestionJob.Session(),
		cutoverSignal: cutoverProvider,
		cutoverCh:     make(chan struct{}),
		group:         ctxgroup.WithContext(ctx),
		client:        client,
		rekeyer:       rekeyer,
	}, nil
}

func (sc *spanConfigIngestor) close(ctx context.Context) {
	if err := sc.client.Close(ctx); err != nil {
		log.Errorf(ctx, "error on client.close(): %s", err)
	}
	if err := sc.group.Wait(); err != nil {
		log.Errorf(ctx, "error on close(): %s", err)
	}
}

func (sc *spanConfigIngestor) ingestSpanConfigs(
	ctx context.Context, tenantName roachpb.TenantName,
) error {
	sub, err := sc.client.SetupSpanConfigsStream(ctx, tenantName)
	if err != nil {
		return err
	}

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(sub.Subscribe)
	g.GoCtx(sc.checkForCutover)

	return sc.consumeSpanConfigs(ctx, sub)
}

func (sc *spanConfigIngestor) checkForCutover(ctx context.Context) error {
	tick := time.NewTicker(cutoverSignalPollInterval.Get(&sc.settings.SV))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			cutoverReached, err := sc.cutoverSignal.cutoverReached(ctx)
			if err != nil {
				return err
			}
			if cutoverReached {
				close(sc.cutoverCh)
				return nil
			}
		}
	}
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
		case <-sc.cutoverCh:
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
		return sc.flushEvents(ctx)
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
		// No need to replicate the span cfgs for ephemeral tables in the app tenant
		//
		// TODO(msbutler): This error handling isn't ideal as the span for this span
		// cfg doesn't necessarily cover only the ephemeral tables. It would be
		// better to remove this logic out of the rekeyer.
		return nil
	}
	targetSpan := roachpb.Span{Key: destStartKey, EndKey: destEndKey}

	if sc.lastBufferedSourceTimestamp.Less(update.Timestamp) && len(sc.bufferedUpdates) != 0 {
		// If this event was originally written at a later timestamp than what's in the buffer, flush the buffer.
		if err := sc.flushEvents(ctx); err != nil {
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

func (sc *spanConfigIngestor) flushEvents(ctx context.Context) error {
	log.VEventf(ctx, 3, "flushing span config %d updates", len(sc.bufferedUpdates))
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
