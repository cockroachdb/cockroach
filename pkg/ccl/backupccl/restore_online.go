// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupccl

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var onlineRestoreLinkWorkers = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"backup.restore.online_worker_count",
	"workers to use for online restore worker phase",
	8,
	settings.PositiveInt,
)

// sendAddRemoteSSTs is a stubbed out, very simplistic version of restore used
// to test out ingesting "remote" SSTs. It will be replaced with a real distsql
// plan and processors in the future.
func sendAddRemoteSSTs(
	ctx context.Context,
	execCtx sql.JobExecContext,
	job *jobs.Job,
	dataToRestore restorationData,
	encryption *jobspb.BackupEncryptionOptions,
	uris []string,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	requestFinishedCh chan<- struct{},
	tracingAggCh chan *execinfrapb.TracingAggregatorEvents,
	genSpan func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error,
) (approxRows int64, approxDataSize int64, err error) {
	defer close(requestFinishedCh)
	defer close(tracingAggCh)

	if encryption != nil {
		return 0, 0, errors.AssertionFailedf("encryption not supported with online restore")
	}
	if len(uris) > 1 {
		return 0, 0, errors.AssertionFailedf("online restore can only restore data from a full backup")
	}

	restoreSpanEntriesCh := make(chan execinfrapb.RestoreSpanEntry, 1)

	grp := ctxgroup.WithContext(ctx)
	grp.GoCtx(func(ctx context.Context) error {
		return genSpan(ctx, restoreSpanEntriesCh)
	})

	kr, err := MakeKeyRewriterFromRekeys(execCtx.ExecCfg().Codec, dataToRestore.getRekeys(), dataToRestore.getTenantRekeys(),
		false /* restoreTenantFromStream */)
	if err != nil {
		return 0, 0, errors.Wrap(err, "creating key rewriter from rekeys")
	}

	fromSystemTenant := isFromSystemTenant(dataToRestore.getTenantRekeys())

	restoreWorkers := int(onlineRestoreLinkWorkers.Get(&execCtx.ExecCfg().Settings.SV))
	for i := 0; i < restoreWorkers; i++ {
		grp.GoCtx(sendAddRemoteSSTWorker(execCtx, restoreSpanEntriesCh, requestFinishedCh, *kr, fromSystemTenant, &approxRows, &approxDataSize))
	}

	if err := grp.Wait(); err != nil {
		return 0, 0, errors.Wrap(err, "failed to generate and send remote file spans")
	}
	return approxRows, approxDataSize, nil
}

func assertCommonPrefix(span roachpb.Span, elidedPrefixType execinfrapb.ElidePrefix) error {
	syntheticPrefix, err := elidedPrefix(span.Key, elidedPrefixType)
	if err != nil {
		return err
	}

	endKeyPrefix, err := elidedPrefix(span.EndKey, elidedPrefixType)
	if err != nil {
		return err
	}
	if !bytes.Equal(syntheticPrefix, endKeyPrefix) {
		return errors.AssertionFailedf("span start key %s and end key %s have different prefixes", span.Key, span.EndKey)
	}
	return nil
}

// rewriteSpan rewrites the span start and end key, potentially in place.
func rewriteSpan(
	kr *KeyRewriter, span roachpb.Span, elidedPrefixType execinfrapb.ElidePrefix,
) (roachpb.Span, error) {
	var (
		ok  bool
		err error
	)
	if err = assertCommonPrefix(span, elidedPrefixType); err != nil {
		return span, err
	}
	span.Key, ok, err = kr.RewriteKey(span.Key, 0)
	if !ok || err != nil {
		return span, errors.Wrapf(err, "span start key %s was not rewritten", span.Key)
	}
	span.EndKey, ok, err = kr.RewriteKey(span.EndKey, 0)
	if !ok || err != nil {
		return span, errors.Wrapf(err, "span end key %s was not rewritten ", span.Key)
	}
	return span, nil
}

func sendAddRemoteSSTWorker(
	execCtx sql.JobExecContext,
	restoreSpanEntriesCh <-chan execinfrapb.RestoreSpanEntry,
	requestFinishedCh chan<- struct{},
	kr KeyRewriter,
	fromSystemTenant bool,
	approxRows *int64,
	approxDataSize *int64,
) func(context.Context) error {
	return func(ctx context.Context) error {
		var toAdd []execinfrapb.RestoreFileSpec
		var batchSize int64
		var err error
		const targetBatchSize = 440 << 20

		flush := func(splitAt roachpb.Key, elidedPrefixType execinfrapb.ElidePrefix) error {
			if len(toAdd) == 0 {
				return nil
			}

			if len(splitAt) > 0 {
				if err := sendSplitAt(ctx, execCtx, splitAt); err != nil {
					log.Warningf(ctx, "failed to split during experimental restore: %v", err)
				}
			}

			for _, file := range toAdd {
				if err := sendRemoteAddSSTable(ctx, execCtx, file, elidedPrefixType, fromSystemTenant); err != nil {
					return err
				}
			}
			toAdd = nil
			batchSize = 0
			return nil
		}

		for entry := range restoreSpanEntriesCh {
			firstSplitDone := false
			if err := assertCommonPrefix(entry.Span, entry.ElidedPrefix); err != nil {
				return err
			}
			for _, file := range entry.Files {
				if err := assertCommonPrefix(file.BackupFileEntrySpan, entry.ElidedPrefix); err != nil {
					return err
				}

				restoringSubspan := file.BackupFileEntrySpan.Intersect(entry.Span)
				if !restoringSubspan.Valid() {
					return errors.AssertionFailedf("file %s with span %s has no overlap with restore span %s",
						file.Path,
						file.BackupFileEntrySpan,
						entry.Span,
					)
				}
				if !file.BackupFileEntrySpan.Equal(restoringSubspan) {
					return errors.AssertionFailedf("file span %s at path %s is not contained in restore span %s", file.BackupFileEntrySpan, file.Path, entry.Span)
				}
				// Clone the key because rewriteSpan could modify the keys in place, but
				// we reuse backup files across restore span entries.
				restoringSubspan, err = rewriteSpan(&kr, restoringSubspan.Clone(), entry.ElidedPrefix)
				if err != nil {
					return err
				}
				log.Infof(ctx, "experimental restore: sending span %s of file %s (file span: %s) as part of restore span (old key space) %s",
					restoringSubspan, file.Path, file.BackupFileEntrySpan, entry.Span)
				file.BackupFileEntrySpan = restoringSubspan
				if !firstSplitDone {
					if err := sendSplitAt(ctx, execCtx, restoringSubspan.Key); err != nil {
						log.Warningf(ctx, "failed to split during experimental restore: %v", err)
					}
					if err := sendAdminScatter(ctx, execCtx, restoringSubspan.Key); err != nil {
						log.Warningf(ctx, "failed to scatter during experimental restore: %v", err)
					}
					firstSplitDone = true
				}

				// If we've queued up a batch size of files, split before the next one
				// then flush the ones we queued. We do this accumulate-into-batch, then
				// split, then flush so that when we split we are splitting an empty
				// span rather than one we have added to, since we add with estimated
				// stats and splitting a span with estimated stats is slow.
				if batchSize+file.BackupFileEntryCounts.DataSize > targetBatchSize {
					log.Infof(ctx, "flushing %s batch of %d SSTs due to size limit. split at %s in span (old keyspace) %s", sz(batchSize), len(toAdd), file.BackupFileEntrySpan.Key, entry.Span)
					if err := flush(file.BackupFileEntrySpan.Key, entry.ElidedPrefix); err != nil {
						return err
					}
				}

				// Add this file to the batch to flush after we put a split to its RHS.
				toAdd = append(toAdd, file)
				batchSize += file.BackupFileEntryCounts.DataSize
			}
			// TODO(msbutler): think hard about if this restore span entry is a safe
			// key to split on. Note that it only is safe with
			// https://github.com/cockroachdb/cockroach/pull/114464
			log.Infof(ctx, "flushing %s batch of %d SSTs at end of restore span entry %s", sz(batchSize), len(toAdd), entry.Span)
			rewrittenFlushKey, ok, err := kr.RewriteKey(entry.Span.EndKey.Clone(), 0)
			if !ok || err != nil {
				return errors.Newf("flush key %s could not be rewritten", entry.Span.EndKey)
			}
			if err := flush(rewrittenFlushKey, entry.ElidedPrefix); err != nil {
				return err
			}
			var rows, dataSize int64
			for _, file := range entry.Files {
				rows += file.BackupFileEntryCounts.Rows
				dataSize += int64(file.ApproximatePhysicalSize)
			}
			atomic.AddInt64(approxRows, rows)
			atomic.AddInt64(approxDataSize, dataSize)
			requestFinishedCh <- struct{}{}
		}
		return nil
	}
}

// TODO(ssd): Perhaps the relevant DB functions should start tracing
// spans.
func sendSplitAt(ctx context.Context, execCtx sql.JobExecContext, splitKey roachpb.Key) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupccl.sendSplitAt")
	defer sp.Finish()

	expiration := execCtx.ExecCfg().Clock.Now().AddDuration(time.Hour)
	return execCtx.ExecCfg().DB.AdminSplit(ctx, splitKey, expiration)
}

func sendAdminScatter(
	ctx context.Context, execCtx sql.JobExecContext, scatterKey roachpb.Key,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupccl.sendAdminScatter")
	defer sp.Finish()

	const maxSize = 4 << 20
	_, err := execCtx.ExecCfg().DB.AdminScatter(ctx, scatterKey, maxSize)
	return err
}

func sendRemoteAddSSTable(
	ctx context.Context,
	execCtx sql.JobExecContext,
	file execinfrapb.RestoreFileSpec,
	elidedPrefixType execinfrapb.ElidePrefix,
	fromSystemTenant bool,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupccl.sendRemoteAddSSTable")
	defer sp.Finish()

	// NB: Since the restored span is a subset of the BackupFileEntrySpan,
	// these counts may be an overestimate of what actually gets restored.
	counts := file.BackupFileEntryCounts
	fileSize := file.ApproximatePhysicalSize

	// If we don't have physical file size info, just use the mvcc size; it
	// isn't physical size but is good enough for reflecting that there are
	// some number of bytes in this sst span for the purposes tracking which
	// spans have non-zero remote bytes.
	if fileSize == 0 {
		fileSize = uint64(counts.DataSize)
	}

	// If MVCC stats are _also_ zero just guess. Any non-zero value is fine.
	if fileSize == 0 {
		fileSize = 16 << 20
	}
	syntheticPrefix, err := elidedPrefix(file.BackupFileEntrySpan.Key, elidedPrefixType)
	if err != nil {
		return err
	}

	// TODO(dt): see if KV has any better ideas for making these up.
	fileStats := &enginepb.MVCCStats{
		ContainsEstimates: 1,
		KeyBytes:          counts.DataSize / 2,
		ValBytes:          counts.DataSize / 2,
		LiveBytes:         counts.DataSize,
		KeyCount:          counts.Rows + counts.IndexEntries,
		LiveCount:         counts.Rows + counts.IndexEntries,
	}

	var batchTimestamp hlc.Timestamp
	if writeAtBatchTS(ctx, file.BackupFileEntrySpan, fromSystemTenant) {
		batchTimestamp = execCtx.ExecCfg().DB.Clock().Now()
	}

	loc := kvpb.LinkExternalSSTableRequest_ExternalFile{
		Locator:                 file.Dir.URI,
		Path:                    file.Path,
		ApproximatePhysicalSize: fileSize,
		BackingFileSize:         file.BackingFileSize,
		SyntheticPrefix:         syntheticPrefix,
		UseSyntheticSuffix:      batchTimestamp.IsSet(),
		MVCCStats:               fileStats,
	}

	return execCtx.ExecCfg().DB.LinkExternalSSTable(
		ctx, file.BackupFileEntrySpan, loc, batchTimestamp)
}

// checkManifestsForOnlineCompat returns an error if the set of
// manifests appear to be from a backup that we cannot currently
// support for online restore.
func checkManifestsForOnlineCompat(ctx context.Context, manifests []backuppb.BackupManifest) error {
	if len(manifests) < 1 {
		return errors.AssertionFailedf("expected at least 1 backup manifest")
	}
	// TODO(online-restore): Remove once we support layer ordering.
	if len(manifests) > 1 {
		return pgerror.Newf(pgcode.FeatureNotSupported, "experimental online restore: restoring from an incremental backup not supported")
	}

	// TODO(online-restore): Remove once we support layer ordering and have tested some reasonable number of layers.
	const layerLimit = 16
	if len(manifests) > layerLimit {
		return pgerror.Newf(pgcode.FeatureNotSupported, "experimental online restore: too many incremental layers %d (from backup) > %d (limit)", len(manifests), layerLimit)
	}

	for _, manifest := range manifests {
		if !manifest.RevisionStartTime.IsEmpty() || !manifest.StartTime.IsEmpty() || manifest.MVCCFilter == backuppb.MVCCFilter_All {
			return pgerror.Newf(pgcode.FeatureNotSupported, "experimental online restore: restoring from a revision history backup not supported")
		}
	}
	return nil
}

// checkBackupElidedPrefixForOnlineCompat ensures the backup is online
// restorable depending on the kind of elided prefix in the backup. If no
// prefixes were stripped in the backup, the restore cannot rewrite table
// descriptors.
func checkBackupElidedPrefixForOnlineCompat(
	ctx context.Context, manifests []backuppb.BackupManifest, rewrites jobspb.DescRewriteMap,
) error {
	elidePrefix := manifests[0].ElidedPrefix

	for _, manifest := range manifests {
		if manifest.ElidedPrefix != elidePrefix {
			return errors.AssertionFailedf("incremental backup elided prefix is not the same as full backup")
		}
	}
	switch elidePrefix {
	case execinfrapb.ElidePrefix_TenantAndTable:
		return nil
	case execinfrapb.ElidePrefix_Tenant:
		return nil
	case execinfrapb.ElidePrefix_None:
		for oldID, rw := range rewrites {
			if rw.ID != oldID {
				return pgerror.Newf(pgcode.FeatureNotSupported, "experimental online restore: descriptor rewrites not supported but required (%d -> %d) on backup without stripped table prefixes", oldID, rw.ID)
			}
		}
		return nil
	default:
		return errors.AssertionFailedf("unexpected elided prefix value")
	}
}

func (r *restoreResumer) maybeCalculateTotalDownloadSpans(
	ctx context.Context, execCtx sql.JobExecContext, details jobspb.RestoreDetails,
) (uint64, error) {
	total := r.job.Progress().Details.(*jobspb.Progress_Restore).Restore.TotalDownloadRequired

	// If this is a resumption of a job that has already calculated the total
	// spans to download, we can skip this step.
	if total != 0 {
		return total, nil
	}

	ctx, sp := tracing.ChildSpan(ctx, "backupccl.maybeCalculateDownloadSpans")
	defer sp.Finish()

	// If this is the first resumption of this job, we need to find out the total
	// amount we expect to download and persist it so that we can indicate our
	// progress as that number goes down later.
	log.Infof(ctx, "calculating total download size (across all stores) to complete restore")
	if err := r.job.NoTxn().RunningStatus(ctx, "Calculating total download size..."); err != nil {
		return 0, errors.Wrapf(err, "failed to update running status of job %d", r.job.ID())
	}

	for _, span := range details.DownloadSpans {
		remainingForSpan, err := getRemainingExternalFileBytes(ctx, execCtx, span)
		if err != nil {
			return 0, err
		}
		total += remainingForSpan
	}
	if total == 0 {
		return total, nil
	}

	if err := r.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		md.Progress.GetRestore().TotalDownloadRequired = total
		md.Progress.RunningStatus = fmt.Sprintf("Downloading %s of restored data...", sz(total))
		ju.UpdateProgress(md.Progress)
		return nil
	}); err != nil {
		return 0, errors.Wrapf(err, "failed to update job %d", r.job.ID())
	}

	return total, nil
}

func (r *restoreResumer) sendDownloadWorker(
	execCtx sql.JobExecContext, spans roachpb.Spans, completionPoller chan struct{},
) func(context.Context) error {
	return func(ctx context.Context) error {
		ctx, tsp := tracing.ChildSpan(ctx, "backupccl.sendDownloadWorker")
		defer tsp.Finish()

		for rt := retry.StartWithCtx(
			ctx, retry.Options{InitialBackoff: time.Millisecond * 100, MaxBackoff: time.Second * 10},
		); ; rt.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}

			if err := sendDownloadSpan(ctx, execCtx, spans); err != nil {
				return err
			}

			// Wait for the completion poller to signal that it has checked our work.
			select {
			case _, ok := <-completionPoller:
				if !ok {
					return nil
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

var useCopy = envutil.EnvOrDefaultBool("COCKROACH_DOWNLOAD_COPY", true)

func sendDownloadSpan(ctx context.Context, execCtx sql.JobExecContext, spans roachpb.Spans) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupccl.sendDownloadSpan")
	defer sp.Finish()

	log.Infof(ctx, "sending download request for %d spans", len(spans))
	resp, err := execCtx.ExecCfg().TenantStatusServer.DownloadSpan(ctx, &serverpb.DownloadSpanRequest{
		Spans:                  spans,
		ViaBackingFileDownload: useCopy,
	})
	if err != nil {
		return err
	}
	log.Infof(ctx, "finished sending download requests for %d spans, %d errors", len(spans), len(resp.ErrorsByNodeID))
	for n, err := range resp.ErrorsByNodeID {
		return errors.Newf("failed to download spans on %d nodes; n%d returned %v", len(resp.ErrorsByNodeID), n, err)
	}
	return nil
}

func (r *restoreResumer) maybeWriteDownloadJob(
	ctx context.Context,
	execConfig *sql.ExecutorConfig,
	preRestoreData *restorationDataBase,
	mainRestoreData *mainRestorationData,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)
	if !details.ExperimentalOnline {
		return nil
	}

	kr, err := MakeKeyRewriterFromRekeys(execConfig.Codec, mainRestoreData.getRekeys(), mainRestoreData.getTenantRekeys(),
		false /* restoreTenantFromStream */)
	if err != nil {
		return errors.Wrap(err, "creating key rewriter from rekeys")
	}
	downloadSpans := mainRestoreData.getSpans()
	for i := range downloadSpans {
		var err error
		downloadSpans[i], err = rewriteSpan(kr, downloadSpans[i].Clone(), execinfrapb.ElidePrefix_None)
		if err != nil {
			return err
		}
	}

	log.Infof(ctx, "creating job to track downloads in %d spans", len(downloadSpans))
	downloadJobRecord := jobs.Record{
		Description: fmt.Sprintf("Background Data Download for %s", r.job.Payload().Description),
		Username:    r.job.Payload().UsernameProto.Decode(),
		Details: jobspb.RestoreDetails{
			DownloadJob:                        true,
			DownloadSpans:                      downloadSpans,
			PostDownloadTableAutoStatsSettings: details.PostDownloadTableAutoStatsSettings},
		Progress: jobspb.RestoreProgress{},
	}

	return execConfig.InternalDB.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		downloadJobID := r.job.ID() + 1
		if _, err := execConfig.JobRegistry.CreateJobWithTxn(ctx, downloadJobRecord, downloadJobID, txn); err != nil {
			return err
		}
		r.downloadJobID = downloadJobID
		return nil
	})
}

// waitForDownloadToComplete waits until there are no more ExternalFileBytes
// remaining to be downloaded for the restore. It sends a signal on the passed
// channel each time it polls the span, and closes it when it stops.
func (r *restoreResumer) waitForDownloadToComplete(
	ctx context.Context, execCtx sql.JobExecContext, details jobspb.RestoreDetails, ch chan struct{},
) error {
	defer close(ch)

	ctx, tsp := tracing.ChildSpan(ctx, "backupccl.waitForDownloadToComplete")
	defer tsp.Finish()

	total, err := r.maybeCalculateTotalDownloadSpans(ctx, execCtx, details)
	if err != nil {
		return errors.Wrap(err, "failed to calculate total number of spans to download")
	}

	// Download is already complete or there is nothing to be downloaded, in
	// either case we can mark the job as done.
	if total == 0 {
		return r.job.NoTxn().FractionProgressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
			return 1.0
		})
	}

	var lastProgressUpdate time.Time
	for rt := retry.StartWithCtx(
		ctx, retry.Options{InitialBackoff: time.Second, MaxBackoff: time.Second * 10},
	); ; rt.Next() {

		var remaining uint64
		for _, span := range details.DownloadSpans {
			remainingForSpan, err := getRemainingExternalFileBytes(ctx, execCtx, span)
			if err != nil {
				return err
			}
			remaining += remainingForSpan
		}

		// Sometimes a new virtual/external file sneaks in after we count total; the
		// amount this skews the informational percentage isn't enough to recompute
		// total but we still don't want total-remaining to be negative as that
		// leads to nonsensical progress.
		if remaining > total {
			total = remaining
		}

		fractionComplete := float32(total-remaining) / float32(total)
		log.Infof(ctx, "restore download phase, %s downloaded, %s remaining of %s total (%.2f complete)",
			sz(total-remaining), sz(remaining), sz(total), fractionComplete,
		)

		if remaining == 0 {
			r.notifyStatsRefresherOfNewTables()
			return nil
		}

		if timeutil.Since(lastProgressUpdate) > time.Minute {
			if err := r.job.NoTxn().FractionProgressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
				return fractionComplete
			}); err != nil {
				return err
			}
			lastProgressUpdate = timeutil.Now()
		}
		// Signal the download job if it is waiting that we've polled and found work
		// left for it to do.
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func getRemainingExternalFileBytes(
	ctx context.Context, execCtx sql.JobExecContext, span roachpb.Span,
) (uint64, error) {
	ctx, sp := tracing.ChildSpan(ctx, "backupccl.getRemainingExternalFileBytes")
	defer sp.Finish()

	resp, err := execCtx.ExecCfg().TenantStatusServer.SpanStats(ctx, &roachpb.SpanStatsRequest{
		NodeID:        "0", // Fan out to all nodes.
		Spans:         []roachpb.Span{span},
		SkipMvccStats: true,
	})
	if err != nil {
		return 0, err
	}

	var remaining uint64
	for _, stats := range resp.SpanToStats {
		remaining += stats.ExternalFileBytes
	}
	return remaining, nil
}

func (r *restoreResumer) doDownloadFiles(ctx context.Context, execCtx sql.JobExecContext) error {
	details := r.job.Details().(jobspb.RestoreDetails)

	grp := ctxgroup.WithContext(ctx)
	completionPoller := make(chan struct{})

	grp.GoCtx(r.sendDownloadWorker(execCtx, details.DownloadSpans, completionPoller))
	grp.GoCtx(func(ctx context.Context) error {
		return r.waitForDownloadToComplete(ctx, execCtx, details, completionPoller)
	})

	if err := grp.Wait(); err != nil {
		return errors.Wrap(err, "failed to generate and send download spans")
	}
	return r.cleanupAfterDownload(ctx, details)
}

func (r *restoreResumer) cleanupAfterDownload(
	ctx context.Context, details jobspb.RestoreDetails,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupccl.cleanupAfterDownload")
	defer sp.Finish()

	// Try to restore automatic stats collection preference on each restored
	// table.
	for id, settings := range details.PostDownloadTableAutoStatsSettings {
		if err := sql.DescsTxn(ctx, r.execCfg, func(
			ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
		) error {
			b := txn.KV().NewBatch()
			newTableDesc, err := descsCol.MutableByID(txn.KV()).Table(ctx, catid.DescID(id))
			if err != nil {
				return err
			}
			newTableDesc.AutoStatsSettings = settings
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, newTableDesc, b); err != nil {
				return err
			}
			if err := txn.KV().Run(ctx, b); err != nil {
				return err
			}
			return nil
		}); err != nil {
			// Re-enabling stats is best effort. The user may have dropped the table
			// since it came online.
			log.Warningf(ctx, "failed to re-enable auto stats on table %d", id)
		}
	}
	return nil
}

func createImportRollbackJob(
	ctx context.Context,
	jr *jobs.Registry,
	txn isql.Txn,
	username username.SQLUsername,
	tableDesc *tabledesc.Mutable,
) error {
	jobRecord := jobs.Record{
		Description:   fmt.Sprintf("ROLLBACK IMPORT INTO %s", tableDesc.GetName()),
		Username:      username,
		NonCancelable: true,
		DescriptorIDs: descpb.IDs{tableDesc.GetID()},
		Details: jobspb.ImportRollbackDetails{
			TableID: tableDesc.GetID(),
		},
		Progress: jobspb.ImportRollbackProgress{},
	}
	_, err := jr.CreateJobWithTxn(ctx, jobRecord, jr.MakeJobID(), txn)
	return err
}
