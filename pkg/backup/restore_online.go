// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backupsink"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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
	"workers to use for online restore link phase",
	32,
	settings.PositiveInt,
)

// splitAndScatter runs through all entries produced by genSpans splitting and
// scattering the key-space designated by the passed rewriter such that if all
// files in the entries in those spans were ingested the amount ingested between
// splits would be about targetRangeSize.
func splitAndScatter(
	ctx context.Context,
	execCtx sql.JobExecContext,
	genSpans func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error,
	kr KeyRewriter,
	fromSystemTenant bool,
	targetRangeSize int64,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backup.spitAndScatter")
	defer sp.Finish()

	log.Infof(ctx, "splitting and scattering spans")

	workers := int(onlineRestoreLinkWorkers.Get(&execCtx.ExecCfg().Settings.SV))
	toScatter := make(chan execinfrapb.RestoreSpanEntry, 1)
	toSplit := make(chan execinfrapb.RestoreSpanEntry, workers)

	// scatterer splits at the start and end of each entry sent to toScatter and
	// then scatters the span between those two splits before putting the entry on
	// the toSplit channel to be further split, closing that channel when done.
	scatterer := func(ctx context.Context) error {
		defer close(toSplit)

		var lastSplit roachpb.Key
		for entry := range toScatter {
			sp, err := rewriteSpan(&kr, entry.Span.Clone(), entry.ElidedPrefix)
			if err != nil {
				return err
			}

			// Split at start of the first chunk if it isn't the RHS of last chunk
			// which was just split in the previous iteration.
			if !lastSplit.Equal(sp.Key) {
				if err := sendSplitAt(ctx, execCtx, sp.Key); err != nil {
					log.Warningf(ctx, "failed to split during experimental restore: %v", err)
				}
			}
			// Split at the end of the chunk so that anything which happens to the
			// right of this chunk's span, including splitting other chunks, does not
			// interact with this span's scatter, ingests or additional splits.
			if err := sendSplitAt(ctx, execCtx, sp.EndKey); err != nil {
				log.Warningf(ctx, "failed to split during experimental restore: %v", err)
			}
			lastSplit = append(lastSplit[:0], sp.EndKey...)

			// Scatter the chunk's span now that it is is split at both sides.
			if err := sendAdminScatter(ctx, execCtx, sp.Key); err != nil {
				log.Warningf(ctx, "failed to scatter during experimental restore: %v", err)
			}

			toSplit <- entry
		}
		return nil
	}

	// splitter iterates the files in the entries sent to toSplit and splits at
	// the start of each file that would cause the sum of file data size since the
	// last split to exceed the targetRangeSize.
	splitter := func(ctx context.Context) error {
		for entry := range toSplit {
			var rangeSize int64

			for _, file := range entry.Files {
				// If this file does not fit in the range, split before it.
				if rangeSize+file.BackupFileEntryCounts.DataSize > targetRangeSize {
					fileStart := file.BackupFileEntrySpan.Intersect(entry.Span).Key
					start, ok, err := kr.RewriteKey(fileStart.Clone(), 0)
					if !ok || err != nil {
						return errors.Wrapf(err, "span start key %s was not rewritten", fileStart)
					}
					if err := sendSplitAt(ctx, execCtx, start); err != nil {
						log.Warningf(ctx, "failed to split during experimental restore: %v", err)
					}
					rangeSize = 0
				}
				rangeSize += file.BackupFileEntryCounts.DataSize
			}
		}
		return nil
	}

	grp := ctxgroup.WithContext(ctx)
	grp.GoCtx(func(ctx context.Context) error { return genSpans(ctx, toScatter) })
	grp.GoCtx(scatterer)
	for i := 0; i < workers; i++ {
		grp.GoCtx(splitter)
	}

	return grp.Wait()
}

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
	defer close(tracingAggCh)

	if encryption != nil {
		return 0, 0, errors.AssertionFailedf("encryption not supported with online restore")
	}

	const targetRangeSize = 440 << 20

	kr, err := MakeKeyRewriterFromRekeys(execCtx.ExecCfg().Codec, dataToRestore.getRekeys(), dataToRestore.getTenantRekeys(),
		false /* restoreTenantFromStream */)
	if err != nil {
		return 0, 0, errors.Wrap(err, "creating key rewriter from rekeys")
	}

	fromSystemTenant := isFromSystemTenant(dataToRestore.getTenantRekeys())

	if err := execCtx.ExecCfg().JobRegistry.CheckPausepoint("restore.before_split"); err != nil {
		return 0, 0, err
	}

	if err := job.NoTxn().UpdateStatusMessage(ctx, "Splitting and distributing spans"); err != nil {
		return 0, 0, err
	}

	if err := splitAndScatter(ctx, execCtx, genSpan, *kr, fromSystemTenant, targetRangeSize); err != nil {
		return 0, 0, errors.Wrap(err, "failed to split and scatter spans")
	}

	if err := execCtx.ExecCfg().JobRegistry.CheckPausepoint("restore.before_link"); err != nil {
		return 0, 0, err
	}

	if err := job.NoTxn().UpdateStatusMessage(ctx, ""); err != nil {
		return 0, 0, err
	}

	approxRows, approxDataSize, err = linkExternalFiles(
		ctx, execCtx, genSpan, *kr, fromSystemTenant, requestFinishedCh,
	)
	return approxRows, approxDataSize, errors.Wrap(err, "failed to ingest into remote files")
}

func assertCommonPrefix(span roachpb.Span, elidedPrefixType execinfrapb.ElidePrefix) error {
	syntheticPrefix, err := backupsink.ElidedPrefix(span.Key, elidedPrefixType)
	if err != nil {
		return err
	}

	endKeyPrefix, err := backupsink.ElidedPrefix(span.EndKey, elidedPrefixType)
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
	if err := assertCommonPrefix(span, elidedPrefixType); err != nil {
		return roachpb.Span{}, err
	}
	return kr.RewriteSpan(span)
}

// linkExternalFiles runs through all entries produced by genSpans and links in
// all files in the entries rewritten using the passed rewriter. It assumes that
// the target spans have already been split and scattered.
func linkExternalFiles(
	ctx context.Context,
	execCtx sql.JobExecContext,
	genSpans func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error,
	kr KeyRewriter,
	fromSystemTenant bool,
	requestFinishedCh chan<- struct{},
) (approxRows int64, approxDataSize int64, err error) {
	ctx, sp := tracing.ChildSpan(ctx, "backup.linkExternalFiles")
	defer sp.Finish()
	defer close(requestFinishedCh)

	log.Infof(ctx, "ingesting remote files")

	workers := int(onlineRestoreLinkWorkers.Get(&execCtx.ExecCfg().Settings.SV))

	grp := ctxgroup.WithContext(ctx)
	ch := make(chan execinfrapb.RestoreSpanEntry, workers)
	grp.GoCtx(func(ctx context.Context) error { return genSpans(ctx, ch) })
	for i := 0; i < workers; i++ {
		grp.GoCtx(sendAddRemoteSSTWorker(
			execCtx, ch, requestFinishedCh, kr, fromSystemTenant, &approxRows, &approxDataSize,
		))
	}
	if err := grp.Wait(); err != nil {
		return 0, 0, err
	}
	return approxRows, approxDataSize, nil
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
		for entry := range restoreSpanEntriesCh {
			log.VInfof(ctx, 1, "starting restore of backed up span %s containing %d files", entry.Span, len(entry.Files))

			if err := assertCommonPrefix(entry.Span, entry.ElidedPrefix); err != nil {
				return err
			}

			var currentLayer int32
			for _, file := range entry.Files {
				if file.Layer < currentLayer {
					return errors.AssertionFailedf("files not sorted by layer")
				}
				currentLayer = file.Layer
				if file.HasRangeKeys {
					return errors.Newf("online restore of range keys not supported")
				}
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

				restoringSubspan, err := rewriteSpan(&kr, restoringSubspan.Clone(), entry.ElidedPrefix)
				if err != nil {
					return err
				}

				log.VInfof(ctx, 1, "restoring span %s of file %s (file span: %s)", restoringSubspan, file.Path, file.BackupFileEntrySpan)
				file.BackupFileEntrySpan = restoringSubspan
				if err := sendRemoteAddSSTable(ctx, execCtx, file, entry.ElidedPrefix, fromSystemTenant); err != nil {
					return err
				}
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
	ctx, sp := tracing.ChildSpan(ctx, "backup.sendSplitAt")
	defer sp.Finish()

	expiration := execCtx.ExecCfg().Clock.Now().AddDuration(time.Hour)
	return execCtx.ExecCfg().DB.AdminSplit(ctx, splitKey, expiration)
}

func sendAdminScatter(
	ctx context.Context, execCtx sql.JobExecContext, scatterKey roachpb.Key,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backup.sendAdminScatter")
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
	ctx, sp := tracing.ChildSpan(ctx, "backup.sendRemoteAddSSTable")
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
	syntheticPrefix, err := backupsink.ElidedPrefix(file.BackupFileEntrySpan.Key, elidedPrefixType)
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

	if len(manifests) > 1 && !clusterversion.V24_1.Version().Less(manifests[0].ClusterVersion) {
		return errors.Newf("the backup must be on a cluster version greater than %s to run online restore with an incremental backup", clusterversion.V24_1.String())
	}

	// TODO(online-restore): Remove once we support layer ordering and have tested some reasonable number of layers.
	const layerLimit = 3
	if len(manifests) > layerLimit {
		return pgerror.Newf(pgcode.FeatureNotSupported, "experimental online restore: too many incremental layers %d (from backup) > %d (limit)", len(manifests), layerLimit)
	}

	for _, manifest := range manifests {
		if !manifest.RevisionStartTime.IsEmpty() || manifest.MVCCFilter == backuppb.MVCCFilter_All {
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

	ctx, sp := tracing.ChildSpan(ctx, "backup.maybeCalculateDownloadSpans")
	defer sp.Finish()

	// If this is the first resumption of this job, we need to find out the total
	// amount we expect to download and persist it so that we can indicate our
	// progress as that number goes down later.
	log.Infof(ctx, "calculating total download size (across all stores) to complete restore")
	if err := r.job.NoTxn().UpdateStatusMessage(ctx, "Calculating total download size..."); err != nil {
		return 0, errors.Wrapf(err, "failed to update running status of job %d", r.job.ID())
	}

	for _, span := range details.DownloadSpans {
		remainingForSpan, err := getRemainingExternalFileBytes(ctx, execCtx, span)
		if err != nil {
			return 0, err
		}
		total += remainingForSpan
	}

	log.Infof(ctx, "total download size (across all stores) to complete restore: %s", sz(total))

	if total == 0 {
		return total, nil
	}

	if err := r.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		md.Progress.GetRestore().TotalDownloadRequired = total
		md.Progress.StatusMessage = fmt.Sprintf("Downloading %s of restored data...", sz(total))
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
		ctx, tsp := tracing.ChildSpan(ctx, "backup.sendDownloadWorker")
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
	ctx, sp := tracing.ChildSpan(ctx, "backup.sendDownloadSpan")
	defer sp.Finish()

	log.VInfof(ctx, 1, "sending download request for %d spans", len(spans))
	resp, err := execCtx.ExecCfg().TenantStatusServer.DownloadSpan(ctx, &serverpb.DownloadSpanRequest{
		Spans:                  spans,
		ViaBackingFileDownload: useCopy,
	})
	if err != nil {
		return err
	}
	log.VInfof(ctx, 1, "finished sending download requests for %d spans, %d errors", len(spans), len(resp.Errors))
	for n, encoded := range resp.Errors {
		err := errors.DecodeError(ctx, encoded)
		return errors.Wrapf(err,
			"failed to download spans on %d nodes; n%d err", len(resp.Errors), n)
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
	rekey := mainRestoreData.getRekeys()
	rekey = append(rekey, preRestoreData.getRekeys()...)

	tenantRekey := mainRestoreData.getTenantRekeys()
	tenantRekey = append(tenantRekey, preRestoreData.getTenantRekeys()...)
	kr, err := MakeKeyRewriterFromRekeys(execConfig.Codec, rekey, tenantRekey,
		false /* restoreTenantFromStream */)
	if err != nil {
		return errors.Wrap(err, "creating key rewriter from rekeys")
	}
	downloadSpans := mainRestoreData.getSpans()

	// Intentionally download preRestoreData after the main data. During a cluster
	// restore, preRestore data are linked to a temp system db that are then
	// copied over to the real system db. This temp system db is then deleted and
	// should never be queried. We still want to download this data, however, to
	// protect against external storage deletions of these linked in ssts, but at
	// lower priority to the main data.
	downloadSpans = append(downloadSpans, preRestoreData.getSpans()...)
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

	ctx, tsp := tracing.ChildSpan(ctx, "backup.waitForDownloadToComplete")
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
		log.VInfof(ctx, 1, "restore download phase, %s downloaded, %s remaining of %s total (%.2f complete)",
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
	ctx, sp := tracing.ChildSpan(ctx, "backup.getRemainingExternalFileBytes")
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
		if errors.HasType(err, &kvpb.InsufficientSpaceError{}) {
			return jobs.MarkPauseRequestError(errors.UnwrapAll(err))
		}
		return errors.Wrap(err, "failed to generate and send download spans")
	}
	return r.cleanupAfterDownload(ctx, details)
}

func (r *restoreResumer) cleanupAfterDownload(
	ctx context.Context, details jobspb.RestoreDetails,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backup.cleanupAfterDownload")
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
