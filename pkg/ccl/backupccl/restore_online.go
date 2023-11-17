// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var onlineRestoreLinkWorkers = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"backup.restore.online_worker_count",
	"workers to use for online restore worker phase",
	8,
	settings.PositiveInt,
)

// sendAddRemoteSSTs is a stubbed out, very simplisitic version of restore used
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
	progCh chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	tracingAggCh chan *execinfrapb.TracingAggregatorEvents,
	genSpan func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error,
) error {
	defer close(progCh)
	defer close(tracingAggCh)

	if encryption != nil {
		return errors.AssertionFailedf("encryption not supported with online restore")
	}
	if len(uris) > 1 {
		return errors.AssertionFailedf("online restore can only restore data from a full backup")
	}

	restoreSpanEntriesCh := make(chan execinfrapb.RestoreSpanEntry, 1)

	grp := ctxgroup.WithContext(ctx)
	grp.GoCtx(func(ctx context.Context) error {
		return genSpan(ctx, restoreSpanEntriesCh)
	})

	restoreWorkers := int(onlineRestoreLinkWorkers.Get(&execCtx.ExecCfg().Settings.SV))
	for i := 0; i < restoreWorkers; i++ {
		grp.GoCtx(sendAddRemoteSSTWorker(execCtx, restoreSpanEntriesCh))
	}

	if err := grp.Wait(); err != nil {
		return errors.Wrap(err, "failed to generate and send remote file spans")
	}

	downloadSpans := dataToRestore.getSpans()

	log.Infof(ctx, "creating job to track downloads in %d spans", len(downloadSpans))
	downloadJobRecord := jobs.Record{
		Description: fmt.Sprintf("Background Data Download for %s", job.Payload().Description),
		Username:    job.Payload().UsernameProto.Decode(),
		Details:     jobspb.RestoreDetails{DownloadSpans: downloadSpans},
		Progress:    jobspb.RestoreProgress{},
	}

	return execCtx.ExecCfg().InternalDB.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		_, err := execCtx.ExecCfg().JobRegistry.CreateJobWithTxn(ctx, downloadJobRecord, job.ID()+1, txn)
		return err
	})
}

func sendAddRemoteSSTWorker(
	execCtx sql.JobExecContext, restoreSpanEntriesCh <-chan execinfrapb.RestoreSpanEntry,
) func(context.Context) error {
	return func(ctx context.Context) error {
		var toAdd []execinfrapb.RestoreFileSpec
		var batchSize int64
		const targetBatchSize = 384 << 20

		flush := func(splitAt roachpb.Key) error {
			if len(toAdd) == 0 {
				return nil
			}

			if len(splitAt) > 0 {
				expiration := execCtx.ExecCfg().Clock.Now().AddDuration(time.Hour)
				if err := execCtx.ExecCfg().DB.AdminSplit(ctx, splitAt, expiration); err != nil {
					log.Warningf(ctx, "failed to split during experimental restore: %v", err)
				}
			}

			for _, file := range toAdd {
				// NB: Since the restored span is a subset of the BackupFileEntrySpan,
				// these counts may be an overestimate of what actually gets restored.
				counts := file.BackupFileEntryCounts

				loc := kvpb.AddSSTableRequest_RemoteFile{
					Locator:         file.Dir.URI,
					Path:            file.Path,
					BackingFileSize: uint64(counts.DataSize),
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
				var err error
				_, _, err = execCtx.ExecCfg().DB.AddRemoteSSTable(ctx,
					file.BackupFileEntrySpan, loc,
					fileStats)
				if err != nil {
					return err
				}
			}
			toAdd = nil
			batchSize = 0
			return nil
		}

		for entry := range restoreSpanEntriesCh {
			firstSplitDone := false
			for _, file := range entry.Files {
				restoringSubspan := file.BackupFileEntrySpan.Intersect(entry.Span)
				if !restoringSubspan.Valid() {
					return errors.AssertionFailedf("file %s with span %s has no overlap with restore span %s",
						file.Path,
						file.BackupFileEntrySpan,
						entry.Span,
					)
				}

				log.Infof(ctx, "experimental restore: sending span %s of file %s (file span: %s) as part of restore span %s",
					restoringSubspan, file.Path, file.BackupFileEntrySpan, entry.Span)
				file.BackupFileEntrySpan = restoringSubspan
				if !firstSplitDone {
					expiration := execCtx.ExecCfg().Clock.Now().AddDuration(time.Hour)
					if err := execCtx.ExecCfg().DB.AdminSplit(ctx, restoringSubspan.Key, expiration); err != nil {
						log.Warningf(ctx, "failed to split during experimental restore: %v", err)
					}
					if _, err := execCtx.ExecCfg().DB.AdminScatter(ctx, restoringSubspan.Key, 4<<20); err != nil {
						log.Warningf(ctx, "failed to scatter during experimental restore: %v", err)
					}
					firstSplitDone = true
				}

				// If we've queued up a batch size of files, split before the next one
				// then flush the ones we queued. We do this accumulate-into-batch, then
				// split, then flush so that when we split we are splitting an empty
				// span rather than one we have added to, since we add with estimated
				// stats and splitting a span with estimated stats is slow.
				if batchSize > targetBatchSize {
					if err := flush(file.BackupFileEntrySpan.Key); err != nil {
						return err
					}
				}

				// Add this file to the batch to flush after we put a split to its RHS.
				toAdd = append(toAdd, file)
				batchSize += file.BackupFileEntryCounts.DataSize
			}
		}
		return flush(nil)
	}
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

// checkRewritesAreNoops returns an error if any of the rewrites in
// the rewrite map actually require key rewriting. We currently don't
// rewrite keys, so this would be a problem.
func checkRewritesAreNoops(rewrites jobspb.DescRewriteMap) error {
	for oldID, rw := range rewrites {
		if rw.ID != oldID {
			return pgerror.Newf(pgcode.FeatureNotSupported, "experimental online restore: descriptor rewrites not supported but required (%d -> %d)", oldID, rw.ID)
		}
	}
	return nil
}
