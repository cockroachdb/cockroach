// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backupdest"
	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backupsink"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

// StartCompactionJob kicks off an asynchronous job to compact the backups at
// the collection URI within the start and end timestamps.
//
// Note that planner should be a sql.PlanHookState. Due to import cycles with
// the sql and builtins package, the interface{} type is used.
func StartCompactionJob(
	ctx context.Context,
	planner interface{},
	collectionURI, incrLoc []string,
	fullBackupPath string,
	encryptionOpts jobspb.BackupEncryptionOptions,
	start, end hlc.Timestamp,
) (jobspb.JobID, error) {
	planHook, ok := planner.(sql.PlanHookState)
	if !ok {
		return 0, errors.New("missing job execution context")
	}
	details := jobspb.BackupDetails{
		StartTime: start,
		EndTime:   end,
		Destination: jobspb.BackupDetails_Destination{
			To:                 collectionURI,
			IncrementalStorage: incrLoc,
			Subdir:             fullBackupPath,
			Exists:             true,
		},
		EncryptionOptions: &encryptionOpts,
		Compact:           true,
	}
	jobID := planHook.ExecCfg().JobRegistry.MakeJobID()
	description, err := compactionJobDescription(details)
	if err != nil {
		return 0, err
	}
	jobRecord := jobs.Record{
		Description: description,
		Details:     details,
		Progress:    jobspb.BackupProgress{},
		Username:    planHook.User(),
	}
	if _, err := planHook.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
		ctx, jobRecord, jobID, planHook.InternalSQLTxn(),
	); err != nil {
		return 0, err
	}
	return jobID, nil
}

func (b *backupResumer) ResumeCompaction(
	ctx context.Context,
	initialDetails jobspb.BackupDetails,
	execCtx sql.JobExecContext,
	kmsEnv cloud.KMSEnv,
) error {
	// We interleave the computation of the compaction chain between the destination
	// resolution and writing of backup lock due to the need to verify that the
	// compaction chain is a valid chain.
	prevManifests, localityInfo, encryption, allIters, err := getBackupChain(ctx, execCtx, initialDetails, kmsEnv)
	if err != nil {
		return err
	}
	compactChain, err := newCompactionChain(
		prevManifests,
		initialDetails.StartTime,
		initialDetails.EndTime,
		localityInfo,
		allIters,
	)
	if err != nil {
		return err
	}

	var backupManifest *backuppb.BackupManifest
	updatedDetails := initialDetails
	if initialDetails.URI == "" {
		// Resolve the backup destination. If we have already resolved and persisted
		// the destination during a previous resumption of this job, we can re-use
		// the previous resolution.
		backupDest, err := backupdest.ResolveDestForCompaction(ctx, execCtx, initialDetails)
		if err != nil {
			return err
		}
		if err = maybeWriteBackupLock(ctx, execCtx, backupDest, b.job.ID()); err != nil {
			return err
		}
		updatedDetails, err = updateCompactionBackupDetails(
			ctx, compactChain, initialDetails, backupDest, encryption, kmsEnv,
		)
		if err != nil {
			return err
		}
		backupManifest, err = createCompactionManifest(ctx, execCtx, updatedDetails, compactChain)
		if err != nil {
			return err
		}

		if err := execCtx.ExecCfg().JobRegistry.CheckPausepoint("backup.before.write_first_checkpoint"); err != nil {
			return err
		}

		if err := backupinfo.WriteBackupManifestCheckpoint(
			ctx, updatedDetails.URI, updatedDetails.EncryptionOptions, kmsEnv,
			backupManifest, execCtx.ExecCfg(), execCtx.User(),
		); err != nil {
			return err
		}

		if err := execCtx.ExecCfg().JobRegistry.CheckPausepoint("backup.after.write_first_checkpoint"); err != nil {
			return err
		}

		description := maybeUpdateJobDescription(
			initialDetails, updatedDetails, b.job.Payload().Description,
		)

		// Update the job payload (non-volatile job definition) once, with the now
		// resolved destination, updated description, etc. If we resume again we'll
		// skip this whole block so this isn't an excessive update of payload.
		if err := b.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			if err := md.CheckRunningOrReverting(); err != nil {
				return err
			}
			md.Payload.Details = jobspb.WrapPayloadDetails(updatedDetails)
			md.Payload.Description = description
			ju.UpdatePayload(md.Payload)
			return nil
		}); err != nil {
			return err
		}

		if err := execCtx.ExecCfg().JobRegistry.CheckPausepoint("backup.after.details_has_checkpoint"); err != nil {
			return err
		}
		// TODO (kev-cao): Add telemetry for backup compactions.
	}

	// For all backups, partitioned or not, the main BACKUP manifest is stored at
	// details.URI.
	defaultConf, err := cloud.ExternalStorageConfFromURI(updatedDetails.URI, execCtx.User())
	if err != nil {
		return errors.Wrapf(err, "export configuration")
	}
	defaultStore, err := execCtx.ExecCfg().DistSQLSrv.ExternalStorage(ctx, defaultConf)
	if err != nil {
		return errors.Wrapf(err, "make storage")
	}
	defer defaultStore.Close()

	// EncryptionInfo is non-nil only when new encryption information has been
	// generated during BACKUP planning.
	redactedURI := backuputils.RedactURIForErrorMessage(updatedDetails.URI)
	if updatedDetails.EncryptionInfo != nil {
		if err := backupencryption.WriteEncryptionInfoIfNotExists(
			ctx, updatedDetails.EncryptionInfo, defaultStore,
		); err != nil {
			return errors.Wrapf(err, "creating encryption info file to %s", redactedURI)
		}
	}

	storageByLocalityKV := make(map[string]*cloudpb.ExternalStorage)
	for kv, uri := range updatedDetails.URIsByLocalityKV {
		conf, err := cloud.ExternalStorageConfFromURI(uri, execCtx.User())
		if err != nil {
			return err
		}
		storageByLocalityKV[kv] = &conf
	}

	mem := execCtx.ExecCfg().RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)
	var memSize int64

	if backupManifest == nil {
		backupManifest, memSize, err = b.readManifestOnResume(ctx, &mem, execCtx.ExecCfg(), defaultStore,
			updatedDetails, execCtx.User(), kmsEnv)
		if err != nil {
			return err
		}
	}

	// We retry on pretty generic failures -- any rpc error. If a worker node were
	// to restart, it would produce this kind of error, but there may be other
	// errors that are also rpc errors. Don't retry too aggressively.
	retryOpts := retry.Options{
		MaxBackoff: 1 * time.Second,
		MaxRetries: 5,
	}

	if execCtx.ExecCfg().BackupRestoreTestingKnobs != nil &&
		execCtx.ExecCfg().BackupRestoreTestingKnobs.BackupDistSQLRetryPolicy != nil {
		retryOpts = *execCtx.ExecCfg().BackupRestoreTestingKnobs.BackupDistSQLRetryPolicy
	}

	if err := execCtx.ExecCfg().JobRegistry.CheckPausepoint("backup.before.flow"); err != nil {
		return err
	}

	// We want to retry a backup if there are transient failures (i.e. worker nodes
	// dying), so if we receive a retryable error, re-plan and retry the backup.
	// TODO (kev-cao): Add progress tracking to compactions.
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		if err = compactChain.Compact(ctx, execCtx, updatedDetails, backupManifest, defaultStore, kmsEnv); err == nil {
			break
		}

		if joberror.IsPermanentBulkJobError(err) {
			return errors.Wrap(err, "failed to run backup compaction")
		}

		// If we are draining, it is unlikely we can start a
		// new DistSQL flow. Exit with a retryable error so
		// that another node can pick up the job.
		if execCtx.ExecCfg().JobRegistry.IsDraining() {
			return jobs.MarkAsRetryJobError(errors.Wrapf(err, "job encountered retryable error on draining node"))
		}

		log.Warningf(ctx, "encountered retryable error: %+v", err)

		// Reload the backup manifest to pick up any spans we may have completed on
		// previous attempts.
		// TODO (kev-cao): Compactions currently do not create checkpoints, but this
		// can be used to reload the manifest once we add checkpointing.
		var reloadBackupErr error
		mem.Shrink(ctx, memSize)
		backupManifest, memSize, reloadBackupErr = b.readManifestOnResume(ctx, &mem, execCtx.ExecCfg(),
			defaultStore, updatedDetails, execCtx.User(), kmsEnv)
		if reloadBackupErr != nil {
			return errors.Wrap(reloadBackupErr, "could not reload backup manifest when retrying")
		}
	}
	// We have exhausted retries without getting a "PermanentBulkJobError", but
	// something must be wrong if we keep seeing errors so give up and fail to
	// ensure that any alerting on failures is triggered and that any subsequent
	// schedule runs are not blocked.
	if err != nil {
		return errors.Wrap(err, "exhausted retries")
	}

	return b.maybeNotifyScheduledJobCompletion(
		ctx, jobs.StateSucceeded, execCtx.ExecCfg().JobsKnobs(), execCtx.ExecCfg().InternalDB,
	)
}

type compactionChain struct {
	// backupChain is the linear chain of backups up to the end time required
	// for a restore.
	backupChain    []backuppb.BackupManifest
	chainToCompact []backuppb.BackupManifest
	// start refers to the start time of the first backup to be compacted.
	// end refers to the end time of the last backup to be compacted.
	start, end hlc.Timestamp
	// Inclusive startIdx and exclusive endIdx of the sub-chain to compact.
	startIdx, endIdx int
	// Locality info per layer in the compacted chain.
	compactedLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo
	// All iter factories for all backups in the chain.
	allIters backupinfo.LayerToBackupManifestFileIterFactory
	// Iter factory for just the backups in the chain to compact.
	compactedIterFactory backupinfo.LayerToBackupManifestFileIterFactory
}

// Compact runs compaction on the chain according to the job details and
// associated backup manifest.
func (c *compactionChain) Compact(
	ctx context.Context,
	execCtx sql.JobExecContext,
	details jobspb.BackupDetails,
	backupManifest *backuppb.BackupManifest,
	defaultStore cloud.ExternalStorage,
	kmsEnv cloud.KMSEnv,
) error {
	ctx, span := tracing.ChildSpan(ctx, "backup.compaction")
	defer span.Finish()
	log.Infof(
		ctx, "beginning compaction of %d backups: %s",
		len(c.chainToCompact), util.Map(c.chainToCompact, func(m backuppb.BackupManifest) string {
			return m.ID.String()
		}),
	)
	backupLocalityMap, err := makeBackupLocalityMap(c.compactedLocalityInfo, execCtx.User())
	if err != nil {
		return err
	}

	introducedSpanFrontier, err := createIntroducedSpanFrontier(c.backupChain, backupManifest.EndTime)
	if err != nil {
		return err
	}
	defer introducedSpanFrontier.Release()

	spanCh := make(chan execinfrapb.RestoreSpanEntry, 1000)
	backupCodec, err := backupinfo.MakeBackupCodec(c.chainToCompact)
	if err != nil {
		return err
	}
	var tables []catalog.TableDescriptor
	for _, desc := range backupManifest.Descriptors {
		catDesc := backupinfo.NewDescriptorForManifest(&desc)
		if table, ok := catDesc.(catalog.TableDescriptor); ok {
			tables = append(tables, table)
		}
	}
	targetSize := targetRestoreSpanSize.Get(&execCtx.ExecCfg().Settings.SV)
	maxFiles := maxFileCount.Get(&execCtx.ExecCfg().Settings.SV)

	var fsc fileSpanComparator = &exclusiveEndKeyComparator{}
	filter, err := makeSpanCoveringFilter(
		backupManifest.Spans,
		[]jobspb.RestoreProgress_FrontierEntry{},
		introducedSpanFrontier,
		targetSize,
		maxFiles,
	)
	if err != nil {
		return err
	}
	spans, err := spansForAllRestoreTableIndexes(
		backupCodec,
		tables,
		nil,   /* revs */
		false, /* schemaOnly */
		false, /* forOnlineRestore */
	)
	if err != nil {
		return err
	}
	completedSpans, completedIntroducedSpans, err := getCompletedSpans(
		ctx, execCtx, backupManifest, defaultStore, details.EncryptionOptions, kmsEnv,
	)
	if err != nil {
		return err
	}
	spans = filterSpans(spans, completedSpans)
	spans = filterSpans(spans, completedIntroducedSpans)

	genSpan := func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error {
		defer close(spanCh)
		if err != nil {
			return err
		}
		return errors.Wrap(generateAndSendImportSpans(
			ctx,
			spans,
			c.chainToCompact,
			c.compactedIterFactory,
			backupLocalityMap,
			filter,
			fsc,
			spanCh,
		), "generate and send import spans")
	}

	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
	var tasks []func(context.Context) error
	encryption := details.EncryptionOptions
	tasks = append(tasks, func(ctx context.Context) error {
		return genSpan(ctx, spanCh)
	})
	tasks = append(tasks, func(ctx context.Context) error {
		return runCompaction(ctx, execCtx, encryption, spanCh, details, backupManifest, progCh, defaultStore)
	})
	tasks = append(tasks, func(ctx context.Context) error {
		return processProgress(ctx, backupManifest, progCh)
	})

	if err := ctxgroup.GoAndWait(ctx, tasks...); err != nil {
		return err
	}
	return concludeBackupCompaction(ctx, execCtx, defaultStore, encryption, kmsEnv, backupManifest)
}

// lastBackup returns the last backup of the chain to compact.
func (c *compactionChain) lastBackup() backuppb.BackupManifest {
	return c.backupChain[c.endIdx-1]
}

// newCompactionChain returns a new compacted backup chain based on the specified start and end
// timestamps from a chain of backups. The start and end times must specify specific backups.
func newCompactionChain(
	manifests []backuppb.BackupManifest,
	start, end hlc.Timestamp,
	localityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	layerToIterFactory backupinfo.LayerToBackupManifestFileIterFactory,
) (compactionChain, error) {
	// The start and end timestamps indicate a chain of incrementals and therefore should not
	// include the full backup.
	if start.Less(manifests[0].EndTime) {
		return compactionChain{}, errors.Errorf(
			"start time %s is before full backup end time %s",
			start, manifests[0].EndTime,
		)
	}
	var startIdx, endIdx int
	for idx, m := range manifests {
		if m.StartTime.Equal(start) {
			startIdx = idx
		}
		if m.EndTime.Equal(end) {
			endIdx = idx + 1
		}
	}
	if startIdx == 0 {
		return compactionChain{}, errors.Newf(
			"no incrementals found with the specified start time %s", start,
		)
	} else if endIdx == 0 {
		return compactionChain{}, errors.Newf("no incrementals found with the specified end time %s", end)
	}

	compactedIters := make(backupinfo.LayerToBackupManifestFileIterFactory)
	for i := startIdx; i < endIdx; i++ {
		compactedIters[i-startIdx] = layerToIterFactory[i]
	}
	return compactionChain{
		backupChain:           manifests,
		chainToCompact:        manifests[startIdx:endIdx],
		startIdx:              startIdx,
		endIdx:                endIdx,
		start:                 start,
		end:                   end,
		compactedLocalityInfo: localityInfo[startIdx:endIdx],
		allIters:              layerToIterFactory,
		compactedIterFactory:  compactedIters,
	}, nil
}

func runCompaction(
	ctx context.Context,
	execCtx sql.JobExecContext,
	encryption *jobspb.BackupEncryptionOptions,
	entries chan execinfrapb.RestoreSpanEntry,
	details jobspb.BackupDetails,
	manifest *backuppb.BackupManifest,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	store cloud.ExternalStorage,
) error {
	defer close(progCh)
	var encryptionOptions *kvpb.FileEncryptionOptions
	if encryption != nil {
		encryptionOptions = &kvpb.FileEncryptionOptions{Key: encryption.Key}
	}
	sinkConf := backupsink.SSTSinkConf{
		ID:        execCtx.ExecCfg().DistSQLSrv.NodeID.SQLInstanceID(),
		Enc:       encryptionOptions,
		ProgCh:    progCh,
		Settings:  &execCtx.ExecCfg().Settings.SV,
		ElideMode: manifest.ElidedPrefix,
	}
	sink, err := backupsink.MakeSSTSinkKeyWriter(sinkConf, store, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := sink.Flush(ctx); err != nil {
			log.Warningf(ctx, "failed to flush sink: %v", err)
			logClose(ctx, sink, "SST sink")
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entry, ok := <-entries:
			if !ok {
				return nil
			}

			sstIter, err := openSSTs(ctx, execCtx, entry, encryptionOptions, details)
			if err != nil {
				return errors.Wrap(err, "opening SSTs")
			}

			if err := processSpanEntry(ctx, sstIter, sink); err != nil {
				return errors.Wrap(err, "processing span entry")
			}
		}
	}
}

func processSpanEntry(
	ctx context.Context, sstIter mergedSST, sink *backupsink.SSTSinkKeyWriter,
) error {
	defer sstIter.cleanup()
	entry := sstIter.entry
	prefix, err := backupsink.ElidedPrefix(entry.Span.Key, entry.ElidedPrefix)
	if err != nil {
		return err
	} else if prefix == nil {
		return errors.New("backup compactions does not supported non-elided keys")
	}
	trimmedStart := storage.MVCCKey{Key: bytes.TrimPrefix(entry.Span.Key, prefix)}
	trimmedEnd := storage.MVCCKey{Key: bytes.TrimPrefix(entry.Span.EndKey, prefix)}
	if err := sink.Reset(ctx, entry.Span); err != nil {
		return err
	}
	scratch := make([]byte, 0, len(prefix))
	scratch = append(scratch, prefix...)
	iter := sstIter.iter
	for iter.SeekGE(trimmedStart); ; iter.NextKey() {
		var key storage.MVCCKey
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		key = iter.UnsafeKey()
		if !key.Less(trimmedEnd) {
			break
		}
		value, err := iter.UnsafeValue()
		if err != nil {
			return err
		}
		// The sst sink requires full keys including their prefix, so for every
		// key, we need to prepend the prefix to the key. To avoid unnecessary
		// allocations, we reuse the scratch buffer to build the full key.
		scratch = append(scratch[:len(prefix)], key.Key...)
		key.Key = scratch
		if err := sink.WriteKey(ctx, key, value); err != nil {
			return err
		}
	}
	sink.AssumeNotMidRow()
	return nil
}

func openSSTs(
	ctx context.Context,
	execCtx sql.JobExecContext,
	entry execinfrapb.RestoreSpanEntry,
	encryptionOptions *kvpb.FileEncryptionOptions,
	details jobspb.BackupDetails,
) (mergedSST, error) {
	var dirs []cloud.ExternalStorage
	storeFiles := make([]storageccl.StoreFile, 0, len(entry.Files))
	for idx := 0; idx < len(entry.Files); idx++ {
		file := entry.Files[idx]
		dir, err := execCtx.ExecCfg().DistSQLSrv.ExternalStorage(ctx, file.Dir)
		if err != nil {
			return mergedSST{}, err
		}
		dirs = append(dirs, dir)
		storeFiles = append(storeFiles, storageccl.StoreFile{Store: dir, FilePath: file.Path})
	}
	iterOpts := storage.IterOptions{
		// TODO (kev-cao): Come back and update this to range keys when
		// SSTSinkKeyWriter has been updated to support range keys.
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}
	iter, err := storageccl.ExternalSSTReader(ctx, storeFiles, encryptionOptions, iterOpts)
	if err != nil {
		return mergedSST{}, err
	}
	compactionIter, err := storage.NewBackupCompactionIterator(iter, details.EndTime)
	if err != nil {
		return mergedSST{}, err
	}
	return mergedSST{
		entry: entry,
		iter:  compactionIter,
		cleanup: func() {
			log.VInfof(ctx, 1, "finished with and closing %d files in span %d %v", len(entry.Files), entry.ProgressIdx, entry.Span.String())
			compactionIter.Close()
			for _, dir := range dirs {
				if err := dir.Close(); err != nil {
					log.Warningf(ctx, "close export storage failed: %v", err)
				}
			}
		},
		completeUpTo: details.EndTime,
	}, nil
}

// updateCompactionBackupDetails takes a backup chain (up until the end timestamp)
// and returns a corresponding BackupDetails for the compacted
// backup of backups from the start timestamp to the end timestamp.
func updateCompactionBackupDetails(
	ctx context.Context,
	compactionChain compactionChain,
	initialDetails jobspb.BackupDetails,
	resolvedDest backupdest.ResolvedDestination,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (jobspb.BackupDetails, error) {
	if len(compactionChain.chainToCompact) == 0 {
		return jobspb.BackupDetails{}, errors.New("no backup manifests to compact")
	}
	var encryptionInfo *jobspb.EncryptionInfo
	if encryption != nil {
		var err error
		_, encryptionInfo, err = backupencryption.MakeNewEncryptionOptions(
			ctx, encryption, kmsEnv,
		)
		if err != nil {
			return jobspb.BackupDetails{}, err
		}
	}
	lastBackup := compactionChain.lastBackup()
	allDescs, _, err := backupinfo.LoadSQLDescsFromBackupsAtTime(
		ctx,
		compactionChain.chainToCompact,
		compactionChain.compactedIterFactory,
		compactionChain.end,
	)
	if err != nil {
		return jobspb.BackupDetails{}, err
	}
	allDescsPb := util.Map(allDescs, func(desc catalog.Descriptor) descpb.Descriptor {
		return *desc.DescriptorProto()
	})
	destination := initialDetails.Destination
	destination.Subdir = resolvedDest.ChosenSubdir
	compactedDetails := jobspb.BackupDetails{
		Destination:         destination,
		StartTime:           compactionChain.start,
		EndTime:             compactionChain.end,
		URI:                 resolvedDest.DefaultURI,
		URIsByLocalityKV:    resolvedDest.URIsByLocalityKV,
		EncryptionOptions:   encryption,
		EncryptionInfo:      encryptionInfo,
		CollectionURI:       resolvedDest.CollectionURI,
		ResolvedTargets:     allDescsPb,
		ResolvedCompleteDbs: lastBackup.CompleteDbs,
		FullCluster:         lastBackup.DescriptorCoverage == tree.AllDescriptors,
		Compact:             true,
	}
	return compactedDetails, nil
}

// compactIntroducedSpans takes a compacted backup manifest and the full chain of backups it belongs
// to and computes the introduced spans for the compacted backup.
func compactIntroducedSpans(
	ctx context.Context, manifest backuppb.BackupManifest, chain compactionChain,
) (roachpb.Spans, error) {
	if err := checkCoverage(ctx, manifest.Spans, chain.backupChain); err != nil {
		return roachpb.Spans{}, err
	}
	return filterSpans(
			manifest.Spans,
			chain.backupChain[chain.startIdx-1].Spans,
		),
		nil
}

// resolveBackupSubdir returns the resolved base full backup subdirectory from a
// specified sub-directory. subdir may be a specified path or the string
// "LATEST" to resolve the latest subdirectory.
func resolveBackupSubdir(
	ctx context.Context, p sql.JobExecContext, mainFullBackupURI string, subdir string,
) (string, error) {
	if strings.EqualFold(subdir, backupbase.LatestFileName) {
		latest, err := backupdest.ReadLatestFile(ctx, mainFullBackupURI,
			p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, p.User())
		if err != nil {
			return "", err
		}
		return latest, nil
	}
	return subdir, nil
}

// resolveBackupDirs resolves the sub-directory, base backup directory, and
// incremental backup directories for a backup collection. incrementalURIs may
// be empty if an incremental location is not specified. subdir can be a resolved
// sub-directory or the string "LATEST" to resolve the latest sub-directory.
func resolveBackupDirs(
	ctx context.Context,
	p sql.JobExecContext,
	collectionURIs []string,
	incrementalURIs []string,
	subdir string,
) ([]string, []string, string, error) {
	resolvedSubdir, err := resolveBackupSubdir(ctx, p, collectionURIs[0], subdir)
	if err != nil {
		return nil, nil, "", err
	}
	resolvedBaseDirs, err := backuputils.AppendPaths(collectionURIs[:], resolvedSubdir)
	if err != nil {
		return nil, nil, "", err
	}
	resolvedIncDirs, err := backupdest.ResolveIncrementalsBackupLocation(
		ctx, p.User(), p.ExecCfg(), incrementalURIs, collectionURIs, resolvedSubdir,
	)
	if err != nil {
		return nil, nil, "", err
	}
	return resolvedBaseDirs, resolvedIncDirs, resolvedSubdir, nil
}

// createCompactionManifest creates a new manifest for a compaction job and its
// compacted chain.
func createCompactionManifest(
	ctx context.Context,
	execCtx sql.JobExecContext,
	details jobspb.BackupDetails,
	compactChain compactionChain,
) (*backuppb.BackupManifest, error) {
	var tenantSpans []roachpb.Span
	var tenantInfos []mtinfopb.TenantInfoWithUsage
	if err := execCtx.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		tenantSpans, tenantInfos, err = getTenantInfo(ctx, execCtx.ExecCfg().Codec, txn, details)
		return err
	}); err != nil {
		return nil, err
	}
	// TODO (kev-cao): Will need to update the SSTSinkKeyWriter to support
	// range keys.
	if len(tenantSpans) != 0 || len(tenantInfos) != 0 {
		return nil, errors.New("backup compactions does not yet support range keys")
	}
	m, err := createBackupManifest(
		ctx,
		execCtx.ExecCfg(),
		tenantSpans,
		tenantInfos,
		details,
		compactChain.backupChain,
		compactChain.allIters,
	)
	if err != nil {
		return nil, err
	}
	m.IntroducedSpans, err = compactIntroducedSpans(ctx, m, compactChain)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// getBackupChain fetches the current shortest chain of backups (and its
// associated info) required to restore the to the end time specified in the details.
func getBackupChain(
	ctx context.Context,
	execCtx sql.JobExecContext,
	details jobspb.BackupDetails,
	kmsEnv cloud.KMSEnv,
) (
	[]backuppb.BackupManifest,
	[]jobspb.RestoreDetails_BackupLocalityInfo,
	*jobspb.BackupEncryptionOptions,
	map[int]*backupinfo.IterFactory,
	error,
) {
	dest := details.Destination
	resolvedBaseDirs, resolvedIncDirs, _, err := resolveBackupDirs(
		ctx, execCtx, dest.To, dest.IncrementalStorage, dest.Subdir,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	mkStore := execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI
	baseStores, baseCleanup, err := backupdest.MakeBackupDestinationStores(
		ctx, execCtx.User(), mkStore, resolvedBaseDirs,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer func() {
		if err := baseCleanup(); err != nil {
			log.Warningf(ctx, "failed to cleanup base backup stores: %+v", err)
		}
	}()
	incStores, incCleanup, err := backupdest.MakeBackupDestinationStores(
		ctx, execCtx.User(), mkStore, resolvedIncDirs,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer func() {
		if err := incCleanup(); err != nil {
			log.Warningf(ctx, "failed to cleanup incremental backup stores: %+v", err)
		}
	}()
	encryption, err := backupencryption.GetEncryptionFromBaseStore(
		ctx, baseStores[0], details.EncryptionOptions, kmsEnv,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	mem := execCtx.ExecCfg().RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	_, manifests, localityInfo, memReserved, err := backupdest.ResolveBackupManifests(
		ctx, &mem, baseStores, incStores, mkStore, resolvedBaseDirs,
		resolvedIncDirs, details.EndTime, encryption, kmsEnv,
		execCtx.User(), false,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer func() {
		mem.Shrink(ctx, memReserved)
	}()
	allIters, err := backupinfo.GetBackupManifestIterFactories(
		ctx, execCtx.ExecCfg().DistSQLSrv.ExternalStorage, manifests, encryption, kmsEnv,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return manifests, localityInfo, encryption, allIters, nil
}

// concludeBackupCompaction completes the backup compaction process after the backup has been
// completed by writing the manifest and associated metadata to the backup destination.
//
// TODO (kev-cao): Can move this helper to the backup code at some point.
func concludeBackupCompaction(
	ctx context.Context,
	execCtx sql.JobExecContext,
	store cloud.ExternalStorage,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	backupManifest *backuppb.BackupManifest,
) error {
	backupID := uuid.MakeV4()
	backupManifest.ID = backupID

	if err := backupinfo.WriteBackupManifest(ctx, store, backupbase.BackupManifestName,
		encryption, kmsEnv, backupManifest); err != nil {
		return err
	}
	if backupinfo.WriteMetadataWithExternalSSTsEnabled.Get(&execCtx.ExecCfg().Settings.SV) {
		if err := backupinfo.WriteMetadataWithExternalSSTs(ctx, store, encryption,
			kmsEnv, backupManifest); err != nil {
			return err
		}
	}

	statsTable := getTableStatsForBackup(ctx, execCtx.ExecCfg().TableStatsCache, backupManifest.Descriptors)
	return backupinfo.WriteTableStatistics(ctx, store, encryption, kmsEnv, &statsTable)
}

// processProgress processes progress updates from the bulk processor for a backup and updates
// the associated manifest.
func processProgress(
	ctx context.Context,
	manifest *backuppb.BackupManifest,
	progCh <-chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	// When a processor is done exporting a span, it will send a progress update
	// to progCh.
	for progress := range progCh {
		var progDetails backuppb.BackupManifest_Progress
		if err := types.UnmarshalAny(&progress.ProgressDetails, &progDetails); err != nil {
			log.Errorf(ctx, "unable to unmarshal backup progress details: %+v", err)
			return err
		}
		for _, file := range progDetails.Files {
			manifest.Files = append(manifest.Files, file)
			manifest.EntryCounts.Add(file.EntryCounts)
		}
	}
	return nil
}

func compactionJobDescription(details jobspb.BackupDetails) (string, error) {
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	redactedURIs, err := sanitizeURIList(details.Destination.To)
	if err != nil {
		return "", err
	}
	fmtCtx.WriteString("COMPACT BACKUPS FROM ")
	fmtCtx.WriteString(details.Destination.Subdir)
	fmtCtx.WriteString(" IN ")
	fmtCtx.FormatURIs(redactedURIs)
	fmtCtx.WriteString(" BETWEEN ")
	fmtCtx.WriteString(details.StartTime.String())
	fmtCtx.WriteString(" AND ")
	fmtCtx.WriteString(details.EndTime.String())
	if details.Destination.IncrementalStorage != nil {
		redactedIncURIs, err := sanitizeURIList(details.Destination.IncrementalStorage)
		if err != nil {
			return "", err
		}
		fmtCtx.WriteString("WITH (incremental_location = ")
		fmtCtx.FormatURIs(redactedIncURIs)
		fmtCtx.WriteString(")")
	}
	return fmtCtx.CloseAndGetString(), nil
}

func init() {
	builtins.StartCompactionJob = StartCompactionJob
}
