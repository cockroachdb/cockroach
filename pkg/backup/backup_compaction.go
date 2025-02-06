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
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

// CompactBackups performs a compaction of the backups at the given collection
// URI within the start and end timestamps.
//
// Note that exCtx should be a sql.JobExecContext. Due to import cycles with
// the sql and builtins package, the interface{} type is used.
func CompactBackups(
	ctx context.Context,
	exCtx interface{},
	collectionURI, incrLoc []string,
	fullBackupPath string,
	encryptionOpts jobspb.BackupEncryptionOptions,
	start, end hlc.Timestamp,
) error {
	execCtx, ok := exCtx.(sql.JobExecContext)
	if !ok {
		return errors.New("missing job execution context")
	}
	if len(incrLoc) == 0 {
		var err error
		incrLoc, err = backuputils.AppendPaths(
			collectionURI, backupbase.DefaultIncrementalsSubdir,
		)
		if err != nil {
			return err
		}
	} else if len(collectionURI) != len(incrLoc) {
		return errors.New(
			"incremental locations must contain the same number of locality " +
				"aware URIs as the full backup destination",
		)
	}
	resolvedBaseDirs, resolvedIncDirs, _, err := resolveBackupDirs(
		ctx, execCtx, collectionURI, incrLoc, fullBackupPath,
	)
	if err != nil {
		return err
	}
	mkStore := execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI
	baseStores, baseCleanup, err := backupdest.MakeBackupDestinationStores(
		ctx, execCtx.User(), mkStore, resolvedBaseDirs,
	)
	if err != nil {
		return err
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
		return err
	}
	defer func() {
		if err := incCleanup(); err != nil {
			log.Warningf(ctx, "failed to cleanup incremental backup stores: %+v", err)
		}
	}()
	ioConf := baseStores[0].ExternalIOConf()
	kmsEnv := backupencryption.MakeBackupKMSEnv(
		execCtx.ExecCfg().Settings,
		&ioConf,
		execCtx.ExecCfg().InternalDB,
		execCtx.User(),
	)
	encryption, err := backupencryption.GetEncryptionFromBaseStore(
		ctx, baseStores[0], encryptionOpts, &kmsEnv,
	)
	if err != nil {
		return err
	}
	mem := execCtx.ExecCfg().RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	_, manifests, localityInfo, memReserved, err := backupdest.ResolveBackupManifests(
		ctx, &mem, baseStores, incStores, mkStore, resolvedBaseDirs,
		resolvedIncDirs, end, encryption, &kmsEnv,
		execCtx.User(), false,
	)
	if err != nil {
		return err
	}
	defer func() {
		mem.Shrink(ctx, memReserved)
	}()
	dest := jobspb.BackupDetails_Destination{
		To:                 collectionURI,
		Subdir:             fullBackupPath,
		IncrementalStorage: incrLoc,
		Exists:             true,
	}
	return compactIncrementals(
		ctx, execCtx, dest, start, end, manifests, encryption, &kmsEnv, localityInfo,
	)
}

func compactIncrementals(
	ctx context.Context,
	execCtx sql.JobExecContext,
	dest jobspb.BackupDetails_Destination,
	start, end hlc.Timestamp,
	backupChain []backuppb.BackupManifest,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	localityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
) error {
	// TODO (kev-cao): For now, use a random job ID. Once compaction has been jobified, can come
	// back around and properly assign a job ID.
	jobID := execCtx.ExecCfg().JobRegistry.MakeJobID()
	ctx, span := tracing.ChildSpan(ctx, "backup.compaction")
	defer span.Finish()
	allIters, err := backupinfo.GetBackupManifestIterFactories(
		ctx, execCtx.ExecCfg().DistSQLSrv.ExternalStorage, backupChain, encryption, kmsEnv,
	)
	if err != nil {
		return err
	}
	compactChain, err := newCompactionChain(backupChain, start, end, allIters)
	if err != nil {
		return err
	}
	localityInfo = localityInfo[compactChain.startIdx:compactChain.endIdx]
	chainToCompact := compactChain.chainToCompact
	log.Infof(
		ctx, "beginning compaction of %d backups: %s",
		len(chainToCompact), util.Map(chainToCompact, func(m backuppb.BackupManifest) string {
			return m.ID.String()
		}),
	)
	backupManifest, newDetails, err := prepareCompactedBackupMeta(
		ctx, execCtx, jobID, dest, compactChain, encryption, kmsEnv, allIters,
	)
	if err != nil {
		return err
	}
	if err := backupinfo.WriteBackupManifestCheckpoint(
		ctx, newDetails.URI, encryption, kmsEnv,
		backupManifest, execCtx.ExecCfg(), execCtx.User(),
	); err != nil {
		return err
	}
	backupLocalityMap, err := makeBackupLocalityMap(localityInfo, execCtx.User())
	if err != nil {
		return err
	}

	introducedSpanFrontier, err := createIntroducedSpanFrontier(backupChain, backupManifest.EndTime)
	if err != nil {
		return err
	}
	defer introducedSpanFrontier.Release()

	spanCh := make(chan execinfrapb.RestoreSpanEntry, 1000)
	backupCodec, err := backupinfo.MakeBackupCodec(chainToCompact)
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

	genSpan := func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error {
		defer close(spanCh)
		if err != nil {
			return err
		}
		return errors.Wrap(generateAndSendImportSpans(
			ctx,
			spans,
			chainToCompact,
			compactChain.compactedIterFactory,
			backupLocalityMap,
			filter,
			fsc,
			spanCh,
		), "generate and send import spans")
	}

	store, err := execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, newDetails.URI, execCtx.User())
	if err != nil {
		return err
	}
	defer store.Close()
	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
	var tasks []func(context.Context) error
	tasks = append(tasks, func(ctx context.Context) error {
		return genSpan(ctx, spanCh)
	})
	tasks = append(tasks, func(ctx context.Context) error {
		return runCompaction(ctx, execCtx, encryption, spanCh, newDetails, backupManifest, progCh, store)
	})
	tasks = append(tasks, func(ctx context.Context) error {
		return processProgress(ctx, backupManifest, progCh)
	})

	if err := ctxgroup.GoAndWait(ctx, tasks...); err != nil {
		return err
	}
	return concludeBackupCompaction(ctx, execCtx, store, encryption, kmsEnv, backupManifest)
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

// makeCompactionBackupDetails takes a backup chain (up until the end timestamp)
// and returns a corresponding BackupDetails for the compacted
// backup of backups from the start timestamp to the end timestamp.
func makeCompactionBackupDetails(
	ctx context.Context,
	compactionChain compactionChain,
	dest jobspb.BackupDetails_Destination,
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
			ctx,
			*encryption,
			kmsEnv,
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
	compactedDetails := jobspb.BackupDetails{
		Destination:         dest,
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
	}
	return compactedDetails, nil
}

// compactIntroducedSpans takes a compacted backup manifest and the full chain of backups it belongs
// to and computes the introduced spans for the compacted backup.
func compactIntroducedSpans(
	ctx context.Context, manifest backuppb.BackupManifest, chain compactionChain,
) (roachpb.Spans, error) {
	if err := checkCoverage(ctx, manifest.Spans, chain.restoreChain); err != nil {
		return roachpb.Spans{}, err
	}
	return filterSpans(
			manifest.Spans,
			chain.restoreChain[chain.startIdx-1].Spans,
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

// maybeWriteBackupLock attempts to write a backup lock for the given jobID, if
// it does not already exist. If another backup lock file for another job is
// found, it will return an error.
//
// TODO (kev-cao): At some point should move this helper so it can be ysed by
// the backup code as well.
func maybeWriteBackupLock(
	ctx context.Context,
	execCtx sql.JobExecContext,
	dest backupdest.ResolvedDestination,
	jobID jobspb.JobID,
) error {
	foundLockFile, err := backupinfo.CheckForBackupLock(
		ctx,
		execCtx.ExecCfg(),
		dest.DefaultURI,
		jobID,
		execCtx.User(),
	)
	if err != nil {
		return err
	}
	if foundLockFile {
		return nil
	}
	if err := backupinfo.CheckForPreviousBackup(
		ctx,
		execCtx.ExecCfg(),
		dest.DefaultURI,
		jobID,
		execCtx.User(),
	); err != nil {
		return err
	}
	return backupinfo.WriteBackupLock(
		ctx,
		execCtx.ExecCfg(),
		dest.DefaultURI,
		jobID,
		execCtx.User(),
	)
}

// prepareCompactedBackupMeta prepares the manifest, job details,
// and resolved destination for the compacted backup based on the chain of backups.
func prepareCompactedBackupMeta(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	dest jobspb.BackupDetails_Destination,
	compactionChain compactionChain,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	layerToIterFactory backupinfo.LayerToBackupManifestFileIterFactory,
) (*backuppb.BackupManifest, jobspb.BackupDetails, error) {
	resolvedDest, err := backupdest.ResolveDest(
		ctx,
		execCtx.User(),
		dest,
		// While the end time of this compacted backup matches the end time of
		// the last backup in the chain to compact, when resolving the
		// destination we need to adjust the end time to ensure that the backup
		// location doesn't clobber the last backup in the chain. We do this by
		// adding a small duration (large enough to change the backup path)
		// to the end time.
		compactionChain.end.AddDuration(10*time.Millisecond),
		execCtx.ExecCfg(),
	)
	if err != nil {
		return nil, jobspb.BackupDetails{}, err
	}
	details, err := makeCompactionBackupDetails(
		ctx, compactionChain, dest, resolvedDest, encryption, kmsEnv,
	)
	if err != nil {
		return nil, jobspb.BackupDetails{}, err
	}
	if err = maybeWriteBackupLock(ctx, execCtx, resolvedDest, jobID); err != nil {
		return nil, jobspb.BackupDetails{}, err
	}

	var tenantSpans []roachpb.Span
	var tenantInfos []mtinfopb.TenantInfoWithUsage
	insqlDB := execCtx.ExecCfg().InternalDB
	if err = insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		tenantSpans, tenantInfos, err = getTenantInfo(ctx, execCtx.ExecCfg().Codec, txn, details)
		return err
	}); err != nil {
		return nil, jobspb.BackupDetails{}, err
	}
	// TODO (kev-cao): Will need to update the SSTSinkKeyWriter to support
	// range keys.
	if len(tenantSpans) != 0 || len(tenantInfos) != 0 {
		return nil, jobspb.BackupDetails{}, errors.New("backup compactions does not yet support range keys")
	}
	m, err := createBackupManifest(
		ctx,
		execCtx.ExecCfg(),
		tenantSpans,
		tenantInfos,
		details,
		compactionChain.restoreChain,
		layerToIterFactory,
	)
	if err != nil {
		return nil, jobspb.BackupDetails{}, err
	}
	manifest := &m
	manifest.IntroducedSpans, err = compactIntroducedSpans(ctx, *manifest, compactionChain)
	return manifest, details, err
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

type compactionChain struct {
	// restoreChain is the linear chain of backups up to the end time required
	// for a restore.
	restoreChain   []backuppb.BackupManifest
	chainToCompact []backuppb.BackupManifest
	start, end     hlc.Timestamp
	// Inclusive startIdx and exclusive endIdx of the sub-chain to compact.
	startIdx, endIdx int
	// Iter factory for just the backups in the chain to compact.
	compactedIterFactory backupinfo.LayerToBackupManifestFileIterFactory
}

// lastBackup returns the last backup of the chain to compact.
func (c *compactionChain) lastBackup() backuppb.BackupManifest {
	return c.restoreChain[c.endIdx-1]
}

// newCompactionChain returns a new compacted backup chain based on the specified start and end
// timestamps from a chain of backups. The start and end times must specify specific backups.
func newCompactionChain(
	manifests []backuppb.BackupManifest,
	start, end hlc.Timestamp,
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
		restoreChain:         manifests,
		chainToCompact:       manifests[startIdx:endIdx],
		startIdx:             startIdx,
		endIdx:               endIdx,
		start:                start,
		end:                  end,
		compactedIterFactory: compactedIters,
	}, nil
}

func init() {
	builtins.CompactBackups = CompactBackups
}
