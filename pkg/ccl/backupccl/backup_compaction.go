// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package backupccl

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupdest"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

func maybeCompactIncrementals(
	ctx context.Context,
	p sql.JobExecContext,
	lastIncDetails jobspb.BackupDetails,
	jobID jobspb.JobID,
) error {
	if !lastIncDetails.Destination.Exists {
		return nil
	}
	fullyResolvedBaseDirs, fullyResolvedIncDirs, _, err := backupdest.ResolveBackupDirs(
		ctx, p, lastIncDetails.Destination.To, lastIncDetails.Destination.IncrementalStorage, lastIncDetails.Destination.Subdir,
	)
	mkStore := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI
	baseStores, baseCleanupFn, err := backupdest.MakeBackupDestinationStores(ctx, p.User(), mkStore, fullyResolvedBaseDirs)
	if err != nil {
		return err
	}
	defer func() {
		if err := baseCleanupFn(); err != nil {
			log.Warningf(ctx, "failed to cleanup base stores: %v", err)
		}
	}()
	incStores, incCleanupFn, err := backupdest.MakeBackupDestinationStores(ctx, p.User(), mkStore, fullyResolvedIncDirs)
	if err != nil {
		return err
	}
	defer func() {
		if err := incCleanupFn(); err != nil {
			log.Warningf(ctx, "failed to cleanup inc stores: %v", err)
		}
	}()

	ioConf := baseStores[0].ExternalIOConf()
	kmsEnv := backupencryption.MakeBackupKMSEnv(p.ExecCfg().Settings, &ioConf, p.ExecCfg().InternalDB, p.User())
	encryption, err := backupencryption.GetEncryptionFromBaseStore(
		ctx, baseStores[0], *lastIncDetails.EncryptionOptions, &kmsEnv,
	)
	if err != nil {
		return err
	}

	mem := p.ExecCfg().RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	_, manifests, localityInfo, memReserved, err := backupdest.ResolveBackupManifests(
		ctx, &mem, baseStores, incStores, mkStore, fullyResolvedBaseDirs, fullyResolvedIncDirs, lastIncDetails.EndTime,
		encryption, &kmsEnv, p.User(), false,
	)
	if err != nil {
		return err
	}

	defer func() {
		mem.Shrink(ctx, memReserved)
	}()

	if len(manifests) <= 3 {
		return nil
	}
	return compactIncrementals(ctx, p, lastIncDetails, jobID, manifests, encryption, &kmsEnv, localityInfo)
}

func compactIncrementals(
	ctx context.Context,
	p sql.JobExecContext,
	oldDetails jobspb.BackupDetails,
	jobID jobspb.JobID,
	backupChain []backuppb.BackupManifest,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	localityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
) error {
	newDetails := makeCompactionBackupDetails(oldDetails, backupChain[1:])
	dest, err := backupdest.ResolveDest(
		ctx,
		p.User(),
		newDetails.Destination,
		newDetails.EndTime.AddDuration(10*time.Millisecond), /* Incremented to avoid clashing directory paths with last backup */
		p.ExecCfg(),
	)
	if err != nil {
		return err
	}
	foundLockFile, err := backupinfo.CheckForBackupLock(ctx, p.ExecCfg(), dest.DefaultURI, jobID, p.User())
	if err != nil {
		return err
	}

	if !foundLockFile {
		if err := backupinfo.CheckForPreviousBackup(ctx, p.ExecCfg(), dest.DefaultURI, jobID, p.User()); err != nil {
			return err
		}
		if err := backupinfo.WriteBackupLock(ctx, p.ExecCfg(), dest.DefaultURI, jobID, p.User()); err != nil {
			return err
		}
	}

	var backupManifest *backuppb.BackupManifest
	insqlDB := p.ExecCfg().InternalDB
	if err := insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		details, m, err := getBackupDetailAndManifest(
			ctx, p.ExecCfg(), txn, newDetails, p.User(), dest,
		)
		if err != nil {
			return err
		}
		details.StartTime, details.EndTime = newDetails.StartTime, newDetails.EndTime
		m.StartTime, m.EndTime = newDetails.StartTime, newDetails.EndTime
		newDetails = details
		backupManifest = &m
		return nil
	}); err != nil {
		return err
	}
	if err := compactIntroducedSpans(ctx, backupManifest, backupChain); err != nil {
		return err
	}

	if err := backupinfo.WriteBackupManifestCheckpoint(
		ctx, dest.DefaultURI, newDetails.EncryptionOptions, kmsEnv, backupManifest, p.ExecCfg(), p.User(),
	); err != nil {
		return err
	}

	layerToIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx, p.ExecCfg().DistSQLSrv.ExternalStorage, backupChain[1:], encryption, kmsEnv)
	backupLocalityMap, err := makeBackupLocalityMap(localityInfo, p.User())
	if err != nil {
		return err
	}

	introducedSpanFrontier, err := createIntroducedSpanFrontier(backupChain[1:], backupManifest.EndTime)
	if err != nil {
		return err
	}
	defer introducedSpanFrontier.Release()

	targetSize := targetRestoreSpanSize.Get(&p.ExecCfg().Settings.SV)
	maxFileCount := maxFileCount.Get(&p.ExecCfg().Settings.SV)

	var fsc fileSpanComparator = &exclusiveEndKeyComparator{}
	filter, err := makeSpanCoveringFilter(
		backupManifest.Spans,
		[]jobspb.RestoreProgress_FrontierEntry{},
		introducedSpanFrontier,
		targetSize,
		maxFileCount,
	)

	spanCh := make(chan execinfrapb.RestoreSpanEntry, 1000)
	genSpan := func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error {
		defer close(spanCh)
		backupCodec, err := backupinfo.MakeBackupCodec(backupChain[1:])
		if err != nil {
			return err
		}
		catalogDescs := make([]catalog.Descriptor, 0, len(backupManifest.Descriptors))
		for _, desc := range backupManifest.Descriptors {
			catalogDescs = append(catalogDescs, backupinfo.NewDescriptorForManifest(&desc))
		}
		var tables []catalog.TableDescriptor
		for _, desc := range catalogDescs {
			if table, ok := desc.(catalog.TableDescriptor); ok {
				tables = append(tables, table)
			}
		}
		spans, err := spansForAllRestoreTableIndexes(
			backupCodec,
			tables,
			nil,
			false,
			false,
		)
		if err != nil {
			return err
		}
		return errors.Wrap(generateAndSendImportSpans(
			ctx,
			spans,
			backupChain[1:],
			layerToIterFactory,
			backupLocalityMap,
			filter,
			fsc,
			spanCh,
		), "generate and send import spans")
	}

	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
	processProg := func(
		ctx context.Context,
		progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
		backupManifest *backuppb.BackupManifest,
	) {
		// When a processor is done exporting a span, it will send a progress update
		// to progCh.
		var numBackedUpFiles int64
		for progress := range progCh {
			var progDetails backuppb.BackupManifest_Progress
			if err := types.UnmarshalAny(&progress.ProgressDetails, &progDetails); err != nil {
				log.Errorf(ctx, "unable to unmarshal backup progress details: %+v", err)
			}
			if backupManifest.RevisionStartTime.Less(progDetails.RevStartTime) {
				backupManifest.RevisionStartTime = progDetails.RevStartTime
			}
			for _, file := range progDetails.Files {
				backupManifest.Files = append(backupManifest.Files, file)
				backupManifest.EntryCounts.Add(file.EntryCounts)
				numBackedUpFiles++
			}

			// Update the per-component progress maintained by the job profiler.
			perComponentProgress := make(map[execinfrapb.ComponentID]float32)
			component := execinfrapb.ComponentID{
				SQLInstanceID: progress.NodeID,
				FlowID:        progress.FlowID,
				Type:          execinfrapb.ComponentID_PROCESSOR,
			}
			for processorID, fraction := range progress.CompletedFraction {
				component.ID = processorID
				perComponentProgress[component] = fraction
			}
		}
	}

	store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, dest.DefaultURI, p.User())
	if err != nil {
		return err
	}
	defer store.Close()
	var tasks []func(context.Context) error
	tasks = append(tasks,
		func(ctx context.Context) error {
			return genSpan(ctx, spanCh)
		})
	tasks = append(tasks, func(ctx context.Context) error {
		return runCompaction(ctx, p, encryption, spanCh, newDetails, backupManifest, progCh, store)
	})
	tasks = append(tasks, func(ctx context.Context) error {
		processProg(ctx, progCh, backupManifest)
		return nil
	})

	if err := ctxgroup.GoAndWait(ctx, tasks...); err != nil {
		return err
	}
	backupID := uuid.MakeV4()
	backupManifest.ID = backupID

	if err := backupinfo.WriteBackupManifest(ctx, store, backupbase.BackupManifestName,
		encryption, kmsEnv, backupManifest); err != nil {
		return err
	}
	if backupinfo.WriteMetadataWithExternalSSTsEnabled.Get(&p.ExecCfg().Settings.SV) {
		if err := backupinfo.WriteMetadataWithExternalSSTs(ctx, store, encryption,
			kmsEnv, backupManifest); err != nil {
			return err
		}
	}

	statsTable := getTableStatsForBackup(ctx, p.ExecCfg().TableStatsCache, backupManifest.Descriptors)
	if err := backupinfo.WriteTableStatistics(ctx, store, encryption, kmsEnv, &statsTable); err != nil {
		return err
	}

	if backupinfo.WriteMetadataSST.Get(&p.ExecCfg().Settings.SV) {
		if err := backupinfo.WriteBackupMetadataSST(ctx, store, encryption, kmsEnv, backupManifest,
			statsTable.Statistics); err != nil {
			err = errors.Wrap(err, "writing forward-compat metadata sst")
			if !build.IsRelease() {
				return err
			}
			log.Warningf(ctx, "%+v", err)
		}
	}
	return nil
}

func runCompaction(
	ctx context.Context,
	p sql.JobExecContext,
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
	sinkConf := sstSinkConf{
		id:       p.ExecCfg().DistSQLSrv.NodeID.SQLInstanceID(),
		enc:      encryptionOptions,
		progCh:   progCh,
		settings: &p.ExecCfg().Settings.SV,
	}
	sink := makeFileSSTSink(sinkConf, store, nil)
	sink.elideMode = manifest.ElidedPrefix
	defer func() {
		if err := sink.flush(ctx); err != nil {
			log.Warningf(ctx, "failed to flush sink: %v", err)
		}
		logClose(ctx, sink, "SST sink")
	}()
	for {
		select {
		case entry, ok := <-entries:
			if !ok {
				return nil
			}

			sstIter, err := openSSTs(ctx, p, entry, encryption, details)
			if err != nil {
				return errors.Wrap(err, "opening SSTs")
			}

			if err := processSpanEntry(ctx, sstIter, sink, manifest); err != nil {
				return errors.Wrap(err, "processing span entry")
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func processSpanEntry(
	ctx context.Context, sstIter mergedSST, sink *fileSSTSink, manifest *backuppb.BackupManifest,
) error {
	defer sstIter.cleanup()
	entry := sstIter.entry
	startKeyMVCC, endKeyMVCC := storage.MVCCKey{Key: entry.Span.Key}, storage.MVCCKey{Key: entry.Span.EndKey}
	prefix, err := elidedPrefix(entry.Span.Key, entry.ElidedPrefix)
	if err != nil {
		return err
	}
	if prefix != nil {
		startKeyMVCC.Key = bytes.TrimPrefix(startKeyMVCC.Key, prefix)
		endKeyMVCC.Key = bytes.TrimPrefix(endKeyMVCC.Key, prefix)
	}
	iter := sstIter.iter
	for iter.SeekGE(startKeyMVCC); ; iter.NextKey() {
		ok, err := iter.Valid()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		key := iter.UnsafeKey()
		if !key.Less(endKeyMVCC) {
			break
		}
		uv, err := iter.UnsafeValue()
		v, err := storage.DecodeMVCCValueAndErr(uv, err)
		if err != nil {
			return err
		}
		if err := sink.writeKey(ctx, prefix, key, v, manifest.StartTime, manifest.EndTime); err != nil {
			return err
		}
	}
	return sink.flushFile(ctx)
}

func openSSTs(
	ctx context.Context,
	p sql.JobExecContext,
	entry execinfrapb.RestoreSpanEntry,
	encryption *jobspb.BackupEncryptionOptions,
	details jobspb.BackupDetails,
) (mergedSST, error) {
	var dirs []cloud.ExternalStorage

	defer func() {
		for _, dir := range dirs {
			if err := dir.Close(); err != nil {
				log.Warningf(ctx, "close export storage failed: %v", err)
			}
		}
	}()

	getIter := func(iter storage.SimpleMVCCIterator, dirsToSend []cloud.ExternalStorage, completeUpTo hlc.Timestamp) (mergedSST, error) {
		readCompleteAsOfIter := storage.NewReadCompleteAsOfIterator(iter, completeUpTo)
		cleanup := func() {
			log.VInfof(ctx, 1, "finished with and closing %d files in span %d [%s-%s)", len(entry.Files), entry.ProgressIdx, entry.Span.Key, entry.Span.EndKey)
			readCompleteAsOfIter.Close()

			for _, dir := range dirsToSend {
				if err := dir.Close(); err != nil {
					log.Warningf(ctx, "close export storage failed %v", err)
				}
			}
		}

		mSST := mergedSST{
			entry:        entry,
			iter:         readCompleteAsOfIter,
			cleanup:      cleanup,
			completeUpTo: completeUpTo,
		}

		return mSST, nil
	}

	storeFiles := make([]storageccl.StoreFile, 0, len(entry.Files))
	idx := 0
	for ; idx < len(entry.Files); idx++ {
		file := entry.Files[idx]
		dir, err := p.ExecCfg().DistSQLSrv.ExternalStorage(ctx, file.Dir)
		if err != nil {
			return mergedSST{}, err
		}
		dirs = append(dirs, dir)
		storeFiles = append(storeFiles, storageccl.StoreFile{Store: dir, FilePath: file.Path})
	}
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}
	var encryptionOptions *kvpb.FileEncryptionOptions
	if encryption != nil {
		encryptionOptions = &kvpb.FileEncryptionOptions{Key: encryption.Key}
	}
	iter, err := storageccl.ExternalSSTReader(ctx, storeFiles, encryptionOptions, iterOpts)
	if err != nil {
		return mergedSST{}, err
	}

	return getIter(iter, dirs, details.EndTime)
}

// makeCompactionBackupDetails takes the details of the last backup of the chain it is compacting and
// returns a corresponding BackupDetails for the compacted backup. It is assumed there exists at least one
// incremental backup in the manifests.
func makeCompactionBackupDetails(
	details jobspb.BackupDetails, manifests []backuppb.BackupManifest,
) jobspb.BackupDetails {
	details.StartTime = manifests[0].StartTime
	details.EndTime = manifests[len(manifests)-1].EndTime
	return details
}

// compactIntroducedSpans takes a compacted backup manifest and the full chain of backups it belongs to and
// recomputes the introduced spans for the compacted backup. It mutates the passed in manifest.
func compactIntroducedSpans(
	ctx context.Context, manifest *backuppb.BackupManifest, backupChain []backuppb.BackupManifest,
) error {
	if err := checkCoverage(ctx, manifest.Spans, backupChain); err != nil {
		return err
	}
	manifest.IntroducedSpans = filterSpans(manifest.Spans, backupChain[0].Spans)
	return nil
}
