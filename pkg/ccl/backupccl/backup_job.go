// Copyright 2016 The Cockroach Authors.
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
	"net/url"
	"path"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupdest"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuputils"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

// BackupCheckpointInterval is the interval at which backup progress is saved
// to durable storage.
var BackupCheckpointInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"bulkio.backup.checkpoint_interval",
	"the minimum time between writing progress checkpoints during a backup",
	time.Minute)

var forceReadBackupManifest = util.ConstantWithMetamorphicTestBool("backup-read-manifest", false)

func countRows(raw roachpb.BulkOpSummary, pkIDs map[uint64]bool) roachpb.RowCount {
	res := roachpb.RowCount{DataSize: raw.DataSize}
	for id, count := range raw.EntryCounts {
		if _, ok := pkIDs[id]; ok {
			res.Rows += count
		} else {
			res.IndexEntries += count
		}
	}
	return res
}

// filterSpans returns the spans that represent the set difference
// (includes - excludes).
func filterSpans(includes []roachpb.Span, excludes []roachpb.Span) []roachpb.Span {
	var cov roachpb.SpanGroup
	cov.Add(includes...)
	cov.Sub(excludes...)
	return cov.Slice()
}

// clusterNodeCount returns the approximate number of nodes in the cluster.
func clusterNodeCount(gw gossip.OptionalGossip) (int, error) {
	g, err := gw.OptionalErr(47970)
	if err != nil {
		return 0, err
	}
	var nodes int
	err = g.IterateInfos(
		gossip.KeyNodeDescPrefix, func(_ string, _ gossip.Info) error {
			nodes++
			return nil
		},
	)
	if err != nil {
		return 0, err
	}
	// If we somehow got 0 and return it, a caller may panic if they divide by
	// such a nonsensical nodecount.
	if nodes == 0 {
		return 1, errors.New("failed to count nodes")
	}
	return nodes, nil
}

// backup exports a snapshot of every kv entry into ranged sstables.
//
// The output is an sstable per range with files in the following locations:
//   - <dir>/<unique_int>.sst
//   - <dir> is given by the user and may be cloud storage
//   - Each file contains data for a key range that doesn't overlap with any other
//     file.
func backup(
	ctx context.Context,
	execCtx sql.JobExecContext,
	defaultURI string,
	urisByLocalityKV map[string]string,
	settings *cluster.Settings,
	defaultStore cloud.ExternalStorage,
	storageByLocalityKV map[string]*cloudpb.ExternalStorage,
	job *jobs.Job,
	backupManifest *backuppb.BackupManifest,
	makeExternalStorage cloud.ExternalStorageFactory,
	encryption *jobspb.BackupEncryptionOptions,
	statsCache *stats.TableStatisticsCache,
) (roachpb.RowCount, error) {
	resumerSpan := tracing.SpanFromContext(ctx)
	var lastCheckpoint time.Time

	var completedSpans, completedIntroducedSpans []roachpb.Span
	kmsEnv := backupencryption.MakeBackupKMSEnv(
		execCtx.ExecCfg().Settings,
		&execCtx.ExecCfg().ExternalIODirConfig,
		execCtx.ExecCfg().InternalDB,
		execCtx.User(),
	)
	// TODO(benesch): verify these files, rather than accepting them as truth
	// blindly.
	// No concurrency yet, so these assignments are safe.
	it, err := makeBackupManifestFileIterator(ctx, execCtx.ExecCfg().DistSQLSrv.ExternalStorage,
		*backupManifest, encryption, &kmsEnv)
	if err != nil {
		return roachpb.RowCount{}, err
	}
	defer it.close()
	for f, hasNext := it.next(); hasNext; f, hasNext = it.next() {
		if f.StartTime.IsEmpty() && !f.EndTime.IsEmpty() {
			completedIntroducedSpans = append(completedIntroducedSpans, f.Span)
		} else {
			completedSpans = append(completedSpans, f.Span)
		}
	}
	if it.err() != nil {
		return roachpb.RowCount{}, it.err()
	}

	// Subtract out any completed spans.
	spans := filterSpans(backupManifest.Spans, completedSpans)
	introducedSpans := filterSpans(backupManifest.IntroducedSpans, completedIntroducedSpans)

	pkIDs := make(map[uint64]bool)
	for i := range backupManifest.Descriptors {
		if t, _, _, _, _ := descpb.GetDescriptors(&backupManifest.Descriptors[i]); t != nil {
			pkIDs[roachpb.BulkOpSummaryID(uint64(t.ID), uint64(t.PrimaryIndex.ID))] = true
		}
	}

	evalCtx := execCtx.ExtendedEvalContext()
	dsp := execCtx.DistSQLPlanner()

	// We don't return the compatible nodes here since PartitionSpans will
	// filter out incompatible nodes.
	planCtx, _, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCtx.ExecCfg())
	if err != nil {
		return roachpb.RowCount{}, errors.Wrap(err, "failed to determine nodes on which to run")
	}

	backupSpecs, err := distBackupPlanSpecs(
		ctx,
		planCtx,
		execCtx,
		dsp,
		int64(job.ID()),
		spans,
		introducedSpans,
		pkIDs,
		defaultURI,
		urisByLocalityKV,
		encryption,
		&kmsEnv,
		roachpb.MVCCFilter(backupManifest.MVCCFilter),
		backupManifest.StartTime,
		backupManifest.EndTime,
	)
	if err != nil {
		return roachpb.RowCount{}, err
	}

	numTotalSpans := 0
	for _, spec := range backupSpecs {
		numTotalSpans += len(spec.IntroducedSpans) + len(spec.Spans)
	}

	progressLogger := jobs.NewChunkProgressLogger(job, numTotalSpans, job.FractionCompleted(), jobs.ProgressUpdateOnly)

	requestFinishedCh := make(chan struct{}, numTotalSpans) // enough buffer to never block
	var jobProgressLoop func(ctx context.Context) error
	if numTotalSpans > 0 {
		jobProgressLoop = func(ctx context.Context) error {
			// Currently the granularity of backup progress is the % of spans
			// exported. Would improve accuracy if we tracked the actual size of each
			// file.
			return progressLogger.Loop(ctx, requestFinishedCh)
		}
	}

	progCh := make(chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
	checkpointLoop := func(ctx context.Context) error {
		// When a processor is done exporting a span, it will send a progress update
		// to progCh.
		defer close(requestFinishedCh)
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

			// Signal that an ExportRequest finished to update job progress.
			for i := int32(0); i < progDetails.CompletedSpans; i++ {
				requestFinishedCh <- struct{}{}
			}

			interval := BackupCheckpointInterval.Get(&execCtx.ExecCfg().Settings.SV)
			if timeutil.Since(lastCheckpoint) > interval {
				resumerSpan.RecordStructured(&backuppb.BackupProgressTraceEvent{
					TotalNumFiles:     numBackedUpFiles,
					TotalEntryCounts:  backupManifest.EntryCounts,
					RevisionStartTime: backupManifest.RevisionStartTime,
				})

				err := backupinfo.WriteBackupManifestCheckpoint(
					ctx, defaultURI, encryption, &kmsEnv, backupManifest, execCtx.ExecCfg(), execCtx.User(),
				)
				if err != nil {
					log.Errorf(ctx, "unable to checkpoint backup descriptor: %+v", err)
				}
				lastCheckpoint = timeutil.Now()
				if execCtx.ExecCfg().TestingKnobs.AfterBackupCheckpoint != nil {
					execCtx.ExecCfg().TestingKnobs.AfterBackupCheckpoint()
				}
			}
		}
		return nil
	}

	resumerSpan.RecordStructured(&types.StringValue{Value: "starting DistSQL backup execution"})
	runBackup := func(ctx context.Context) error {
		return distBackup(
			ctx,
			execCtx,
			planCtx,
			dsp,
			progCh,
			backupSpecs,
		)
	}

	if err := ctxgroup.GoAndWait(ctx, jobProgressLoop, checkpointLoop, runBackup); err != nil {
		return roachpb.RowCount{}, errors.Wrapf(err, "exporting %d ranges", errors.Safe(numTotalSpans))
	}

	backupID := uuid.MakeV4()
	backupManifest.ID = backupID
	// Write additional partial descriptors to each node for partitioned backups.
	if len(storageByLocalityKV) > 0 {
		resumerSpan.RecordStructured(&types.StringValue{Value: "writing partition descriptors for partitioned backup"})
		filesByLocalityKV := make(map[string][]backuppb.BackupManifest_File)
		for _, file := range backupManifest.Files {
			filesByLocalityKV[file.LocalityKV] = append(filesByLocalityKV[file.LocalityKV], file)
		}

		nextPartitionedDescFilenameID := 1
		for kv, conf := range storageByLocalityKV {
			backupManifest.LocalityKVs = append(backupManifest.LocalityKVs, kv)
			// Set a unique filename for each partition backup descriptor. The ID
			// ensures uniqueness, and the kv string appended to the end is for
			// readability.
			filename := fmt.Sprintf("%s_%d_%s", backupPartitionDescriptorPrefix,
				nextPartitionedDescFilenameID, backupinfo.SanitizeLocalityKV(kv))
			nextPartitionedDescFilenameID++
			backupManifest.PartitionDescriptorFilenames = append(backupManifest.PartitionDescriptorFilenames, filename)
			desc := backuppb.BackupPartitionDescriptor{
				LocalityKV: kv,
				Files:      filesByLocalityKV[kv],
				BackupID:   backupID,
			}

			if err := func() error {
				store, err := makeExternalStorage(ctx, *conf)
				if err != nil {
					return err
				}
				defer store.Close()
				return backupinfo.WriteBackupPartitionDescriptor(ctx, store, filename,
					encryption, &kmsEnv, &desc)
			}(); err != nil {
				return roachpb.RowCount{}, err
			}
		}
	}

	// Write a `BACKUP_MANIFEST` file to support backups in mixed-version clusters
	// with 22.2 nodes.
	//
	// TODO(adityamaru): We can stop writing `BACKUP_MANIFEST` in 23.2
	// because a mixed-version cluster with 23.1 nodes will read the
	// `BACKUP_METADATA` instead.
	if err := backupinfo.WriteBackupManifest(ctx, defaultStore, backupbase.BackupManifestName,
		encryption, &kmsEnv, backupManifest); err != nil {
		return roachpb.RowCount{}, err
	}

	// Write a `BACKUP_METADATA` file along with SSTs for all the alloc heavy
	// fields elided from the `BACKUP_MANIFEST`.
	//
	// TODO(adityamaru,rhu713): Once backup/restore switches from writing and
	// reading backup manifests to `metadata.sst` we can stop writing the slim
	// manifest.
	if err := backupinfo.WriteFilesListMetadataWithSSTs(ctx, defaultStore, encryption,
		&kmsEnv, backupManifest); err != nil {
		return roachpb.RowCount{}, err
	}

	statsTable := getTableStatsForBackup(ctx, statsCache, backupManifest.Descriptors)
	if err := backupinfo.WriteTableStatistics(ctx, defaultStore, encryption, &kmsEnv, &statsTable); err != nil {
		return roachpb.RowCount{}, err
	}

	if backupinfo.WriteMetadataSST.Get(&settings.SV) {
		if err := backupinfo.WriteBackupMetadataSST(ctx, defaultStore, encryption, &kmsEnv, backupManifest,
			statsTable.Statistics); err != nil {
			err = errors.Wrap(err, "writing forward-compat metadata sst")
			if !build.IsRelease() {
				return roachpb.RowCount{}, err
			}
			log.Warningf(ctx, "%+v", err)
		}
	}

	return backupManifest.EntryCounts, nil
}

func releaseProtectedTimestamp(
	ctx context.Context, pts protectedts.Storage, ptsID *uuid.UUID,
) error {
	// If the job doesn't have a protected timestamp then there's nothing to do.
	if ptsID == nil {
		return nil
	}
	err := pts.Release(ctx, *ptsID)
	if errors.Is(err, protectedts.ErrNotExists) {
		// No reason to return an error which might cause problems if it doesn't
		// seem to exist.
		log.Warningf(ctx, "failed to release protected which seems not to exist: %v", err)
		err = nil
	}
	return err
}

// getTableStatsForBackup collects all stats for tables found in descs.
//
// We do not fail if we can't retrieve statistiscs. Successfully
// backed up data is more valuable than table stats that can be
// recomputed after restore. The lack of stats on restore could lead
// to suboptimal performance when reading/writing to this table until
// the stats have been recomputed.
func getTableStatsForBackup(
	ctx context.Context, statsCache *stats.TableStatisticsCache, descs []descpb.Descriptor,
) backuppb.StatsTable {
	var tableStatistics []*stats.TableStatisticProto
	for i := range descs {
		if tbl, _, _, _, _ := descpb.GetDescriptors(&descs[i]); tbl != nil {
			tableDesc := tabledesc.NewBuilder(tbl).BuildImmutableTable()
			tableStatisticsAcc, err := statsCache.GetTableStats(ctx, tableDesc)
			if err != nil {
				log.Warningf(ctx, "failed to collect stats for table: %s, "+
					"table ID: %d during a backup: %s", tableDesc.GetName(), tableDesc.GetID(),
					err.Error())
				continue
			}

			for _, stat := range tableStatisticsAcc {
				if statShouldBeIncludedInBackupRestore(&stat.TableStatisticProto) {
					tableStatistics = append(tableStatistics, &stat.TableStatisticProto)
				}
			}
		}
	}
	return backuppb.StatsTable{
		Statistics: tableStatistics,
	}
}

func statShouldBeIncludedInBackupRestore(stat *stats.TableStatisticProto) bool {
	// Forecasts and merged stats are computed from the persisted
	// stats on demand and do not need to be backed up or
	// restored.
	return stat.Name != jobspb.ForecastStatsName && stat.Name != jobspb.MergedStatsName
}

type backupResumer struct {
	job         *jobs.Job
	backupStats roachpb.RowCount

	testingKnobs struct {
		ignoreProtectedTimestamps bool
	}
}

var _ jobs.TraceableJob = &backupResumer{}

// ForceRealSpan implements the TraceableJob interface.
func (b *backupResumer) ForceRealSpan() bool {
	return true
}

// Resume is part of the jobs.Resumer interface.
func (b *backupResumer) Resume(ctx context.Context, execCtx interface{}) error {
	// The span is finished by the registry executing the job.
	details := b.job.Details().(jobspb.BackupDetails)
	p := execCtx.(sql.JobExecContext)
	kmsEnv := backupencryption.MakeBackupKMSEnv(
		p.ExecCfg().Settings,
		&p.ExecCfg().ExternalIODirConfig,
		p.ExecCfg().InternalDB,
		p.User(),
	)

	// Resolve the backup destination. We can skip this step if we
	// have already resolved and persisted the destination either
	// during a previous resumption of this job.
	defaultURI := details.URI
	var backupDest backupdest.ResolvedDestination
	if details.URI == "" {
		var err error
		backupDest, err = backupdest.ResolveDest(ctx, p.User(), details.Destination, details.EndTime,
			details.IncrementalFrom, p.ExecCfg())
		if err != nil {
			return err
		}
		defaultURI = backupDest.DefaultURI
	}

	// The backup job needs to lay claim to the bucket it is writing to, to
	// prevent concurrent backups from writing to the same location.
	//
	// If we have already locked the location, either on a previous resume of the
	// job or during planning because `clusterversion.BackupResolutionInJob` isn't
	// active, we do not want to lock it again.
	foundLockFile, err := backupinfo.CheckForBackupLock(ctx, p.ExecCfg(), defaultURI, b.job.ID(), p.User())
	if err != nil {
		return err
	}

	// TODO(ssd): If we restricted how old a resumed job could be,
	// we could remove the check for details.URI == "". This is
	// present to guard against the case where we have already
	// written a BACKUP-LOCK file during planning and do not want
	// to re-check and re-write the lock file. In that case
	// `details.URI` will non-empty.
	if details.URI == "" && !foundLockFile {
		if err := backupinfo.CheckForPreviousBackup(ctx, p.ExecCfg(), backupDest.DefaultURI, b.job.ID(),
			p.User()); err != nil {
			return err
		}

		if err := p.ExecCfg().JobRegistry.CheckPausepoint("backup.before.write_lock"); err != nil {
			return err
		}

		if err := backupinfo.WriteBackupLock(ctx, p.ExecCfg(), backupDest.DefaultURI,
			b.job.ID(), p.User()); err != nil {
			return err
		}

		if err := p.ExecCfg().JobRegistry.CheckPausepoint("backup.after.write_lock"); err != nil {
			return err
		}
	}

	var backupManifest *backuppb.BackupManifest

	// Populate the BackupDetails with the resolved backup
	// destination, and construct the BackupManifest to be written
	// to external storage as a BACKUP-CHECKPOINT. We can skip
	// this step if the job has already persisted the resolved
	// details and manifest in a prior resumption.
	//
	// TODO(adityamaru: Break this code block into helper methods.
	insqlDB := p.ExecCfg().InternalDB
	if details.URI == "" {
		initialDetails := details
		if err := insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			backupDetails, m, err := getBackupDetailAndManifest(
				ctx, p.ExecCfg(), txn, details, p.User(), backupDest,
			)
			if err != nil {
				return err
			}
			details = backupDetails
			backupManifest = &m
			return nil
		}); err != nil {
			return err
		}

		// Now that we have resolved the details, and manifest, write a protected
		// timestamp record on the backup's target spans/schema object.
		//
		// This closure updates `details` to store the protected timestamp records
		// UUID so that it can be released on job completion. The updated details
		// are persisted in the job record further down.
		{
			protectedtsID := uuid.MakeV4()
			details.ProtectedTimestampRecord = &protectedtsID

			if details.ProtectedTimestampRecord != nil {
				if err := insqlDB.Txn(ctx, func(
					ctx context.Context, txn isql.Txn,
				) error {
					ptp := p.ExecCfg().ProtectedTimestampProvider.WithTxn(txn)
					return protectTimestampForBackup(
						ctx, b.job.ID(), ptp, backupManifest, details,
					)
				}); err != nil {
					return err
				}
			}
		}

		if err := p.ExecCfg().JobRegistry.CheckPausepoint("backup.before.write_first_checkpoint"); err != nil {
			return err
		}

		if err := backupinfo.WriteBackupManifestCheckpoint(
			ctx, details.URI, details.EncryptionOptions, &kmsEnv, backupManifest, p.ExecCfg(), p.User(),
		); err != nil {
			return err
		}

		if err := p.ExecCfg().JobRegistry.CheckPausepoint("backup.after.write_first_checkpoint"); err != nil {
			return err
		}

		if err := insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return planSchedulePTSChaining(ctx, p.ExecCfg().JobsKnobs(), txn, &details, b.job.CreatedBy())
		}); err != nil {
			return err
		}

		// The description picked during original planning might still say "LATEST",
		// if resolving that to the actual directory only just happened above here.
		// Ideally we'd re-render the description now that we know the subdir, but
		// we don't have backup AST node anymore to easily call the rendering func.
		// Instead we can just do a bit of dirty string replacement iff there is one
		// "INTO 'LATEST' IN" (if there's >1, someone has a weird table/db names and
		// we should just leave the description as-is, since it is just for humans).
		description := b.job.Payload().Description
		const unresolvedText = "INTO 'LATEST' IN"
		// Note, we are using initialDetails below which is a copy of the
		// BackupDetails before destination resolution.
		if initialDetails.Destination.Subdir == "LATEST" && strings.Count(description, unresolvedText) == 1 {
			description = strings.ReplaceAll(description, unresolvedText, fmt.Sprintf("INTO '%s' IN", details.Destination.Subdir))
		}

		// Update the job payload (non-volatile job definition) once, with the now
		// resolved destination, updated description, etc. If we resume again we'll
		// skip this whole block so this isn't an excessive update of payload.
		if err := b.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			if err := md.CheckRunningOrReverting(); err != nil {
				return err
			}
			md.Payload.Details = jobspb.WrapPayloadDetails(details)
			md.Payload.Description = description
			ju.UpdatePayload(md.Payload)
			return nil
		}); err != nil {
			return err
		}

		if err := p.ExecCfg().JobRegistry.CheckPausepoint("backup.after.details_has_checkpoint"); err != nil {
			return err
		}

		// Collect telemetry, once per backup after resolving its destination.
		lic := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(), "",
		) != nil
		collectTelemetry(ctx, backupManifest, initialDetails, details, lic, b.job.ID())
	}

	// For all backups, partitioned or not, the main BACKUP manifest is stored at
	// details.URI.
	defaultConf, err := cloud.ExternalStorageConfFromURI(details.URI, p.User())
	if err != nil {
		return errors.Wrapf(err, "export configuration")
	}
	defaultStore, err := p.ExecCfg().DistSQLSrv.ExternalStorage(ctx, defaultConf)
	if err != nil {
		return errors.Wrapf(err, "make storage")
	}
	defer defaultStore.Close()

	// EncryptionInfo is non-nil only when new encryption information has been
	// generated during BACKUP planning.
	redactedURI := backuputils.RedactURIForErrorMessage(details.URI)
	if details.EncryptionInfo != nil {
		if err := backupencryption.WriteEncryptionInfoIfNotExists(ctx, details.EncryptionInfo,
			defaultStore); err != nil {
			return errors.Wrapf(err, "creating encryption info file to %s", redactedURI)
		}
	}

	storageByLocalityKV := make(map[string]*cloudpb.ExternalStorage)
	for kv, uri := range details.URIsByLocalityKV {
		conf, err := cloud.ExternalStorageConfFromURI(uri, p.User())
		if err != nil {
			return err
		}
		storageByLocalityKV[kv] = &conf
	}

	mem := p.ExecCfg().RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	var memSize int64
	defer func() {
		if memSize != 0 {
			mem.Shrink(ctx, memSize)
		}
	}()

	if backupManifest == nil || forceReadBackupManifest {
		backupManifest, memSize, err = b.readManifestOnResume(ctx, &mem, p.ExecCfg(), defaultStore,
			details, p.User(), &kmsEnv)
		if err != nil {
			return err
		}
	}

	statsCache := p.ExecCfg().TableStatsCache
	// We retry on pretty generic failures -- any rpc error. If a worker node were
	// to restart, it would produce this kind of error, but there may be other
	// errors that are also rpc errors. Don't retry to aggressively.
	retryOpts := retry.Options{
		MaxBackoff: 1 * time.Second,
		MaxRetries: 5,
	}

	if err := p.ExecCfg().JobRegistry.CheckPausepoint("backup.before.flow"); err != nil {
		return err
	}

	// We want to retry a backup if there are transient failures (i.e. worker nodes
	// dying), so if we receive a retryable error, re-plan and retry the backup.
	var res roachpb.RowCount
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		res, err = backup(
			ctx,
			p,
			details.URI,
			details.URIsByLocalityKV,
			p.ExecCfg().Settings,
			defaultStore,
			storageByLocalityKV,
			b.job,
			backupManifest,
			p.ExecCfg().DistSQLSrv.ExternalStorage,
			details.EncryptionOptions,
			statsCache,
		)
		if err == nil {
			break
		}

		if joberror.IsPermanentBulkJobError(err) {
			return errors.Wrap(err, "failed to run backup")
		}

		log.Warningf(ctx, `BACKUP job encountered retryable error: %+v`, err)

		// Reload the backup manifest to pick up any spans we may have completed on
		// previous attempts.
		var reloadBackupErr error
		mem.Shrink(ctx, memSize)
		memSize = 0
		backupManifest, memSize, reloadBackupErr = b.readManifestOnResume(ctx, &mem, p.ExecCfg(),
			defaultStore, details, p.User(), &kmsEnv)
		if reloadBackupErr != nil {
			return errors.Wrap(reloadBackupErr, "could not reload backup manifest when retrying")
		}
	}

	// We have exhausted retries, but we have not seen a "PermanentBulkJobError" so
	// it is possible that this is a transient error that is taking longer than
	// our configured retry to go away.
	//
	// Let's pause the job instead of failing it so that the user can decide
	// whether to resume it or cancel it.
	if err != nil {
		return jobs.MarkPauseRequestError(errors.Wrap(err, "exhausted retries"))
	}

	var backupDetails jobspb.BackupDetails
	var ok bool
	if backupDetails, ok = b.job.Details().(jobspb.BackupDetails); !ok {
		return errors.Newf("unexpected job details type %T", b.job.Details())
	}

	if err := maybeUpdateSchedulePTSRecord(ctx, p.ExecCfg(), backupDetails, b.job.ID()); err != nil {
		return err
	}

	if details.ProtectedTimestampRecord != nil && !b.testingKnobs.ignoreProtectedTimestamps {
		if err := p.ExecCfg().InternalDB.Txn(ctx, func(
			ctx context.Context, txn isql.Txn,
		) error {
			details := b.job.Details().(jobspb.BackupDetails)
			pts := p.ExecCfg().ProtectedTimestampProvider.WithTxn(txn)
			return releaseProtectedTimestamp(ctx, pts, details.ProtectedTimestampRecord)
		}); err != nil {
			log.Errorf(ctx, "failed to release protected timestamp: %v", err)
		}
	}

	// If this is a full backup that was automatically nested in a collection of
	// backups, record the path under which we wrote it to the LATEST file in the
	// root of the collection. Note: this file *not* encrypted, as it only
	// contains the name of another file that is in the same folder -- if you can
	// get to this file to read it, you could already find its contents from the
	// listing of the directory it is in -- it exists only to save us a
	// potentially expensive listing of a giant backup collection to find the most
	// recent completed entry.
	if backupManifest.StartTime.IsEmpty() && details.CollectionURI != "" {
		backupURI, err := url.Parse(details.URI)
		if err != nil {
			return err
		}
		collectionURI, err := url.Parse(details.CollectionURI)
		if err != nil {
			return err
		}

		suffix := strings.TrimPrefix(path.Clean(backupURI.Path), path.Clean(collectionURI.Path))

		c, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, details.CollectionURI, p.User())
		if err != nil {
			return err
		}
		defer c.Close()

		if err := backupdest.WriteNewLatestFile(ctx, p.ExecCfg().Settings, c, suffix); err != nil {
			return err
		}
	}

	b.backupStats = res

	// Collect telemetry.
	{
		numClusterNodes, err := clusterNodeCount(p.ExecCfg().Gossip)
		if err != nil {
			if !build.IsRelease() && p.ExecCfg().Codec.ForSystemTenant() {
				return err
			}
			log.Warningf(ctx, "unable to determine cluster node count: %v", err)
			numClusterNodes = 1
		}

		telemetry.Count("backup.total.succeeded")
		const mb = 1 << 20
		sizeMb := res.DataSize / mb
		sec := int64(timeutil.Since(timeutil.FromUnixMicros(b.job.Payload().StartedMicros)).Seconds())
		var mbps int64
		if sec > 0 {
			mbps = mb / sec
		}
		if details.StartTime.IsEmpty() {
			telemetry.CountBucketed("backup.duration-sec.full-succeeded", sec)
			telemetry.CountBucketed("backup.size-mb.full", sizeMb)
			telemetry.CountBucketed("backup.speed-mbps.full.total", mbps)
			telemetry.CountBucketed("backup.speed-mbps.full.per-node", mbps/int64(numClusterNodes))
		} else {
			telemetry.CountBucketed("backup.duration-sec.inc-succeeded", sec)
			telemetry.CountBucketed("backup.size-mb.inc", sizeMb)
			telemetry.CountBucketed("backup.speed-mbps.inc.total", mbps)
			telemetry.CountBucketed("backup.speed-mbps.inc.per-node", mbps/int64(numClusterNodes))
		}
		logJobCompletion(ctx, b.getTelemetryEventType(), b.job.ID(), true, nil)
	}

	return b.maybeNotifyScheduledJobCompletion(
		ctx, jobs.StatusSucceeded, p.ExecCfg().JobsKnobs(), p.ExecCfg().InternalDB,
	)
}

// ReportResults implements JobResultsReporter interface.
func (b *backupResumer) ReportResults(ctx context.Context, resultsCh chan<- tree.Datums) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(b.job.ID())),
		tree.NewDString(string(jobs.StatusSucceeded)),
		tree.NewDFloat(tree.DFloat(1.0)),
		tree.NewDInt(tree.DInt(b.backupStats.Rows)),
		tree.NewDInt(tree.DInt(b.backupStats.IndexEntries)),
		tree.NewDInt(tree.DInt(b.backupStats.DataSize)),
	}:
		return nil
	}
}

func getBackupDetailAndManifest(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	txn isql.Txn,
	initialDetails jobspb.BackupDetails,
	user username.SQLUsername,
	backupDestination backupdest.ResolvedDestination,
) (jobspb.BackupDetails, backuppb.BackupManifest, error) {
	makeCloudStorage := execCfg.DistSQLSrv.ExternalStorageFromURI

	kmsEnv := backupencryption.MakeBackupKMSEnv(
		execCfg.Settings,
		&execCfg.ExternalIODirConfig,
		execCfg.InternalDB,
		user,
	)

	mem := execCfg.RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	var prevBackups []backuppb.BackupManifest
	var baseEncryptionOptions *jobspb.BackupEncryptionOptions
	if len(backupDestination.PrevBackupURIs) != 0 {
		var err error
		baseEncryptionOptions, err = backupencryption.GetEncryptionFromBase(ctx, user, makeCloudStorage,
			backupDestination.PrevBackupURIs[0], *initialDetails.EncryptionOptions, &kmsEnv)
		if err != nil {
			return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
		}

		var memSize int64
		prevBackups, memSize, err = backupinfo.GetBackupManifests(ctx, &mem, user,
			makeCloudStorage, backupDestination.PrevBackupURIs, baseEncryptionOptions, &kmsEnv)

		if err != nil {
			return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
		}
		defer mem.Shrink(ctx, memSize)
	}

	if len(prevBackups) > 0 {
		baseManifest := prevBackups[0]
		if baseManifest.DescriptorCoverage == tree.AllDescriptors &&
			!initialDetails.FullCluster {
			return jobspb.BackupDetails{}, backuppb.BackupManifest{}, errors.Errorf("cannot append a backup of specific tables or databases to a cluster backup")
		}

		if err := requireEnterprise(execCfg, "incremental"); err != nil {
			return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
		}
		lastEndTime := prevBackups[len(prevBackups)-1].EndTime
		if lastEndTime.Compare(initialDetails.EndTime) > 0 {
			return jobspb.BackupDetails{}, backuppb.BackupManifest{},
				errors.Newf("`AS OF SYSTEM TIME` %s must be greater than "+
					"the previous backup's end time of %s.",
					initialDetails.EndTime.GoTime(), lastEndTime.GoTime())
		}
	}

	localityKVs := make([]string, len(backupDestination.URIsByLocalityKV))
	i := 0
	for k := range backupDestination.URIsByLocalityKV {
		localityKVs[i] = k
		i++
	}

	for i := range prevBackups {
		prevBackup := prevBackups[i]
		// IDs are how we identify tables, and those are only meaningful in the
		// context of their own cluster, so we need to ensure we only allow
		// incremental previous backups that we created.
		if fromCluster := prevBackup.ClusterID; !fromCluster.Equal(execCfg.NodeInfo.LogicalClusterID()) {
			return jobspb.BackupDetails{}, backuppb.BackupManifest{}, errors.Newf("previous BACKUP belongs to cluster %s", fromCluster.String())
		}

		prevLocalityKVs := prevBackup.LocalityKVs

		// Checks that each layer in the backup uses the same localities
		// Does NOT check that each locality/layer combination is actually at the
		// expected locations.
		// This is complex right now, but should be easier shortly.
		// TODO(benbardin): Support verifying actual existence of localities for
		// each layer after deprecating TO-syntax in 22.2
		sort.Strings(localityKVs)
		sort.Strings(prevLocalityKVs)
		if !(len(localityKVs) == 0 && len(prevLocalityKVs) == 0) && !reflect.DeepEqual(localityKVs,
			prevLocalityKVs) {
			// Note that this won't verify the default locality. That's not
			// necessary, because the default locality defines the backup manifest
			// location. If that URI isn't right, the backup chain will fail to
			// load.
			return jobspb.BackupDetails{}, backuppb.BackupManifest{}, errors.Newf(
				"Requested backup has localities %s, but a previous backup layer in this collection has localities %s. "+
					"Mismatched backup layers are not supported. Please take a new full backup with the new localities, or an "+
					"incremental backup with matching localities.",
				localityKVs, prevLocalityKVs,
			)
		}
	}

	// updatedDetails and backupManifest should be treated as read-only after
	// they're returned from their respective functions. Future changes to those
	// objects should be made within those functions.
	updatedDetails, err := updateBackupDetails(
		ctx,
		initialDetails,
		backupDestination.CollectionURI,
		backupDestination.DefaultURI,
		backupDestination.ChosenSubdir,
		backupDestination.URIsByLocalityKV,
		prevBackups,
		baseEncryptionOptions,
		&kmsEnv)
	if err != nil {
		return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
	}

	backupManifest, err := createBackupManifest(
		ctx,
		execCfg,
		txn,
		updatedDetails,
		prevBackups)
	if err != nil {
		return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
	}

	return updatedDetails, backupManifest, nil
}

func (b *backupResumer) readManifestOnResume(
	ctx context.Context,
	mem *mon.BoundAccount,
	cfg *sql.ExecutorConfig,
	defaultStore cloud.ExternalStorage,
	details jobspb.BackupDetails,
	user username.SQLUsername,
	kmsEnv cloud.KMSEnv,
) (*backuppb.BackupManifest, int64, error) {
	// We don't read the table descriptors from the backup descriptor, but
	// they could be using either the new or the old foreign key
	// representations. We should just preserve whatever representation the
	// table descriptors were using and leave them alone.
	desc, memSize, err := backupinfo.ReadBackupCheckpointManifest(ctx, mem, defaultStore,
		backupinfo.BackupManifestCheckpointName, details.EncryptionOptions, kmsEnv)
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return nil, 0, errors.Wrapf(err, "reading backup checkpoint")
		}
		// Try reading temp checkpoint.
		tmpCheckpoint := backupinfo.TempCheckpointFileNameForJob(b.job.ID())
		desc, memSize, err = backupinfo.ReadBackupCheckpointManifest(ctx, mem, defaultStore,
			tmpCheckpoint, details.EncryptionOptions, kmsEnv)
		if err != nil {
			return nil, 0, err
		}
		// "Rename" temp checkpoint.
		if err := backupinfo.WriteBackupManifestCheckpoint(
			ctx, details.URI, details.EncryptionOptions, kmsEnv, &desc, cfg, user,
		); err != nil {
			mem.Shrink(ctx, memSize)
			return nil, 0, errors.Wrapf(err, "renaming temp checkpoint file")
		}
		// Best effort remove temp checkpoint.
		if err := defaultStore.Delete(ctx, tmpCheckpoint); err != nil {
			log.Errorf(ctx, "error removing temporary checkpoint %s", tmpCheckpoint)
		}
		if err := defaultStore.Delete(ctx, backupinfo.BackupProgressDirectory+"/"+tmpCheckpoint); err != nil {
			log.Errorf(ctx, "error removing temporary checkpoint %s", backupinfo.BackupProgressDirectory+"/"+tmpCheckpoint)
		}
	}

	if !desc.ClusterID.Equal(cfg.NodeInfo.LogicalClusterID()) {
		mem.Shrink(ctx, memSize)
		return nil, 0, errors.Newf("cannot resume backup started on another cluster (%s != %s)",
			desc.ClusterID, cfg.NodeInfo.LogicalClusterID())
	}
	return &desc, memSize, nil
}

func (b *backupResumer) maybeNotifyScheduledJobCompletion(
	ctx context.Context, jobStatus jobs.Status, knobs *jobs.TestingKnobs, db isql.DB,
) error {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs != nil && knobs.JobSchedulerEnv != nil {
		env = knobs.JobSchedulerEnv
	}

	err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// We cannot rely on b.job containing created_by_id because on job
		// resumption the registry does not populate the resumer's CreatedByInfo.
		datums, err := txn.QueryRowEx(
			ctx,
			"lookup-schedule-info",
			txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(
				"SELECT created_by_id FROM %s WHERE id=$1 AND created_by_type=$2",
				env.SystemJobsTableName()),
			b.job.ID(), jobs.CreatedByScheduledJobs)
		if err != nil {
			return errors.Wrap(err, "schedule info lookup")
		}
		if datums == nil {
			// Not a scheduled backup.
			return nil
		}

		scheduleID := int64(tree.MustBeDInt(datums[0]))
		if err := jobs.NotifyJobTermination(
			ctx, txn, env, b.job.ID(), jobStatus, b.job.Details(), scheduleID,
		); err != nil {
			return errors.Wrapf(err,
				"failed to notify schedule %d of completion of job %d", scheduleID, b.job.ID())
		}
		return nil
	})
	return err
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (b *backupResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	telemetry.Count("backup.total.failed")
	telemetry.CountBucketed("backup.duration-sec.failed",
		int64(timeutil.Since(timeutil.FromUnixMicros(b.job.Payload().StartedMicros)).Seconds()))
	logJobCompletion(ctx, b.getTelemetryEventType(), b.job.ID(), false, jobErr)

	p := execCtx.(sql.JobExecContext)
	cfg := p.ExecCfg()
	b.deleteCheckpoint(ctx, cfg, p.User())
	if err := cfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		details := b.job.Details().(jobspb.BackupDetails)
		pts := cfg.ProtectedTimestampProvider.WithTxn(txn)
		return releaseProtectedTimestamp(ctx, pts, details.ProtectedTimestampRecord)
	}); err != nil {
		return err
	}

	// This should never return an error unless resolving the schedule that the
	// job is being run under fails. This could happen if the schedule is dropped
	// while the job is executing.
	if err := b.maybeNotifyScheduledJobCompletion(
		ctx, jobs.StatusFailed, cfg.JobsKnobs(), cfg.InternalDB,
	); err != nil {
		log.Errorf(ctx, "failed to notify job %d on completion of OnFailOrCancel: %+v",
			b.job.ID(), err)
	}
	return nil //nolint:returnerrcheck
}

func (b *backupResumer) deleteCheckpoint(
	ctx context.Context, cfg *sql.ExecutorConfig, user username.SQLUsername,
) {
	// Attempt to delete BACKUP-CHECKPOINT(s) in /progress directory.
	if err := func() error {
		details := b.job.Details().(jobspb.BackupDetails)
		// For all backups, partitioned or not, the main BACKUP manifest is stored at
		// details.URI.
		exportStore, err := cfg.DistSQLSrv.ExternalStorageFromURI(ctx, details.URI, user)
		if err != nil {
			return err
		}
		defer exportStore.Close()
		// We first attempt to delete from base directory to account for older
		// backups, and then from the progress directory.
		err = exportStore.Delete(ctx, backupinfo.BackupManifestCheckpointName)
		if err != nil {
			log.Warningf(ctx, "unable to delete checkpointed backup descriptor file in base directory: %+v", err)
		}
		err = exportStore.Delete(ctx, backupinfo.BackupManifestCheckpointName+backupinfo.BackupManifestChecksumSuffix)
		if err != nil {
			log.Warningf(ctx, "unable to delete checkpoint checksum file in base directory: %+v", err)
		}
		// Delete will not delete a nonempty directory, so we have to go through
		// all files and delete each file one by one.
		return exportStore.List(ctx, backupinfo.BackupProgressDirectory, "", func(p string) error {
			return exportStore.Delete(ctx, backupinfo.BackupProgressDirectory+p)
		})
	}(); err != nil {
		log.Warningf(ctx, "unable to delete checkpointed backup descriptor file in progress directory: %+v", err)
	}
}

func (b *backupResumer) getTelemetryEventType() eventpb.RecoveryEventType {
	if b.job.Details().(jobspb.BackupDetails).ScheduleID != 0 {
		return scheduledBackupJobEventType
	}
	return backupJobEventType
}

var _ jobs.Resumer = &backupResumer{}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeBackup,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &backupResumer{
				job: job,
			}
		},
		jobs.UsesTenantCostControl,
	)
}
