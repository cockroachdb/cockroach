// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backupdest"
	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvfollowerreadsccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	bulkutil "github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/types"
)

// BackupCheckpointInterval is the interval at which backup progress is saved
// to durable storage.
var BackupCheckpointInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"bulkio.backup.checkpoint_interval",
	"the minimum time between writing progress checkpoints during a backup",
	time.Minute)

var forceReadBackupManifest = metamorphic.ConstantWithTestBool("backup-read-manifest", false)

var useBulkOracle = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"bulkio.backup.balanced_distribution.enabled",
	"randomize the selection of which replica backs up each range",
	true)

func countRows(raw kvpb.BulkOpSummary, pkIDs map[uint64]bool) roachpb.RowCount {
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

// backup exports a snapshot of every kv entry into ranged sstables.
//
// The output is an sstable per range with files in the following locations:
//   - <dir>/<unique_int>.sst
//   - <dir> is given by the user and may be cloud storage
//   - Each file contains data for a key range that doesn't overlap with any other
//     file.
//
// - numBackupInstances indicates the number of SQL instances that were used to
// execute the backup.
func backup(
	ctx context.Context,
	execCtx sql.JobExecContext,
	defaultURI string,
	urisByLocalityKV map[string]string,
	settings *cluster.Settings,
	defaultStore cloud.ExternalStorage,
	storageByLocalityKV map[string]*cloudpb.ExternalStorage,
	resumer *backupResumer,
	backupManifest *backuppb.BackupManifest,
	makeExternalStorage cloud.ExternalStorageFactory,
	encryption *jobspb.BackupEncryptionOptions,
	statsCache *stats.TableStatisticsCache,
	execLocality roachpb.Locality,
) (_ roachpb.RowCount, numBackupInstances int, _ error) {
	resumerSpan := tracing.SpanFromContext(ctx)
	var lastCheckpoint time.Time

	kmsEnv := backupencryption.MakeBackupKMSEnv(
		execCtx.ExecCfg().Settings,
		&execCtx.ExecCfg().ExternalIODirConfig,
		execCtx.ExecCfg().InternalDB,
		execCtx.User(),
	)
	completedSpans, completedIntroducedSpans, err := getCompletedSpans(
		ctx, execCtx, backupManifest, defaultStore, encryption, &kmsEnv,
	)
	if err != nil {
		return roachpb.RowCount{}, 0, err
	}

	// Subtract out any completed spans.
	spans := filterSpans(backupManifest.Spans, completedSpans)
	introducedSpans := filterSpans(backupManifest.IntroducedSpans, completedIntroducedSpans)

	pkIDs := make(map[uint64]bool)
	for i := range backupManifest.Descriptors {
		if t, _, _, _, _ := descpb.GetDescriptors(&backupManifest.Descriptors[i]); t != nil {
			pkIDs[kvpb.BulkOpSummaryID(uint64(t.ID), uint64(t.PrimaryIndex.ID))] = true
		}
	}

	evalCtx := execCtx.ExtendedEvalContext()
	dsp := execCtx.DistSQLPlanner()

	oracle := physicalplan.DefaultReplicaChooser
	if useBulkOracle.Get(&evalCtx.Settings.SV) {
		oracle = kvfollowerreadsccl.NewBulkOracle(
			dsp.ReplicaOracleConfig(evalCtx.Locality), execLocality, kvfollowerreadsccl.StreakConfig{},
		)
	}

	// We don't return the compatible nodes here since PartitionSpans will
	// filter out incompatible nodes.
	planCtx, _, err := dsp.SetupAllNodesPlanningWithOracle(
		ctx, evalCtx, execCtx.ExecCfg(), oracle, execLocality,
	)
	if err != nil {
		return roachpb.RowCount{}, 0, errors.Wrap(err, "failed to determine nodes on which to run")
	}

	job := resumer.job
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
		kvpb.MVCCFilter(backupManifest.MVCCFilter),
		backupManifest.StartTime,
		backupManifest.EndTime,
		backupManifest.ElidedPrefix,
		backupManifest.ClusterVersion.AtLeast(clusterversion.V24_1.Version()),
	)
	if err != nil {
		return roachpb.RowCount{}, 0, err
	}

	numBackupInstances = len(backupSpecs)
	numTotalSpans := 0
	for _, spec := range backupSpecs {
		numTotalSpans += len(spec.IntroducedSpans) + len(spec.Spans)
	}

	progressLogger := jobs.NewChunkProgressLoggerForJob(job, numTotalSpans, job.FractionCompleted(), jobs.ProgressUpdateOnly)

	requestFinishedCh := make(chan struct{}, numTotalSpans) // enough buffer to never block
	var jobProgressLoop func(ctx context.Context) error
	if numTotalSpans > 0 {
		jobProgressLoop = func(ctx context.Context) error {
			// Currently the granularity of backup progress is the % of spans
			// exported. Would improve accuracy if we tracked the actual size of each
			// file.
			return errors.Wrap(progressLogger.Loop(ctx, requestFinishedCh), "updating job progress")
		}
	}

	// Create a channel that is large enough that it does not block.
	perNodeProgressCh := make(chan map[execinfrapb.ComponentID]float32, numTotalSpans)
	storePerNodeProgressLoop := func(ctx context.Context) error {
		for {
			select {
			case prog, ok := <-perNodeProgressCh:
				if !ok {
					return nil
				}
				jobsprofiler.StorePerNodeProcessorProgressFraction(
					ctx, execCtx.ExecCfg().InternalDB, job.ID(), prog)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	progCh := make(chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
	checkpointLoop := func(ctx context.Context) error {
		// When a processor is done exporting a span, it will send a progress update
		// to progCh.
		defer close(requestFinishedCh)
		defer close(perNodeProgressCh)
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
				if execCtx.ExecCfg().BackupRestoreTestingKnobs != nil &&
					execCtx.ExecCfg().BackupRestoreTestingKnobs.AfterBackupChunk != nil {
					execCtx.ExecCfg().BackupRestoreTestingKnobs.AfterBackupChunk()
				}
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
			select {
			// This send to a buffered channel should never block but incase it does
			// we will fallthrough to the default case.
			case perNodeProgressCh <- perComponentProgress:
			default:
				log.Warningf(ctx, "skipping persisting per component progress as buffered channel was full")
			}

			// Check if we should persist a checkpoint backup manifest.
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
				if execCtx.ExecCfg().BackupRestoreTestingKnobs != nil &&
					execCtx.ExecCfg().BackupRestoreTestingKnobs.AfterBackupCheckpoint != nil {
					execCtx.ExecCfg().BackupRestoreTestingKnobs.AfterBackupCheckpoint()
				}
			}
		}
		return nil
	}

	// tracingAggLoop is responsible for draining the channel on which processors
	// in the DistSQL flow will send back their tracing aggregator stats. These
	// stats will be persisted to the job_info table whenever the job is told to
	// collect a profile.
	tracingAggCh := make(chan *execinfrapb.TracingAggregatorEvents)
	tracingAggLoop := func(ctx context.Context) error {
		for agg := range tracingAggCh {
			componentID := execinfrapb.ComponentID{
				FlowID:        agg.FlowID,
				SQLInstanceID: agg.SQLInstanceID,
			}

			// Update the running aggregate of the component with the latest received
			// aggregate.
			resumer.mu.Lock()
			resumer.mu.perNodeAggregatorStats[componentID] = agg.Events
			resumer.mu.Unlock()
		}
		return nil
	}

	runBackup := func(ctx context.Context) error {
		return errors.Wrapf(distBackup(
			ctx,
			execCtx,
			planCtx,
			dsp,
			progCh,
			tracingAggCh,
			backupSpecs,
		), "running distributed backup to export %d ranges", errors.Safe(numTotalSpans))
	}

	testingKnobs := execCtx.ExecCfg().BackupRestoreTestingKnobs
	if testingKnobs != nil && testingKnobs.RunBeforeBackupFlow != nil {
		if err := testingKnobs.RunBeforeBackupFlow(); err != nil {
			return roachpb.RowCount{}, 0, err
		}
	}

	if err := ctxgroup.GoAndWait(
		ctx,
		jobProgressLoop,
		checkpointLoop,
		storePerNodeProgressLoop,
		tracingAggLoop,
		runBackup,
	); err != nil {
		return roachpb.RowCount{}, 0, err
	}

	if testingKnobs != nil && testingKnobs.RunAfterBackupFlow != nil {
		if err := testingKnobs.RunAfterBackupFlow(); err != nil {
			return roachpb.RowCount{}, 0, err
		}
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
				return roachpb.RowCount{}, 0, err
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
		return roachpb.RowCount{}, 0, err
	}

	// Write a `BACKUP_METADATA` file along with SSTs for all the alloc heavy
	// fields elided from the `BACKUP_MANIFEST`.
	if backupinfo.WriteMetadataWithExternalSSTsEnabled.Get(&settings.SV) {
		if err := backupinfo.WriteMetadataWithExternalSSTs(ctx, defaultStore, encryption,
			&kmsEnv, backupManifest); err != nil {
			return roachpb.RowCount{}, 0, err
		}
	}

	statsTable := getTableStatsForBackup(ctx, statsCache, backupManifest.Descriptors)
	if err := backupinfo.WriteTableStatistics(ctx, defaultStore, encryption, &kmsEnv, &statsTable); err != nil {
		return roachpb.RowCount{}, 0, err
	}

	return backupManifest.EntryCounts, numBackupInstances, nil
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
			// nil typeResolver means that we'll use the latest committed type
			// metadata which is acceptable.
			tableStatisticsAcc, err := statsCache.GetTableStats(ctx, tableDesc, nil /* typeResolver */)
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

	mu struct {
		syncutil.Mutex
		// perNodeAggregatorStats is a per component running aggregate of trace
		// driven AggregatorStats pushed backed to the resumer from all the
		// processors running the backup.
		perNodeAggregatorStats bulkutil.ComponentAggregatorStats
	}

	testingKnobs struct {
		ignoreProtectedTimestamps bool
	}
}

var _ jobs.TraceableJob = &backupResumer{}

// ForceRealSpan implements the TraceableJob interface.
func (b *backupResumer) ForceRealSpan() bool {
	return true
}

// DumpTraceAfterRun implements the TraceableJob interface.
func (b *backupResumer) DumpTraceAfterRun() bool {
	return true
}

// Resume is part of the jobs.Resumer interface.
func (b *backupResumer) Resume(ctx context.Context, execCtx interface{}) error {
	// The span is finished by the registry executing the job.
	p := execCtx.(sql.JobExecContext)
	initialDetails := b.job.Details().(jobspb.BackupDetails)

	if err := maybeRelocateJobExecution(
		ctx, b.job.ID(), p, initialDetails.ExecutionLocality, "BACKUP",
	); err != nil {
		return err
	}

	if err := b.ensureClusterIDMatches(p.ExecCfg().NodeInfo.LogicalClusterID()); err != nil {
		return err
	}

	kmsEnv := backupencryption.MakeBackupKMSEnv(
		p.ExecCfg().Settings,
		&p.ExecCfg().ExternalIODirConfig,
		p.ExecCfg().InternalDB,
		p.User(),
	)

	if initialDetails.Compact {
		return b.ResumeCompaction(ctx, initialDetails, p, &kmsEnv)
	}
	// Resolve the backup destination. We can skip this step if we
	// have already resolved and persisted the destination either
	// during a previous resumption of this job.
	var backupManifest *backuppb.BackupManifest
	details := initialDetails
	if initialDetails.URI == "" {
		// Choose which scheduled backup pts we will update at the end of the
		// backup _before_ we resolve the destination of the backup. This avoids a
		// race with inc backups where backup destination resolution leads this backup
		// to extend a chain that is about to be superseded by a new full backup
		// chain, which could cause this inc to accidentally push the pts for the
		// _new_ chain instead of the old chain it is a part of. By choosing the pts to
		// move before we resolve the destination, we guarantee that we push the old
		// chain.
		insqlDB := p.ExecCfg().InternalDB

		if err := insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return planSchedulePTSChaining(ctx, p.ExecCfg().JobsKnobs(), txn, &initialDetails, b.job.CreatedBy())
		}); err != nil {
			return err
		}
		backupDest, err := backupdest.ResolveDest(
			ctx, p.User(), initialDetails.Destination, initialDetails.EndTime, p.ExecCfg(),
		)
		if err != nil {
			return err
		}

		// TODO(ssd): If we restricted how old a resumed job could be,
		// we could remove the check for details.URI == "". This is
		// present to guard against the case where a pre-BACKUP_LOCK cluster
		// started a backup job, and then that job was resumed post-BACKUP_LOCK.
		// No lock file will be found, but CheckForPreviousBackup will
		// prevent the job from running. In that case, details.URI will be
		// non-empty.
		if err := maybeWriteBackupLock(ctx, p, backupDest, b.job.ID()); err != nil {
			return err
		}

		// Populate the BackupDetails with the resolved backup
		// destination, and construct the BackupManifest to be written
		// to external storage as a BACKUP-CHECKPOINT. We can skip
		// this step if the job has already persisted the resolved
		// details and manifest in a prior resumption.
		if details, backupManifest, err = prepBackupMeta(
			ctx, p, initialDetails, backupDest,
		); err != nil {
			return err
		}
		ptsID, err := writePTSRecordOnBackup(ctx, p, backupManifest, details, b.job.ID())
		if err != nil {
			return err
		}
		details.ProtectedTimestampRecord = &ptsID

		if err := p.ExecCfg().JobRegistry.CheckPausepoint("backup.before.write_first_checkpoint"); err != nil {
			return err
		}

		if err := backupinfo.WriteBackupManifestCheckpoint(
			ctx, details.URI, details.EncryptionOptions, &kmsEnv,
			backupManifest, p.ExecCfg(), p.User(),
		); err != nil {
			return err
		}

		if err := p.ExecCfg().JobRegistry.CheckPausepoint("backup.after.write_first_checkpoint"); err != nil {
			return err
		}

		description := maybeUpdateJobDescription(
			initialDetails, details, b.job.Payload().Description,
		)

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
		collectTelemetry(ctx, backupManifest, initialDetails, details, true, b.job.ID())
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
		if err := backupencryption.WriteEncryptionInfoIfNotExists(
			ctx, details.EncryptionInfo, defaultStore,
		); err != nil {
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
	// errors that are also rpc errors. Don't retry too aggressively.
	retryOpts := retry.Options{
		MaxBackoff: 1 * time.Second,
		MaxRetries: 5,
	}

	if p.ExecCfg().BackupRestoreTestingKnobs != nil &&
		p.ExecCfg().BackupRestoreTestingKnobs.BackupDistSQLRetryPolicy != nil {
		retryOpts = *p.ExecCfg().BackupRestoreTestingKnobs.BackupDistSQLRetryPolicy
	}

	if err := p.ExecCfg().JobRegistry.CheckPausepoint("backup.before.flow"); err != nil {
		return err
	}

	// We want to retry a backup if there are transient failures (i.e. worker nodes
	// dying), so if we receive a retryable error, re-plan and retry the backup.
	var res roachpb.RowCount
	var lastProgress float32
	var numBackupInstances int
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		res, numBackupInstances, err = backup(
			ctx,
			p,
			details.URI,
			details.URIsByLocalityKV,
			p.ExecCfg().Settings,
			defaultStore,
			storageByLocalityKV,
			b,
			backupManifest,
			p.ExecCfg().DistSQLSrv.ExternalStorage,
			details.EncryptionOptions,
			statsCache,
			details.ExecutionLocality,
		)
		if err == nil {
			break
		}

		if joberror.IsPermanentBulkJobError(err) {
			return errors.Wrap(err, "failed to run backup")
		}

		// If we are draining, it is unlikely we can start a
		// new DistSQL flow. Exit with a retryable error so
		// that another node can pick up the job.
		if p.ExecCfg().JobRegistry.IsDraining() {
			return jobs.MarkAsRetryJobError(errors.Wrapf(err, "job encountered retryable error on draining node"))
		}

		log.Warningf(ctx, "encountered retryable error: %+v", err)

		// Reload the backup manifest to pick up any spans we may have completed on
		// previous attempts.
		var reloadBackupErr error
		mem.Shrink(ctx, memSize)
		backupManifest, memSize, reloadBackupErr = b.readManifestOnResume(ctx, &mem, p.ExecCfg(),
			defaultStore, details, p.User(), &kmsEnv)
		if reloadBackupErr != nil {
			return errors.Wrap(reloadBackupErr, "could not reload backup manifest when retrying")
		}
		// Re-load the job in order to update our progress object, which
		// may have been updated since the flow started.
		reloadedJob, reloadErr := p.ExecCfg().JobRegistry.LoadClaimedJob(ctx, b.job.ID())
		if reloadErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Warningf(ctx, "BACKUP job %d could not reload job progress when retrying: %+v",
				b.job.ID(), reloadErr)
		} else {
			curProgress := reloadedJob.FractionCompleted()
			// If we made decent progress with the BACKUP, reset the last
			// progress state.
			if madeProgress := curProgress - lastProgress; madeProgress >= 0.01 {
				log.Infof(ctx, "backport made %d%% progress, resetting retry duration", int(math.Round(float64(100*madeProgress))))
				lastProgress = curProgress
				r.Reset()
			}
		}
	}

	// We have exhausted retries without getting a "PermanentBulkJobError", but
	// something must be wrong if we keep seeing errors so give up and fail to
	// ensure that any alerting on failures is triggered and that any subsequent
	// schedule runs are not blocked.
	if err != nil {
		return errors.Wrap(err, "exhausted retries")
	}

	var jobDetails jobspb.BackupDetails
	var ok bool
	if jobDetails, ok = b.job.Details().(jobspb.BackupDetails); !ok {
		return errors.Newf("unexpected job details type %T", b.job.Details())
	}

	if err := maybeUpdateSchedulePTSRecord(ctx, p.ExecCfg(), jobDetails, b.job.ID()); err != nil {
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
		telemetry.Count("backup.total.succeeded")
		const mb = 1 << 20
		sizeMb := res.DataSize / mb
		sec := int64(timeutil.Since(timeutil.FromUnixMicros(b.job.Payload().StartedMicros)).Seconds())
		var mbps int64
		if sec > 0 {
			mbps = mb / sec
		}
		if numBackupInstances == 0 {
			// This can happen when we didn't have anything to back up.
			numBackupInstances = 1
		}
		if details.StartTime.IsEmpty() {
			telemetry.CountBucketed("backup.duration-sec.full-succeeded", sec)
			telemetry.CountBucketed("backup.size-mb.full", sizeMb)
			telemetry.CountBucketed("backup.speed-mbps.full.total", mbps)
			telemetry.CountBucketed("backup.speed-mbps.full.per-node", mbps/int64(numBackupInstances))
		} else {
			telemetry.CountBucketed("backup.duration-sec.inc-succeeded", sec)
			telemetry.CountBucketed("backup.size-mb.inc", sizeMb)
			telemetry.CountBucketed("backup.speed-mbps.inc.total", mbps)
			telemetry.CountBucketed("backup.speed-mbps.inc.per-node", mbps/int64(numBackupInstances))
		}
		logutil.LogJobCompletion(ctx, b.getTelemetryEventType(), b.job.ID(), true, nil, res.Rows)
	}

	return b.maybeNotifyScheduledJobCompletion(
		ctx, jobs.StateSucceeded, p.ExecCfg().JobsKnobs(), p.ExecCfg().InternalDB,
	)
}

// ensureClusterIDMatches verifies that this job record matches
// the cluster ID of this cluster.
// This check ensures that if the backup job has been restored from a
// backup of a tenant, or from streaming replication, then we will fail
// this backup since resuming the backup may overwrite backup metadata created by a different cluster.
func (b *backupResumer) ensureClusterIDMatches(clusterID uuid.UUID) error {
	if createdBy := b.job.Payload().CreationClusterID; createdBy != uuid.Nil && clusterID != createdBy {
		return errors.Newf("cannot resume backup started on another cluster (%s != %s)", createdBy, clusterID)
	}
	return nil
}

func prepBackupMeta(
	ctx context.Context,
	execCtx sql.JobExecContext,
	initialDetails jobspb.BackupDetails,
	backupDest backupdest.ResolvedDestination,
) (jobspb.BackupDetails, *backuppb.BackupManifest, error) {
	db := execCtx.ExecCfg().InternalDB
	var tenantSpans []roachpb.Span
	var tenantInfos []mtinfopb.TenantInfoWithUsage
	var err error
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		tenantSpans, tenantInfos, err = getTenantInfo(
			ctx, execCtx.ExecCfg().Codec, txn, initialDetails,
		)
		return err
	}); err != nil {
		return jobspb.BackupDetails{}, nil, err
	}
	return getBackupDetailAndManifest(
		ctx, execCtx.ExecCfg(), tenantSpans, tenantInfos, initialDetails, execCtx.User(), backupDest,
	)
}

// writePTSRecordOnBackup writes a protected timestamp record on the backup's
// target spans/schema objects. It returns the ID of the PTS to be saved to the
// job details.
func writePTSRecordOnBackup(
	ctx context.Context,
	execCtx sql.JobExecContext,
	backupManifest *backuppb.BackupManifest,
	details jobspb.BackupDetails,
	jobID jobspb.JobID,
) (uuid.UUID, error) {
	protectedtsID := uuid.MakeV4()
	details.ProtectedTimestampRecord = &protectedtsID
	if err := execCtx.ExecCfg().InternalDB.Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) error {
		ptp := execCtx.ExecCfg().ProtectedTimestampProvider.WithTxn(txn)
		return protectTimestampForBackup(
			ctx, jobID, ptp, backupManifest, details,
		)
	}); err != nil {
		return uuid.UUID{}, err
	}
	return protectedtsID, nil
}

// maybeUpdateJobDescription will try to replace 'LATEST' with the resolved
// subdir as the description picked during original planning might still say
// "LATEST" if resolving that to the actual directory only just happened during
// initial job execution. Ideally we'd re-render the description now that we
// know the subdir, but for classic backup, we don't have backup AST node
// anymore to easily call the rendering func. Instead, we can just do a bit of
// dirty string replacement iff there is one "'LATEST' IN" (if there's >1,
// someone has a weird table/db names and we should just leave the description
// as-is, since it is just for humans).
func maybeUpdateJobDescription(
	initialDetails jobspb.BackupDetails, resolvedDetails jobspb.BackupDetails, curDesc string,
) string {
	const unresolvedText = "'LATEST' IN"
	if initialDetails.Destination.Subdir != "LATEST" || strings.Count(curDesc, unresolvedText) != 1 {
		return curDesc
	}
	return strings.ReplaceAll(
		curDesc,
		unresolvedText,
		fmt.Sprintf("'%s' IN", resolvedDetails.Destination.Subdir),
	)
}

// ReportResults implements JobResultsReporter interface.
func (b *backupResumer) ReportResults(ctx context.Context, resultsCh chan<- tree.Datums) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(b.job.ID())),
		tree.NewDString(string(jobs.StateSucceeded)),
		tree.NewDFloat(tree.DFloat(1.0)),
		tree.NewDInt(tree.DInt(b.backupStats.Rows)),
	}:
		return nil
	}
}

func collectTelemetry(
	ctx context.Context,
	backupManifest *backuppb.BackupManifest,
	initialDetails, backupDetails jobspb.BackupDetails,
	licensed bool,
	jobID jobspb.JobID,
) {
	// sourceSuffix specifies if this schedule was created by a schedule.
	sourceSuffix := ".manual"
	if backupDetails.ScheduleID != 0 {
		sourceSuffix = ".scheduled"
	}

	// countSource emits a telemetry counter and also adds a ".scheduled"
	// suffix if the job was created by a schedule.
	countSource := func(feature string) {
		telemetry.Count(feature + sourceSuffix)
	}

	countSource("backup.total.started")
	if backupManifest.IsIncremental() || backupDetails.EncryptionOptions != nil {
		countSource("backup.using-enterprise-features")
	}
	if licensed {
		countSource("backup.licensed")
	} else {
		countSource("backup.free")
	}
	if backupDetails.StartTime.IsEmpty() {
		countSource("backup.span.full")
	} else {
		countSource("backup.span.incremental")
		telemetry.CountBucketed("backup.incremental-span-sec",
			int64(backupDetails.EndTime.GoTime().Sub(backupDetails.StartTime.GoTime()).Seconds()))
		countSource("backup.auto-incremental")
	}
	if len(backupDetails.URIsByLocalityKV) > 1 {
		countSource("backup.partitioned")
	}
	if backupManifest.MVCCFilter == backuppb.MVCCFilter_All {
		countSource("backup.revision-history")
	}
	if backupDetails.EncryptionOptions != nil {
		countSource("backup.encrypted")
		switch backupDetails.EncryptionOptions.Mode {
		case jobspb.EncryptionMode_Passphrase:
			countSource("backup.encryption.passphrase")
		case jobspb.EncryptionMode_KMS:
			countSource("backup.encryption.kms")
		}
	}
	if backupDetails.CollectionURI != "" {
		countSource("backup.nested")
		timeBaseSubdir := true
		if _, err := time.Parse(backupbase.DateBasedIntoFolderName,
			initialDetails.Destination.Subdir); err != nil {
			timeBaseSubdir = false
		}
		if backupDetails.StartTime.IsEmpty() {
			if !timeBaseSubdir {
				countSource("backup.deprecated-full-nontime-subdir")
			} else if initialDetails.Destination.Exists {
				countSource("backup.deprecated-full-time-subdir")
			} else {
				countSource("backup.full-no-subdir")
			}
		} else {
			if initialDetails.Destination.Subdir == backupbase.LatestFileName {
				countSource("backup.incremental-latest-subdir")
			} else if !timeBaseSubdir {
				countSource("backup.deprecated-incremental-nontime-subdir")
			} else {
				countSource("backup.incremental-explicit-subdir")
			}
		}
	} else {
		countSource("backup.deprecated-non-collection")
	}
	if backupManifest.DescriptorCoverage == tree.AllDescriptors {
		countSource("backup.targets.full_cluster")
	}

	logBackupTelemetry(ctx, initialDetails, jobID)
}

// includeTableSpans returns true if the backup should include spans for the
// given table descriptor.
func includeTableSpans(table *descpb.TableDescriptor) bool {
	// We do not backup spans for views here as they do not contain data.
	//
	// Additionally, because we do not split ranges at view boundaries, it is
	// possible that the range the view span belongs to is shared by another
	// object in the cluster (that we may or may not be backing up) that might
	// have its own bespoke zone configurations, namely one with a short GC TTL.
	// This could lead to a situation where the short GC TTL on the range we are
	// not backing up causes our protectedts verification to fail when attempting
	// to backup the view span.
	return table.IsPhysicalTable()
}

// forEachPublicIndexTableSpan constructs a span for each public index of the
// provided table and runs the given function on each of them. The added map is
// used to track duplicates. Duplicate indexes are not passed to the provided
// function.
func forEachPublicIndexTableSpan(
	table *descpb.TableDescriptor,
	added map[tableAndIndex]bool,
	codec keys.SQLCodec,
	f func(span roachpb.Span),
) {
	if !includeTableSpans(table) {
		return
	}

	table.ForEachPublicIndex(func(idx *descpb.IndexDescriptor) {
		key := tableAndIndex{tableID: table.GetID(), indexID: idx.ID}
		if added[key] {
			return
		}
		added[key] = true
		prefix := roachpb.Key(rowenc.MakeIndexKeyPrefix(codec, table.GetID(), idx.ID))
		f(roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()})
	})
}

// spansForAllTableIndexes returns non-overlapping spans for every index and
// table passed in. They would normally overlap if any of them are interleaved.
// Overlapping index spans are merged so as to optimize the size/number of the
// spans we BACKUP and lay protected ts records for.
func spansForAllTableIndexes(
	execCfg *sql.ExecutorConfig,
	tables []catalog.TableDescriptor,
	revs []backuppb.BackupManifest_DescriptorRevision,
) ([]roachpb.Span, error) {

	added := make(map[tableAndIndex]bool, len(tables))
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	insertSpan := func(indexSpan roachpb.Span) {
		if err := sstIntervalTree.Insert(intervalSpan(indexSpan), true); err != nil {
			panic(errors.NewAssertionErrorWithWrappedErrf(err, "IndexSpan"))
		}
	}

	for _, table := range tables {
		forEachPublicIndexTableSpan(table.TableDesc(), added, execCfg.Codec, insertSpan)
	}

	// If there are desc revisions, ensure that we also add any index spans
	// in them that we didn't already get above e.g. indexes or tables that are
	// not in latest because they were dropped during the time window in question.
	for _, rev := range revs {
		// If the table was dropped during the last interval, it will have
		// at least 2 revisions, and the first one should have the table in a PUBLIC
		// state. We want (and do) ignore tables that have been dropped for the
		// entire interval. DROPPED tables should never later become PUBLIC.
		rawTbl, _, _, _, _ := descpb.GetDescriptors(rev.Desc)
		if rawTbl != nil && rawTbl.Public() {
			forEachPublicIndexTableSpan(rawTbl, added, execCfg.Codec, insertSpan)
		}
	}

	sstIntervalTree.AdjustRanges()
	spans := make([]roachpb.Span, 0, sstIntervalTree.Len())
	_ = sstIntervalTree.Do(func(r interval.Interface) bool {
		spans = append(spans, roachpb.Span{
			Key:    roachpb.Key(r.Range().Start),
			EndKey: roachpb.Key(r.Range().End),
		})
		return false
	})

	// Attempt to merge any contiguous spans generated from the tables and revs.
	// No need to check if the spans are distinct, since some of the merged
	// indexes may overlap between different revisions of the same descriptor.
	mergedSpans, _ := roachpb.MergeSpans(&spans)

	knobs := execCfg.BackupRestoreTestingKnobs
	if knobs != nil && knobs.CaptureResolvedTableDescSpans != nil {
		knobs.CaptureResolvedTableDescSpans(mergedSpans)
	}

	return mergedSpans, nil
}

func getScheduledBackupExecutionArgsFromSchedule(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	storage jobs.ScheduledJobStorage,
	scheduleID jobspb.ScheduleID,
) (*jobs.ScheduledJob, *backuppb.ScheduledBackupExecutionArgs, error) {
	// Load the schedule that has spawned this job.
	sj, err := storage.Load(ctx, env, scheduleID)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to load scheduled job %d", scheduleID)
	}

	args := &backuppb.ScheduledBackupExecutionArgs{}
	if err := types.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, nil, errors.Wrap(err, "un-marshaling args")
	}

	return sj, args, nil
}

// planSchedulePTSChaining populates backupDetails with information relevant to
// the chaining of protected timestamp records between scheduled backups.
// Depending on whether backupStmt is a full or incremental backup, we populate
// relevant fields that are used to perform this chaining, on successful
// completion of the backup job.
func planSchedulePTSChaining(
	ctx context.Context,
	knobs *jobs.TestingKnobs,
	txn isql.Txn,
	backupDetails *jobspb.BackupDetails,
	createdBy *jobs.CreatedByInfo,
) error {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs != nil && knobs.JobSchedulerEnv != nil {
		env = knobs.JobSchedulerEnv
	}
	// If this is not a scheduled backup, we do not chain pts records.
	if createdBy == nil || createdBy.Name != jobs.CreatedByScheduledJobs {
		return nil
	}

	_, args, err := getScheduledBackupExecutionArgsFromSchedule(
		ctx, env, jobs.ScheduledJobTxn(txn), createdBy.ScheduleID(),
	)
	if err != nil {
		return err
	}

	// If chaining of protected timestamp records is disabled, noop.
	if !args.ChainProtectedTimestampRecords {
		return nil
	}

	if args.BackupType == backuppb.ScheduledBackupExecutionArgs_FULL {
		// Check if there is a dependent incremental schedule associated with the
		// full schedule running the current backup.
		//
		// If present, the full backup on successful completion, will release the
		// pts record found on the incremental schedule, and replace it with a new
		// pts record protecting after the EndTime of the full backup.
		if args.DependentScheduleID == 0 {
			return nil
		}

		_, incArgs, err := getScheduledBackupExecutionArgsFromSchedule(
			ctx, env, jobs.ScheduledJobTxn(txn), args.DependentScheduleID,
		)
		if err != nil {
			// We should always be able to resolve the dependent schedule ID. If the
			// incremental schedule was dropped then it would have unlinked itself
			// from the full schedule. Thus, we treat all errors as a problem.
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"dependent schedule %d could not be resolved", args.DependentScheduleID)
		}
		backupDetails.SchedulePTSChainingRecord = &jobspb.SchedulePTSChainingRecord{
			ProtectedTimestampRecord: incArgs.ProtectedTimestampRecord,
			Action:                   jobspb.SchedulePTSChainingRecord_RELEASE,
		}
	} else {
		// In the case of a scheduled incremental backup we save the pts record id
		// that the job should update on successful completion, to protect data
		// after the current backups' EndTime.
		// We save this information on the job instead of reading it from the
		// schedule on completion, to prevent an "overhang" incremental from
		// incorrectly pulling forward a pts record that was written by a new full
		// backup that completed while the incremental was still executing.
		//
		// NB: An overhang incremental is defined as a scheduled incremental backup
		// that appends to the old full backup chain, and completes after a new full
		// backup has started another chain.
		backupDetails.SchedulePTSChainingRecord = &jobspb.SchedulePTSChainingRecord{
			ProtectedTimestampRecord: args.ProtectedTimestampRecord,
			Action:                   jobspb.SchedulePTSChainingRecord_UPDATE,
		}
	}
	return nil
}

func getProtectedTimestampTargetForBackup(
	backupManifest *backuppb.BackupManifest,
) (*ptpb.Target, error) {
	if backupManifest.DescriptorCoverage == tree.AllDescriptors {
		return ptpb.MakeClusterTarget(), nil
	}

	if len(backupManifest.Tenants) > 0 {
		tenantID := make([]roachpb.TenantID, 0, len(backupManifest.Tenants))
		for _, tenant := range backupManifest.Tenants {
			tid, err := roachpb.MakeTenantID(tenant.ID)
			if err != nil {
				return nil, err
			}
			tenantID = append(tenantID, tid)
		}
		return ptpb.MakeTenantsTarget(tenantID), nil
	}

	// ResolvedCompleteDBs contains all the "complete" databases being backed up.
	//
	// This includes explicit `BACKUP DATABASE` targets as well as expansions as a
	// result of `BACKUP TABLE db.*`. In both cases we want to write a protected
	// timestamp record that covers the entire database.
	if len(backupManifest.CompleteDbs) > 0 {
		return ptpb.MakeSchemaObjectsTarget(backupManifest.CompleteDbs), nil
	}

	// At this point we are dealing with a `BACKUP TABLE`, so we write a protected
	// timestamp record on each table being backed up.
	tableIDs := make(descpb.IDs, 0)
	for _, desc := range backupManifest.Descriptors {
		t, _, _, _, _ := descpb.GetDescriptors(&desc)
		if t != nil {
			tableIDs = append(tableIDs, t.GetID())
		}
	}
	return ptpb.MakeSchemaObjectsTarget(tableIDs), nil
}

func protectTimestampForBackup(
	ctx context.Context,
	jobID jobspb.JobID,
	pts protectedts.Storage,
	backupManifest *backuppb.BackupManifest,
	backupDetails jobspb.BackupDetails,
) error {
	tsToProtect := backupManifest.EndTime
	if !backupManifest.StartTime.IsEmpty() {
		tsToProtect = backupManifest.StartTime
	}

	// Resolve the target that the PTS record will protect as part of this
	// backup.
	target, err := getProtectedTimestampTargetForBackup(backupManifest)
	if err != nil {
		return err
	}

	// Records written by the backup job should be ignored when making GC
	// decisions on any table that has been marked as
	// `exclude_data_from_backup`. This ensures that the backup job does not
	// holdup GC on that table span for the duration of execution.
	target.IgnoreIfExcludedFromBackup = true
	return pts.Protect(ctx, jobsprotectedts.MakeRecord(
		*backupDetails.ProtectedTimestampRecord,
		int64(jobID),
		tsToProtect,
		backupManifest.Spans,
		jobsprotectedts.Jobs,
		target,
	))
}

func maybeRelocateJobExecution(
	ctx context.Context,
	jobID jobspb.JobID,
	p sql.JobExecContext,
	locality roachpb.Locality,
	jobDesc redact.SafeString,
) error {
	if locality.NonEmpty() {
		current, err := p.DistSQLPlanner().GetSQLInstanceInfo(p.ExecCfg().JobRegistry.ID())
		if err != nil {
			return err
		}
		if ok, missedTier := current.Locality.Matches(locality); !ok {
			log.Infof(ctx,
				"%s job %d initially adopted on instance %d but it does not match locality filter %s, finding a new coordinator",
				jobDesc, jobID, current.NodeID, missedTier.String(),
			)

			instancesInRegion, err := p.DistSQLPlanner().GetAllInstancesByLocality(ctx, locality)
			if err != nil {
				return err
			}
			rng, _ := randutil.NewPseudoRand()
			dest := instancesInRegion[rng.Intn(len(instancesInRegion))]

			var res error
			if err := p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				var err error
				res, err = p.ExecCfg().JobRegistry.RelocateLease(ctx, txn, jobID, dest.InstanceID, dest.SessionID)
				return err
			}); err != nil {
				return errors.Wrapf(err, "failed to relocate job coordinator to %d", dest.InstanceID)
			}
			return res
		}
	}
	return nil
}

// checkForNewTables returns an error if any new tables were introduced with the
// following exceptions:
// 1. A previous backup contained the entire DB.
// 2. The table was truncated after a previous backup was taken, so it's ID has
// changed.
func checkForNewTables(
	ctx context.Context,
	codec keys.SQLCodec,
	db *kv.DB,
	targetDescs []catalog.Descriptor,
	tablesInPrev map[descpb.ID]struct{},
	dbsInPrev map[descpb.ID]struct{},
	priorIDs map[descpb.ID]descpb.ID,
	startTime hlc.Timestamp,
	endTime hlc.Timestamp,
) error {
	for _, d := range targetDescs {
		t, ok := d.(catalog.TableDescriptor)
		if !ok {
			continue
		}
		// If we're trying to use a previous backup for this table, ideally it
		// actually contains this table.
		if _, ok := tablesInPrev[t.GetID()]; ok {
			continue
		}
		// This table isn't in the previous backup... maybe was added to a
		// DB that the previous backup captured?
		if _, ok := dbsInPrev[t.GetParentID()]; ok {
			continue
		}
		// Maybe this table is missing from the previous backup because it was
		// truncated?
		if replacement := t.GetReplacementOf(); replacement.ID != descpb.InvalidID {
			// Check if we need to lazy-load the priorIDs (i.e. if this is the first
			// truncate we've encountered in non-MVCC backup).
			if priorIDs == nil {
				priorIDs = make(map[descpb.ID]descpb.ID)
				_, err := getAllDescChanges(ctx, codec, db, startTime, endTime, priorIDs)
				if err != nil {
					return err
				}
			}
			found := false
			for was := replacement.ID; was != descpb.InvalidID && !found; was = priorIDs[was] {
				_, found = tablesInPrev[was]
			}
			if found {
				continue
			}
		}
		return errors.Errorf("previous backup does not contain table %q", t.GetName())
	}
	return nil
}

func getTenantInfo(
	ctx context.Context, codec keys.SQLCodec, txn isql.Txn, jobDetails jobspb.BackupDetails,
) ([]roachpb.Span, []mtinfopb.TenantInfoWithUsage, error) {
	var spans []roachpb.Span
	var tenants []mtinfopb.TenantInfoWithUsage
	var err error
	if jobDetails.FullCluster && codec.ForSystemTenant() && jobDetails.IncludeAllSecondaryTenants {
		// Include all tenants.
		tenants, err = retrieveAllTenantsMetadata(ctx, txn)
		if err != nil {
			return nil, nil, err
		}
	} else if len(jobDetails.SpecificTenantIds) > 0 {
		for _, id := range jobDetails.SpecificTenantIds {
			tenantInfo, err := retrieveSingleTenantMetadata(ctx, txn, id)
			if err != nil {
				return nil, nil, err
			}
			tenants = append(tenants, tenantInfo)
		}
	}
	if len(tenants) > 0 && jobDetails.RevisionHistory {
		return spans, tenants, errors.UnimplementedError(
			errors.IssueLink{IssueURL: build.MakeIssueURL(47896)},
			"can not backup tenants with revision history",
		)
	}
	for i := range tenants {
		// NB: We use MustMakeTenantID here since the data is
		// coming from the tenants table and we should only
		// ever have valid tenant IDs returned to us.
		prefix := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(tenants[i].ID))

		spans = append(spans, backupTenantSpan(prefix))
	}
	return spans, tenants, nil
}

// backupTenantSpan creates span to back up a whole tenant. We append MaxKey to
// the tenant's prefix rather than using PrefixEnd since we want an EndKey that
// remains within the tenant's prefix and does not equal the next tenant's start
// key, as that can cause our spans to get merged but they need to remain
// separate for backup prefix elision.
func backupTenantSpan(prefix roachpb.Key) roachpb.Span {
	return roachpb.Span{Key: prefix, EndKey: append(prefix, keys.MaxKey...)}
}

// checkForNewDatabases returns an error if any new complete databases were
// introduced.
func checkForNewCompleteDatabases(
	targetDescs []catalog.Descriptor, curDBs []descpb.ID, prevDBs map[descpb.ID]struct{},
) error {
	for _, dbID := range curDBs {
		if _, inPrevious := prevDBs[dbID]; !inPrevious {
			// Search for the name for a nicer error message.
			violatingDatabase := strconv.Itoa(int(dbID))
			for _, desc := range targetDescs {
				if desc.GetID() == dbID {
					violatingDatabase = desc.GetName()
					break
				}
			}
			return errors.Errorf("previous backup does not contain the complete database %q",
				violatingDatabase)
		}
	}
	return nil
}

func createBackupManifest(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tenantSpans []roachpb.Span,
	tenantInfos []mtinfopb.TenantInfoWithUsage,
	jobDetails jobspb.BackupDetails,
	prevBackups []backuppb.BackupManifest,
	layerToIterFactory backupinfo.LayerToBackupManifestFileIterFactory,
) (backuppb.BackupManifest, error) {
	mvccFilter := backuppb.MVCCFilter_Latest
	if jobDetails.RevisionHistory {
		mvccFilter = backuppb.MVCCFilter_All
	}
	endTime := jobDetails.EndTime
	var targetDescs []catalog.Descriptor
	var descriptorProtos []descpb.Descriptor
	var err error
	if jobDetails.FullCluster {
		targetDescs, _, err = fullClusterTargetsBackup(ctx, execCfg, endTime)
		if err != nil {
			return backuppb.BackupManifest{}, err
		}
		descriptorProtos = make([]descpb.Descriptor, len(targetDescs))
		for i, desc := range targetDescs {
			descriptorProtos[i] = *desc.DescriptorProto()
		}
	} else {
		descriptorProtos = jobDetails.ResolvedTargets
		targetDescs = make([]catalog.Descriptor, 0, len(descriptorProtos))
		for i := range descriptorProtos {
			targetDescs = append(targetDescs, backupinfo.NewDescriptorForManifest(&descriptorProtos[i]))
		}
	}

	startTime := jobDetails.StartTime

	var tables []catalog.TableDescriptor
	statsFiles := make(map[descpb.ID]string)
	for _, desc := range targetDescs {
		switch desc := desc.(type) {
		case catalog.TableDescriptor:
			tables = append(tables, desc)
			// TODO (anzo): look into the tradeoffs of having all objects in the array to be in the same file,
			// vs having each object in a separate file, or somewhere in between.
			statsFiles[desc.GetID()] = backupinfo.BackupStatisticsFileName
		}
	}

	var newSpans roachpb.Spans
	var priorIDs map[descpb.ID]descpb.ID

	var revs []backuppb.BackupManifest_DescriptorRevision
	if mvccFilter == backuppb.MVCCFilter_All {
		priorIDs = make(map[descpb.ID]descpb.ID)
		revs, err = getRelevantDescChanges(ctx, execCfg, startTime, endTime, targetDescs,
			jobDetails.ResolvedCompleteDbs, priorIDs, jobDetails.FullCluster)
		if err != nil {
			return backuppb.BackupManifest{}, err
		}
	}

	var spans []roachpb.Span
	var tenants []mtinfopb.TenantInfoWithUsage
	spans = append(spans, tenantSpans...)
	tenants = append(tenants, tenantInfos...)

	tableSpans, err := spansForAllTableIndexes(execCfg, tables, revs)
	if err != nil {
		return backuppb.BackupManifest{}, err
	}
	spans = append(spans, tableSpans...)

	if len(prevBackups) > 0 {
		tablesInPrev := make(map[descpb.ID]struct{})
		dbsInPrev := make(map[descpb.ID]struct{})

		descIt := layerToIterFactory[len(prevBackups)-1].NewDescIter(ctx)
		defer descIt.Close()
		for ; ; descIt.Next() {
			if ok, err := descIt.Valid(); err != nil {
				return backuppb.BackupManifest{}, err
			} else if !ok {
				break
			}

			if t, _, _, _, _ := descpb.GetDescriptors(descIt.Value()); t != nil {
				tablesInPrev[t.ID] = struct{}{}
			}
		}
		for _, d := range prevBackups[len(prevBackups)-1].CompleteDbs {
			dbsInPrev[d] = struct{}{}
		}

		if !jobDetails.FullCluster {
			if err := checkForNewTables(ctx, execCfg.Codec, execCfg.DB, targetDescs, tablesInPrev, dbsInPrev, priorIDs, startTime, endTime); err != nil {
				return backuppb.BackupManifest{}, err
			}
			// Let's check that we're not widening the scope of this backup to an
			// entire database, even if no tables were created in the meantime.
			if err := checkForNewCompleteDatabases(targetDescs, jobDetails.ResolvedCompleteDbs, dbsInPrev); err != nil {
				return backuppb.BackupManifest{}, err
			}
		}

		newSpans = filterSpans(spans, prevBackups[len(prevBackups)-1].Spans)
	}

	// if CompleteDbs is lost by a 1.x node, FormatDescriptorTrackingVersion
	// means that a 2.0 node will disallow `RESTORE DATABASE foo`, but `RESTORE
	// foo.table1, foo.table2...` will still work. MVCCFilter would be
	// mis-handled, but is disallowed above. IntroducedSpans may also be lost by
	// a 1.x node, meaning that if 1.1 nodes may resume a backup, the limitation
	// of requiring full backups after schema changes remains.

	coverage := tree.RequestedDescriptors
	if jobDetails.FullCluster {
		coverage = tree.AllDescriptors
	}
	elide := execinfrapb.ElidePrefix_None
	if len(prevBackups) > 0 {
		elide = prevBackups[0].ElidedPrefix
	} else {
		if len(tenants) > 0 {
			elide = execinfrapb.ElidePrefix_Tenant
		} else {
			elide = execinfrapb.ElidePrefix_TenantAndTable
		}
	}

	backupManifest := backuppb.BackupManifest{
		StartTime:           startTime,
		EndTime:             endTime,
		MVCCFilter:          mvccFilter,
		Descriptors:         descriptorProtos,
		Tenants:             tenants,
		DescriptorChanges:   revs,
		CompleteDbs:         jobDetails.ResolvedCompleteDbs,
		Spans:               spans,
		IntroducedSpans:     newSpans,
		FormatVersion:       backupinfo.BackupFormatDescriptorTrackingVersion,
		BuildInfo:           build.GetInfo(),
		ClusterVersion:      execCfg.Settings.Version.ActiveVersion(ctx).Version,
		ClusterID:           execCfg.NodeInfo.LogicalClusterID(),
		StatisticsFilenames: statsFiles,
		DescriptorCoverage:  coverage,
		ElidedPrefix:        elide,
	}
	if err := checkCoverage(ctx, backupManifest.Spans, append(prevBackups, backupManifest)); err != nil {
		return backuppb.BackupManifest{}, errors.Wrap(err, "new backup would not cover expected time")
	}
	return backupManifest, nil
}

func updateBackupDetails(
	ctx context.Context,
	details jobspb.BackupDetails,
	collectionURI string,
	defaultURI string,
	resolvedSubdir string,
	urisByLocalityKV map[string]string,
	prevBackups []backuppb.BackupManifest,
	encryptionOptions *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (jobspb.BackupDetails, error) {
	var err error
	var startTime hlc.Timestamp
	if len(prevBackups) > 0 {
		startTime = prevBackups[len(prevBackups)-1].EndTime
	}

	// If we didn't load any prior backups from which get encryption info, we
	// need to generate encryption specific data.
	var encryptionInfo *jobspb.EncryptionInfo
	if encryptionOptions == nil {
		encryptionOptions, encryptionInfo, err = backupencryption.MakeNewEncryptionOptions(ctx, details.EncryptionOptions, kmsEnv)
		if err != nil {
			return jobspb.BackupDetails{}, err
		}
	}

	details.Destination = jobspb.BackupDetails_Destination{Subdir: resolvedSubdir}
	details.StartTime = startTime
	details.URI = defaultURI
	details.URIsByLocalityKV = urisByLocalityKV
	details.EncryptionOptions = encryptionOptions
	details.EncryptionInfo = encryptionInfo
	details.CollectionURI = collectionURI

	return details, nil
}

func getBackupDetailAndManifest(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tenantSpans []roachpb.Span,
	tenantInfos []mtinfopb.TenantInfoWithUsage,
	initialDetails jobspb.BackupDetails,
	user username.SQLUsername,
	backupDestination backupdest.ResolvedDestination,
) (jobspb.BackupDetails, *backuppb.BackupManifest, error) {
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
			backupDestination.PrevBackupURIs[0], initialDetails.EncryptionOptions, &kmsEnv)
		if err != nil {
			return jobspb.BackupDetails{}, nil, err
		}

		var memSize int64
		prevBackups, memSize, err = backupinfo.GetBackupManifests(ctx, &mem, user,
			makeCloudStorage, backupDestination.PrevBackupURIs, baseEncryptionOptions, &kmsEnv)

		if err != nil {
			return jobspb.BackupDetails{}, nil, err
		}
		defer mem.Shrink(ctx, memSize)
	}

	if len(prevBackups) > 0 {
		baseManifest := prevBackups[0]
		if baseManifest.DescriptorCoverage == tree.AllDescriptors &&
			!initialDetails.FullCluster {
			return jobspb.BackupDetails{}, nil, errors.Errorf("cannot append a backup of specific tables or databases to a cluster backup")
		}

		lastEndTime := prevBackups[len(prevBackups)-1].EndTime
		if lastEndTime.Compare(initialDetails.EndTime) > 0 {
			return jobspb.BackupDetails{}, nil,
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
			return jobspb.BackupDetails{}, nil, errors.Newf("previous BACKUP belongs to cluster %s", fromCluster.String())
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
			return jobspb.BackupDetails{}, nil, errors.Newf(
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
		return jobspb.BackupDetails{}, nil, err
	}

	layerToIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx, execCfg.DistSQLSrv.ExternalStorage, prevBackups, baseEncryptionOptions, &kmsEnv)
	if err != nil {
		return jobspb.BackupDetails{}, nil, err
	}

	backupManifest, err := createBackupManifest(
		ctx,
		execCfg,
		tenantSpans,
		tenantInfos,
		updatedDetails,
		prevBackups,
		layerToIterFactory,
	)
	if err != nil {
		return jobspb.BackupDetails{}, nil, err
	}

	return updatedDetails, &backupManifest, nil
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
	ctx context.Context, jobState jobs.State, knobs *jobs.TestingKnobs, db isql.DB,
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

		scheduleID := jobspb.ScheduleID(tree.MustBeDInt(datums[0]))
		if err := jobs.NotifyJobTermination(ctx, txn, env, b.job.ID(), jobState, b.job.Details(), scheduleID); err != nil {
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
	logutil.LogJobCompletion(ctx, b.getTelemetryEventType(), b.job.ID(), false, jobErr, b.backupStats.Rows)

	p := execCtx.(sql.JobExecContext)
	cfg := p.ExecCfg()
	details := b.job.Details().(jobspb.BackupDetails)

	b.deleteCheckpoint(ctx, cfg, p.User())
	if err := cfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		pts := cfg.ProtectedTimestampProvider.WithTxn(txn)
		return releaseProtectedTimestamp(ctx, pts, details.ProtectedTimestampRecord)
	}); err != nil {
		return err
	}

	// If this backup should update cluster monitoring metrics, update the
	// metrics.
	if details.UpdatesClusterMonitoringMetrics {
		metrics := p.ExecCfg().JobRegistry.MetricsStruct().Backup.(*BackupMetrics)
		if cloud.IsKMSInaccessible(jobErr) {
			now := timeutil.Now()
			metrics.LastKMSInaccessibleErrorTime.Update(now.Unix())
		}
	}

	// This should never return an error unless resolving the schedule that the
	// job is being run under fails. This could happen if the schedule is dropped
	// while the job is executing.
	if err := b.maybeNotifyScheduledJobCompletion(
		ctx, jobs.StateFailed, cfg.JobsKnobs(), cfg.InternalDB,
	); err != nil {
		log.Errorf(ctx, "failed to notify job %d on completion of OnFailOrCancel: %+v",
			b.job.ID(), err)
	}
	return nil //nolint:returnerrcheck
}

// CollectProfile is a part of the Resumer interface.
func (b *backupResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)

	// TODO(adityamaru): We should move the logic that decorates the DistSQL
	// diagram with per processor progress to this method.

	var aggStatsCopy bulkutil.ComponentAggregatorStats
	func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		aggStatsCopy = b.mu.perNodeAggregatorStats.DeepCopy()
	}()
	return bulkutil.FlushTracingAggregatorStats(ctx, b.job.ID(),
		p.ExecCfg().InternalDB, aggStatsCopy)
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

// maybeWriteBackupLock attempts to write a backup lock for the given jobID, if
// it does not already exist. If another backup lock file for another job is
// found, it will return an error.
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
	if err := execCtx.ExecCfg().JobRegistry.CheckPausepoint("backup.before.write_lock"); err != nil {
		return err
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
	if err := backupinfo.WriteBackupLock(
		ctx,
		execCtx.ExecCfg(),
		dest.DefaultURI,
		jobID,
		execCtx.User(),
	); err != nil {
		return err
	}
	return execCtx.ExecCfg().JobRegistry.CheckPausepoint("backup.after.write_lock")
}

// getCompletedSpans inspects a backup manifest and returns all spans and
// introduced spans that have already been backed up.
func getCompletedSpans(
	ctx context.Context,
	execCtx sql.JobExecContext,
	backupManifest *backuppb.BackupManifest,
	defaultStore cloud.ExternalStorage,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) ([]roachpb.Span, []roachpb.Span, error) {
	var completedSpans, completedIntroducedSpans []roachpb.Span
	// TODO(benesch): verify these files, rather than accepting them as truth
	// blindly.
	// No concurrency yet, so these assignments are safe.
	iterFactory := backupinfo.NewIterFactory(backupManifest, defaultStore, encryption, kmsEnv)
	it, err := iterFactory.NewFileIter(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return nil, nil, err
		} else if !ok {
			break
		}

		f := it.Value()
		if f.StartTime.IsEmpty() && !f.EndTime.IsEmpty() {
			completedIntroducedSpans = append(completedIntroducedSpans, f.Span)
		} else {
			completedSpans = append(completedSpans, f.Span)
		}
	}

	// Add the spans for any tables that are excluded from backup to the set of
	// already-completed spans, as there is nothing to do for them.
	descs := iterFactory.NewDescIter(ctx)
	defer descs.Close()
	for ; ; descs.Next() {
		if ok, err := descs.Valid(); err != nil {
			return nil, nil, err
		} else if !ok {
			break
		}

		if tbl, _, _, _, _ := descpb.GetDescriptors(descs.Value()); tbl != nil && tbl.ExcludeDataFromBackup {
			prefix := execCtx.ExecCfg().Codec.TablePrefix(uint32(tbl.ID))
			completedSpans = append(completedSpans, roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()})
		}
	}
	return completedSpans, completedIntroducedSpans, nil
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
			b := &backupResumer{
				job: job,
			}
			b.mu.perNodeAggregatorStats = make(bulkutil.ComponentAggregatorStats)
			return b
		},
		jobs.UsesTenantCostControl,
	)
}
