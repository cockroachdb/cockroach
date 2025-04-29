// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backupdest"
	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

var (
	backupCompactionThreshold = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"backup.compaction.threshold",
		"the required backup chain length for compaction to be triggered (0 to disable compactions)",
		0,
		settings.WithVisibility(settings.Reserved),
		settings.IntInRangeOrZeroDisable(3, math.MaxInt64),
	)
)

// maybeStartCompactionJob will initiate a compaction job off of a triggering
// incremental job if the backup chain length exceeds the threshold.
// backupStmt should be the original backup statement that triggered the job.
// It is the responsibility of the caller to ensure that the backup details'
// destination contains a resolved subdir.
func maybeStartCompactionJob(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	triggerJob jobspb.BackupDetails,
) (jobspb.JobID, error) {
	threshold := backupCompactionThreshold.Get(&execCfg.Settings.SV)
	if threshold == 0 || triggerJob.StartTime.IsEmpty() {
		return 0, nil
	}

	switch {
	case len(triggerJob.ExecutionLocality.Tiers) != 0:
		return 0, errors.New("execution locality not supported for compaction")
	case triggerJob.RevisionHistory:
		return 0, errors.New("revision history not supported for compaction")
	case triggerJob.ScheduleID == 0:
		return 0, errors.New("only scheduled backups can be compacted")
	case len(triggerJob.Destination.IncrementalStorage) != 0:
		return 0, errors.New("custom incremental storage location not supported for compaction")
	case len(triggerJob.SpecificTenantIds) != 0 || triggerJob.IncludeAllSecondaryTenants:
		return 0, errors.New("backups of tenants not supported for compaction")
	case len(triggerJob.URIsByLocalityKV) != 0:
		return 0, errors.New("locality aware backups not supported for compaction")
	}

	env := scheduledjobs.ProdJobSchedulerEnv
	knobs := execCfg.JobsKnobs()
	if knobs != nil && knobs.JobSchedulerEnv != nil {
		env = knobs.JobSchedulerEnv
	}
	kmsEnv := backupencryption.MakeBackupKMSEnv(
		execCfg.Settings,
		&execCfg.ExternalIODirConfig,
		execCfg.InternalDB,
		user,
	)
	chain, _, _, _, err := getBackupChain(
		ctx, execCfg, user, triggerJob.Destination, triggerJob.EncryptionOptions,
		triggerJob.EndTime, &kmsEnv,
	)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get backup chain")
	}
	if int64(len(chain)) < threshold {
		return 0, nil
	}
	var backupStmt string
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, args, err := getScheduledBackupExecutionArgsFromSchedule(
			ctx, env, jobs.ScheduledJobTxn(txn), triggerJob.ScheduleID,
		)
		if err != nil {
			return errors.Wrapf(
				err, "failed to get scheduled backup execution args for schedule %d", triggerJob.ScheduleID,
			)
		}
		backupStmt = args.BackupStatement
		return nil
	}); err != nil {
		return 0, err
	}
	start, end, err := minSizeDeltaHeuristic(ctx, execCfg, chain)
	if err != nil {
		return 0, err
	}
	startTS, endTS := chain[start].StartTime, chain[end-1].EndTime
	log.Infof(ctx, "compacting backups from %s to %s", startTS, endTS)
	var jobID jobspb.JobID
	err = execCfg.InternalDB.Txn(ctx, func(ctx context.Context,
		txn isql.Txn) error {
		datums, err := txn.QueryRowEx(
			ctx,
			"start-compaction-job",
			txn.KV(),
			sessiondata.NoSessionDataOverride,
			`SELECT crdb_internal.backup_compaction($1, $2, $3::DECIMAL, $4::DECIMAL)`,
			backupStmt,
			triggerJob.Destination.Subdir,
			startTS.AsOfSystemTime(),
			endTS.AsOfSystemTime(),
		)
		if err != nil {
			return err
		}
		idDatum, ok := tree.AsDInt(datums[0])
		if !ok {
			return errors.Newf("expected job ID: unexpected result type %T", datums[0])
		}
		jobID = jobspb.JobID(idDatum)
		return nil
	})
	return jobID, err
}

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
	prevManifests, localityInfo, baseEncryptionOpts, allIters, err := getBackupChain(
		ctx, execCtx.ExecCfg(), execCtx.User(), initialDetails.Destination,
		initialDetails.EncryptionOptions, initialDetails.EndTime, kmsEnv,
	)
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
	testingKnobs := execCtx.ExecCfg().BackupRestoreTestingKnobs
	if initialDetails.URI == "" {
		if testingKnobs != nil && testingKnobs.RunBeforeResolvingCompactionDest != nil {
			if err := testingKnobs.RunBeforeResolvingCompactionDest(); err != nil {
				return err
			}
		}
		// Resolve the backup destination. If we have already resolved and persisted
		// the destination during a previous resumption of this job, we can re-use
		// the previous resolution.
		backupDest, err := backupdest.ResolveDest(
			ctx, execCtx.User(), initialDetails.Destination,
			initialDetails.StartTime, initialDetails.EndTime,
			execCtx.ExecCfg(), initialDetails.EncryptionOptions, kmsEnv,
		)
		if err != nil {
			return err
		}
		if err = maybeWriteBackupLock(ctx, execCtx, backupDest, b.job.ID()); err != nil {
			return err
		}
		updatedDetails, err = updateCompactionBackupDetails(
			ctx, compactChain, initialDetails, backupDest, baseEncryptionOpts,
		)
		if err != nil {
			return err
		}
		backupManifest, err = compactChain.createCompactionManifest(ctx, updatedDetails)
		if err != nil {
			return err
		}

		if err := backupinfo.WriteBackupManifestCheckpoint(
			ctx, updatedDetails.URI, updatedDetails.EncryptionOptions, kmsEnv,
			backupManifest, execCtx.ExecCfg(), execCtx.User(),
		); err != nil {
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

		if err := execCtx.ExecCfg().JobRegistry.CheckPausepoint("backup_compaction.after.details_has_checkpoint"); err != nil {
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

	if testingKnobs != nil && testingKnobs.AfterLoadingCompactionManifestOnResume != nil {
		testingKnobs.AfterLoadingCompactionManifestOnResume(backupManifest)
	}

	// We retry on pretty generic failures -- any rpc error. If a worker node were
	// to restart, it would produce this kind of error, but there may be other
	// errors that are also rpc errors. Don't retry too aggressively.
	retryOpts := retry.Options{
		MaxBackoff: 1 * time.Second,
		MaxRetries: 5,
	}

	if testingKnobs != nil && testingKnobs.BackupDistSQLRetryPolicy != nil {
		retryOpts = *testingKnobs.BackupDistSQLRetryPolicy
	}

	// We want to retry a backup if there are transient failures (i.e. worker nodes
	// dying), so if we receive a retryable error, re-plan and retry the backup.
	// TODO (kev-cao): Add progress tracking to compactions.
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		if err = doCompaction(
			ctx, execCtx, b.job.ID(), updatedDetails, compactChain, backupManifest, defaultStore, kmsEnv,
		); err == nil {
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

	return b.processScheduledBackupCompletion(ctx, jobs.StateSucceeded, execCtx, updatedDetails)
}

type compactionChain struct {
	// backupChain is the linear chain of backups up to the end time required
	// for a restore.
	backupChain    []backuppb.BackupManifest
	chainToCompact []backuppb.BackupManifest
	// start refers to the start time of the first backup to be compacted. This
	// will always be greater than 0 as a full backup is not a candidate for
	// compaction.
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

// lastBackup returns the last backup of the chain to compact.
func (c *compactionChain) lastBackup() backuppb.BackupManifest {
	return c.backupChain[c.endIdx-1]
}

// newCompactionChain returns a new compacted backup chain based on the specified start and end
// timestamps from a chain of backups. The start and end times must specify specific backups.
// layerToIterFactory must map accordingly to the passed in manifests.
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

// updateCompactionBackupDetails takes a backup chain (up until the end timestamp)
// and returns a corresponding BackupDetails for the compacted
// backup of backups from the start timestamp to the end timestamp.
func updateCompactionBackupDetails(
	ctx context.Context,
	compactionChain compactionChain,
	initialDetails jobspb.BackupDetails,
	resolvedDest backupdest.ResolvedDestination,
	baseEncryptOpts *jobspb.BackupEncryptionOptions,
) (jobspb.BackupDetails, error) {
	if len(compactionChain.chainToCompact) == 0 {
		return jobspb.BackupDetails{}, errors.New("no backup manifests to compact")
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
		Destination:      destination,
		StartTime:        compactionChain.start,
		EndTime:          compactionChain.end,
		URI:              resolvedDest.DefaultURI,
		URIsByLocalityKV: resolvedDest.URIsByLocalityKV,
		// Because we cannot have nice things, we persist a semi hydrated version of
		// EncryptionOptions at planning, and then add the key at the beginning of
		// resume using the same data structure in details. NB: EncryptionInfo is
		// left blank and only added on the full backup (i think).
		EncryptionOptions:   baseEncryptOpts,
		CollectionURI:       resolvedDest.CollectionURI,
		ResolvedTargets:     allDescsPb,
		ResolvedCompleteDbs: lastBackup.CompleteDbs,
		FullCluster:         lastBackup.DescriptorCoverage == tree.AllDescriptors,
		Compact:             true,
	}
	return compactedDetails, nil
}

// createCompactionManifest creates a new manifest for a compaction job and its
// compacted chain. The details should have its targets resolved.
func (c compactionChain) createCompactionManifest(
	ctx context.Context, details jobspb.BackupDetails,
) (*backuppb.BackupManifest, error) {
	// TODO (kev-cao): Will need to update the SSTSinkKeyWriter to support
	// range keys.
	lastBackup := c.lastBackup()
	if len(lastBackup.Tenants) != 0 {
		return nil, errors.New("backup compactions does not support range keys")
	}
	if err := checkCoverage(ctx, c.lastBackup().Spans, c.backupChain); err != nil {
		return nil, err
	}
	// In compaction, any span that is not included in the backup *preceding* the
	// compaction chain is considered introduced.
	introducedSpans := filterSpans(
		c.lastBackup().Spans,
		c.backupChain[c.startIdx-1].Spans,
	)
	cManifest := lastBackup
	cManifest.ID = uuid.MakeV4()
	cManifest.StartTime = c.start
	cManifest.Descriptors = details.ResolvedTargets
	cManifest.IntroducedSpans = introducedSpans
	cManifest.IsCompacted = true

	cManifest.Dir = cloudpb.ExternalStorage{}
	// As this manifest will be used prior to the manifest actually being written
	// (and therefore its external files being created) we first set
	// HasExternalManifestSSTs to false and allow it to be set to true later when
	// it is written to storage.
	cManifest.HasExternalManifestSSTs = false
	// While we do not compaction on revision history backups, we still need to
	// nil out DescriptorChanges to avoid the placeholder descriptor from the
	// manifest of the last incremental.
	cManifest.DescriptorChanges = nil
	cManifest.Files = nil
	cManifest.EntryCounts = roachpb.RowCount{}

	// The StatisticsFileNames is inherited from the stats of the latest
	// incremental we are compacting as the compacted manifest will have the same
	// targets as the last incremental.
	return &cManifest, nil
}

// resolveBackupSubdir returns the resolved base full backup subdirectory from a
// specified sub-directory. subdir may be a specified path or the string
// "LATEST" to resolve the latest subdirectory.
func resolveBackupSubdir(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	mainFullBackupURI string,
	subdir string,
) (string, error) {
	if strings.EqualFold(subdir, backupbase.LatestFileName) {
		latest, err := backupdest.ReadLatestFile(ctx, mainFullBackupURI,
			execCfg.DistSQLSrv.ExternalStorageFromURI, user)
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
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	collectionURIs []string,
	incrementalURIs []string,
	subdir string,
) ([]string, []string, string, error) {
	resolvedSubdir, err := resolveBackupSubdir(ctx, execCfg, user, collectionURIs[0], subdir)
	if err != nil {
		return nil, nil, "", err
	}
	resolvedBaseDirs, err := backuputils.AppendPaths(collectionURIs[:], resolvedSubdir)
	if err != nil {
		return nil, nil, "", err
	}
	resolvedIncDirs, err := backupdest.ResolveIncrementalsBackupLocation(
		ctx, user, execCfg, incrementalURIs, collectionURIs, resolvedSubdir,
	)
	if err != nil {
		return nil, nil, "", err
	}
	return resolvedBaseDirs, resolvedIncDirs, resolvedSubdir, nil
}

// getBackupChain fetches the current shortest chain of backups (and its
// associated info) required to restore to the end time specified in the details.
func getBackupChain(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	dest jobspb.BackupDetails_Destination,
	encryptionOpts *jobspb.BackupEncryptionOptions,
	endTime hlc.Timestamp,
	kmsEnv cloud.KMSEnv,
) (
	[]backuppb.BackupManifest,
	[]jobspb.RestoreDetails_BackupLocalityInfo,
	*jobspb.BackupEncryptionOptions,
	map[int]*backupinfo.IterFactory,
	error,
) {
	resolvedBaseDirs, resolvedIncDirs, _, err := resolveBackupDirs(
		ctx, execCfg, user, dest.To, dest.IncrementalStorage, dest.Subdir,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	mkStore := execCfg.DistSQLSrv.ExternalStorageFromURI
	baseStores, baseCleanup, err := backupdest.MakeBackupDestinationStores(
		ctx, user, mkStore, resolvedBaseDirs,
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
		ctx, user, mkStore, resolvedIncDirs,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer func() {
		if err := incCleanup(); err != nil {
			log.Warningf(ctx, "failed to cleanup incremental backup stores: %+v", err)
		}
	}()
	baseEncryptionInfo := encryptionOpts
	if encryptionOpts != nil && !encryptionOpts.HasKey() {
		baseEncryptionInfo, err = backupencryption.GetEncryptionFromBaseStore(
			ctx, baseStores[0], encryptionOpts, kmsEnv,
		)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}

	mem := execCfg.RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	_, manifests, localityInfo, memReserved, err := backupdest.ResolveBackupManifests(
		ctx, &mem, baseStores, incStores, mkStore, resolvedBaseDirs,
		resolvedIncDirs, endTime, baseEncryptionInfo, kmsEnv,
		user, false /*includeSkipped */, true, /*includeCompacted */
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer func() {
		mem.Shrink(ctx, memReserved)
	}()
	allIters, err := backupinfo.GetBackupManifestIterFactories(
		ctx, execCfg.DistSQLSrv.ExternalStorage, manifests, baseEncryptionInfo, kmsEnv,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return manifests, localityInfo, baseEncryptionInfo, allIters, nil
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

	statsTable := getTableStatsForBackup(ctx, execCtx.ExecCfg().InternalDB.Executor(), backupManifest.Descriptors)
	return backupinfo.WriteTableStatistics(ctx, store, encryption, kmsEnv, &statsTable)
}

// processProgress processes progress updates from the bulk processor for a backup and updates
// the associated manifest.
func processProgress(
	ctx context.Context,
	execCtx sql.JobExecContext,
	details jobspb.BackupDetails,
	manifest *backuppb.BackupManifest,
	progCh <-chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	kmsEnv cloud.KMSEnv,
) error {
	var lastCheckpointTime time.Time
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

		// TODO (kev-cao): Add per node progress updates.

		if wroteCheckpoint, err := maybeWriteBackupCheckpoint(
			ctx, execCtx, details, manifest, lastCheckpointTime, kmsEnv,
		); err != nil {
			log.Errorf(ctx, "unable to checkpoint compaction: %+v", err)
		} else if wroteCheckpoint {
			lastCheckpointTime = timeutil.Now()
			if err := execCtx.ExecCfg().JobRegistry.CheckPausepoint("backup_compaction.after.write_checkpoint"); err != nil {
				return err
			}
		}
	}
	return nil
}

// compactionJobDescription generates a redacted description of the job.
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

// doCompaction initiates and manages the entire backup compaction process.
//
// This function starts the compaction process, handles checkpointing and tracing
// during the compaction, and concludes the compaction by writing the necessary
// metadata. It coordinates the execution of the compaction plan and processes
// progress updates to generate the backup manifest.
func doCompaction(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	details jobspb.BackupDetails,
	compactChain compactionChain,
	manifest *backuppb.BackupManifest,
	defaultStore cloud.ExternalStorage,
	kmsEnv cloud.KMSEnv,
) error {
	progCh := make(chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
	runDistCompaction := func(ctx context.Context) error {
		defer close(progCh)
		return runCompactionPlan(
			ctx, execCtx, jobID, details, compactChain, manifest, defaultStore, kmsEnv, progCh,
		)
	}
	checkpointLoop := func(ctx context.Context) error {
		return processProgress(ctx, execCtx, details, manifest, progCh, kmsEnv)
	}
	// TODO (kev-cao): Add trace aggregator loop.

	if err := ctxgroup.GoAndWait(
		ctx, runDistCompaction, checkpointLoop,
	); err != nil {
		return err
	}

	return concludeBackupCompaction(
		ctx, execCtx, defaultStore, details.EncryptionOptions, kmsEnv, manifest,
	)
}

// maybeWriteBackupCheckpoint writes a checkpoint for the backup if
// the time since the last checkpoint exceeds the configured interval. If a
// checkpoint is written, the function returns true.
func maybeWriteBackupCheckpoint(
	ctx context.Context,
	execCtx sql.JobExecContext,
	details jobspb.BackupDetails,
	manifest *backuppb.BackupManifest,
	lastCheckpointTime time.Time,
	kmsEnv cloud.KMSEnv,
) (bool, error) {
	if details.URI == "" {
		return false, errors.New("backup details does not contain a default URI")
	}
	execCfg := execCtx.ExecCfg()
	interval := BackupCheckpointInterval.Get(&execCfg.Settings.SV)
	if timeutil.Since(lastCheckpointTime) < interval {
		return false, nil
	}
	if err := backupinfo.WriteBackupManifestCheckpoint(
		ctx, details.URI, details.EncryptionOptions, kmsEnv,
		manifest, execCfg, execCtx.User(),
	); err != nil {
		return false, err
	}
	return true, nil
}

func init() {
	builtins.StartCompactionJob = StartCompactionJob
}
