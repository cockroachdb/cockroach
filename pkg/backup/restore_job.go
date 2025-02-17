// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/externalcatalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/ingesting"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/rewrite"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbackup"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	bulkutil "github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// restoreStatsInsertBatchSize is an arbitrarily chosen value of the number of
// tables we process in a single txn when restoring their table statistics.
const restoreStatsInsertBatchSize = 10

var restoreStatsInsertionConcurrency = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.restore.insert_stats_workers",
	"number of concurrent workers that will restore backed up table statistics",
	5,
	settings.PositiveInt,
)

var replanFrequency = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"bulkio.restore.replan_flow_frequency",
	"frequency at which RESTORE checks the number of lagging nodes to see if the job should be replanned",
	time.Minute*10,
	settings.PositiveDuration,
)

var laggingRestoreProcErr = errors.New("try re-planning due to lagging restore processors")

// rewriteBackupSpanKey rewrites a backup span start key for the purposes of
// splitting up the target key-space to send out the actual work of restoring.
//
// Keys for the primary index of the top-level table are rewritten to the just
// the overall start of the table. That is, /Table/51/1 becomes /Table/51.
//
// Any suffix of the key that does is not rewritten by kr's configured rewrites
// is truncated. For instance if a passed span has key /Table/51/1/77#/53/2/1
// but kr only configured with a rewrite for 51, it would return /Table/51/1/77.
// Such span boundaries are usually due to a interleaved table which has since
// been dropped -- any splits that happened to pick one of its rows live on, but
// include an ID of a table that no longer exists.
//
// Note that the actual restore process (i.e the restore processor method which
// writes the KVs) does not use these keys -- they are only used to split the
// key space and distribute those requests, thus truncation is fine. In the rare
// case where multiple backup spans are truncated to the same prefix (i.e.
// entire spans resided under the same interleave parent row) we'll generate
// some no-op splits and route the work to the same range, but the actual
// imported data is unaffected.
func rewriteBackupSpanKey(
	codec keys.SQLCodec, kr *KeyRewriter, key roachpb.Key,
) (roachpb.Key, error) {
	newKey, rewritten, err := kr.RewriteKey(append([]byte(nil), key...),
		0 /*wallTimeForImportElision*/)
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"could not rewrite span start key: %s", key)
	}
	if !rewritten && bytes.Equal(newKey, key) {
		// if nothing was changed, we didn't match the top-level key at all.
		return nil, errors.AssertionFailedf(
			"no rewrite for span start key: %s", key)
	}
	if bytes.HasPrefix(newKey, keys.TenantPrefix) {
		return newKey, nil
	}

	// Modify all spans that begin at the primary index to instead begin at the
	// start of the table. That is, change a span start key from /Table/51/1 to
	// /Table/51. Otherwise a permanently empty span at /Table/51-/Table/51/1
	// will be created.
	if b, id, idx, err := codec.DecodeIndexPrefix(newKey); err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"could not rewrite span start key: %s", key)
	} else if idx == 1 && len(b) == 0 {
		newKey = codec.TablePrefix(id)
	}
	return newKey, nil
}

func restoreWithRetry(
	restoreCtx context.Context,
	execCtx sql.JobExecContext,
	backupManifests []backuppb.BackupManifest,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
	dataToRestore restorationData,
	resumer *restoreResumer,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (roachpb.RowCount, error) {

	// We retry on pretty generic failures -- any rpc error. If a worker node were
	// to restart, it would produce this kind of error, but there may be other
	// errors that are also rpc errors. Don't retry to aggressively.
	retryOpts := retry.Options{
		MaxBackoff: 1 * time.Second,
		MaxRetries: 5,
	}
	if execCtx.ExecCfg().BackupRestoreTestingKnobs != nil &&
		execCtx.ExecCfg().BackupRestoreTestingKnobs.RestoreDistSQLRetryPolicy != nil {
		retryOpts = *execCtx.ExecCfg().BackupRestoreTestingKnobs.RestoreDistSQLRetryPolicy
	}

	// We want to retry a restore if there are transient failures (i.e. worker nodes
	// dying), so if we receive a retryable error, re-plan and retry the backup.
	var (
		res                    roachpb.RowCount
		err                    error
		previousPersistedSpans jobspb.RestoreFrontierEntries
		currentPersistedSpans  jobspb.RestoreFrontierEntries
	)

	for r := retry.StartWithCtx(restoreCtx, retryOpts); r.Next(); {
		res, err = restore(
			restoreCtx,
			execCtx,
			backupManifests,
			backupLocalityInfo,
			endTime,
			dataToRestore,
			resumer,
			encryption,
			kmsEnv,
		)

		if err == nil {
			break
		}

		if errors.HasType(err, &kvpb.InsufficientSpaceError{}) || errors.Is(err, restoreProcError) {
			return roachpb.RowCount{}, jobs.MarkPauseRequestError(errors.UnwrapAll(err))
		}

		if joberror.IsPermanentBulkJobError(err) && !errors.Is(err, retryableRestoreProcError) {
			return roachpb.RowCount{}, err
		}

		// If we are draining, it is unlikely we can start a
		// new DistSQL flow. Exit with a retryable error so
		// that another node can pick up the job.
		if execCtx.ExecCfg().JobRegistry.IsDraining() {
			return roachpb.RowCount{}, jobs.MarkAsRetryJobError(errors.Wrapf(err, "job encountered retryable error on draining node"))
		}

		log.Warningf(restoreCtx, "encountered retryable error: %+v", err)
		currentPersistedSpans = resumer.job.Progress().Details.(*jobspb.Progress_Restore).Restore.Checkpoint
		if !currentPersistedSpans.Equal(previousPersistedSpans) {
			// If the previous persisted spans are different than the current, it
			// implies that further progress has been persisted.
			r.Reset()
			log.Infof(restoreCtx, "restored frontier has advanced since last retry, resetting retry counter")
		}
		previousPersistedSpans = currentPersistedSpans

		testingKnobs := execCtx.ExecCfg().BackupRestoreTestingKnobs
		if testingKnobs != nil && testingKnobs.RunAfterRetryIteration != nil {
			if err := testingKnobs.RunAfterRetryIteration(err); err != nil {
				return roachpb.RowCount{}, err
			}
		}
	}

	// We have exhausted retries, but we have not seen a "PermanentBulkJobError" so
	// it is possible that this is a transient error that is taking longer than
	// our configured retry to go away.
	//
	// Let's pause the job instead of failing it so that the user can decide
	// whether to resume it or cancel it.
	if err != nil {
		return res, jobs.MarkPauseRequestError(errors.Wrap(err, "exhausted retries"))
	}
	return res, nil
}

type storeByLocalityKV map[string]cloudpb.ExternalStorage

func makeBackupLocalityMap(
	backupLocalityInfos []jobspb.RestoreDetails_BackupLocalityInfo, user username.SQLUsername,
) (map[int]storeByLocalityKV, error) {

	backupLocalityMap := make(map[int]storeByLocalityKV)
	for i, localityInfo := range backupLocalityInfos {
		storesByLocalityKV := make(storeByLocalityKV)
		if localityInfo.URIsByOriginalLocalityKV != nil {
			for kv, uri := range localityInfo.URIsByOriginalLocalityKV {
				conf, err := cloud.ExternalStorageConfFromURI(uri, user)
				if err != nil {
					return nil, errors.Wrap(err,
						"creating locality external storage configuration")
				}
				conf.URI = uri
				storesByLocalityKV[kv] = conf
			}
		}
		backupLocalityMap[i] = storesByLocalityKV
	}

	return backupLocalityMap, nil
}

// restore imports a SQL table (or tables) from sets of non-overlapping sstable
// files.
func restore(
	restoreCtx context.Context,
	execCtx sql.JobExecContext,
	backupManifests []backuppb.BackupManifest,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
	dataToRestore restorationData,
	resumer *restoreResumer,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (roachpb.RowCount, error) {
	user := execCtx.User()
	// A note about contexts and spans in this method: the top-level context
	// `restoreCtx` is used for orchestration logging. All operations that carry
	// out work get their individual contexts.
	emptyRowCount := roachpb.RowCount{}

	// If there isn't any data to restore, then return early.
	if dataToRestore.isEmpty() {
		return emptyRowCount, nil
	}

	// If we've already migrated some of the system tables we're about to
	// restore, this implies that a previous attempt restored all of this data.
	// We want to avoid restoring again since we'll be shadowing migrated keys.
	job := resumer.job
	details := job.Details().(jobspb.RestoreDetails)
	if alreadyMigrated := checkForMigratedData(details, dataToRestore); alreadyMigrated {
		return emptyRowCount, nil
	}

	backupLocalityMap, err := makeBackupLocalityMap(backupLocalityInfo, user)
	if err != nil {
		return emptyRowCount, errors.Wrap(err, "resolving locality locations")
	}

	if err := checkCoverage(restoreCtx, dataToRestore.getSpans(), backupManifests); err != nil {
		return emptyRowCount, err
	}

	restoreCheckpoint := job.Progress().Details.(*jobspb.Progress_Restore).Restore.Checkpoint
	requiredSpans := dataToRestore.getSpans()
	progressTracker, err := makeProgressTracker(
		requiredSpans,
		restoreCheckpoint,
		restoreCheckpointMaxBytes.Get(&execCtx.ExecCfg().Settings.SV),
		endTime)
	if err != nil {
		return emptyRowCount, err
	}
	defer progressTracker.close()

	introducedSpanFrontier, err := createIntroducedSpanFrontier(backupManifests, endTime)
	if err != nil {
		return emptyRowCount, err
	}
	defer introducedSpanFrontier.Release()

	targetSize := targetRestoreSpanSize.Get(&execCtx.ExecCfg().Settings.SV)
	if details.ExperimentalOnline {
		targetSize = targetOnlineRestoreSpanSize.Get(&execCtx.ExecCfg().Settings.SV)
	}
	maxFileCount := maxFileCount.Get(&execCtx.ExecCfg().Settings.SV)
	if details.ExperimentalOnline {
		// Online Restore does not need to limit the number of files per restore
		// span entry as the files are never opened when processing the span. The
		// span is only used to create split points.
		maxFileCount = math.MaxInt
	}

	var filter spanCoveringFilter
	if filter, err = func() (spanCoveringFilter, error) {
		return makeSpanCoveringFilter(
			requiredSpans,
			restoreCheckpoint,
			introducedSpanFrontier,
			targetSize,
			maxFileCount)
	}(); err != nil {
		return roachpb.RowCount{}, err
	}
	defer filter.close()

	// Pivot the backups, which are grouped by time, into requests for import,
	// which are grouped by keyrange.
	layerToIterFactory, err := backupinfo.GetBackupManifestIterFactories(restoreCtx, execCtx.ExecCfg().DistSQLSrv.ExternalStorage, backupManifests, encryption, kmsEnv)
	if err != nil {
		return roachpb.RowCount{}, err
	}

	// If any layer of the backup was produced with revision history before 24.1,
	// we need to assume inclusive end-keys. If no layers used revision history or
	// those that did were produced with #118990 in 24.1+, we can assume exclusive
	// end-keys.
	var fsc fileSpanComparator = &exclusiveEndKeyComparator{}
	for _, i := range backupManifests {
		if i.ClusterVersion.Less(clusterversion.V24_1.Version()) && i.MVCCFilter == backuppb.MVCCFilter_All {
			fsc = &inclusiveEndKeyComparator{}
			break
		}
	}

	countSpansCh := make(chan execinfrapb.RestoreSpanEntry, 1000)
	genSpan := func(ctx context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error {
		defer close(spanCh)
		return errors.Wrap(generateAndSendImportSpans(
			ctx,
			dataToRestore.getSpans(),
			backupManifests,
			layerToIterFactory,
			backupLocalityMap,
			filter,
			fsc,
			spanCh,
		), "generate and send import spans")
	}

	// Count number of import spans.
	var numImportSpans int
	var countTasks []func(ctx context.Context) error
	spanCountTask := func(ctx context.Context) error {
		for range countSpansCh {
			numImportSpans++
		}
		return nil
	}
	countTasks = append(countTasks, spanCountTask)
	countTasks = append(countTasks, func(ctx context.Context) error {
		return genSpan(ctx, countSpansCh)
	})
	if err := ctxgroup.GoAndWait(restoreCtx, countTasks...); err != nil {
		return emptyRowCount, errors.Wrapf(err, "counting number of import spans")
	}

	// requestFinishedCh is pinged every time restore completes the ingestion of a
	// restoreSpanEntry. Each ping updates the 'fraction completed' job progress.
	// Note that online restore pings this channel directly, every time a remote
	// addsstable completes, while conventional restore pings the channel after
	// updating the progress frontier.
	requestFinishedCh := make(chan struct{}, numImportSpans) // enough buffer to never block

	// tasks are the concurrent tasks that are run during the restore.
	var tasks []func(ctx context.Context) error
	if dataToRestore.isMainBundle() {
		// Only update the job progress on the main data bundle. This should account
		// for the bulk of the data to restore. Other data (e.g. zone configs in
		// cluster restores) may be restored first.
		progressLogger := jobs.NewChunkProgressLoggerForJob(job, numImportSpans, job.FractionCompleted(), progressTracker.updateJobCallback)

		jobProgressLoop := func(ctx context.Context) error {
			ctx, progressSpan := tracing.ChildSpan(ctx, "progress-loop")
			defer progressSpan.Finish()
			return errors.Wrap(progressLogger.Loop(ctx, requestFinishedCh), "job progress loop")
		}
		tasks = append(tasks, jobProgressLoop)
	}

	progCh := make(chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
	if !details.ExperimentalOnline {
		// Online restore tracks progress by pinging requestFinishedCh instead
		generativeCheckpointLoop := func(ctx context.Context) error {
			defer close(requestFinishedCh)
			for progress := range progCh {
				if spanDone, err := progressTracker.ingestUpdate(ctx, progress); err != nil {
					return err
				} else if spanDone {
					// Signal that the processor has finished importing a span, to update job
					// progress.
					requestFinishedCh <- struct{}{}
				}
			}
			return nil
		}

		tasks = append(tasks, generativeCheckpointLoop)
	}

	procCompleteCh := make(chan struct{})
	// countCompletedProcLoop is responsible for counting the number of completed
	// processors. The goroutine returns a retryable error such that the job can
	// replan if there are too many lagging nodes.
	countCompletedProcLoop := func(ctx context.Context) error {
		var timer timeutil.Timer
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case _, ok := <-procCompleteCh:
				if !ok {
					return nil
				}
				timer.Reset(replanFrequency.Get(&execCtx.ExecCfg().Settings.SV))
			case <-timer.C:
				timer.Read = true
				// Replan the restore job if it has been 10 minutes since the last
				// processor completed working.
				return errors.Mark(laggingRestoreProcErr, retryableRestoreProcError)
			}
		}
	}

	resumeClusterVersion := execCtx.ExecCfg().Settings.Version.ActiveVersion(restoreCtx).Version
	if clusterversion.V24_3.Version().LessEq(resumeClusterVersion) && !details.ExperimentalOnline {
		tasks = append(tasks, countCompletedProcLoop)
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
	tasks = append(tasks, tracingAggLoop)

	runRestore := func(ctx context.Context) error {
		if details.ExperimentalOnline {
			log.Warningf(ctx, "EXPERIMENTAL ONLINE RESTORE being used")
			approxRows, approxDataSize, err := sendAddRemoteSSTs(
				ctx,
				execCtx,
				job,
				dataToRestore,
				encryption,
				details.URIs,
				backupLocalityInfo,
				requestFinishedCh,
				tracingAggCh,
				genSpan,
			)
			progressTracker.mu.Lock()
			defer progressTracker.mu.Unlock()
			//  During the link phase of online restore, we do not update stats
			// progress as job occurs.  We merely reuse the `progressTracker.mu.res`
			// var to reduce the number of local vars floating around in `restore`.
			progressTracker.mu.res = roachpb.RowCount{Rows: approxRows, DataSize: approxDataSize}
			return errors.Wrap(err, "sending remote AddSSTable requests")
		}
		md := restoreJobMetadata{
			jobID:                job.ID(),
			dataToRestore:        dataToRestore,
			restoreTime:          endTime,
			encryption:           encryption,
			kmsEnv:               kmsEnv,
			uris:                 details.URIs,
			backupLocalityInfo:   backupLocalityInfo,
			spanFilter:           filter,
			numImportSpans:       numImportSpans,
			execLocality:         details.ExecutionLocality,
			exclusiveEndKeys:     fsc.isExclusive(),
			resumeClusterVersion: resumeClusterVersion,
		}
		return errors.Wrap(distRestore(
			ctx,
			execCtx,
			md,
			progCh,
			tracingAggCh,
			procCompleteCh,
		), "running distributed restore")
	}
	tasks = append(tasks, runRestore)
	testingKnobs := execCtx.ExecCfg().BackupRestoreTestingKnobs
	if testingKnobs != nil && testingKnobs.RunBeforeRestoreFlow != nil {
		if err := testingKnobs.RunBeforeRestoreFlow(); err != nil {
			return emptyRowCount, err
		}
	}

	if err := ctxgroup.GoAndWait(restoreCtx, tasks...); err != nil {
		return emptyRowCount, errors.Wrapf(err, "importing %d ranges", numImportSpans)
	}

	if testingKnobs != nil && testingKnobs.RunAfterRestoreFlow != nil {
		if err := testingKnobs.RunAfterRestoreFlow(); err != nil {
			return emptyRowCount, err
		}
	}

	// progress go routines should be shutdown, but use lock just to be safe.
	progressTracker.mu.Lock()
	defer progressTracker.mu.Unlock()
	return progressTracker.mu.res, nil
}

// loadBackupSQLDescs extracts the backup descriptors, the latest backup
// descriptor, and all the Descriptors for a backup to be restored. It upgrades
// the table descriptors to the new FK representation if necessary. FKs that
// can't be restored because the necessary tables are missing are omitted; if
// skip_missing_foreign_keys was set, we should have aborted the RESTORE and
// returned an error prior to this.
//
// The caller is responsible for shrinking `mem` by the returned size once it's
// done with the returned manifests (unless an error is returned).
// TODO(anzoteh96): this method returns two things: backup manifests
// and the descriptors of the relevant manifests. Ideally, this should
// be broken down into two methods.
func loadBackupSQLDescs(
	ctx context.Context,
	mem *mon.BoundAccount,
	p sql.JobExecContext,
	details jobspb.RestoreDetails,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (
	_ []backuppb.BackupManifest,
	_ backuppb.BackupManifest,
	_ []catalog.Descriptor,
	memSize int64,
	retErr error,
) {
	backupManifests, sz, err := backupinfo.LoadBackupManifestsAtTime(ctx, mem, details.URIs,
		p.User(), p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, encryption, kmsEnv, details.EndTime)
	if err != nil {
		return nil, backuppb.BackupManifest{}, nil, 0, err
	}
	defer func() {
		if retErr != nil {
			mem.Shrink(ctx, sz)
		}
	}()

	layerToBackupManifestFileIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx, p.ExecCfg().DistSQLSrv.ExternalStorage,
		backupManifests, encryption, kmsEnv)
	if err != nil {
		return nil, backuppb.BackupManifest{}, nil, 0, err
	}

	allDescs, latestBackupManifest, err := backupinfo.LoadSQLDescsFromBackupsAtTime(ctx, backupManifests, layerToBackupManifestFileIterFactory, details.EndTime)
	if err != nil {
		return nil, backuppb.BackupManifest{}, nil, 0, err
	}

	for _, m := range details.DatabaseModifiers {
		for _, typ := range m.ExtraTypeDescs {
			allDescs = append(allDescs, typedesc.NewBuilder(typ).BuildCreatedMutableType())
		}
	}

	var sqlDescs []catalog.Descriptor
	for _, desc := range allDescs {
		id := desc.GetID()
		switch desc := desc.(type) {
		case *dbdesc.Mutable:
			if m, ok := details.DatabaseModifiers[id]; ok {
				desc.SetRegionConfig(m.RegionConfig)
			}
		}
		if _, ok := details.DescriptorRewrites[id]; ok {
			sqlDescs = append(sqlDescs, desc)
		}
	}
	activeVersion := p.ExecCfg().Settings.Version.ActiveVersion(ctx)
	if err := maybeUpgradeDescriptors(activeVersion, sqlDescs, true /* skipFKsWithNoMatchingTable */); err != nil {
		return nil, backuppb.BackupManifest{}, nil, 0, err
	}

	return backupManifests, latestBackupManifest, sqlDescs, sz, nil
}

// restoreResumer should only store a reference to the job it's running. State
// should not be stored here, but rather in the job details.
type restoreResumer struct {
	job *jobs.Job

	settings      *cluster.Settings
	execCfg       *sql.ExecutorConfig
	restoreStats  roachpb.RowCount
	downloadJobID jobspb.JobID

	mu struct {
		syncutil.Mutex
		// perNodeAggregatorStats is a per component running aggregate of trace
		// driven AggregatorStats pushed backed to the resumer from all the
		// processors running the backup.
		perNodeAggregatorStats bulkutil.ComponentAggregatorStats
	}

	testingKnobs struct {
		// beforePublishingDescriptors is called right before publishing
		// descriptors, after any data has been restored.
		beforePublishingDescriptors func() error
		// afterPublishingDescriptors is called after committing the transaction to
		// publish new descriptors in the public state.
		afterPublishingDescriptors func() error
		// duringSystemTableRestoration is called once for every system table we
		// restore. It is used to simulate any errors that we may face at this point
		// of the restore.
		duringSystemTableRestoration func(systemTableName string) error
		// afterOfflineTableCreation is called after creating the OFFLINE table
		// descriptors we're ingesting. If an error is returned, we fail the
		// restore.
		afterOfflineTableCreation func() error
		// afterPreRestore runs on cluster restores after restoring the "preRestore"
		// data.
		afterPreRestore func() error
		// checksumRecover
		checksumRecover func() error
	}
}

var _ jobs.TraceableJob = &restoreResumer{}

// ForceRealSpan implements the TraceableJob interface.
func (r *restoreResumer) ForceRealSpan() bool {
	return true
}

// DumpTraceAfterRun implements the TraceableJob interface.
func (r *restoreResumer) DumpTraceAfterRun() bool {
	return true
}

// remapAndFilterRelevantStatistics changes the table ID references in
// the stats from those they had in the backed up database to what
// they should be in the restored database.
//
// It also selects only the statistics which belong to one of the
// tables being restored. If the descriptorRewrites can re-write the
// table ID, then that table is being restored.
//
// Any statistics forecasts are ignored.
func remapAndFilterRelevantStatistics(
	ctx context.Context,
	tableStatistics []*stats.TableStatisticProto,
	descriptorRewrites jobspb.DescRewriteMap,
	tableDescs []*descpb.TableDescriptor,
) []*stats.TableStatisticProto {
	relevantTableStatistics := make([]*stats.TableStatisticProto, 0, len(tableStatistics))

	tableHasStatsInBackup := make(map[descpb.ID]struct{})
	for _, stat := range tableStatistics {
		if statShouldBeIncludedInBackupRestore(stat) {
			tableHasStatsInBackup[stat.TableID] = struct{}{}
			if tableRewrite, ok := descriptorRewrites[stat.TableID]; ok {
				// We only restore statistics when all necessary descriptor rewrites are
				// present in the rewrite map.
				stat.TableID = tableRewrite.ID
				// We also need to remap the type OID in the histogram for UDTs.
				if stat.HistogramData != nil && stat.HistogramData.ColumnType != nil {
					if typ := stat.HistogramData.ColumnType; typ.UserDefined() {
						typDescID := typedesc.GetUserDefinedTypeDescID(typ)
						if _, ok := descriptorRewrites[typDescID]; !ok {
							continue
						}
						rewrite.RewriteIDsInTypesT(typ, descriptorRewrites)
					}
				}
				relevantTableStatistics = append(relevantTableStatistics, stat)
			}
		}
	}

	// Check if we are missing stats for any table that is being restored. This
	// could be because we ran into an error when computing stats during the
	// backup.
	for _, desc := range tableDescs {
		if _, ok := tableHasStatsInBackup[desc.GetID()]; !ok {
			log.Warningf(ctx, "statistics for table: %s, table ID: %d not found in the backup. "+
				"Query performance on this table could suffer until statistics are recomputed.",
				desc.GetName(), desc.GetID())
		}
	}

	return relevantTableStatistics
}

// isDatabaseEmpty checks if there exists any tables in the given database.
// It pretends that the ignoredChildren do not exist for the purposes of
// checking if a database is empty.
//
// It is used to construct a transaction which deletes a set of tables as well
// as some empty databases. However, we want to check that the databases are
// empty _after_ the transaction would have completed, so we want to ignore
// the tables that we're deleting in the same transaction. It is done this way
// to avoid having 2 transactions reading and writing the same keys one right
// after the other.
func isDatabaseEmpty(
	ctx context.Context,
	txn *kv.Txn,
	dbID descpb.ID,
	allDescs []catalog.Descriptor,
	ignoredChildren map[descpb.ID]struct{},
) (bool, error) {
	for _, desc := range allDescs {
		if _, ok := ignoredChildren[desc.GetID()]; ok {
			continue
		}
		if desc.GetParentID() == dbID {
			return false, nil
		}
	}
	return true, nil
}

// isSchemaEmpty is like isDatabaseEmpty for schemas: it returns whether the
// schema is empty, disregarding the contents of ignoredChildren.
func isSchemaEmpty(
	ctx context.Context,
	txn *kv.Txn,
	schemaID descpb.ID,
	allDescs []catalog.Descriptor,
	ignoredChildren map[descpb.ID]struct{},
) (bool, error) {
	for _, desc := range allDescs {
		if _, ok := ignoredChildren[desc.GetID()]; ok {
			continue
		}
		if desc.GetParentSchemaID() == schemaID {
			return false, nil
		}
	}
	return true, nil
}

// spansForAllRestoreTableIndexes returns non-overlapping spans for every index
// and table passed in. They would normally overlap if any of them are
// interleaved.
func spansForAllRestoreTableIndexes(
	codec keys.SQLCodec,
	tables []catalog.TableDescriptor,
	revs []backuppb.BackupManifest_DescriptorRevision,
	schemaOnly bool,
	forOnlineRestore bool,
) ([]roachpb.Span, error) {

	skipTableData := func(table catalog.TableDescriptor) bool {
		// The only table data restored during a schemaOnly restore are from system tables,
		// which only get covered during a cluster restore.
		if table.GetParentID() != keys.SystemDatabaseID && schemaOnly {
			return true
		}
		// We only import spans for physical tables.
		if !table.IsPhysicalTable() {
			return true
		}
		return false
	}

	added := make(map[tableAndIndex]bool, len(tables))
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	for _, table := range tables {
		if skipTableData(table) {
			continue
		}
		for _, index := range table.ActiveIndexes() {
			if err := sstIntervalTree.Insert(intervalSpan(table.IndexSpan(codec, index.GetID())), false); err != nil {
				panic(errors.NewAssertionErrorWithWrappedErrf(err, "IndexSpan"))
			}
			added[tableAndIndex{tableID: table.GetID(), indexID: index.GetID()}] = true
		}
	}
	// If there are desc revisions, ensure that we also add any index spans
	// in them that we didn't already get above e.g. indexes or tables that are
	// not in latest because they were dropped during the time window in question.
	for _, rev := range revs {
		// If the table was dropped during the last interval, it will have
		// at least 2 revisions, and the first one should have the table in a PUBLIC
		// state. We want (and do) ignore tables that have been dropped for the
		// entire interval. DROPPED tables should never later become PUBLIC.
		// TODO(pbardea): Consider and test the interaction between revision_history
		// backups and OFFLINE tables.
		rawTbl, _, _, _, _ := descpb.GetDescriptors(rev.Desc)
		if rawTbl != nil && !rawTbl.Dropped() {
			tbl := tabledesc.NewBuilder(rawTbl).BuildImmutableTable()
			if skipTableData(tbl) {
				continue
			}
			for _, idx := range tbl.ActiveIndexes() {
				key := tableAndIndex{tableID: tbl.GetID(), indexID: idx.GetID()}
				if !added[key] {
					if err := sstIntervalTree.Insert(intervalSpan(tbl.IndexSpan(codec, idx.GetID())), false); err != nil {
						panic(errors.NewAssertionErrorWithWrappedErrf(err, "IndexSpan"))
					}
					added[key] = true
				}
			}
		}
	}

	var spans []roachpb.Span
	_ = sstIntervalTree.Do(func(r interval.Interface) bool {
		spans = append(spans, roachpb.Span{
			Key:    roachpb.Key(r.Range().Start),
			EndKey: roachpb.Key(r.Range().End),
		})
		return false
	})
	return spans, nil
}

func shouldPreRestore(table *tabledesc.Mutable) bool {
	if table.GetParentID() != keys.SystemDatabaseID {
		return false
	}
	tablesToPreRestore := getSystemTablesToRestoreBeforeData()
	_, ok := tablesToPreRestore[table.GetName()]
	return ok
}

// backedUpDescriptorWithInProgressImportInto returns true if the backed up descriptor represents a table with an in
// progress import that started in a cluster finalized to version 22.2.
func backedUpDescriptorWithInProgressImportInto(desc catalog.Descriptor) bool {
	table, ok := desc.(catalog.TableDescriptor)
	if !ok {
		return false
	}

	return table.GetInProgressImportStartTime() > 0
}

// epochBasedInProgressImport returns true if the backup up descriptor
// represents a table with an inprogress import that used
// ImportEpochs.
func epochBasedInProgressImport(desc catalog.Descriptor) bool {
	table, ok := desc.(catalog.TableDescriptor)
	if !ok {
		return false
	}

	return table.GetInProgressImportStartTime() > 0 &&
		table.TableDesc().ImportEpoch > 0 &&
		table.TableDesc().ImportType == descpb.ImportType_IMPORT_WITH_IMPORT_EPOCH
}

// createImportingDescriptors creates the tables that we will restore into and returns up to three
// configurations for separate restoration flows. The three restoration flows are
//
//  1. dataToPreRestore: a restoration flow cfg to ingest a subset of
//     system tables (e.g. zone configs) during a cluster restore that are
//     required to be set up before the rest of the data gets restored.
//     This should be empty during non-cluster restores.
//
//  2. preValidation: a restoration flow cfg to ingest the remainder of system tables,
//     during a verify_backup_table_data, cluster level, restores. This should be empty otherwise.
//
//  3. trackedRestore: a restoration flow cfg to ingest the remainder of
//     restore targets. This flow should get executed last and should contain the
//     bulk of the work, as it is used for job progress tracking.
func createImportingDescriptors(
	ctx context.Context,
	p sql.JobExecContext,
	backupCodec keys.SQLCodec,
	sqlDescs []catalog.Descriptor,
	r *restoreResumer,
	manifest backuppb.BackupManifest,
) (
	dataToPreRestore *restorationDataBase,
	preValidation *restorationDataBase,
	trackedRestore *mainRestorationData,
	err error,
) {
	details := r.job.Details().(jobspb.RestoreDetails)
	const kvTrace = false

	var allMutableDescs []catalog.MutableDescriptor
	var databases []catalog.DatabaseDescriptor
	var writtenTypes []catalog.TypeDescriptor
	var schemas []*schemadesc.Mutable
	var types []*typedesc.Mutable
	var functions []*funcdesc.Mutable

	// Store the tables as both the concrete mutable structs and the interface
	// to deal with the lack of slice covariance in go. We want the slice of
	// mutable descriptors for rewriting but ultimately want to return the
	// tables as the slice of interfaces.
	var mutableTables []*tabledesc.Mutable
	var mutableDatabases []*dbdesc.Mutable

	oldTableIDs := make([]descpb.ID, 0)

	// offlineSchemas is a slice of all the backed up schemas in a database that
	// were in an offline state at the time of the backup. These offline schemas
	// are not restored and need to be elided from the list of schemas when
	// constructing the database descriptor.
	offlineSchemas := make(map[descpb.ID]struct{})

	tables := make([]catalog.TableDescriptor, 0)
	postRestoreTables := make([]catalog.TableDescriptor, 0)

	preRestoreTables := make([]catalog.TableDescriptor, 0)

	for _, desc := range sqlDescs {
		// Decide which offline tables to include in the restore:
		//
		// - An offline table created by RESTORE or IMPORT PGDUMP is
		//   fully discarded.  The table will not exist in the restoring
		//   cluster.
		//
		// - An offline table undergoing an IMPORT INTO in traditional
		//   restore has all importing data elided in the restore
		//   processor and is restored online to its pre import state.
		//
		// - An offline table undergoing an IMPORT INTO in online
		//   restore with no ImportEpoch cannot be restored and an error
		//   is returned.
		//
		// - An offline table undergoing an IMPORT INTO in online
		//   restore with an ImportEpoch is restored with an Offline
		//   table and a revert job is queued that will bring the table
		//   back online.
		if desc.Offline() {
			if schema, ok := desc.(catalog.SchemaDescriptor); ok {
				offlineSchemas[schema.GetID()] = struct{}{}
			}

			if backedUpDescriptorWithInProgressImportInto(desc) {
				if details.ExperimentalOnline && !epochBasedInProgressImport(desc) {
					return nil, nil, nil, errors.Newf("table %s (id %d) in restoring backup has an in-progress import, but online restore cannot be run on a table with an in progress import", desc.GetName(), desc.GetID())
				}
			} else {
				continue
			}
		}

		switch desc := desc.(type) {
		case catalog.TableDescriptor:
			mut := tabledesc.NewBuilder(desc.TableDesc()).BuildCreatedMutableTable()
			if shouldPreRestore(mut) {
				preRestoreTables = append(preRestoreTables, mut)
			} else {
				postRestoreTables = append(postRestoreTables, mut)
			}
			tables = append(tables, mut)
			mutableTables = append(mutableTables, mut)
			allMutableDescs = append(allMutableDescs, mut)
			oldTableIDs = append(oldTableIDs, mut.GetID())
		case catalog.DatabaseDescriptor:
			if _, ok := details.DescriptorRewrites[desc.GetID()]; ok {
				mut := dbdesc.NewBuilder(desc.DatabaseDesc()).BuildCreatedMutableDatabase()
				databases = append(databases, mut)
				mutableDatabases = append(mutableDatabases, mut)
				allMutableDescs = append(allMutableDescs, mut)
			}
		case catalog.SchemaDescriptor:
			mut := schemadesc.NewBuilder(desc.SchemaDesc()).BuildCreatedMutableSchema()
			schemas = append(schemas, mut)
			allMutableDescs = append(allMutableDescs, mut)
		case catalog.TypeDescriptor:
			mut := typedesc.NewBuilder(desc.TypeDesc()).BuildCreatedMutableType()
			types = append(types, mut)
			allMutableDescs = append(allMutableDescs, mut)
		case catalog.FunctionDescriptor:
			mut := funcdesc.NewBuilder(desc.FuncDesc()).BuildCreatedMutableFunction()
			functions = append(functions, mut)
			allMutableDescs = append(allMutableDescs, mut)
		}
	}

	tempSystemDBID := tempSystemDatabaseID(details, tables)
	if tempSystemDBID != descpb.InvalidID {
		tempSystemDB := dbdesc.NewInitial(tempSystemDBID, restoreTempSystemDB,
			username.AdminRoleName(), dbdesc.WithPublicSchemaID(keys.SystemPublicSchemaID))
		databases = append(databases, tempSystemDB)
	}

	// We get the spans of the restoring tables _as they appear in the backup_,
	// that is, in the 'old' keyspace, before we reassign the table IDs.
	preRestoreSpans, err := spansForAllRestoreTableIndexes(backupCodec, preRestoreTables, nil, details.SchemaOnly, details.ExperimentalOnline)
	if err != nil {
		return nil, nil, nil, err
	}
	postRestoreSpans, err := spansForAllRestoreTableIndexes(backupCodec, postRestoreTables, nil, details.SchemaOnly, details.ExperimentalOnline)
	if err != nil {
		return nil, nil, nil, err
	}
	var verifySpans []roachpb.Span
	if details.VerifyData {
		// verifySpans contains the spans that should be read and checksum'd during a
		// verify_backup_table_data RESTORE
		verifySpans, err = spansForAllRestoreTableIndexes(backupCodec, postRestoreTables, nil, false, details.ExperimentalOnline)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	log.Eventf(ctx, "starting restore for %d tables", len(mutableTables))

	// Assign new IDs to the database descriptors.
	if err := rewrite.DatabaseDescs(mutableDatabases, details.DescriptorRewrites, offlineSchemas); err != nil {
		return nil, nil, nil, err
	}

	databaseDescs := make([]*descpb.DatabaseDescriptor, len(mutableDatabases))
	for i, database := range mutableDatabases {
		databaseDescs[i] = database.DatabaseDesc()
	}

	// Collect all schemas that are going to be restored.
	var schemasToWrite []*schemadesc.Mutable
	var writtenSchemas []catalog.SchemaDescriptor
	for i := range schemas {
		sc := schemas[i]
		rw, ok := details.DescriptorRewrites[sc.ID]
		if ok {
			if !rw.ToExisting {
				schemasToWrite = append(schemasToWrite, sc)
				writtenSchemas = append(writtenSchemas, sc)
			}
		}
	}

	if err := rewrite.SchemaDescs(schemasToWrite, details.DescriptorRewrites); err != nil {
		return nil, nil, nil, err
	}

	if err := remapPublicSchemas(ctx, p, mutableDatabases, &schemasToWrite, &writtenSchemas, &details); err != nil {
		return nil, nil, nil, err
	}

	// Assign new IDs and privileges to the tables, and update all references to
	// use the new IDs.
	if err := rewrite.TableDescs(
		mutableTables, details.DescriptorRewrites, details.OverrideDB,
	); err != nil {
		return nil, nil, nil, err
	}
	tableDescs := make([]*descpb.TableDescriptor, len(mutableTables))
	for i, table := range mutableTables {
		tableDescs[i] = table.TableDesc()
	}

	// For each type, we might be writing the type in the backup, or we could be
	// remapping to an existing type descriptor. Split up the descriptors into
	// these two groups.
	var typesToWrite []*typedesc.Mutable
	existingTypeIDs := make(map[descpb.ID]struct{})
	for i := range types {
		typ := types[i]
		rewrite := details.DescriptorRewrites[typ.GetID()]
		if rewrite.ToExisting {
			existingTypeIDs[rewrite.ID] = struct{}{}
		} else {
			typesToWrite = append(typesToWrite, typ)
			writtenTypes = append(writtenTypes, typ)
		}
	}

	// Perform rewrites on ALL type descriptors that are present in the rewrite
	// mapping.
	//
	// `types` contains a mix of existing type descriptors in the restoring
	// cluster, and new type descriptors we will write from the backup.
	//
	// New type descriptors need to be rewritten with their generated IDs before
	// they are written out to disk.
	//
	// Existing type descriptors need to be rewritten to the type ID of the type
	// they are referring to in the restoring cluster. This ID is different from
	// the ID the descriptor had when it was backed up. Changes to existing type
	// descriptors will not be written to disk, and is only for accurate,
	// in-memory resolution hereon out.
	if err := rewrite.TypeDescs(types, details.DescriptorRewrites); err != nil {
		return nil, nil, nil, err
	}

	// TODO(chengxiong): for now, we know that functions are not referenced by any
	// other objects, so that function descriptors are only restored when
	// restoring databases. This means that all function descriptors are not
	// remaps. Which means that we don't need resolve collisions between functions
	// being restored and existing functions in target DB However, this won't be
	// true when we start supporting udf references from other objects. For
	// example, we need extra logic to handle remaps for udfs used by a table when
	// backup/restore is on table level.
	functionsToWrite := make([]*funcdesc.Mutable, len(functions))
	writtenFunctions := make([]catalog.FunctionDescriptor, len(functions))
	for i, fn := range functions {
		functionsToWrite[i] = fn
		writtenFunctions[i] = fn
	}
	if err := rewrite.FunctionDescs(functions, details.DescriptorRewrites, details.OverrideDB); err != nil {
		return nil, nil, nil, err
	}

	// Finally, clean up / update any schema changer state inside descriptors
	// globally.
	if err := rewrite.MaybeClearSchemaChangerStateInDescs(allMutableDescs); err != nil {
		return nil, nil, nil, err
	}

	// Set the new descriptors' states to offline.
	for _, desc := range mutableTables {
		desc.SetOffline("restoring")

		// Remove any LDR Jobs from the table descriptor, ensuring schema changes
		// can be run on the table descriptor.
		desc.LDRJobIDs = nil
	}
	for _, desc := range typesToWrite {
		desc.SetOffline("restoring")
	}
	for _, desc := range schemasToWrite {
		desc.SetOffline("restoring")
	}
	for _, desc := range mutableDatabases {
		desc.SetOffline("restoring")
	}
	for _, desc := range functionsToWrite {
		desc.SetOffline("restoring")
	}

	if tempSystemDBID != descpb.InvalidID {
		for _, desc := range mutableTables {
			if desc.GetParentID() == tempSystemDBID {
				desc.SetPublic()
				desc.LocalityConfig = nil
			}
		}
	}

	// Collect all types after they have had their ID's rewritten.
	typesByID := make(map[descpb.ID]catalog.TypeDescriptor)
	for i := range types {
		typesByID[types[i].GetID()] = types[i]
	}

	if details.RemoveRegions {
		// Can't restore multi-region tables into non-multi-region database
		for _, t := range tables {
			t.TableDesc().LocalityConfig = nil
		}

		for _, d := range databases {
			d.DatabaseDesc().RegionConfig = nil
		}
	}

	// Collect all databases, for doing lookups of whether a database is new when
	// updating schema references later on.
	dbsByID := make(map[descpb.ID]catalog.DatabaseDescriptor)
	for i := range databases {
		dbsByID[databases[i].GetID()] = databases[i]
	}

	if !details.PrepareCompleted {
		err := p.ExecCfg().InternalDB.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) error {
			// A couple of pieces of cleanup are required for multi-region databases.
			// First, we need to find all of the MULTIREGION_ENUMs types and remap the
			// IDs stored in the corresponding database descriptors to match the type's
			// new ID. Secondly, we need to rebuild the zone configuration for each
			// multi-region database. We don't perform the zone configuration rebuild on
			// cluster restores, as they will have the zone configurations restored as
			// as the system tables are restored.
			mrEnumsFound := make(map[descpb.ID]descpb.ID)
			for _, t := range typesByID {
				regionTypeDesc := typedesc.NewBuilder(t.TypeDesc()).BuildImmutableType().AsRegionEnumTypeDescriptor()
				if regionTypeDesc == nil {
					continue
				}

				// When stripping localities, there is no longer a need for a region config. In addition,
				// we need to make sure that multi-region databases no longer get tagged as such - meaning
				// that we want to change the TypeDescriptor_MULTIREGION_ENUM to a normal enum. We `continue`
				// to skip the multi-region work below.
				if details.RemoveRegions {
					t.TypeDesc().Kind = descpb.TypeDescriptor_ENUM
					t.TypeDesc().RegionConfig = nil
					continue
				}

				// Check to see if we've found more than one multi-region enum on any
				// given database.
				if id, ok := mrEnumsFound[regionTypeDesc.GetParentID()]; ok {
					return errors.AssertionFailedf(
						"unexpectedly found more than one MULTIREGION_ENUM (IDs = %d, %d) "+
							"on database %d during restore", id, regionTypeDesc.GetID(), regionTypeDesc.GetParentID())
				}
				mrEnumsFound[regionTypeDesc.GetParentID()] = regionTypeDesc.GetID()

				if db, ok := dbsByID[regionTypeDesc.GetParentID()]; ok {
					desc := db.DatabaseDesc()
					if db.GetName() == restoreTempSystemDB {
						t.TypeDesc().Kind = descpb.TypeDescriptor_ENUM
						t.TypeDesc().RegionConfig = nil
						// TODO(foundations): should these be rewritten instead of blank? Does it matter since we drop the whole DB before the job exits?
						t.TypeDesc().ReferencingDescriptorIDs = nil
						continue
					}
					if desc.RegionConfig == nil {
						return errors.AssertionFailedf(
							"found MULTIREGION_ENUM on non-multi-region database %s", desc.Name)
					}

					// Update the RegionEnumID to record the new multi-region enum ID.
					desc.RegionConfig.RegionEnumID = t.GetID()

					// If we're not in a cluster restore, rebuild the database-level zone
					// configuration.
					if details.DescriptorCoverage != tree.AllDescriptors {
						log.Infof(ctx, "restoring zone configuration for database %d", desc.ID)
						var regionNames []catpb.RegionName
						_ = regionTypeDesc.ForEachPublicRegion(func(name catpb.RegionName) error {
							regionNames = append(regionNames, name)

							return nil
						})
						regionConfig := multiregion.MakeRegionConfig(
							regionNames,
							desc.RegionConfig.PrimaryRegion,
							desc.RegionConfig.SurvivalGoal,
							desc.RegionConfig.RegionEnumID,
							desc.RegionConfig.Placement,
							regionTypeDesc.TypeDesc().RegionConfig.SuperRegions,
							regionTypeDesc.TypeDesc().RegionConfig.ZoneConfigExtensions,
						)
						if err := sql.ApplyZoneConfigFromDatabaseRegionConfig(
							ctx,
							desc.GetID(),
							regionConfig,
							txn,
							p.ExecCfg(),
							!details.SkipLocalitiesCheck,
							p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
						); err != nil {
							return err
						}
					}
				}
			}

			// Allocate no schedule to the row-level TTL.
			// This will be re-written when the descriptor is published.
			for _, table := range mutableTables {
				if table.HasRowLevelTTL() {
					table.RowLevelTTL.ScheduleID = 0
				}
			}
			descsCol := txn.Descriptors()
			// Write the new descriptors which are set in the OFFLINE state.
			includePublicSchemaCreatePriv := sqlclustersettings.PublicSchemaCreatePrivilegeEnabled.Get(&p.ExecCfg().Settings.SV)
			if err := ingesting.WriteDescriptors(
				ctx, txn.KV(), p.User(), descsCol, databases, writtenSchemas, tables, writtenTypes, writtenFunctions,
				details.DescriptorCoverage, nil /* extra */, restoreTempSystemDB, includePublicSchemaCreatePriv,
				true, /* deprecatedAllowCrossDatabaseRefs */
			); err != nil {
				return errors.Wrapf(err, "restoring %d TableDescriptors from %d databases", len(tables), len(databases))
			}

			b := txn.KV().NewBatch()

			// For new schemas with existing parent databases, the schema map on the
			// database descriptor needs to be updated.
			existingDBsWithNewSchemas := make(map[descpb.ID][]catalog.SchemaDescriptor)
			for _, sc := range writtenSchemas {
				parentID := sc.GetParentID()
				if _, ok := dbsByID[parentID]; !ok {
					existingDBsWithNewSchemas[parentID] = append(existingDBsWithNewSchemas[parentID], sc)
				}
			}
			// Write the updated databases.
			for dbID, schemas := range existingDBsWithNewSchemas {
				log.Infof(ctx, "writing %d schema entries to database %d", len(schemas), dbID)
				desc, err := descsCol.MutableByID(txn.KV()).Desc(ctx, dbID)
				if err != nil {
					return err
				}
				db := desc.(*dbdesc.Mutable)
				for _, sc := range schemas {
					db.AddSchemaToDatabase(sc.GetName(), descpb.DatabaseDescriptor_SchemaInfo{ID: sc.GetID()})
				}
				if err := descsCol.WriteDescToBatch(
					ctx, kvTrace, db, b,
				); err != nil {
					return err
				}
			}

			// We could be restoring tables that point to existing types. We need to
			// ensure that those existing types are updated with back references pointing
			// to the new tables being restored.
			for _, table := range mutableTables {
				// Collect all types used by this table.
				dbDesc, err := descsCol.ByIDWithoutLeased(txn.KV()).WithoutDropped().Get().Database(ctx, table.GetParentID())
				if err != nil {
					return err
				}
				typeIDs, _, err := table.GetAllReferencedTypeIDs(dbDesc, func(id descpb.ID) (catalog.TypeDescriptor, error) {
					t, ok := typesByID[id]
					if !ok {
						return nil, errors.AssertionFailedf("type with id %d was not found in rewritten type mapping", id)
					}
					return t, nil
				})
				if err != nil {
					return err
				}
				for _, id := range typeIDs {
					// If the type was restored as part of the backup, then the backreference
					// already exists.
					_, ok := existingTypeIDs[id]
					if !ok {
						continue
					}
					// Otherwise, add a backreference to this table.
					typDesc, err := descsCol.MutableByID(txn.KV()).Type(ctx, id)
					if err != nil {
						return err
					}
					typDesc.AddReferencingDescriptorID(table.GetID())
					if err := descsCol.WriteDescToBatch(
						ctx, kvTrace, typDesc, b,
					); err != nil {
						return err
					}
				}
			}
			if err := txn.KV().Run(ctx, b); err != nil {
				return err
			}

			// Now that all of the descriptors have been written to disk, rebuild
			// the zone configurations for any multi-region tables. We only do this
			// in cases where this is not a full cluster restore, because in cluster
			// restore cases, the zone configurations will be restored when the
			// system tables are restored.
			if details.DescriptorCoverage != tree.AllDescriptors {
				for _, table := range tableDescs {
					if lc := table.GetLocalityConfig(); lc != nil {
						desc, err := descsCol.ByIDWithoutLeased(txn.KV()).WithoutDropped().Get().Database(ctx, table.ParentID)
						if err != nil {
							return err
						}
						if desc.GetRegionConfig() == nil {
							return errors.AssertionFailedf(
								"found multi-region table %d in non-multi-region database %d",
								table.ID, table.ParentID)
						}

						mutTable, err := descsCol.MutableByID(txn.KV()).Table(ctx, table.GetID())
						if err != nil {
							return err
						}

						regionConfig, err := sql.SynthesizeRegionConfig(
							ctx,
							txn.KV(),
							desc.GetID(),
							descsCol,
							multiregion.SynthesizeRegionConfigOptionIncludeOffline,
						)
						if err != nil {
							return err
						}
						if err := sql.ApplyZoneConfigForMultiRegionTable(
							ctx,
							txn,
							p.ExecCfg(),
							p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
							regionConfig,
							mutTable,
							sql.ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
						); err != nil {
							return err
						}
					}
				}
			}

			if len(details.Tenants) > 0 {
				initialTenantZoneConfig, err := sql.GetHydratedZoneConfigForTenantsRange(ctx, txn.KV(), descsCol)
				if err != nil {
					return err
				}
				for _, tenantInfoCopy := range details.Tenants {
					switch tenantInfoCopy.DataState {
					case mtinfopb.DataStateReady:
						// If the tenant was backed up in the `READY` state then we create
						// the restored record in an `ADD` state and mark it `READY` at
						// the end of the restore.
						tenantInfoCopy.ServiceMode = mtinfopb.ServiceModeNone
						tenantInfoCopy.DataState = mtinfopb.DataStateAdd
					case mtinfopb.DataStateDrop, mtinfopb.DataStateAdd:
					// If the tenant was backed up in a `DROP` or `ADD` state then we must
					// create the restored tenant record in that state as well.
					default:
						return errors.AssertionFailedf("unknown tenant data state %v", tenantInfoCopy)
					}
					if p, err := roachpb.MakeTenantID(tenantInfoCopy.ID); err == nil {
						if details.PreRewriteTenantId != nil {
							p = *details.PreRewriteTenantId
						}
						ts := details.EndTime
						if ts.IsEmpty() {
							ts = manifest.EndTime
						}
						tenantInfoCopy.PreviousSourceTenant = &mtinfopb.PreviousSourceTenant{
							ClusterID:        manifest.ClusterID,
							TenantID:         p,
							CutoverTimestamp: ts,
						}
					}
					spanConfigs := p.ExecCfg().SpanConfigKVAccessor.WithTxn(ctx, txn.KV())
					if _, err := sql.CreateTenantRecord(
						ctx,
						p.ExecCfg().Codec,
						p.ExecCfg().Settings,
						txn,
						spanConfigs,
						&tenantInfoCopy,
						initialTenantZoneConfig,
						false, /* ifNotExists */
						p.ExecCfg().TenantTestingKnobs,
					); err != nil {
						return err
					}
				}
			}

			details.PrepareCompleted = true
			details.DatabaseDescs = databaseDescs
			details.TableDescs = tableDescs
			details.TypeDescs = make([]*descpb.TypeDescriptor, len(typesToWrite))
			for i := range typesToWrite {
				details.TypeDescs[i] = typesToWrite[i].TypeDesc()
			}
			details.SchemaDescs = make([]*descpb.SchemaDescriptor, len(schemasToWrite))
			for i := range schemasToWrite {
				details.SchemaDescs[i] = schemasToWrite[i].SchemaDesc()
			}
			details.FunctionDescs = make([]*descpb.FunctionDescriptor, len(functionsToWrite))
			for i, fn := range functionsToWrite {
				details.FunctionDescs[i] = fn.FuncDesc()
			}

			// Update the job once all descs have been prepared for ingestion.
			err := r.job.WithTxn(txn).SetDetails(ctx, details)

			// Emit to the event log now that the job has finished preparing descs.
			emitRestoreJobEvent(ctx, p, jobs.StateRunning, r.job)

			return err
		})
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// Get TableRekeys to use when importing raw data.
	var rekeys []execinfrapb.TableRekey
	for i := range tables {
		tableToSerialize := tables[i]
		newDescBytes, err := protoutil.Marshal(tableToSerialize.DescriptorProto())
		if err != nil {
			return nil, nil, nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"marshaling descriptor")
		}
		rekeys = append(rekeys, execinfrapb.TableRekey{
			OldID:   uint32(oldTableIDs[i]),
			NewDesc: newDescBytes,
		})
	}

	_, backupTenantID, err := keys.DecodeTenantPrefix(backupCodec.TenantPrefix())
	if err != nil {
		return nil, nil, nil, err
	}
	if !backupCodec.TenantPrefix().Equal(p.ExecCfg().Codec.TenantPrefix()) {
		// Ensure old processors fail if this is a previously unsupported restore of
		// a tenant backup by the system tenant, which the old rekey processor would
		// mishandle since it assumed the system tenant always restored tenant keys
		// to tenant prefixes, i.e. as tenant restore.
		if backupTenantID != roachpb.SystemTenantID && p.ExecCfg().Codec.ForSystemTenant() {
			// This empty table rekey acts as a poison-pill, which will be ignored by
			// a current processor but reliably cause an older processor, which would
			// otherwise mishandle tenant-made backup keys, to fail as it will be
			// unable to decode the zero ID table desc.
			rekeys = append(rekeys, execinfrapb.TableRekey{})
		}
	}

	// If, and only if, the backup was made by a system tenant, can it contain
	// backed up tenants, which the processor needs to know when is rekeying -- if
	// the backup contains tenants, then a key with a tenant prefix should be
	// restored if, and only if, we're restoring that tenant, and restored to a
	// tenant. Otherwise, if this backup was not made by a system tenant, it does
	// not contain tenants, so the rekey will assume if a key has a tenant prefix,
	// it is because the tenant produced the backup, and it should be removed to
	// then decode the remainder of the key. We communicate this distinction to
	// the processor with a special tenant rekey _into_ the system tenant, which
	// would never otherwise be valid. It will discard this rekey but it signals
	// to it that we're rekeying a system-made backup.
	var tenantRekeys []execinfrapb.TenantRekey
	if backupTenantID == roachpb.SystemTenantID {
		tenantRekeys = append(tenantRekeys, isBackupFromSystemTenantRekey)
	}

	pkIDs := make(map[uint64]bool)
	for _, tbl := range tables {
		pkIDs[kvpb.BulkOpSummaryID(uint64(tbl.GetID()), uint64(tbl.GetPrimaryIndexID()))] = true
	}

	dataToPreRestore = &restorationDataBase{
		spans:        preRestoreSpans,
		tableRekeys:  rekeys,
		tenantRekeys: tenantRekeys,
		pkIDs:        pkIDs,
	}

	trackedRestore = &mainRestorationData{
		restorationDataBase{
			spans:        postRestoreSpans,
			tableRekeys:  rekeys,
			tenantRekeys: tenantRekeys,
			pkIDs:        pkIDs,
		},
	}

	preValidation = &restorationDataBase{}
	// During a RESTORE with verify_backup_table_data data, progress on
	// verifySpans should be the source of job progress (as it will take the most time); therefore,
	// wrap them in a mainRestoration struct and unwrap postRestoreSpans
	// (only relevant during a cluster restore).
	if details.VerifyData {
		trackedRestore.restorationDataBase.spans = verifySpans
		trackedRestore.restorationDataBase.validateOnly = true

		// Before the main (validation) flow, during a cluster level restore,
		// we still need to restore system tables that do NOT get restored in the dataToPreRestore
		// flow. This restoration will not get tracked during job progress.
		if (details.DescriptorCoverage != tree.AllDescriptors) && len(postRestoreSpans) != 0 {
			return nil, nil, nil, errors.AssertionFailedf(
				"no spans should get restored in a non cluster, verify_backup_table_data restore")
		}
		preValidation.spans = postRestoreSpans
		preValidation.tableRekeys = rekeys
		preValidation.pkIDs = pkIDs
	}

	if tempSystemDBID != descpb.InvalidID {
		for _, table := range preRestoreTables {
			if table.GetParentID() == tempSystemDBID {
				dataToPreRestore.systemTables = append(dataToPreRestore.systemTables, table)
			}
		}
		for _, table := range postRestoreTables {
			if table.GetParentID() == tempSystemDBID {
				if details.VerifyData {
					// During a verify_backup_table_data RESTORE, system tables are
					// restored pre validation. Note that the system tables are still
					// added to the trackedRestore flow because after ingestion, the
					// restore job uses systemTable metadata hanging from the
					// trackedRestore object.
					preValidation.systemTables = append(preValidation.systemTables, table)
				}
				trackedRestore.systemTables = append(trackedRestore.systemTables, table)
			}
		}
	}
	return dataToPreRestore, preValidation, trackedRestore, nil
}

// protectRestoreTargets issues a protected timestamp over the targets we seek
// to restore and writes the pts record to the job record. If a pts already
// exists in the job record, due to previous call of this function, this noops.
func protectRestoreTargets(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	job *jobs.Job,
	details jobspb.RestoreDetails,
	tenantRekeys []execinfrapb.TenantRekey,
) (jobsprotectedts.Cleaner, error) {
	if details.ProtectedTimestampRecord != nil {
		// A protected time stamp has already been set. No need to write a new one.
		return nil, nil
	}
	var target *ptpb.Target
	switch {
	case details.DescriptorCoverage == tree.AllDescriptors:
		// During a cluster restore, protect the whole key space.
		target = ptpb.MakeClusterTarget()
	case len(details.Tenants) > 0:
		// During restores of tenants, protect whole tenant key spans.
		tenantIDs := make([]roachpb.TenantID, 0, len(tenantRekeys))
		for _, tenant := range tenantRekeys {
			if tenant.OldID == roachpb.SystemTenantID {
				// The system tenant rekey acts as metadata for restore processors during
				// restores of tenants. The host tenant's keyspace does not need protection.
				// https://github.com/cockroachdb/cockroach/pull/73647
				continue
			}
			tenantIDs = append(tenantIDs, tenant.NewID)
		}
		target = ptpb.MakeTenantsTarget(tenantIDs)
	case len(details.DatabaseDescs) > 0:
		// During database restores, protect whole databases.
		databaseIDs := make([]descpb.ID, 0, len(details.DatabaseDescs))
		for i := range details.DatabaseDescs {
			databaseIDs = append(databaseIDs, details.DatabaseDescs[i].GetID())
		}
		target = ptpb.MakeSchemaObjectsTarget(databaseIDs)
	default:
		// Else, protect individual tables.
		tableIDs := make([]descpb.ID, 0, len(details.TableDescs))
		for i := range details.TableDescs {
			tableIDs = append(tableIDs, details.TableDescs[i].GetID())
		}
		target = ptpb.MakeSchemaObjectsTarget(tableIDs)
	}
	// Set the PTS with a timestamp less than any upcoming batch request
	// timestamps from future addSSTable requests. This ensures that a target's
	// gcthreshold never creeps past a batch request timestamp, preventing a slow
	// addSStable request from failing.
	protectedTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	return execCfg.ProtectedTimestampManager.Protect(ctx, job, target, protectedTime)
}

// remapPublicSchemas is used to create a descriptor backed public schema
// for databases that have virtual public schemas.
// The rewrite map is updated with the new public schema id.
func remapPublicSchemas(
	ctx context.Context,
	p sql.JobExecContext,
	mutableDatabases []*dbdesc.Mutable,
	schemasToWrite *[]*schemadesc.Mutable,
	writtenSchemas *[]catalog.SchemaDescriptor,
	details *jobspb.RestoreDetails,
) error {
	databaseToPublicSchemaID := make(map[descpb.ID]descpb.ID)
	for _, db := range mutableDatabases {
		if db.HasPublicSchemaWithDescriptor() {
			continue
		}
		// mutableDatabases contains the list of databases being restored,
		// if the database does not have a public schema backed by a descriptor
		// (meaning they were created before 22.1), we need to create a public
		// schema descriptor for it.
		id, err := p.ExecCfg().DescIDGenerator.GenerateUniqueDescID(ctx)
		if err != nil {
			return err
		}

		includeCreatePriv := sqlclustersettings.PublicSchemaCreatePrivilegeEnabled.Get(p.ExecCfg().SV())

		db.AddSchemaToDatabase(catconstants.PublicSchemaName, descpb.DatabaseDescriptor_SchemaInfo{ID: id})
		// Every database must be initialized with the public schema.
		// Create the SchemaDescriptor.
		publicSchemaPrivileges := catpb.NewPublicSchemaPrivilegeDescriptor(db.Privileges.Owner(), includeCreatePriv)
		publicSchemaDesc := schemadesc.NewBuilder(&descpb.SchemaDescriptor{
			ParentID:   db.GetID(),
			Name:       catconstants.PublicSchemaName,
			ID:         id,
			Privileges: publicSchemaPrivileges,
			Version:    1,
		}).BuildCreatedMutableSchema()

		*schemasToWrite = append(*schemasToWrite, publicSchemaDesc)
		*writtenSchemas = append(*writtenSchemas, publicSchemaDesc)
		databaseToPublicSchemaID[db.GetID()] = id
	}

	// Now we need to handle rewriting the table parent schema ids.
	for id, rw := range details.DescriptorRewrites {
		if publicSchemaID, ok := databaseToPublicSchemaID[rw.ParentID]; ok {
			// For all items that were previously mapped to a synthetic public
			// schemas ID, update the ParentSchemaID to be the newly allocated ID.
			//
			// We also have to consider restoring tables from the system table
			// where the system public schema still uses 29 as an ID.
			if details.DescriptorRewrites[id].ParentSchemaID == keys.PublicSchemaIDForBackup ||
				details.DescriptorRewrites[id].ParentSchemaID == descpb.InvalidID {
				details.DescriptorRewrites[id].ParentSchemaID = publicSchemaID
			}
		}
	}

	return nil
}

// Resume is part of the jobs.Resumer interface.
func (r *restoreResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	if err := r.doResume(ctx, execCtx); err != nil {
		// Need to return the pause "error" as the main error here
		return errors.CombineErrors(p.ExecCfg().JobRegistry.CheckPausepoint("restore.after_restore_failure"), err)
	}
	return nil
}

func (r *restoreResumer) doResume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	r.execCfg = p.ExecCfg()

	details := r.job.Details().(jobspb.RestoreDetails)

	if err := maybeRelocateJobExecution(ctx, r.job.ID(), p, details.ExecutionLocality, "RESTORE"); err != nil {
		return err
	}
	if details.DownloadJob {
		if err := p.ExecCfg().JobRegistry.CheckPausepoint("restore.before_do_download_files"); err != nil {
			return err
		}
		return r.doDownloadFiles(ctx, p)
	}

	if err := p.ExecCfg().JobRegistry.CheckPausepoint("restore.before_load_descriptors_from_backup"); err != nil {
		return err
	}

	kmsEnv := backupencryption.MakeBackupKMSEnv(
		p.ExecCfg().Settings,
		&p.ExecCfg().ExternalIODirConfig,
		p.ExecCfg().InternalDB,
		p.User(),
	)
	mem := p.ExecCfg().RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)
	// Note that we ignore memSize because the memory account is closed in a
	// defer anyway.
	backupManifests, latestBackupManifest, sqlDescs, _, err := loadBackupSQLDescs(
		ctx, &mem, p, details, details.Encryption, &kmsEnv,
	)
	if err != nil {
		return err
	}
	if err := r.validateJobIsResumable(ctx, p.ExecCfg(), backupManifests); err != nil {
		return err
	}
	backupCodec, err := backupinfo.MakeBackupCodec(backupManifests)
	if err != nil {
		return err
	}
	lastBackupIndex, err := backupinfo.GetBackupIndexAtTime(backupManifests, details.EndTime)
	if err != nil {
		return err
	}
	defaultConf, err := cloud.ExternalStorageConfFromURI(details.URIs[lastBackupIndex], p.User())
	if err != nil {
		return errors.Wrapf(err, "creating external store configuration")
	}
	defaultStore, err := p.ExecCfg().DistSQLSrv.ExternalStorage(ctx, defaultConf)
	if err != nil {
		return err
	}
	preData, preValidateData, mainData, err := createImportingDescriptors(ctx, p, backupCodec, sqlDescs, r, latestBackupManifest)
	if err != nil {
		return err
	}

	// Refresh the job details since they may have been updated when creating the
	// importing descriptors.
	details = r.job.Details().(jobspb.RestoreDetails)

	if fn := r.testingKnobs.afterOfflineTableCreation; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}
	var remappedStats []*stats.TableStatisticProto
	backupStats, err := backupinfo.GetStatisticsFromBackup(ctx, defaultStore, details.Encryption,
		&kmsEnv, latestBackupManifest)
	if err == nil {
		remappedStats = remapAndFilterRelevantStatistics(ctx, backupStats, details.DescriptorRewrites,
			details.TableDescs)
	} else {
		// We don't want to fail the restore if we are unable to resolve statistics
		// from the backup, since they can be recomputed after the restore has
		// completed.
		log.Warningf(ctx, "failed to resolve table statistics from backup during restore: %+v",
			err.Error())
	}

	if len(details.TableDescs) == 0 && len(details.Tenants) == 0 && len(details.TypeDescs) == 0 {
		// We have no tables to restore (we are restoring an empty DB).
		// Since we have already created any new databases that we needed,
		// we can return without importing any data.
		log.Warning(ctx, "nothing to restore")
		// The database was created in the offline state and needs to be made
		// public.
		// TODO (lucy): Ideally we'd just create the database in the public state in
		// the first place, as a special case.
		publishDescriptors := func(ctx context.Context, txn descs.Txn) error {
			return r.publishDescriptors(
				ctx, p.ExecCfg().JobRegistry, p.ExecCfg().JobsKnobs(), txn, p.User(), details, p.ExecCfg().NodeInfo.LogicalClusterID(),
			)
		}
		if err := r.execCfg.InternalDB.DescsTxn(ctx, publishDescriptors); err != nil {
			return err
		}
		p.ExecCfg().JobRegistry.NotifyToAdoptJobs()
		if err := p.ExecCfg().JobRegistry.CheckPausepoint(
			"restore.after_publishing_descriptors"); err != nil {
			return err
		}
		if fn := r.testingKnobs.afterPublishingDescriptors; fn != nil {
			if err := fn(); err != nil {
				return err
			}
		}
		if err := r.maybeWriteDownloadJob(ctx, p.ExecCfg(), preData, mainData); err != nil {
			return err
		}
		emitRestoreJobEvent(ctx, p, jobs.StateSucceeded, r.job)
		return nil
	}

	for _, tenant := range details.Tenants {
		to, err := roachpb.MakeTenantID(tenant.ID)
		if err != nil {
			return err
		}
		from := to
		if details.PreRewriteTenantId != nil {
			from = *details.PreRewriteTenantId
		}
		mainData.addTenant(from, to)
	}

	_, err = protectRestoreTargets(ctx, p.ExecCfg(), r.job, details, mainData.tenantRekeys)
	if err != nil {
		return err
	}
	details = r.job.Details().(jobspb.RestoreDetails)
	if err := p.ExecCfg().JobRegistry.CheckPausepoint(
		"restore.before_flow"); err != nil {
		return err
	}

	var resTotal roachpb.RowCount

	if !preData.isEmpty() {
		res, err := restoreWithRetry(
			ctx,
			p,
			backupManifests,
			details.BackupLocalityInfo,
			details.EndTime,
			preData,
			r,
			details.Encryption,
			&kmsEnv,
		)
		if err != nil {
			return err
		}

		resTotal.Add(res)

		if details.DescriptorCoverage == tree.AllDescriptors {
			if err := r.restoreSystemTables(
				ctx, p.ExecCfg().InternalDB, preData.systemTables,
			); err != nil {
				return err
			}
			// Reload the details as we may have updated the job.
			details = r.job.Details().(jobspb.RestoreDetails)
		}

		if fn := r.testingKnobs.afterPreRestore; fn != nil {
			if err := fn(); err != nil {
				return err
			}
		}
		log.Infof(ctx, "finished restoring the pre-data bundle")
	}

	if err := p.ExecCfg().JobRegistry.CheckPausepoint("restore.after_pre_data"); err != nil {
		return err
	}

	if !preValidateData.isEmpty() {
		res, err := restoreWithRetry(
			ctx,
			p,
			backupManifests,
			details.BackupLocalityInfo,
			details.EndTime,
			preValidateData,
			r,
			details.Encryption,
			&kmsEnv,
		)
		if err != nil {
			return err
		}

		resTotal.Add(res)
		log.Infof(ctx, "finished restoring the validate data bundle")
	}
	{
		// Restore the main data bundle. We notably only restore the system tables
		// later.
		res, err := restoreWithRetry(
			ctx,
			p,
			backupManifests,
			details.BackupLocalityInfo,
			details.EndTime,
			mainData,
			r,
			details.Encryption,
			&kmsEnv,
		)
		if err != nil {
			return err
		}

		resTotal.Add(res)
		log.Infof(ctx, "finished restoring the main data bundle")
	}

	if err := insertStats(ctx, r.job, p.ExecCfg(), remappedStats); err != nil {
		return errors.Wrap(err, "inserting table statistics")
	}

	publishDescriptors := func(ctx context.Context, txn descs.Txn) (err error) {
		return r.publishDescriptors(
			ctx, p.ExecCfg().JobRegistry, p.ExecCfg().JobsKnobs(), txn, p.User(),
			details, p.ExecCfg().NodeInfo.LogicalClusterID(),
		)
	}
	if err := r.execCfg.InternalDB.DescsTxn(ctx, publishDescriptors); err != nil {
		return err
	}

	if err := p.ExecCfg().JobRegistry.CheckPausepoint(
		"restore.after_publishing_descriptors"); err != nil {
		return err
	}
	if fn := r.testingKnobs.afterPublishingDescriptors; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}

	// Reload the details as we may have updated the job.
	details = r.job.Details().(jobspb.RestoreDetails)
	p.ExecCfg().JobRegistry.NotifyToAdoptJobs()

	if details.DescriptorCoverage == tree.AllDescriptors {
		// We restore the system tables from the main data bundle so late because it
		// includes the jobs that are being restored. As soon as we restore these
		// jobs, they become accessible to the user, and may start executing. We
		// need this to happen after the descriptors have been marked public.
		if err := r.restoreSystemTables(
			ctx, p.ExecCfg().InternalDB, mainData.systemTables,
		); err != nil {
			return err
		}
		// Reload the details as we may have updated the job.
		details = r.job.Details().(jobspb.RestoreDetails)

		if err := r.cleanupTempSystemTables(ctx); err != nil {
			return err
		}
	} else if isSystemUserRestore(details) {
		if err := r.restoreSystemUsers(ctx, p.ExecCfg().InternalDB, mainData.systemTables); err != nil {
			return err
		}
		details = r.job.Details().(jobspb.RestoreDetails)

		if err := r.cleanupTempSystemTables(ctx); err != nil {
			return err
		}
	}

	if details.DescriptorCoverage != tree.RequestedDescriptors {
		// Bump the version of the role membership table so that the cache is
		// invalidated.
		if err := r.execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			txn.KV().SetDebugName("system-restore-bump-role-membership-table")
			log.Eventf(ctx, "bumping table version of %s", systemschema.RoleMembersTable.GetName())

			td, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, keys.RoleMembersTableID)
			if err != nil {
				return errors.Wrapf(err, "fetching table %s", systemschema.RoleMembersTable.GetName())
			}
			td.MaybeIncrementVersion()
			if err := txn.Descriptors().WriteDesc(ctx, false, td, txn.KV()); err != nil {
				return errors.Wrapf(err, "bumping table version for %s", systemschema.RoleMembersTable.GetName())
			}

			return nil
		}); err != nil {
			return err
		}
	}

	if err := r.execCfg.ProtectedTimestampManager.Unprotect(ctx, r.job); err != nil {
		log.Errorf(ctx, "failed to release protected timestamp: %v", err)
	}
	if !details.ExperimentalOnline {
		r.notifyStatsRefresherOfNewTables()
	}

	r.restoreStats = resTotal
	if err := r.maybeWriteDownloadJob(ctx, p.ExecCfg(), preData, mainData); err != nil {
		return err
	}

	// Emit an event now that the restore job has completed.
	emitRestoreJobEvent(ctx, p, jobs.StateSucceeded, r.job)

	// Restore used all available SQL instances.
	_, sqlInstanceIDs, err := p.DistSQLPlanner().SetupAllNodesPlanning(ctx, p.ExtendedEvalContext(), p.ExecCfg())
	if err != nil {
		return err
	}
	numNodes := len(sqlInstanceIDs)
	if numNodes == 0 {
		// This shouldn't ever happen, but we know that we have at least one
		// instance (which is running this code right now).
		numNodes = 1
	}

	// Collect telemetry.
	{
		telemetry.Count("restore.total.succeeded")
		const mb = 1 << 20
		sizeMb := resTotal.DataSize / mb
		sec := int64(timeutil.Since(timeutil.FromUnixMicros(r.job.Payload().StartedMicros)).Seconds())
		var mbps int64
		if sec > 0 {
			mbps = mb / sec
		}
		telemetry.CountBucketed("restore.duration-sec.succeeded", sec)
		telemetry.CountBucketed("restore.size-mb.full", sizeMb)
		telemetry.CountBucketed("restore.speed-mbps.total", mbps)
		telemetry.CountBucketed("restore.speed-mbps.per-node", mbps/int64(numNodes))
		// Tiny restores may skew throughput numbers due to overhead.
		if sizeMb > 10 {
			telemetry.CountBucketed("restore.speed-mbps.over10mb", mbps)
			telemetry.CountBucketed("restore.speed-mbps.over10mb.per-node", mbps/int64(numNodes))
		}
		logutil.LogJobCompletion(ctx, restoreJobEventType, r.job.ID(), true, nil, resTotal.Rows)
	}
	return nil
}

func clusterRestoreDuringUpgradeErr(
	clusterVersion roachpb.Version, binaryVersion roachpb.Version,
) error {
	return errors.Errorf("cluster restore not supported during major version upgrade: restore started at cluster version %s but binary version is %s",
		clusterVersion,
		binaryVersion)
}

// validateJobDetails returns an error if this job cannot be resumed.
func (r *restoreResumer) validateJobIsResumable(
	ctx context.Context,
	execConfig *sql.ExecutorConfig,
	mainBackupManifests []backuppb.BackupManifest,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)

	allowUnsafeRestore := details.UnsafeRestoreIncompatibleVersion
	if strings.Contains(r.job.Payload().Description, "unsafe_restore_incompatible_version") {
		// This added check ensures we continue with unsafe restore in the
		// following mixed version case: user plans a restore on 23.1 which will
		// not set the unsafe restore field in the job proto, and resumes it on
		// a 23.2.
		allowUnsafeRestore = true
	}

	if err := checkBackupManifestVersionCompatability(ctx, execConfig.Settings.Version,
		mainBackupManifests, allowUnsafeRestore); err != nil {
		return err
	}

	// Validate that we aren't in the middle of an upgrade. To avoid unforseen
	// issues, we want to avoid full cluster restores if it is possible that an
	// upgrade is in progress. We also check this during planning.
	//
	// Note: If the cluster began in a mixed version state,
	// the CreationClusterVersion may still be equal to binaryVersion,
	// which means the cluster restore will proceed.
	creationClusterVersion := r.job.Payload().CreationClusterVersion
	latestVersion := execConfig.Settings.Version.LatestVersion()
	isClusterRestore := details.DescriptorCoverage == tree.AllDescriptors
	if isClusterRestore && creationClusterVersion.Less(latestVersion) {
		return clusterRestoreDuringUpgradeErr(creationClusterVersion, latestVersion)
	}
	return nil
}

// isSystemUserRestore checks if the user called RESTORE SYSTEM USERS and guards
// against any mixed version issues. In 22.2, details.DescriptorCoverage
// identifies a system user restore, while in 22.1, details.RestoreSystemUsers
// identified this flavour of restore.
//
// TODO(msbutler): delete in 23.1
func isSystemUserRestore(details jobspb.RestoreDetails) bool {
	return details.DescriptorCoverage == tree.SystemUsers || details.RestoreSystemUsers
}

// ReportResults implements JobResultsReporter interface.
func (r *restoreResumer) ReportResults(ctx context.Context, resultsCh chan<- tree.Datums) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case resultsCh <- func() tree.Datums {
		details := r.job.Details().(jobspb.RestoreDetails)
		if details.ExperimentalOnline {
			return tree.Datums{
				tree.NewDInt(tree.DInt(r.job.ID())),
				tree.NewDInt(tree.DInt(len(details.TableDescs))),
				tree.NewDInt(tree.DInt(r.restoreStats.Rows)),
				tree.NewDInt(tree.DInt(r.restoreStats.DataSize)),
				tree.NewDInt(tree.DInt(r.downloadJobID)),
			}
		} else {
			return tree.Datums{
				tree.NewDInt(tree.DInt(r.job.ID())),
				tree.NewDString(string(jobs.StateSucceeded)),
				tree.NewDFloat(tree.DFloat(1.0)),
				tree.NewDInt(tree.DInt(r.restoreStats.Rows)),
			}
		}
	}():
		return nil
	}
}

// Initiate a run of CREATE STATISTICS. We don't know the actual number of
// rows affected per table, so we use a large number because we want to make
// sure that stats always get created/refreshed here.
func (r *restoreResumer) notifyStatsRefresherOfNewTables() {
	details := r.job.Details().(jobspb.RestoreDetails)
	for i := range details.TableDescs {
		desc := tabledesc.NewBuilder(details.TableDescs[i]).BuildImmutableTable()
		r.execCfg.StatsRefresher.NotifyMutation(desc, math.MaxInt32 /* rowsAffected */)
	}
}

// tempSystemDatabaseID returns the ID of the descriptor for the temporary
// system database used in full cluster restores, by finding a table in the
// rewrites that had the static system database ID as its parent and returning
// the new parent assigned in the rewrites during planning. Returns InvalidID if
// no such table appears in the rewrites.
func tempSystemDatabaseID(
	details jobspb.RestoreDetails, tables []catalog.TableDescriptor,
) descpb.ID {
	if details.DescriptorCoverage != tree.AllDescriptors && !isSystemUserRestore(details) {
		return descpb.InvalidID
	}

	for _, tbl := range tables {
		if tbl.GetParentID() == keys.SystemDatabaseID {
			if details.DescriptorRewrites[tbl.GetID()].ParentID != 0 {
				return details.DescriptorRewrites[tbl.GetID()].ParentID
			}
		}
	}

	return descpb.InvalidID
}

// Insert stats re-inserts the table statistics stored in the backup manifest.
func insertStats(
	ctx context.Context,
	job *jobs.Job,
	execCfg *sql.ExecutorConfig,
	latestStats []*stats.TableStatisticProto,
) error {
	details := job.Details().(jobspb.RestoreDetails)

	if details.SchemaOnly {
		// Only insert table stats from the backup manifest if actual data was restored.
		return nil
	}
	if details.StatsInserted {
		return nil
	}
	if len(latestStats) == 0 {
		return nil
	}

	// We could be restoring hundreds of tables, so insert the new stats in
	// batches instead of all in a single, long-running txn. This prevents intent
	// buildup in the face of txn retries.
	batchSize := restoreStatsInsertBatchSize
	totalNumBatches := len(latestStats) / batchSize
	if len(latestStats)%batchSize != 0 {
		totalNumBatches += 1
	}
	log.Infof(ctx, "restore will insert %d TableStatistics in %d batches", len(latestStats), totalNumBatches)
	insertStatsProgress := log.Every(10 * time.Second)

	startingStatsInsertion := timeutil.Now()
	batchCh := make(chan []*stats.TableStatisticProto, totalNumBatches)
	for {
		if len(latestStats) == 0 {
			break
		}
		if len(latestStats) < batchSize {
			batchSize = len(latestStats)
		}
		batchCh <- latestStats[:batchSize]

		// Truncate the stats that we have inserted in the txn above.
		latestStats = latestStats[batchSize:]
	}
	close(batchCh)

	logStatsProgress := func(remainingBatches, completedBatches int) {
		msg := fmt.Sprintf("restore has %d/%d TableStatistics batches remaining to insert",
			remainingBatches, totalNumBatches)
		timeSinceStart := int(timeutil.Since(startingStatsInsertion).Seconds())
		if completedBatches != 0 && timeSinceStart != 0 {
			rate := completedBatches / timeSinceStart
			msg = fmt.Sprintf("%s; ingesting at the rate of %d batches/sec", msg, rate)
		}
		log.Infof(ctx, "%s", msg)
	}

	mu := struct {
		syncutil.Mutex
		completedBatches int
	}{}
	if err := ctxgroup.GroupWorkers(ctx, int(restoreStatsInsertionConcurrency.Get(&execCfg.Settings.SV)),
		func(ctx context.Context, i int) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case b, ok := <-batchCh:
					if !ok {
						return nil
					}
					if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
						if err := stats.InsertNewStats(
							ctx, execCfg.Settings, txn, b,
						); err != nil {
							return errors.Wrapf(err, "inserting stats from backup")
						}
						return nil
					}); err != nil {
						return err
					}
					mu.Lock()
					mu.completedBatches++
					remainingBatches := totalNumBatches - mu.completedBatches
					completedBatches := mu.completedBatches
					mu.Unlock()
					if insertStatsProgress.ShouldLog() {
						logStatsProgress(remainingBatches, completedBatches)
					}
				}
			}
		}); err != nil {
		return errors.Wrap(err, "failed to restore stats")
	}
	logStatsProgress(0, totalNumBatches)

	// Mark the stats insertion complete.
	details.StatsInserted = true
	return errors.Wrap(job.NoTxn().SetDetails(ctx, details), "updating job marking stats insertion complete")
}

// publishDescriptors updates the RESTORED descriptors' status from OFFLINE to
// PUBLIC. The schema change jobs are returned to be started after the
// transaction commits. The details struct is passed in rather than loaded
// from r.job as the call to r.job.SetDetails will overwrite the job details
// with a new value even if this transaction does not commit.
func (r *restoreResumer) publishDescriptors(
	ctx context.Context,
	jobsRegistry *jobs.Registry,
	jobsKnobs *jobs.TestingKnobs,
	txn descs.Txn,
	user username.SQLUsername,
	details jobspb.RestoreDetails,
	clusterID uuid.UUID,
) (err error) {
	if details.DescriptorsPublished {
		return nil
	}

	if err := jobsRegistry.CheckPausepoint("restore.before_publishing_descriptors"); err != nil {
		return err
	}

	if fn := r.testingKnobs.beforePublishingDescriptors; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}
	log.VEventf(ctx, 1, "making tables live")

	// Write the new descriptors and flip state over to public so they can be
	// accessed.
	const kvTrace = false

	// Pre-fetch all the descriptors into the collection to avoid doing
	// round-trips per descriptor.
	all, err := prefetchDescriptors(ctx, txn.KV(), txn.Descriptors(), details)
	if err != nil {
		return err
	}

	// Create slices of raw descriptors for the restore job details.
	newTables := make([]*descpb.TableDescriptor, 0, len(details.TableDescs))
	newTypes := make([]*descpb.TypeDescriptor, 0, len(details.TypeDescs))
	newSchemas := make([]*descpb.SchemaDescriptor, 0, len(details.SchemaDescs))
	newDBs := make([]*descpb.DatabaseDescriptor, 0, len(details.DatabaseDescs))
	newFunctions := make([]*descpb.FunctionDescriptor, 0, len(details.FunctionDescs))

	// Go through the descriptors and find any declarative schema change jobs
	// affecting them.
	if _, err := scbackup.CreateDeclarativeSchemaChangeJobs(
		ctx, r.execCfg.JobRegistry, txn, all,
	); err != nil {
		return err
	}

	var tableAutoStatsSettings map[uint32]*catpb.AutoStatsSettings
	if details.ExperimentalOnline {
		tableAutoStatsSettings = make(map[uint32]*catpb.AutoStatsSettings, len(details.TableDescs))
	}

	// Write the new TableDescriptors and flip state over to public so they can be
	// accessed.
	for i := range details.TableDescs {
		desc := all.LookupDescriptor(details.TableDescs[i].GetID())
		mutTable := desc.(*tabledesc.Mutable)

		if details.ExperimentalOnline && mutTable.IsTable() {
			// We disable automatic stats refresh on all restored tables until the
			// download job finishes.
			boolean := false
			mutTable.AutoStatsSettings = &catpb.AutoStatsSettings{Enabled: &boolean}

			// Preserve the backed up table stats so the download job re-enables them
			tableAutoStatsSettings[uint32(details.TableDescs[i].ID)] = details.TableDescs[i].AutoStatsSettings
		}

		// Note that we don't need to worry about the re-validated indexes for descriptors
		// with a declarative schema change job.
		if mutTable.GetDeclarativeSchemaChangerState() != nil {
			newTables = append(newTables, mutTable.TableDesc())
			continue
		}

		version := r.settings.Version.ActiveVersion(ctx)
		if err := mutTable.AllocateIDs(ctx, version); err != nil {
			return err
		}
		// Assign a TTL schedule before publishing.
		if mutTable.HasRowLevelTTL() {
			j, err := sql.CreateRowLevelTTLScheduledJob(
				ctx,
				jobsKnobs,
				jobs.ScheduledJobTxn(txn),
				user,
				mutTable,
				clusterID,
				version,
			)
			if err != nil {
				return err
			}
			mutTable.RowLevelTTL.ScheduleID = j.ScheduleID()
		}

		newTables = append(newTables, mutTable.TableDesc())

		// Convert any mutations that were in progress on the table descriptor
		// when the backup was taken, and convert them to schema change jobs.
		if err := createSchemaChangeJobsFromMutations(ctx,
			r.execCfg.JobRegistry, r.execCfg.Codec, txn, r.job.Payload().UsernameProto.Decode(), mutTable,
		); err != nil {
			return err
		}

		if details.ExperimentalOnline && epochBasedInProgressImport(desc) {
			if err := createImportRollbackJob(ctx,
				r.execCfg.JobRegistry, txn, r.job.Payload().UsernameProto.Decode(), mutTable,
			); err != nil {
				return err
			}
		} else {
			// If this was an importing table, it is now effectively _not_
			// importing.
			mutTable.FinalizeImport()
		}
	}
	// For all of the newly created types, make type schema change jobs for any
	// type descriptors that were backed up in the middle of a type schema change.
	for i := range details.TypeDescs {
		typ := all.LookupDescriptor(details.TypeDescs[i].GetID()).(catalog.TypeDescriptor)
		newTypes = append(newTypes, typ.TypeDesc())
		if typ.GetDeclarativeSchemaChangerState() == nil &&
			typ.HasPendingSchemaChanges() {
			if err := createTypeChangeJobFromDesc(
				ctx, r.execCfg.JobRegistry, r.execCfg.Codec, txn, r.job.Payload().UsernameProto.Decode(), typ,
			); err != nil {
				return err
			}
		}
	}
	for i := range details.SchemaDescs {
		sc := all.LookupDescriptor(details.SchemaDescs[i].GetID()).(catalog.SchemaDescriptor)
		newSchemas = append(newSchemas, sc.SchemaDesc())
	}
	for i := range details.DatabaseDescs {
		db := all.LookupDescriptor(details.DatabaseDescs[i].GetID()).(catalog.DatabaseDescriptor)
		newDBs = append(newDBs, db.DatabaseDesc())
	}
	for i := range details.FunctionDescs {
		fn := all.LookupDescriptor(details.FunctionDescs[i].GetID()).(catalog.FunctionDescriptor)
		newFunctions = append(newFunctions, fn.FuncDesc())
	}
	b := txn.KV().NewBatch()
	if err := all.ForEachDescriptor(func(desc catalog.Descriptor) error {
		d := desc.(catalog.MutableDescriptor)
		if details.ExperimentalOnline && epochBasedInProgressImport(desc) {
			log.Infof(ctx, "table %q (%d) with in-progress IMPORT remaining offline", desc.GetName(), desc.GetID())
		} else {
			d.SetPublic()
		}
		return txn.Descriptors().WriteDescToBatch(ctx, kvTrace, d, b)
	}); err != nil {
		return err
	}
	if err := txn.KV().Run(ctx, b); err != nil {
		return errors.Wrap(err, "publishing tables")
	}

	for _, tenant := range details.Tenants {
		switch tenant.DataState {
		case mtinfopb.DataStateReady:
			// If the tenant was backed up in the `READY` state then we must activate
			// the tenant as the final step of the restore. The tenant has already
			// been created at an earlier stage in the restore in an `ADD` state.
			if err := sql.ActivateRestoredTenant(
				ctx, r.execCfg.Settings, r.execCfg.Codec, txn, tenant.ID, tenant.ServiceMode,
			); err != nil {
				return err
			}
		case mtinfopb.DataStateDrop, mtinfopb.DataStateAdd:
		// If the tenant was backed up in a `DROP` or `ADD` state then we do not
		// want to activate the tenant.
		default:
			return errors.AssertionFailedf("unknown tenant data state %v", tenant)
		}
	}

	// Update and persist the state of the job.
	details.DescriptorsPublished = true
	details.TableDescs = newTables
	details.TypeDescs = newTypes
	details.SchemaDescs = newSchemas
	details.DatabaseDescs = newDBs
	details.FunctionDescs = newFunctions
	if details.ExperimentalOnline {
		details.PostDownloadTableAutoStatsSettings = tableAutoStatsSettings
	}
	if err := r.job.WithTxn(txn).SetDetails(ctx, details); err != nil {
		return errors.Wrap(err,
			"updating job details after publishing tables")
	}
	return nil
}

// prefetchDescriptors calculates the set of descriptors needed by looking
// at the relevant fields of the job details. It then fetches all of those
// descriptors in a batch using the descsCol. It packages up that set of
// descriptors into an nstree.Catalog for easy use.
//
// This function also takes care of asserting that the retrieved version
// matches the expectation.
func prefetchDescriptors(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, details jobspb.RestoreDetails,
) (_ nstree.Catalog, _ error) {
	var all nstree.MutableCatalog
	var allDescIDs catalog.DescriptorIDSet
	expVersion := map[descpb.ID]descpb.DescriptorVersion{}
	for i := range details.TableDescs {
		expVersion[details.TableDescs[i].GetID()] = details.TableDescs[i].GetVersion()
		allDescIDs.Add(details.TableDescs[i].GetID())
	}
	for i := range details.TypeDescs {
		expVersion[details.TypeDescs[i].GetID()] = details.TypeDescs[i].GetVersion()
		allDescIDs.Add(details.TypeDescs[i].GetID())
	}
	for i := range details.SchemaDescs {
		expVersion[details.SchemaDescs[i].GetID()] = details.SchemaDescs[i].GetVersion()
		allDescIDs.Add(details.SchemaDescs[i].GetID())
	}
	for i := range details.DatabaseDescs {
		expVersion[details.DatabaseDescs[i].GetID()] = details.DatabaseDescs[i].GetVersion()
		allDescIDs.Add(details.DatabaseDescs[i].GetID())
	}
	for i := range details.FunctionDescs {
		expVersion[details.FunctionDescs[i].GetID()] = details.FunctionDescs[i].GetVersion()
		allDescIDs.Add(details.FunctionDescs[i].GetID())
	}
	// Note that no maximum size is put on the batch here because,
	// in general, we assume that we can fit all of the descriptors
	// in RAM (we have them in RAM as part of the details object,
	// and we're going to write them to KV very soon as part of a
	// single batch).
	ids := allDescIDs.Ordered()
	got, err := descsCol.MutableByID(txn).Descs(ctx, ids)
	if err != nil {
		return nstree.Catalog{}, errors.Wrap(err, "prefetch descriptors")
	}
	for i, id := range ids {
		if got[i].GetVersion() != expVersion[id] {
			return nstree.Catalog{}, errors.Errorf(
				"version mismatch for descriptor %d, expected version %d, got %v",
				got[i].GetID(), expVersion[id], got[i].GetVersion(),
			)
		}
		all.UpsertDescriptor(got[i])
	}
	return all.Catalog, nil
}

func emitRestoreJobEvent(
	ctx context.Context, p sql.JobExecContext, state jobs.State, job *jobs.Job,
) {
	// Emit to the event log now that we have completed the prepare step.
	var restoreEvent eventpb.Restore
	if err := p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return sql.LogEventForJobs(ctx, p.ExecCfg(), txn, &restoreEvent, int64(job.ID()),
			job.Payload(), p.User(), state)
	}); err != nil {
		log.Warningf(ctx, "failed to log event: %v", err)
	}
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes KV data that
// has been committed from a restore that has failed or been canceled. It does
// this by adding the table descriptors in DROP state, which causes the schema
// change stuff to delete the keys in the background.
func (r *restoreResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	p := execCtx.(sql.JobExecContext)
	r.execCfg = p.ExecCfg()

	details := r.job.Details().(jobspb.RestoreDetails)

	// If this is a download-only job, there's no cleanup to do on cancel.
	if len(details.DownloadSpans) > 0 {
		return nil
	}

	// Emit to the event log that the job has started reverting.
	emitRestoreJobEvent(ctx, p, jobs.StateReverting, r.job)

	telemetry.Count("restore.total.failed")
	telemetry.CountBucketed("restore.duration-sec.failed",
		int64(timeutil.Since(timeutil.FromUnixMicros(r.job.Payload().StartedMicros)).Seconds()))

	if err := r.execCfg.ProtectedTimestampManager.Unprotect(ctx, r.job); errors.Is(err, protectedts.ErrNotExists) {
		// No reason to return an error which might cause problems if it doesn't
		// seem to exist.
		log.Warningf(ctx, "failed to release protected which seems not to exist: %v", err)
		err = nil
	} else if err != nil {
		return err
	}

	logutil.LogJobCompletion(ctx, restoreJobEventType, r.job.ID(), false, jobErr, r.restoreStats.Rows)

	execCfg := execCtx.(sql.JobExecContext).ExecCfg()
	if err := execCfg.InternalDB.Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) error {
		for _, tenant := range details.Tenants {
			tenant.DataState = mtinfopb.DataStateDrop
			// This is already a job so no need to spin up a gc job for the tenant;
			// instead just GC the data eagerly.
			if err := sql.GCTenantSync(ctx, execCfg, tenant.ToInfo()); err != nil {
				return err
			}
		}

		if err := r.dropDescriptors(
			ctx, execCfg.JobRegistry, execCfg.Codec, txn, descs.FromTxn(txn),
		); err != nil {
			return err
		}

		if details.DescriptorCoverage == tree.AllDescriptors {
			// We've dropped defaultdb and postgres in the planning phase, we must
			// recreate them now if the full cluster restore failed.
			_, err := txn.Exec(ctx, "recreate-defaultdb", txn.KV(),
				"CREATE DATABASE IF NOT EXISTS defaultdb")
			if err != nil {
				return err
			}

			_, err = txn.Exec(ctx, "recreate-postgres", txn.KV(),
				"CREATE DATABASE IF NOT EXISTS postgres")
			if err != nil {
				return err
			}
		}
		return nil
	}, isql.WithSessionData(p.SessionData())); err != nil {
		return err
	}

	if details.DescriptorCoverage == tree.AllDescriptors {
		// The temporary system table descriptors should already have been dropped
		// in `dropDescriptors` but we still need to drop the temporary system db.
		if err := r.cleanupTempSystemTables(ctx); err != nil {
			return err
		}
	}

	// Emit to the event log that the job has completed reverting.
	emitRestoreJobEvent(ctx, p, jobs.StateFailed, r.job)
	return nil
}

// CollectProfile implements jobs.Resumer interface
func (r *restoreResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)

	var aggStatsCopy bulkutil.ComponentAggregatorStats
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		aggStatsCopy = r.mu.perNodeAggregatorStats.DeepCopy()
	}()
	return bulkutil.FlushTracingAggregatorStats(ctx, r.job.ID(),
		p.ExecCfg().InternalDB, aggStatsCopy)
}

// dropDescriptors implements the OnFailOrCancel logic.
// TODO (lucy): If the descriptors have already been published, we need to queue
// drop jobs for all the descriptors.
func (r *restoreResumer) dropDescriptors(
	ctx context.Context,
	jr *jobs.Registry,
	codec keys.SQLCodec,
	txn isql.Txn,
	descsCol *descs.Collection,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)

	// No need to mark the tables as dropped if they were not even created in the
	// first place.
	if !details.PrepareCompleted {
		return nil
	}

	b := txn.KV().NewBatch()
	const kvTrace = false
	// Collect the tables into mutable versions.
	mutableTables := make([]*tabledesc.Mutable, len(details.TableDescs))
	for i := range details.TableDescs {
		var err error
		mutableTables[i], err = descsCol.MutableByID(txn.KV()).Table(ctx, details.TableDescs[i].ID)
		if err != nil {
			return err
		}
		// Ensure that the version matches what we expect. In the case that it
		// doesn't, it's not really clear what to do. Just log and carry on. If the
		// descriptors have already been published, then there's nothing to fuss
		// about so we only do this check if they have not been published.
		if !details.DescriptorsPublished {
			if got, exp := mutableTables[i].Version, details.TableDescs[i].Version; got != exp {
				log.Errorf(ctx, "version changed for restored descriptor %d before "+
					"drop: got %d, expected %d", mutableTables[i].GetID(), got, exp)
			}
		}

	}

	// Remove any back references installed from existing types to tables being restored.
	if err := r.removeExistingTypeBackReferences(
		ctx, txn.KV(), descsCol, b, mutableTables, &details,
	); err != nil {
		return err
	}

	// Drop the table descriptors that were created at the start of the restore.
	tablesToGC := make([]descpb.ID, 0, len(details.TableDescs))
	// Set the drop time as 1 (ns in Unix time), so that the table gets GC'd
	// immediately.
	dropTime := int64(1)
	scheduledJobs := jobs.ScheduledJobTxn(txn)
	env := sql.JobSchedulerEnv(r.execCfg.JobsKnobs())
	for i := range mutableTables {
		tableToDrop := mutableTables[i]
		tablesToGC = append(tablesToGC, tableToDrop.ID)
		tableToDrop.SetDropped()

		// Drop any schedules we may have implicitly created.
		if tableToDrop.HasRowLevelTTL() {
			scheduleID := tableToDrop.RowLevelTTL.ScheduleID
			if scheduleID != 0 {
				if err := scheduledJobs.DeleteByID(ctx, env, scheduleID); err != nil {
					return err
				}
			}
		}

		// Arrange for fast GC of table data.
		//
		// The new (09-2022) GC job uses range deletion tombstones to clear data and
		// then waits for the MVCC GC process to clear the data before removing any
		// descriptors. To ensure that this happens quickly, we install a zone
		// configuration for every table that we are going to drop with a small GC TTL.
		canSetGCTTL := codec.ForSystemTenant() ||
			(sqlclustersettings.SecondaryTenantZoneConfigsEnabled.Get(&r.execCfg.Settings.SV) &&
				sqlclustersettings.SecondaryTenantsAllZoneConfigsEnabled.Get(&r.execCfg.Settings.SV))
		canSetGCTTL = canSetGCTTL && tableToDrop.IsPhysicalTable()
		if canSetGCTTL {
			if err := externalcatalog.SetGCTTLForDroppingTable(
				ctx, txn, descsCol, tableToDrop,
			); err != nil {
				log.Warningf(ctx, "setting low GC TTL for table %q failed: %s", tableToDrop.GetName(), err.Error())
			}
		} else {
			log.Infof(ctx, "cannot lower GC TTL for table %q", tableToDrop.GetName())
		}

		// In the legacy GC job, setting DropTime ensures a table uses RangeClear
		// for fast data removal. This operation starts at DropTime + the GC TTL. If
		// we used now() here, it would not clean up data until the TTL from the
		// time of the error. Instead, use 1 (that is, 1ns past the epoch) to allow
		// this to be cleaned up as soon as possible. This is safe since the table
		// data was never visible to users, and so we don't need to preserve MVCC
		// semantics.
		tableToDrop.DropTime = dropTime
		if err := descsCol.DeleteNamespaceEntryToBatch(ctx, kvTrace, tableToDrop, b); err != nil {
			return err
		}
		descsCol.NotifyOfDeletedDescriptor(tableToDrop.GetID())
	}

	// Drop the type descriptors that this restore created.
	for i := range details.TypeDescs {
		// TypeDescriptors don't have a GC job process, so we can just write them
		// as dropped here.
		typDesc := details.TypeDescs[i]
		mutType, err := descsCol.MutableByID(txn.KV()).Type(ctx, typDesc.ID)
		if err != nil {
			return err
		}
		mutType.SetDropped()

		if err := descsCol.DeleteNamespaceEntryToBatch(ctx, kvTrace, typDesc, b); err != nil {
			return err
		}
		if err := descsCol.DeleteDescToBatch(ctx, kvTrace, typDesc.GetID(), b); err != nil {
			return err
		}
	}

	for i := range details.FunctionDescs {
		fnDesc := details.FunctionDescs[i]
		mutFn, err := descsCol.MutableByID(txn.KV()).Function(ctx, fnDesc.ID)
		if err != nil {
			return err
		}
		mutFn.SetDropped()
		if err := descsCol.DeleteDescToBatch(ctx, kvTrace, fnDesc.ID, b); err != nil {
			return err
		}
	}

	// Queue a GC job.
	gcDetails := jobspb.SchemaChangeGCDetails{}
	for _, tableID := range tablesToGC {
		gcDetails.Tables = append(gcDetails.Tables, jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       tableID,
			DropTime: dropTime,
		})
	}
	gcJobRecord := jobs.Record{
		Description:   fmt.Sprintf("GC for %s", r.job.Payload().Description),
		Username:      r.job.Payload().UsernameProto.Decode(),
		DescriptorIDs: tablesToGC,
		Details:       gcDetails,
		Progress:      jobspb.SchemaChangeGCProgress{},
		NonCancelable: true,
	}
	if _, err := jr.CreateJobWithTxn(ctx, gcJobRecord, jr.MakeJobID(), txn); err != nil {
		return err
	}

	// Drop the database and schema descriptors that were created at the start of
	// the restore if they are now empty (i.e. no user created a table, etc. in
	// the database or schema during the restore).
	ignoredChildDescIDs := make(map[descpb.ID]struct{})
	for _, table := range details.TableDescs {
		ignoredChildDescIDs[table.ID] = struct{}{}
	}
	for _, typ := range details.TypeDescs {
		ignoredChildDescIDs[typ.ID] = struct{}{}
	}
	for _, schema := range details.SchemaDescs {
		ignoredChildDescIDs[schema.ID] = struct{}{}
	}
	for _, fn := range details.FunctionDescs {
		ignoredChildDescIDs[fn.ID] = struct{}{}
	}
	all, err := descsCol.GetAllDescriptors(ctx, txn.KV())
	if err != nil {
		return err
	}
	allDescs := all.OrderedDescriptors()

	// Delete any schema descriptors that this restore created. Also collect the
	// descriptors so we can update their parent databases later.
	type dbWithDeletedSchemas struct {
		db      *dbdesc.Mutable
		schemas []catalog.Descriptor
	}

	dbsWithDeletedSchemas := make(map[descpb.ID]dbWithDeletedSchemas)
	for _, schemaDesc := range details.SchemaDescs {
		// We need to ignore descriptors we just added since we haven't committed the txn that deletes these.
		isSchemaEmpty, err := isSchemaEmpty(ctx, txn.KV(), schemaDesc.GetID(), allDescs, ignoredChildDescIDs)
		if err != nil {
			return errors.Wrapf(err, "checking if schema %s is empty during restore cleanup", schemaDesc.GetName())
		}

		if !isSchemaEmpty {
			log.Warningf(ctx, "preserving schema %s on restore failure because it contains new child objects", schemaDesc.GetName())
			continue
		}

		mutSchema, err := descsCol.MutableByID(txn.KV()).Desc(ctx, schemaDesc.GetID())
		if err != nil {
			return err
		}
		entry, hasEntry := dbsWithDeletedSchemas[schemaDesc.GetParentID()]
		if !hasEntry {
			mutParent, err := descsCol.MutableByID(txn.KV()).Desc(ctx, schemaDesc.GetParentID())
			if err != nil {
				return err
			}
			entry.db = mutParent.(*dbdesc.Mutable)
		}

		// Delete schema entries in descriptor and namespace system tables.
		if err := descsCol.DeleteNamespaceEntryToBatch(ctx, kvTrace, mutSchema, b); err != nil {
			return err
		}
		if err := descsCol.DeleteDescToBatch(ctx, kvTrace, mutSchema.GetID(), b); err != nil {
			return err
		}
		// Add dropped descriptor as uncommitted to satisfy descriptor validation.
		mutSchema.SetDropped()
		mutSchema.MaybeIncrementVersion()
		if err := descsCol.AddUncommittedDescriptor(ctx, mutSchema); err != nil {
			return err
		}

		// Remove the back-reference to the deleted schema in the parent database.
		if schemaInfo, ok := entry.db.Schemas[schemaDesc.GetName()]; !ok {
			log.Warningf(ctx, "unexpected missing schema entry for %s from db %d; skipping deletion",
				schemaDesc.GetName(), entry.db.GetID())
		} else if schemaInfo.ID != schemaDesc.GetID() {
			log.Warningf(ctx, "unexpected schema entry %d for %s from db %d, expecting %d; skipping deletion",
				schemaInfo.ID, schemaDesc.GetName(), entry.db.GetID(), schemaDesc.GetID())
		} else {
			delete(entry.db.Schemas, schemaDesc.GetName())
		}

		entry.schemas = append(entry.schemas, mutSchema)
		dbsWithDeletedSchemas[entry.db.GetID()] = entry
	}

	// For each database that had a child schema deleted (regardless of whether
	// the db was created in the restore job), if it wasn't deleted just now,
	// write the updated descriptor with the now-deleted child schemas from its
	// schema map.
	//
	// This cleanup must be done prior to dropping the database descriptors in the
	// loop below so that we do not accidentally `b.Put` the descriptor with the
	// modified schema slice after we have issued a `b.Del` to drop it.
	for dbID, entry := range dbsWithDeletedSchemas {
		log.Infof(ctx, "deleting %d schema entries from database %d", len(entry.schemas), dbID)
		if err := descsCol.WriteDescToBatch(
			ctx, kvTrace, entry.db, b,
		); err != nil {
			return err
		}
	}

	// Delete the database descriptors.
	//
	// This should be the last step in mutating the database descriptors to ensure
	// that no batch requests are queued after the `b.Del` to delete the dropped
	// database descriptor.
	deletedDBs := make(map[descpb.ID]struct{})
	for _, dbDesc := range details.DatabaseDescs {

		// We need to ignore descriptors we just added since we haven't committed the txn that deletes these.
		isDBEmpty, err := isDatabaseEmpty(ctx, txn.KV(), dbDesc.GetID(), allDescs, ignoredChildDescIDs)
		if err != nil {
			return errors.Wrapf(err, "checking if database %s is empty during restore cleanup", dbDesc.GetName())
		}
		if !isDBEmpty {
			log.Warningf(ctx, "preserving database %s on restore failure because it contains new child objects or schemas", dbDesc.GetName())
			continue
		}

		db, err := descsCol.MutableByID(txn.KV()).Desc(ctx, dbDesc.GetID())
		if err != nil {
			return err
		}

		// Mark db as dropped and add uncommitted version to pass pre-txn
		// descriptor validation.
		db.SetDropped()
		db.MaybeIncrementVersion()
		if err := descsCol.AddUncommittedDescriptor(ctx, db); err != nil {
			return err
		}
		// We have explicitly to delete the system.namespace entry for the public schema
		// if the database does not have a public schema backed by a descriptor.
		if db := db.(catalog.DatabaseDescriptor); db.HasPublicSchemaWithDescriptor() {
			if err := descsCol.DeleteDescriptorlessPublicSchemaToBatch(ctx, kvTrace, db, b); err != nil {
				return err
			}
		}
		if err := descsCol.DeleteNamespaceEntryToBatch(ctx, kvTrace, db, b); err != nil {
			return err
		}
		if err := descsCol.DeleteDescToBatch(ctx, kvTrace, db.GetID(), b); err != nil {
			return err
		}
		deletedDBs[db.GetID()] = struct{}{}
	}

	// Avoid telling the descriptor collection about the mutated descriptors
	// until after all relevant relations have been retrieved to avoid a
	// scenario whereby we make a descriptor invalid too early.
	for _, t := range mutableTables {
		if err := descsCol.WriteDescToBatch(ctx, kvTrace, t, b); err != nil {
			return errors.Wrap(err, "writing dropping table to batch")
		}
	}

	if err := txn.KV().Run(ctx, b); err != nil {
		return errors.Wrap(err, "dropping tables created at the start of restore caused by fail/cancel")
	}

	return nil
}

// removeExistingTypeBackReferences removes back references from types that
// exist in the cluster to tables restored. It is used when rolling back from
// a failed restore.
func (r *restoreResumer) removeExistingTypeBackReferences(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	b *kv.Batch,
	restoredTables []*tabledesc.Mutable,
	details *jobspb.RestoreDetails,
) error {
	// We first collect the restored types to be addressable by ID.
	restoredTypes := make(map[descpb.ID]catalog.TypeDescriptor)
	existingTypes := make(map[descpb.ID]*typedesc.Mutable)
	for i := range details.TypeDescs {
		typ := details.TypeDescs[i]
		restoredTypes[typ.ID] = typedesc.NewBuilder(typ).BuildImmutableType()
	}
	for _, tbl := range restoredTables {
		lookup := func(id descpb.ID) (catalog.TypeDescriptor, error) {
			// First see if the type was restored.
			restored, ok := restoredTypes[id]
			if ok {
				return restored, nil
			}
			// Finally, look it up using the transaction.
			typ, err := descsCol.MutableByID(txn).Type(ctx, id)
			if err != nil {
				return nil, err
			}
			existingTypes[typ.GetID()] = typ
			return typ, nil
		}

		dbDesc, err := descsCol.ByIDWithoutLeased(txn).WithoutDropped().Get().Database(ctx, tbl.GetParentID())
		if err != nil {
			return err
		}

		// Get all types that this descriptor references.
		referencedTypes, _, err := tbl.GetAllReferencedTypeIDs(dbDesc, lookup)
		if err != nil {
			return err
		}

		// For each type that is existing, remove the backreference from tbl.
		for _, id := range referencedTypes {
			_, restored := restoredTypes[id]
			if !restored {
				desc, err := lookup(id)
				if err != nil {
					return err
				}
				existing := desc.(*typedesc.Mutable)
				existing.MaybeIncrementVersion()
				existing.RemoveReferencingDescriptorID(tbl.ID)
			}
		}
	}

	// Now write any changed existing types.
	for _, typ := range existingTypes {
		if typ.IsUncommittedVersion() {
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, typ, b,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

type systemTableNameWithConfig struct {
	systemTableName  string
	stagingTableName string
	config           systemBackupConfiguration
}

// Restore system.users from the backup into the restoring cluster. Only recreate users
// which are in a backup of system.users but do not currently exist (ignoring those who do)
// and re-grant roles for users if the backup has system.role_members.
func (r *restoreResumer) restoreSystemUsers(
	ctx context.Context, db isql.DB, systemTables []catalog.TableDescriptor,
) error {
	return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		selectNonExistentUsers := "SELECT * FROM crdb_temp_system.users temp " +
			"WHERE NOT EXISTS (SELECT * FROM system.users u WHERE temp.username = u.username)"
		users, err := txn.QueryBuffered(ctx, "get-users",
			txn.KV(), selectNonExistentUsers)
		if err != nil {
			return err
		}

		insertUser := `INSERT INTO system.users ("username", "hashedPassword", "isRole", "user_id") VALUES ($1, $2, $3, $4)`
		newUsernames := make(map[string]catid.RoleID)
		args := make([]interface{}, 4)
		for _, user := range users {
			args[0] = user[0]
			args[1] = user[1]
			args[2] = user[2]
			id, err := descidgen.GenerateUniqueRoleID(ctx, r.execCfg.DB, r.execCfg.Codec)
			if err != nil {
				return err
			}
			args[3] = id
			if _, err = txn.Exec(ctx, "insert-non-existent-users", txn.KV(), insertUser,
				args...); err != nil {
				return err
			}
			newUsernames[user[0].String()] = id
		}

		// We skip granting roles if the backup does not contain system.role_members.
		if hasSystemRoleMembersTable(systemTables) {
			selectNonExistentRoleMembers := "SELECT * FROM crdb_temp_system.role_members temp_rm WHERE " +
				"NOT EXISTS (SELECT * FROM system.role_members rm WHERE temp_rm.role = rm.role AND temp_rm.member = rm.member)"
			roleMembers, err := txn.QueryBuffered(ctx, "get-role-members",
				txn.KV(), selectNonExistentRoleMembers)
			if err != nil {
				return err
			}

			insertRoleMember := `
INSERT INTO system.role_members ("role", "member", "isAdmin", role_id, member_id)
VALUES ($1, $2, $3, (SELECT user_id FROM system.users WHERE username = $1), (SELECT user_id FROM system.users WHERE username = $2))`

			for _, roleMember := range roleMembers {
				member := tree.MustBeDString(roleMember[1])
				// Only grant roles to users that don't currently exist, i.e., new users we just added
				if _, ok := newUsernames[member.String()]; ok {
					role := tree.MustBeDString(roleMember[0])
					isAdmin := tree.MustBeDBool(roleMember[2])
					if _, err := txn.Exec(ctx, "insert-non-existent-role-members", txn.KV(),
						insertRoleMember, role, member, isAdmin,
					); err != nil {
						return err
					}
				}
			}
		}

		if hasSystemRoleOptionsTable(systemTables) {
			selectNonExistentRoleOptions := "SELECT * FROM crdb_temp_system.role_options temp_ro WHERE " +
				"NOT EXISTS (SELECT * FROM system.role_options ro WHERE temp_ro.username = ro.username AND temp_ro.option = ro.option)"
			roleOptions, err := txn.QueryBuffered(ctx, "get-role-options", txn.KV(), selectNonExistentRoleOptions)
			if err != nil {
				return err
			}

			insertRoleOption := `INSERT INTO system.role_options ("username", "option", "value", "user_id") VALUES ($1, $2, $3, $4)`
			for _, roleOption := range roleOptions {
				if roleID, ok := newUsernames[roleOption[0].String()]; ok {
					args := []interface{}{roleOption[0], roleOption[1], roleOption[2], roleID}
					if _, err = txn.Exec(ctx, "insert-non-existent-role-options", txn.KV(),
						insertRoleOption, args...); err != nil {
						return err
					}
				}
			}
		}

		return nil
	})
}

func hasSystemRoleMembersTable(systemTables []catalog.TableDescriptor) bool {
	return hasSystemTableByName(systemschema.RoleMembersTable.GetName(), systemTables)
}

func hasSystemRoleOptionsTable(systemTables []catalog.TableDescriptor) bool {
	return hasSystemTableByName(systemschema.RoleOptionsTable.GetName(), systemTables)
}

func hasSystemTableByName(name string, systemTables []catalog.TableDescriptor) bool {
	for _, t := range systemTables {
		if t.GetName() == name {
			return true
		}
	}
	return false
}

// restoreSystemTables atomically replaces the contents of the system tables
// with the data from the restored system tables.
func (r *restoreResumer) restoreSystemTables(
	ctx context.Context, db isql.DB, tables []catalog.TableDescriptor,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)
	if details.SystemTablesMigrated == nil {
		details.SystemTablesMigrated = make(map[string]bool)
	}
	// Iterate through all the tables that we're restoring, and if it was restored
	// to the temporary system DB then populate the metadata required to restore
	// to the real system table.
	systemTablesToRestore := make([]systemTableNameWithConfig, 0)
	for _, table := range tables {
		systemTableName := table.GetName()
		stagingTableName := restoreTempSystemDB + "." + systemTableName

		config, ok := systemTableBackupConfiguration[systemTableName]
		if !ok {
			log.Warningf(ctx, "no configuration specified for table %s... skipping restoration",
				systemTableName)
		}
		systemTablesToRestore = append(systemTablesToRestore, systemTableNameWithConfig{
			systemTableName:  systemTableName,
			stagingTableName: stagingTableName,
			config:           config,
		})
	}

	// Sort the system tables to be restored based on the order specified in the
	// configuration.
	slices.SortStableFunc(systemTablesToRestore, func(a, b systemTableNameWithConfig) int {
		return cmp.Compare(a.config.restoreInOrder, b.config.restoreInOrder)
	})

	// Copy the data from the temporary system DB to the real system table.
	for _, systemTable := range systemTablesToRestore {
		if systemTable.config.migrationFunc != nil {
			if details.SystemTablesMigrated[systemTable.systemTableName] {
				continue
			}

			if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				if err := systemTable.config.migrationFunc(
					ctx, txn, systemTable.stagingTableName, details.DescriptorRewrites,
				); err != nil {
					return err
				}

				// Keep track of which system tables we've migrated so that future job
				// restarts don't try to import data over our migrated data. This would
				// fail since the restored data would shadow the migrated keys.
				details.SystemTablesMigrated[systemTable.systemTableName] = true
				return r.job.WithTxn(txn).SetDetails(ctx, details)
			}); err != nil {
				return err
			}
		}

		if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			txn.KV().SetDebugName("system-restore-txn")

			restoreFunc := defaultSystemTableRestoreFunc
			if systemTable.config.customRestoreFunc != nil {
				restoreFunc = systemTable.config.customRestoreFunc
				log.Eventf(ctx, "using custom restore function for table %s", systemTable.systemTableName)
			}
			deps := customRestoreFuncDeps{
				settings: r.execCfg.Settings,
				codec:    r.execCfg.Codec,
			}
			log.Eventf(ctx, "restoring system table %s", systemTable.systemTableName)
			err := restoreFunc(ctx, deps, txn, systemTable.systemTableName, systemTable.stagingTableName)
			if err != nil {
				return errors.Wrapf(err, "restoring system table %s", systemTable.systemTableName)
			}
			return nil
		}); err != nil {
			return err
		}

		if fn := r.testingKnobs.duringSystemTableRestoration; fn != nil {
			if err := fn(systemTable.systemTableName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *restoreResumer) cleanupTempSystemTables(ctx context.Context) error {
	executor := r.execCfg.InternalDB.Executor()
	// Check if the temp system database has already been dropped. This can happen
	// if the restore job fails after the system database has cleaned up.
	checkIfDatabaseExists := "SELECT database_name FROM [SHOW DATABASES] WHERE database_name=$1"
	if row, err := executor.QueryRow(ctx, "checking-for-temp-system-db" /* opName */, nil /* txn */, checkIfDatabaseExists, restoreTempSystemDB); err != nil {
		return errors.Wrap(err, "checking for temporary system db")
	} else if row == nil {
		// Temporary system DB might already have been dropped by the restore job.
		return nil
	}

	// After restoring the system tables, drop the temporary database holding the
	// system tables.
	gcTTLQuery := fmt.Sprintf("ALTER DATABASE %s CONFIGURE ZONE USING gc.ttlseconds=1", restoreTempSystemDB)
	if _, err := executor.Exec(ctx, "altering-gc-ttl-temp-system" /* opName */, nil /* txn */, gcTTLQuery); err != nil {
		log.Errorf(ctx, "failed to update the GC TTL of %q: %+v", restoreTempSystemDB, err)
	}
	dropTableQuery := fmt.Sprintf("DROP DATABASE %s CASCADE", restoreTempSystemDB)
	if _, err := executor.Exec(ctx, "drop-temp-system-db" /* opName */, nil /* txn */, dropTableQuery); err != nil {
		return errors.Wrap(err, "dropping temporary system db")
	}
	return nil
}

var _ jobs.Resumer = &restoreResumer{}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeRestore,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			r := &restoreResumer{
				job:      job,
				settings: settings,
			}
			r.mu.perNodeAggregatorStats = make(bulkutil.ComponentAggregatorStats)
			return r
		},
		jobs.UsesTenantCostControl,
	)
}

type sz int64

func (b sz) String() string { return string(humanizeutil.IBytes(int64(b))) }

// TODO(dt): move this to humanizeutil and allow-list it there.
//func (b sz) SafeValue()     {}
