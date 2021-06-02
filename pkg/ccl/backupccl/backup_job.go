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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

// BackupCheckpointInterval is the interval at which backup progress is saved
// to durable storage.
var BackupCheckpointInterval = time.Minute

func (r *RowCount) add(other RowCount) {
	r.DataSize += other.DataSize
	r.Rows += other.Rows
	r.IndexEntries += other.IndexEntries
}

func countRows(raw roachpb.BulkOpSummary, pkIDs map[uint64]bool) RowCount {
	res := RowCount{DataSize: raw.DataSize}
	for id, count := range raw.EntryCounts {
		if _, ok := pkIDs[id]; ok {
			res.Rows += count
		} else {
			res.IndexEntries += count
		}
	}
	return res
}

// coveringFromSpans creates an interval.Covering with a fixed payload from a
// slice of roachpb.Spans.
func coveringFromSpans(spans []roachpb.Span, payload interface{}) covering.Covering {
	var c covering.Covering
	for _, span := range spans {
		c = append(c, covering.Range{
			Start:   []byte(span.Key),
			End:     []byte(span.EndKey),
			Payload: payload,
		})
	}
	return c
}

// filterSpans returns the spans that represent the set difference
// (includes - excludes).
func filterSpans(includes []roachpb.Span, excludes []roachpb.Span) []roachpb.Span {
	type includeMarker struct{}
	type excludeMarker struct{}

	includeCovering := coveringFromSpans(includes, includeMarker{})
	excludeCovering := coveringFromSpans(excludes, excludeMarker{})

	splits := covering.OverlapCoveringMerge(
		[]covering.Covering{includeCovering, excludeCovering},
	)

	var out []roachpb.Span
	for _, split := range splits {
		include := false
		exclude := false
		for _, payload := range split.Payload.([]interface{}) {
			switch payload.(type) {
			case includeMarker:
				include = true
			case excludeMarker:
				exclude = true
			}
		}
		if include && !exclude {
			out = append(out, roachpb.Span{
				Key:    roachpb.Key(split.Start),
				EndKey: roachpb.Key(split.End),
			})
		}
	}
	return out
}

// clusterNodeCount returns the approximate number of nodes in the cluster.
func clusterNodeCount(gw gossip.OptionalGossip) (int, error) {
	g, err := gw.OptionalErr(47970)
	if err != nil {
		return 0, err
	}
	var nodes int
	err = g.IterateInfos(
		gossip.KeyNodeIDPrefix, func(_ string, _ gossip.Info) error {
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
// - <dir>/<unique_int>.sst
// - <dir> is given by the user and may be cloud storage
// - Each file contains data for a key range that doesn't overlap with any other
//   file.
func backup(
	ctx context.Context,
	execCtx sql.JobExecContext,
	defaultURI string,
	urisByLocalityKV map[string]string,
	db *kv.DB,
	settings *cluster.Settings,
	defaultStore cloud.ExternalStorage,
	storageByLocalityKV map[string]*roachpb.ExternalStorage,
	job *jobs.Job,
	backupManifest *BackupManifest,
	makeExternalStorage cloud.ExternalStorageFactory,
	encryption *jobspb.BackupEncryptionOptions,
	statsCache *stats.TableStatisticsCache,
) (RowCount, error) {
	// TODO(dan): Figure out how permissions should work. #6713 is tracking this
	// for grpc.

	resumerSpan := tracing.SpanFromContext(ctx)
	var lastCheckpoint time.Time

	var completedSpans, completedIntroducedSpans []roachpb.Span
	// TODO(benesch): verify these files, rather than accepting them as truth
	// blindly.
	// No concurrency yet, so these assignments are safe.
	for _, file := range backupManifest.Files {
		if file.StartTime.IsEmpty() && !file.EndTime.IsEmpty() {
			completedIntroducedSpans = append(completedIntroducedSpans, file.Span)
		} else {
			completedSpans = append(completedSpans, file.Span)
		}
	}

	// Subtract out any completed spans.
	spans := filterSpans(backupManifest.Spans, completedSpans)
	introducedSpans := filterSpans(backupManifest.IntroducedSpans, completedIntroducedSpans)

	pkIDs := make(map[uint64]bool)
	for i := range backupManifest.Descriptors {
		if t, _, _, _ := descpb.FromDescriptor(&backupManifest.Descriptors[i]); t != nil {
			pkIDs[roachpb.BulkOpSummaryID(uint64(t.ID), uint64(t.PrimaryIndex.ID))] = true
		}
	}

	evalCtx := execCtx.ExtendedEvalContext()
	dsp := execCtx.DistSQLPlanner()

	// We don't return the compatible nodes here since PartitionSpans will
	// filter out incompatible nodes.
	planCtx, _, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCtx.ExecCfg())
	if err != nil {
		return RowCount{}, errors.Wrap(err, "failed to determine nodes on which to run")
	}

	backupSpecs, err := distBackupPlanSpecs(
		planCtx,
		execCtx,
		dsp,
		spans,
		introducedSpans,
		pkIDs,
		defaultURI,
		urisByLocalityKV,
		encryption,
		roachpb.MVCCFilter(backupManifest.MVCCFilter),
		backupManifest.StartTime,
		backupManifest.EndTime,
	)
	if err != nil {
		return RowCount{}, err
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
			var progDetails BackupManifest_Progress
			if err := types.UnmarshalAny(&progress.ProgressDetails, &progDetails); err != nil {
				log.Errorf(ctx, "unable to unmarshal backup progress details: %+v", err)
			}
			if backupManifest.RevisionStartTime.Less(progDetails.RevStartTime) {
				backupManifest.RevisionStartTime = progDetails.RevStartTime
			}
			for _, file := range progDetails.Files {
				backupManifest.Files = append(backupManifest.Files, file)
				backupManifest.EntryCounts.add(file.EntryCounts)
				numBackedUpFiles++
			}

			// Signal that an ExportRequest finished to update job progress.
			for i := int32(0); i < progDetails.CompletedSpans; i++ {
				requestFinishedCh <- struct{}{}
			}
			if timeutil.Since(lastCheckpoint) > BackupCheckpointInterval {
				resumerSpan.RecordStructured(&BackupProgressTraceEvent{
					TotalNumFiles:     numBackedUpFiles,
					TotalEntryCounts:  backupManifest.EntryCounts,
					RevisionStartTime: backupManifest.RevisionStartTime,
				})
				err := writeBackupManifest(
					ctx, settings, defaultStore, backupManifestCheckpointName, encryption, backupManifest,
				)
				if err != nil {
					log.Errorf(ctx, "unable to checkpoint backup descriptor: %+v", err)
				}

				lastCheckpoint = timeutil.Now()
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
		return RowCount{}, errors.Wrapf(err, "exporting %d ranges", errors.Safe(numTotalSpans))
	}

	backupID := uuid.MakeV4()
	backupManifest.ID = backupID
	// Write additional partial descriptors to each node for partitioned backups.
	if len(storageByLocalityKV) > 0 {
		resumerSpan.RecordStructured(&types.StringValue{Value: "writing partition descriptors for partitioned backup"})
		filesByLocalityKV := make(map[string][]BackupManifest_File)
		for _, file := range backupManifest.Files {
			filesByLocalityKV[file.LocalityKV] = append(filesByLocalityKV[file.LocalityKV], file)
		}

		nextPartitionedDescFilenameID := 1
		for kv, conf := range storageByLocalityKV {
			backupManifest.LocalityKVs = append(backupManifest.LocalityKVs, kv)
			// Set a unique filename for each partition backup descriptor. The ID
			// ensures uniqueness, and the kv string appended to the end is for
			// readability.
			filename := fmt.Sprintf("%s_%d_%s",
				backupPartitionDescriptorPrefix, nextPartitionedDescFilenameID, sanitizeLocalityKV(kv))
			nextPartitionedDescFilenameID++
			backupManifest.PartitionDescriptorFilenames = append(backupManifest.PartitionDescriptorFilenames, filename)
			desc := BackupPartitionDescriptor{
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
				return writeBackupPartitionDescriptor(ctx, store, filename, encryption, &desc)
			}(); err != nil {
				return RowCount{}, err
			}
		}
	}

	resumerSpan.RecordStructured(&types.StringValue{Value: "writing backup manifest"})
	if err := writeBackupManifest(ctx, settings, defaultStore, backupManifestName, encryption, backupManifest); err != nil {
		return RowCount{}, err
	}
	var tableStatistics []*stats.TableStatisticProto
	for i := range backupManifest.Descriptors {
		if tableDesc, _, _, _ := descpb.FromDescriptor(&backupManifest.Descriptors[i]); tableDesc != nil {
			// Collect all the table stats for this table.
			tableStatisticsAcc, err := statsCache.GetTableStats(ctx, tableDesc.GetID())
			if err != nil {
				// Successfully backed up data is more valuable than table stats that can
				// be recomputed after restore, and so if we fail to collect the stats of a
				// table we do not want to mark the job as failed.
				// The lack of stats on restore could lead to suboptimal performance when
				// reading/writing to this table until the stats have been recomputed.
				log.Warningf(ctx, "failed to collect stats for table: %s, "+
					"table ID: %d during a backup: %s", tableDesc.GetName(), tableDesc.GetID(),
					err.Error())
				continue
			}
			for _, stat := range tableStatisticsAcc {
				tableStatistics = append(tableStatistics, &stat.TableStatisticProto)
			}
		}
	}
	statsTable := StatsTable{
		Statistics: tableStatistics,
	}

	resumerSpan.RecordStructured(&types.StringValue{Value: "writing backup table statistics"})
	if err := writeTableStatistics(ctx, defaultStore, backupStatisticsFileName, encryption, &statsTable); err != nil {
		return RowCount{}, err
	}

	return backupManifest.EntryCounts, nil
}

func (b *backupResumer) releaseProtectedTimestamp(
	ctx context.Context, txn *kv.Txn, pts protectedts.Storage,
) error {
	details := b.job.Details().(jobspb.BackupDetails)
	ptsID := details.ProtectedTimestampRecord
	// If the job doesn't have a protected timestamp then there's nothing to do.
	if ptsID == nil {
		return nil
	}
	err := pts.Release(ctx, txn, *ptsID)
	if errors.Is(err, protectedts.ErrNotExists) {
		// No reason to return an error which might cause problems if it doesn't
		// seem to exist.
		log.Warningf(ctx, "failed to release protected which seems not to exist: %v", err)
		err = nil
	}
	return err
}

type backupResumer struct {
	job         *jobs.Job
	backupStats RowCount

	testingKnobs struct {
		ignoreProtectedTimestamps bool
	}
}

var _ jobs.TraceableJob = &backupResumer{}

// ForceRealSpan implements the TraceableJob interface.
func (b *backupResumer) ForceRealSpan() {}

// Resume is part of the jobs.Resumer interface.
func (b *backupResumer) Resume(ctx context.Context, execCtx interface{}) error {
	// The span is finished by the registry executing the job.
	resumerSpan := tracing.SpanFromContext(ctx)
	details := b.job.Details().(jobspb.BackupDetails)
	p := execCtx.(sql.JobExecContext)

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
	redactedURI := RedactURIForErrorMessage(details.URI)
	if details.EncryptionInfo != nil {
		if err := writeEncryptionInfoIfNotExists(ctx, details.EncryptionInfo,
			defaultStore); err != nil {
			return errors.Wrapf(err, "creating encryption info file to %s", redactedURI)
		}
	}

	ptsID := details.ProtectedTimestampRecord
	if ptsID != nil && !b.testingKnobs.ignoreProtectedTimestamps {
		resumerSpan.RecordStructured(&types.StringValue{Value: "verifying protected timestamp"})
		if err := p.ExecCfg().ProtectedTimestampProvider.Verify(ctx, *ptsID); err != nil {
			if errors.Is(err, protectedts.ErrNotExists) {
				// No reason to return an error which might cause problems if it doesn't
				// seem to exist.
				log.Warningf(ctx, "failed to release protected which seems not to exist: %v", err)
			} else {
				return err
			}
		}
	}

	storageByLocalityKV := make(map[string]*roachpb.ExternalStorage)
	for kv, uri := range details.URIsByLocalityKV {
		conf, err := cloud.ExternalStorageConfFromURI(uri, p.User())
		if err != nil {
			return err
		}
		storageByLocalityKV[kv] = &conf
	}

	backupManifest, err := b.readManifestOnResume(ctx, p.ExecCfg(), defaultStore, details)
	if err != nil {
		return err
	}

	statsCache := p.ExecCfg().TableStatsCache
	// We retry on pretty generic failures -- any rpc error. If a worker node were
	// to restart, it would produce this kind of error, but there may be other
	// errors that are also rpc errors. Don't retry to aggressively.
	retryOpts := retry.Options{
		MaxBackoff: 1 * time.Second,
		MaxRetries: 5,
	}

	// We want to retry a backup if there are transient failures (i.e. worker nodes
	// dying), so if we receive a retryable error, re-plan and retry the backup.
	var res RowCount
	var retryCount int32
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		retryCount++
		resumerSpan.RecordStructured(&roachpb.RetryTracingEvent{
			Operation:     "backupResumer.Resume",
			AttemptNumber: retryCount,
			RetryError:    tracing.RedactAndTruncateError(err),
		})
		res, err = backup(
			ctx,
			p,
			details.URI,
			details.URIsByLocalityKV,
			p.ExecCfg().DB,
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

		if utilccl.IsPermanentBulkJobError(err) {
			return errors.Wrap(err, "failed to run backup")
		}

		log.Warningf(ctx, `BACKUP job encountered retryable error: %+v`, err)

		// Reload the backup manifest to pick up any spans we may have completed on
		// previous attempts.
		var reloadBackupErr error
		backupManifest, reloadBackupErr = b.readManifestOnResume(ctx, p.ExecCfg(), defaultStore, details)
		if reloadBackupErr != nil {
			return errors.Wrap(reloadBackupErr, "could not reload backup manifest when retrying")
		}
	}
	if err != nil {
		return errors.Wrap(err, "exhausted retries")
	}

	b.deleteCheckpoint(ctx, p.ExecCfg(), p.User())

	if ptsID != nil && !b.testingKnobs.ignoreProtectedTimestamps {
		if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return b.releaseProtectedTimestamp(ctx, txn, p.ExecCfg().ProtectedTimestampProvider)
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
		if err := cloud.WriteFile(ctx, c, latestFileName, strings.NewReader(suffix)); err != nil {
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
	}

	b.maybeNotifyScheduledJobCompletion(ctx, jobs.StatusSucceeded, p.ExecCfg())
	return nil
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

func (b *backupResumer) readManifestOnResume(
	ctx context.Context,
	cfg *sql.ExecutorConfig,
	defaultStore cloud.ExternalStorage,
	details jobspb.BackupDetails,
) (*BackupManifest, error) {
	// We don't read the table descriptors from the backup descriptor, but
	// they could be using either the new or the old foreign key
	// representations. We should just preserve whatever representation the
	// table descriptors were using and leave them alone.
	desc, err := readBackupManifest(ctx, defaultStore, backupManifestCheckpointName,
		details.EncryptionOptions)

	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return nil, errors.Wrapf(err, "reading backup checkpoint")
		}
		// Try reading temp checkpoint.
		tmpCheckpoint := tempCheckpointFileNameForJob(b.job.ID())
		desc, err = readBackupManifest(ctx, defaultStore, tmpCheckpoint, details.EncryptionOptions)
		if err != nil {
			return nil, err
		}

		// "Rename" temp checkpoint.
		if err := writeBackupManifest(
			ctx, cfg.Settings, defaultStore, backupManifestCheckpointName,
			details.EncryptionOptions, &desc,
		); err != nil {
			return nil, errors.Wrapf(err, "renaming temp checkpoint file")
		}
		// Best effort remove temp checkpoint.
		if err := defaultStore.Delete(ctx, tmpCheckpoint); err != nil {
			log.Errorf(ctx, "error removing temporary checkpoint %s", tmpCheckpoint)
		}
	}

	if !desc.ClusterID.Equal(cfg.ClusterID()) {
		return nil, errors.Newf("cannot resume backup started on another cluster (%s != %s)",
			desc.ClusterID, cfg.ClusterID())
	}
	return &desc, nil
}

func (b *backupResumer) maybeNotifyScheduledJobCompletion(
	ctx context.Context, jobStatus jobs.Status, exec *sql.ExecutorConfig,
) {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs, ok := exec.DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}

	if err := exec.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Do not rely on b.job containing created_by_id.  Query it directly.
		datums, err := exec.InternalExecutor.QueryRowEx(
			ctx,
			"lookup-schedule-info",
			txn,
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
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
			ctx, env, b.job.ID(), jobStatus, b.job.Details(), scheduleID, exec.InternalExecutor, txn); err != nil {
			log.Warningf(ctx,
				"failed to notify schedule %d of completion of job %d; err=%s",
				scheduleID, b.job.ID(), err)
		}
		return nil
	}); err != nil {
		log.Errorf(ctx, "maybeNotifySchedule error: %v", err)
	}
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (b *backupResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	defer b.maybeNotifyScheduledJobCompletion(
		ctx,
		jobs.StatusFailed,
		execCtx.(sql.JobExecContext).ExecCfg(),
	)

	telemetry.Count("backup.total.failed")
	telemetry.CountBucketed("backup.duration-sec.failed",
		int64(timeutil.Since(timeutil.FromUnixMicros(b.job.Payload().StartedMicros)).Seconds()))

	p := execCtx.(sql.JobExecContext)
	cfg := p.ExecCfg()
	b.deleteCheckpoint(ctx, cfg, p.User())
	return cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return b.releaseProtectedTimestamp(ctx, txn, cfg.ProtectedTimestampProvider)
	})
}

func (b *backupResumer) deleteCheckpoint(
	ctx context.Context, cfg *sql.ExecutorConfig, user security.SQLUsername,
) {
	// Attempt to delete BACKUP-CHECKPOINT.
	if err := func() error {
		details := b.job.Details().(jobspb.BackupDetails)
		// For all backups, partitioned or not, the main BACKUP manifest is stored at
		// details.URI.
		exportStore, err := cfg.DistSQLSrv.ExternalStorageFromURI(ctx, details.URI, user)
		if err != nil {
			return err
		}
		defer exportStore.Close()
		return exportStore.Delete(ctx, backupManifestCheckpointName)
	}(); err != nil {
		log.Warningf(ctx, "unable to delete checkpointed backup descriptor: %+v", err)
	}
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
	)
}
