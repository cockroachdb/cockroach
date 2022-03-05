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
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
var BackupCheckpointInterval = time.Minute

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
) (roachpb.RowCount, error) {
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
			var progDetails BackupManifest_Progress
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
		return roachpb.RowCount{}, errors.Wrapf(err, "exporting %d ranges", errors.Safe(numTotalSpans))
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
				return roachpb.RowCount{}, err
			}
		}
	}

	resumerSpan.RecordStructured(&types.StringValue{Value: "writing backup manifest"})
	if err := writeBackupManifest(ctx, settings, defaultStore, backupManifestName, encryption, backupManifest); err != nil {
		return roachpb.RowCount{}, err
	}
	var tableStatistics []*stats.TableStatisticProto
	for i := range backupManifest.Descriptors {
		if tbl, _, _, _ := descpb.FromDescriptor(&backupManifest.Descriptors[i]); tbl != nil {
			tableDesc := tabledesc.NewBuilder(tbl).BuildImmutableTable()
			// Collect all the table stats for this table.
			tableStatisticsAcc, err := statsCache.GetTableStats(ctx, tableDesc)
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
		return roachpb.RowCount{}, err
	}

	if writeMetadataSST.Get(&settings.SV) {
		if err := writeBackupMetadataSST(ctx, defaultStore, encryption, backupManifest, tableStatistics); err != nil {
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
	ctx context.Context, txn *kv.Txn, pts protectedts.Storage, ptsID *uuid.UUID,
) error {
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
	resumerSpan := tracing.SpanFromContext(ctx)
	details := b.job.Details().(jobspb.BackupDetails)
	p := execCtx.(sql.JobExecContext)

	var backupManifest *BackupManifest

	// If planning didn't resolve the external destination, then we need to now.
	if details.URI == "" {
		initialDetails := details
		backupDetails, m, err := getBackupDetailAndManifest(
			ctx, p.ExecCfg(), p.ExtendedEvalContext().Txn, details, p.User(),
		)
		if err != nil {
			return err
		}
		details = backupDetails
		backupManifest = &m

		if len(backupManifest.Spans) > 0 && p.ExecCfg().Codec.ForSystemTenant() {
			protectedtsID := uuid.MakeV4()
			details.ProtectedTimestampRecord = &protectedtsID

			if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				return protectTimestampForBackup(
					ctx, p.ExecCfg(), txn, b.job.ID(), m, details,
				)
			}); err != nil {
				return err
			}
		}

		if err := writeBackupManifestCheckpoint(
			ctx, b.job.ID(), backupDetails, backupManifest, p.ExecCfg(), p.User(),
		); err != nil {
			return err
		}

		if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return planSchedulePTSChaining(ctx, p.ExecCfg(), txn, &details, b.job.CreatedBy())
		}); err != nil {
			return err
		}

		// The description picked during original planning might still say "LATEST",
		// if resolving that to the actual directory only just happened above here.
		// Ideally we'd re-render the description now that we know the subdir, but
		// we don't have backup AST node anymore to easily call the rendering func.
		// Instead we can just do a bit of dirty string replacement iff there is one
		// "INTO 'LATEST' IN" (if there's >1, somenoe has a weird table/db names and
		// we should just leave the description as-is, since it is just for humans).
		description := b.job.Payload().Description
		const unresolvedText = "INTO 'LATEST' IN"
		if initialDetails.Destination.Subdir == "LATEST" && strings.Count(description, unresolvedText) == 1 {
			description = strings.ReplaceAll(description, unresolvedText, fmt.Sprintf("INTO '%s' IN", details.Destination.Subdir))
		}

		// Update the job payload (non-volatile job definition) once, with the now
		// resolved destination, updated description, etc. If we resume again we'll
		// skip this whole block so this isn't an excessive update of payload.
		if err := b.job.Update(ctx, nil, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
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

		// Collect telemetry, once per backup after resolving its destination.
		lic := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "",
		) != nil
		collectTelemetry(m, details, details, lic)
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

	mem := p.ExecCfg().RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	var memSize int64
	defer func() {
		if memSize != 0 {
			mem.Shrink(ctx, memSize)
		}
	}()

	if backupManifest == nil || util.ConstantWithMetamorphicTestBool("backup-read-manifest", false) {
		backupManifest, memSize, err = b.readManifestOnResume(ctx, &mem, p.ExecCfg(), defaultStore, details)
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

	// We want to retry a backup if there are transient failures (i.e. worker nodes
	// dying), so if we receive a retryable error, re-plan and retry the backup.
	var res roachpb.RowCount
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

		if joberror.IsPermanentBulkJobError(err) {
			return errors.Wrap(err, "failed to run backup")
		}

		log.Warningf(ctx, `BACKUP job encountered retryable error: %+v`, err)

		// Reload the backup manifest to pick up any spans we may have completed on
		// previous attempts.
		var reloadBackupErr error
		mem.Shrink(ctx, memSize)
		memSize = 0
		backupManifest, memSize, reloadBackupErr = b.readManifestOnResume(ctx, &mem, p.ExecCfg(), defaultStore, details)
		if reloadBackupErr != nil {
			return errors.Wrap(reloadBackupErr, "could not reload backup manifest when retrying")
		}
	}
	if err != nil {
		return errors.Wrap(err, "exhausted retries")
	}

	b.deleteCheckpoint(ctx, p.ExecCfg(), p.User())

	var backupDetails jobspb.BackupDetails
	var ok bool
	if backupDetails, ok = b.job.Details().(jobspb.BackupDetails); !ok {
		return errors.Newf("unexpected job details type %T", b.job.Details())
	}

	if err := maybeUpdateSchedulePTSRecord(ctx, p.ExecCfg(), backupDetails, b.job.ID()); err != nil {
		return err
	}

	if ptsID != nil && !b.testingKnobs.ignoreProtectedTimestamps {
		if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			details := b.job.Details().(jobspb.BackupDetails)
			return releaseProtectedTimestamp(ctx, txn, p.ExecCfg().ProtectedTimestampProvider,
				details.ProtectedTimestampRecord)
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

		if err := writeNewLatestFile(ctx, p.ExecCfg().Settings, c, suffix); err != nil {
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

	return b.maybeNotifyScheduledJobCompletion(ctx, jobs.StatusSucceeded, p.ExecCfg())
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
	mem *mon.BoundAccount,
	cfg *sql.ExecutorConfig,
	defaultStore cloud.ExternalStorage,
	details jobspb.BackupDetails,
) (*BackupManifest, int64, error) {
	// We don't read the table descriptors from the backup descriptor, but
	// they could be using either the new or the old foreign key
	// representations. We should just preserve whatever representation the
	// table descriptors were using and leave them alone.
	desc, memSize, err := readBackupManifest(ctx, mem, defaultStore, backupManifestCheckpointName,
		details.EncryptionOptions)
	if err != nil {
		if !errors.Is(err, cloud.ErrFileDoesNotExist) {
			return nil, 0, errors.Wrapf(err, "reading backup checkpoint")
		}

		// Try reading temp checkpoint.
		tmpCheckpoint := tempCheckpointFileNameForJob(b.job.ID())
		desc, memSize, err = readBackupManifest(ctx, mem, defaultStore, tmpCheckpoint, details.EncryptionOptions)
		if err != nil {
			return nil, 0, err
		}

		// "Rename" temp checkpoint.
		if err := writeBackupManifest(
			ctx, cfg.Settings, defaultStore, backupManifestCheckpointName,
			details.EncryptionOptions, &desc,
		); err != nil {
			mem.Shrink(ctx, memSize)
			return nil, 0, errors.Wrapf(err, "renaming temp checkpoint file")
		}
		// Best effort remove temp checkpoint.
		if err := defaultStore.Delete(ctx, tmpCheckpoint); err != nil {
			log.Errorf(ctx, "error removing temporary checkpoint %s", tmpCheckpoint)
		}
		if err := defaultStore.Delete(ctx, backupProgressDirectory+"/"+tmpCheckpoint); err != nil {
			log.Errorf(ctx, "error removing temporary checkpoint %s", backupProgressDirectory+"/"+tmpCheckpoint)
		}
	}

	if !desc.ClusterID.Equal(cfg.ClusterID()) {
		mem.Shrink(ctx, memSize)
		return nil, 0, errors.Newf("cannot resume backup started on another cluster (%s != %s)",
			desc.ClusterID, cfg.ClusterID())
	}
	return &desc, memSize, nil
}

func (b *backupResumer) maybeNotifyScheduledJobCompletion(
	ctx context.Context, jobStatus jobs.Status, exec *sql.ExecutorConfig,
) error {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs, ok := exec.DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}

	err := exec.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// We cannot rely on b.job containing created_by_id because on job
		// resumption the registry does not populate the resumer's CreatedByInfo.
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
			return errors.Wrapf(err,
				"failed to notify schedule %d of completion of job %d", scheduleID, b.job.ID())
		}
		return nil
	})
	return err
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (b *backupResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	telemetry.Count("backup.total.failed")
	telemetry.CountBucketed("backup.duration-sec.failed",
		int64(timeutil.Since(timeutil.FromUnixMicros(b.job.Payload().StartedMicros)).Seconds()))

	p := execCtx.(sql.JobExecContext)
	cfg := p.ExecCfg()
	b.deleteCheckpoint(ctx, cfg, p.User())
	if err := cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		details := b.job.Details().(jobspb.BackupDetails)
		return releaseProtectedTimestamp(ctx, txn, cfg.ProtectedTimestampProvider,
			details.ProtectedTimestampRecord)
	}); err != nil {
		return err
	}

	// This should never return an error unless resolving the schedule that the
	// job is being run under fails. This could happen if the schedule is dropped
	// while the job is executing.
	if err := b.maybeNotifyScheduledJobCompletion(ctx, jobs.StatusFailed,
		execCtx.(sql.JobExecContext).ExecCfg()); err != nil {
		log.Errorf(ctx, "failed to notify job %d on completion of OnFailOrCancel: %+v",
			b.job.ID(), err)
	}
	return nil //nolint:returnerrcheck
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
		// The checkpoint in the base directory should only exist if the cluster
		// version isn't BackupDoesNotOverwriteLatestAndCheckpoint.
		if !cfg.Settings.Version.IsActive(ctx, clusterversion.BackupDoesNotOverwriteLatestAndCheckpoint) {
			err = exportStore.Delete(ctx, backupProgressDirectory)
			if err != nil {
				return err
			}
		}
		return exportStore.Delete(ctx, backupProgressDirectory+"/"+backupManifestCheckpointName)
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
