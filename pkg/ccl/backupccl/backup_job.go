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
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

func allRangeDescriptors(ctx context.Context, txn *kv.Txn) ([]roachpb.RangeDescriptor, error) {
	rows, err := txn.Scan(ctx, keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, errors.Wrapf(err,
			"unable to scan range descriptors")
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"%s: unable to unmarshal range descriptor", row.Key)
		}
	}
	return rangeDescs, nil
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

// splitAndFilterSpans returns the spans that represent the set difference
// (includes - excludes) while also guaranteeing that each output span does not
// cross the endpoint of a RangeDescriptor in ranges.
func splitAndFilterSpans(
	includes []roachpb.Span, excludes []roachpb.Span, ranges []roachpb.RangeDescriptor,
) []roachpb.Span {
	type includeMarker struct{}
	type excludeMarker struct{}

	includeCovering := coveringFromSpans(includes, includeMarker{})
	excludeCovering := coveringFromSpans(excludes, excludeMarker{})

	var rangeCovering covering.Covering
	for _, rangeDesc := range ranges {
		rangeCovering = append(rangeCovering, covering.Range{
			Start: []byte(rangeDesc.StartKey),
			End:   []byte(rangeDesc.EndKey),
		})
	}

	splits := covering.OverlapCoveringMerge(
		[]covering.Covering{includeCovering, excludeCovering, rangeCovering},
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
func clusterNodeCount(gw gossip.DeprecatedGossip) (int, error) {
	g, err := gw.OptionalErr(47970)
	if err != nil {
		return 0, err
	}
	var nodes int
	_ = g.IterateInfos(
		gossip.KeyNodeIDPrefix, func(_ string, _ gossip.Info) error {
			nodes++
			return nil
		},
	)
	return nodes, nil
}

type spanAndTime struct {
	span       roachpb.Span
	start, end hlc.Timestamp
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
	phs sql.PlanHookState,
	defaultURI string,
	urisByLocalityKV map[string]string,
	db *kv.DB,
	settings *cluster.Settings,
	defaultStore cloud.ExternalStorage,
	storageByLocalityKV map[string]*roachpb.ExternalStorage,
	job *jobs.Job,
	backupManifest *BackupManifest,
	checkpointDesc *BackupManifest,
	makeExternalStorage cloud.ExternalStorageFactory,
	encryption *roachpb.FileEncryptionOptions,
	statsCache *stats.TableStatisticsCache,
) (RowCount, error) {
	// TODO(dan): Figure out how permissions should work. #6713 is tracking this
	// for grpc.

	var files []BackupManifest_File
	var exported RowCount
	var lastCheckpoint time.Time

	var ranges []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		// TODO(benesch): limit the range descriptors we fetch to the ranges that
		// are actually relevant in the backup to speed up small backups on large
		// clusters.
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return RowCount{}, err
	}

	var completedSpans, completedIntroducedSpans []roachpb.Span
	if checkpointDesc != nil {
		// TODO(benesch): verify these files, rather than accepting them as truth
		// blindly.
		// No concurrency yet, so these assignments are safe.
		files = checkpointDesc.Files
		exported = checkpointDesc.EntryCounts
		for _, file := range checkpointDesc.Files {
			if file.StartTime.IsEmpty() && !file.EndTime.IsEmpty() {
				completedIntroducedSpans = append(completedIntroducedSpans, file.Span)
			} else {
				completedSpans = append(completedSpans, file.Span)
			}
		}
	}

	// Subtract out any completed spans and split the remaining spans into
	// range-sized pieces so that we can use the number of completed requests as a
	// rough measure of progress.
	spans := splitAndFilterSpans(backupManifest.Spans, completedSpans, ranges)
	introducedSpans := splitAndFilterSpans(backupManifest.IntroducedSpans, completedIntroducedSpans, ranges)

	progressLogger := jobs.NewChunkProgressLogger(job, len(spans), job.FractionCompleted(), jobs.ProgressUpdateOnly)

	requestFinishedCh := make(chan struct{}, len(spans)) // enough buffer to never block
	g := ctxgroup.WithContext(ctx)
	if len(spans) > 0 {
		g.GoCtx(func(ctx context.Context) error {
			// Currently the granularity of backup progress is the % of spans
			// exported. Would improve accuracy if we tracked the actual size of each
			// file.
			return progressLogger.Loop(ctx, requestFinishedCh)
		})
	}

	progCh := make(chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
	g.GoCtx(func(ctx context.Context) error {
		// When a processor is done exporting a span, it will send a progress update
		// to progCh.
		for progress := range progCh {
			var progDetails BackupManifest_Progress
			if err := types.UnmarshalAny(&progress.ProgressDetails, &progDetails); err != nil {
				log.Errorf(ctx, "unable to unmarshal backup progress details: %+v", err)
			}
			if backupManifest.RevisionStartTime.Less(progDetails.RevStartTime) {
				backupManifest.RevisionStartTime = progDetails.RevStartTime
			}
			for _, file := range progDetails.Files {
				files = append(files, file)
				exported.add(file.EntryCounts)
			}

			// Signal that an ExportRequest finished to update job progress.
			requestFinishedCh <- struct{}{}
			var checkpointFiles BackupFileDescriptors
			if timeutil.Since(lastCheckpoint) > BackupCheckpointInterval {
				checkpointFiles = append(checkpointFiles, files...)

				backupManifest.Files = checkpointFiles
				err := writeBackupManifest(
					ctx, settings, defaultStore, BackupManifestCheckpointName, encryption, backupManifest,
				)
				if err != nil {
					log.Errorf(ctx, "unable to checkpoint backup descriptor: %+v", err)
				}

				lastCheckpoint = timeutil.Now()
			}
		}
		return nil
	})

	pkIDs := make(map[uint64]bool)
	for _, desc := range backupManifest.Descriptors {
		if t := desc.Table(hlc.Timestamp{}); t != nil {
			pkIDs[roachpb.BulkOpSummaryID(uint64(t.ID), uint64(t.PrimaryIndex.ID))] = true
		}
	}

	if err := distBackup(
		ctx,
		phs,
		spans,
		introducedSpans,
		pkIDs,
		defaultURI,
		urisByLocalityKV,
		encryption,
		roachpb.MVCCFilter(backupManifest.MVCCFilter),
		backupManifest.StartTime,
		backupManifest.EndTime,
		progCh,
	); err != nil {
		return RowCount{}, err
	}

	if err := g.Wait(); err != nil {
		return RowCount{}, errors.Wrapf(err, "exporting %d ranges", errors.Safe(len(spans)))
	}

	backupManifest.Files = files
	backupManifest.EntryCounts = exported

	backupID := uuid.MakeV4()
	backupManifest.ID = backupID
	// Write additional partial descriptors to each node for partitioned backups.
	if len(storageByLocalityKV) > 0 {
		filesByLocalityKV := make(map[string][]BackupManifest_File)
		for i := range files {
			file := &files[i]
			filesByLocalityKV[file.LocalityKV] = append(filesByLocalityKV[file.LocalityKV], *file)
		}

		nextPartitionedDescFilenameID := 1
		for kv, conf := range storageByLocalityKV {
			backupManifest.LocalityKVs = append(backupManifest.LocalityKVs, kv)
			// Set a unique filename for each partition backup descriptor. The ID
			// ensures uniqueness, and the kv string appended to the end is for
			// readability.
			filename := fmt.Sprintf("%s_%d_%s",
				BackupPartitionDescriptorPrefix, nextPartitionedDescFilenameID, sanitizeLocalityKV(kv))
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

	if err := writeBackupManifest(ctx, settings, defaultStore, BackupManifestName, encryption, backupManifest); err != nil {
		return RowCount{}, err
	}
	var tableStatistics []*stats.TableStatisticProto
	for _, desc := range backupManifest.Descriptors {
		if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			// Collect all the table stats for this table.
			tableStatisticsAcc, err := statsCache.GetTableStats(ctx, tableDesc.GetID())
			if err != nil {
				return RowCount{}, err
			}
			for _, stat := range tableStatisticsAcc {
				tableStatistics = append(tableStatistics, &stat.TableStatisticProto)
			}
		}
	}
	statsTable := StatsTable{
		Statistics: tableStatistics,
	}

	if err := writeTableStatistics(ctx, defaultStore, BackupStatisticsFileName, encryption, &statsTable); err != nil {
		return RowCount{}, err
	}

	return exported, nil
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
	job *jobs.Job

	testingKnobs struct {
		ignoreProtectedTimestamps bool
	}
}

// Resume is part of the jobs.Resumer interface.
func (b *backupResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := b.job.Details().(jobspb.BackupDetails)
	p := phs.(sql.PlanHookState)

	ptsID := details.ProtectedTimestampRecord
	if ptsID != nil && !b.testingKnobs.ignoreProtectedTimestamps {
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

	if len(details.BackupManifest) == 0 {
		return errors.Newf("missing backup descriptor; cannot resume a backup from an older version")
	}

	var backupManifest BackupManifest
	if err := protoutil.Unmarshal(details.BackupManifest, &backupManifest); err != nil {
		return pgerror.Wrapf(err, pgcode.DataCorrupted,
			"unmarshal backup descriptor")
	}
	// For all backups, partitioned or not, the main BACKUP manifest is stored at
	// details.URI.
	defaultConf, err := cloudimpl.ExternalStorageConfFromURI(details.URI, p.User())
	if err != nil {
		return errors.Wrapf(err, "export configuration")
	}
	defaultStore, err := p.ExecCfg().DistSQLSrv.ExternalStorage(ctx, defaultConf)
	if err != nil {
		return errors.Wrapf(err, "make storage")
	}
	storageByLocalityKV := make(map[string]*roachpb.ExternalStorage)
	for kv, uri := range details.URIsByLocalityKV {
		conf, err := cloudimpl.ExternalStorageConfFromURI(uri, p.User())
		if err != nil {
			return err
		}
		storageByLocalityKV[kv] = &conf
	}
	var checkpointDesc *BackupManifest

	// We don't read the table descriptors from the backup descriptor, but
	// they could be using either the new or the old foreign key
	// representations. We should just preserve whatever representation the
	// table descriptors were using and leave them alone.
	if desc, err := readBackupManifest(ctx, defaultStore, BackupManifestCheckpointName, details.Encryption); err == nil {
		// If the checkpoint is from a different cluster, it's meaningless to us.
		// More likely though are dummy/lock-out checkpoints with no ClusterID.
		if desc.ClusterID.Equal(p.ExecCfg().ClusterID()) {
			checkpointDesc = &desc
		}
	} else {
		// TODO(benesch): distinguish between a missing checkpoint, which simply
		// indicates the prior backup attempt made no progress, and a corrupted
		// checkpoint, which is more troubling. Sadly, storageccl doesn't provide a
		// "not found" error that's consistent across all ExternalStorage
		// implementations.
		log.Warningf(ctx, "unable to load backup checkpoint while resuming job %d: %v", *b.job.ID(), err)
	}

	numClusterNodes, err := clusterNodeCount(p.ExecCfg().Gossip)
	if err != nil {
		return err
	}

	statsCache := p.ExecCfg().TableStatsCache
	res, err := backup(
		ctx,
		p,
		details.URI,
		details.URIsByLocalityKV,
		p.ExecCfg().DB,
		p.ExecCfg().Settings,
		defaultStore,
		storageByLocalityKV,
		b.job,
		&backupManifest,
		checkpointDesc,
		p.ExecCfg().DistSQLSrv.ExternalStorage,
		details.Encryption,
		statsCache,
	)
	if err != nil {
		return err
	}

	err = b.clearStats(ctx, p.ExecCfg().DB)
	if err != nil {
		log.Warningf(ctx, "unable to clear stats from job payload: %+v", err)
	}
	b.deleteCheckpoint(ctx, p.ExecCfg(), p.User())

	if ptsID != nil && !b.testingKnobs.ignoreProtectedTimestamps {
		if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return b.releaseProtectedTimestamp(ctx, txn, p.ExecCfg().ProtectedTimestampProvider)
		}); err != nil {
			log.Errorf(ctx, "failed to release protected timestamp: %v", err)
		}
	}

	resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(*b.job.ID())),
		tree.NewDString(string(jobs.StatusSucceeded)),
		tree.NewDFloat(tree.DFloat(1.0)),
		tree.NewDInt(tree.DInt(res.Rows)),
		tree.NewDInt(tree.DInt(res.IndexEntries)),
		tree.NewDInt(tree.DInt(res.DataSize)),
	}

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

	return nil
}

func (b *backupResumer) clearStats(ctx context.Context, DB *kv.DB) error {
	details := b.job.Details().(jobspb.BackupDetails)
	var backupManifest BackupManifest
	if err := protoutil.Unmarshal(details.BackupManifest, &backupManifest); err != nil {
		return err
	}
	backupManifest.DeprecatedStatistics = nil
	descBytes, err := protoutil.Marshal(&backupManifest)
	if err != nil {
		return err
	}
	details.BackupManifest = descBytes
	err = DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return b.job.WithTxn(txn).SetDetails(ctx, details)
	})
	return err
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (b *backupResumer) OnFailOrCancel(ctx context.Context, phs interface{}) error {
	telemetry.Count("backup.total.failed")
	telemetry.CountBucketed("backup.duration-sec.failed",
		int64(timeutil.Since(timeutil.FromUnixMicros(b.job.Payload().StartedMicros)).Seconds()))

	p := phs.(sql.PlanHookState)
	cfg := p.ExecCfg()
	b.deleteCheckpoint(ctx, cfg, p.User())
	return cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return b.releaseProtectedTimestamp(ctx, txn, cfg.ProtectedTimestampProvider)
	})
}

func (b *backupResumer) deleteCheckpoint(
	ctx context.Context, cfg *sql.ExecutorConfig, user string,
) {
	// Attempt to delete BACKUP-CHECKPOINT.
	if err := func() error {
		details := b.job.Details().(jobspb.BackupDetails)
		// For all backups, partitioned or not, the main BACKUP manifest is stored at
		// details.URI.
		conf, err := cloudimpl.ExternalStorageConfFromURI(details.URI, user)
		if err != nil {
			return err
		}
		exportStore, err := cfg.DistSQLSrv.ExternalStorage(ctx, conf)
		if err != nil {
			return err
		}
		return exportStore.Delete(ctx, BackupManifestCheckpointName)
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
