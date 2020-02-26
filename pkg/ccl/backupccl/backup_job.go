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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// BackupCheckpointInterval is the interval at which backup progress is saved
// to durable storage.
var BackupCheckpointInterval = time.Minute

func allRangeDescriptors(ctx context.Context, txn *client.Txn) ([]roachpb.RangeDescriptor, error) {
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
func clusterNodeCount(g *gossip.Gossip) int {
	var nodes int
	_ = g.IterateInfos(gossip.KeyNodeIDPrefix, func(_ string, _ gossip.Info) error {
		nodes++
		return nil
	})
	return nodes
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
	db *client.DB,
	gossip *gossip.Gossip,
	settings *cluster.Settings,
	defaultStore cloud.ExternalStorage,
	storageByLocalityKV map[string]*roachpb.ExternalStorage,
	job *jobs.Job,
	backupManifest *BackupManifest,
	checkpointDesc *BackupManifest,
	resultsCh chan<- tree.Datums,
	makeExternalStorage cloud.ExternalStorageFactory,
	encryption *roachpb.FileEncryptionOptions,
) (roachpb.BulkOpSummary, error) {
	// TODO(dan): Figure out how permissions should work. #6713 is tracking this
	// for grpc.

	mu := struct {
		syncutil.Mutex
		files          []BackupManifest_File
		exported       roachpb.BulkOpSummary
		lastCheckpoint time.Time
	}{}

	var checkpointMu syncutil.Mutex

	var ranges []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		// TODO(benesch): limit the range descriptors we fetch to the ranges that
		// are actually relevant in the backup to speed up small backups on large
		// clusters.
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return mu.exported, err
	}

	var completedSpans, completedIntroducedSpans []roachpb.Span
	if checkpointDesc != nil {
		// TODO(benesch): verify these files, rather than accepting them as truth
		// blindly.
		// No concurrency yet, so these assignments are safe.
		mu.files = checkpointDesc.Files
		mu.exported = checkpointDesc.EntryCounts
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

	allSpans := make([]spanAndTime, 0, len(spans)+len(introducedSpans))
	for _, s := range introducedSpans {
		allSpans = append(allSpans, spanAndTime{span: s, start: hlc.Timestamp{}, end: backupManifest.StartTime})
	}
	for _, s := range spans {
		allSpans = append(allSpans, spanAndTime{span: s, start: backupManifest.StartTime, end: backupManifest.EndTime})
	}

	// Sequential ranges may have clustered leaseholders, for example a
	// geo-partitioned table likely has all the leaseholders for some contiguous
	// span of the table (i.e. a partition) pinned to just the nodes in a region.
	// In such cases, sending spans sequentially may under-utilize the rest of the
	// cluster given that we have a limit on the number of spans we send out at
	// a given time. Randomizing the order of spans should help ensure a more even
	// distribution of work across the cluster regardless of how leaseholders may
	// or may not be clustered.
	rand.Shuffle(len(allSpans), func(i, j int) {
		allSpans[i], allSpans[j] = allSpans[j], allSpans[i]
	})

	progressLogger := jobs.NewChunkProgressLogger(job, len(spans), job.FractionCompleted(), jobs.ProgressUpdateOnly)

	// We're already limiting these on the server-side, but sending all the
	// Export requests at once would fill up distsender/grpc/something and cause
	// all sorts of badness (node liveness timeouts leading to mass leaseholder
	// transfers, poor performance on SQL workloads, etc) as well as log spam
	// about slow distsender requests. Rate limit them here, too.
	//
	// Each node limits the number of running Export & Import requests it serves
	// to avoid overloading the network, so multiply that by the number of nodes
	// in the cluster and use that as the number of outstanding Export requests
	// for the rate limiting. This attempts to strike a balance between
	// simplicity, not getting slow distsender log spam, and keeping the server
	// side limiter full.
	//
	// TODO(dan): Make this limiting per node.
	//
	// TODO(dan): See if there's some better solution than rate-limiting #14798.
	maxConcurrentExports := clusterNodeCount(gossip) * int(storage.ExportRequestsLimit.Get(&settings.SV)) * 10
	exportsSem := make(chan struct{}, maxConcurrentExports)

	g := ctxgroup.WithContext(ctx)

	requestFinishedCh := make(chan struct{}, len(spans)) // enough buffer to never block

	// Only start the progress logger if there are spans, otherwise this will
	// block forever. This is needed for TestBackupRestoreResume which doesn't
	// have any spans. Users should never hit this.
	if len(spans) > 0 {
		g.GoCtx(func(ctx context.Context) error {
			return progressLogger.Loop(ctx, requestFinishedCh)
		})
	}
	g.GoCtx(func(ctx context.Context) error {
		for i := range allSpans {
			{
				select {
				case exportsSem <- struct{}{}:
				case <-ctx.Done():
					// Break the for loop to avoid creating more work - the backup
					// has failed because either the context has been canceled or an
					// error has been returned. Either way, Wait() is guaranteed to
					// return an error now.
					return ctx.Err()
				}
			}

			span := allSpans[i]
			g.GoCtx(func(ctx context.Context) error {
				defer func() { <-exportsSem }()
				header := roachpb.Header{Timestamp: span.end}
				req := &roachpb.ExportRequest{
					RequestHeader:                       roachpb.RequestHeaderFromSpan(span.span),
					Storage:                             defaultStore.Conf(),
					StorageByLocalityKV:                 storageByLocalityKV,
					StartTime:                           span.start,
					EnableTimeBoundIteratorOptimization: useTBI.Get(&settings.SV),
					MVCCFilter:                          roachpb.MVCCFilter(backupManifest.MVCCFilter),
					Encryption:                          encryption,
				}
				rawRes, pErr := client.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
				if pErr != nil {
					return pErr.GoError()
				}
				res := rawRes.(*roachpb.ExportResponse)

				mu.Lock()
				if backupManifest.RevisionStartTime.Less(res.StartTime) {
					backupManifest.RevisionStartTime = res.StartTime
				}
				for _, file := range res.Files {
					f := BackupManifest_File{
						Span:        file.Span,
						Path:        file.Path,
						Sha512:      file.Sha512,
						EntryCounts: file.Exported,
						LocalityKV:  file.LocalityKV,
					}
					if span.start != backupManifest.StartTime {
						f.StartTime = span.start
						f.EndTime = span.end
					}
					mu.files = append(mu.files, f)
					mu.exported.Add(file.Exported)
				}
				var checkpointFiles BackupFileDescriptors
				if timeutil.Since(mu.lastCheckpoint) > BackupCheckpointInterval {
					// We optimistically assume the checkpoint will succeed to prevent
					// multiple threads from attempting to checkpoint.
					mu.lastCheckpoint = timeutil.Now()
					checkpointFiles = append(checkpointFiles, mu.files...)
				}
				mu.Unlock()

				requestFinishedCh <- struct{}{}

				if checkpointFiles != nil {
					// Make a copy while holding mu to avoid races while marshaling the
					// manifest into the checkpoint file.
					mu.Lock()
					maninfestCopy := *backupManifest
					mu.Unlock()

					checkpointMu.Lock()
					maninfestCopy.Files = checkpointFiles
					err := writeBackupManifest(
						ctx, settings, defaultStore, BackupManifestCheckpointName, encryption, &maninfestCopy,
					)
					checkpointMu.Unlock()
					if err != nil {
						log.Errorf(ctx, "unable to checkpoint backup descriptor: %+v", err)
					}
				}
				return nil
			})
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return mu.exported, errors.Wrapf(err, "exporting %d ranges", errors.Safe(len(spans)))
	}

	// No more concurrency, so no need to acquire locks below.

	backupManifest.Files = mu.files
	backupManifest.EntryCounts = mu.exported

	backupID := uuid.MakeV4()
	backupManifest.ID = backupID
	// Write additional partial descriptors to each node for partitioned backups.
	if len(storageByLocalityKV) > 0 {
		filesByLocalityKV := make(map[string][]BackupManifest_File)
		for i := range mu.files {
			file := &mu.files[i]
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
				return mu.exported, err
			}
		}
	}

	if err := writeBackupManifest(ctx, settings, defaultStore, BackupManifestName, encryption, backupManifest); err != nil {
		return mu.exported, err
	}

	return mu.exported, nil
}

type backupResumer struct {
	job                 *jobs.Job
	settings            *cluster.Settings
	res                 roachpb.BulkOpSummary
	makeExternalStorage cloud.ExternalStorageFactory
}

// Resume is part of the jobs.Resumer interface.
func (b *backupResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := b.job.Details().(jobspb.BackupDetails)
	p := phs.(sql.PlanHookState)
	b.makeExternalStorage = p.ExecCfg().DistSQLSrv.ExternalStorage

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
	defaultConf, err := cloud.ExternalStorageConfFromURI(details.URI)
	if err != nil {
		return errors.Wrapf(err, "export configuration")
	}
	defaultStore, err := b.makeExternalStorage(ctx, defaultConf)
	if err != nil {
		return errors.Wrapf(err, "make storage")
	}
	storageByLocalityKV := make(map[string]*roachpb.ExternalStorage)
	for kv, uri := range details.URIsByLocalityKV {
		conf, err := cloud.ExternalStorageConfFromURI(uri)
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
	res, err := backup(
		ctx,
		p.ExecCfg().DB,
		p.ExecCfg().Gossip,
		p.ExecCfg().Settings,
		defaultStore,
		storageByLocalityKV,
		b.job,
		&backupManifest,
		checkpointDesc,
		resultsCh,
		b.makeExternalStorage,
		details.Encryption,
	)
	if err != nil {
		return err
	}
	b.res = res

	err = b.clearStats(ctx, p.ExecCfg().DB)
	if err != nil {
		log.Warningf(ctx, "unable to clear stats from job payload: %+v", err)
	}
	return nil
}

func (b *backupResumer) clearStats(ctx context.Context, DB *client.DB) error {
	details := b.job.Details().(jobspb.BackupDetails)
	var backupManifest BackupManifest
	if err := protoutil.Unmarshal(details.BackupManifest, &backupManifest); err != nil {
		return err
	}
	backupManifest.Statistics = nil
	descBytes, err := protoutil.Marshal(&backupManifest)
	if err != nil {
		return err
	}
	details.BackupManifest = descBytes
	err = DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		return b.job.WithTxn(txn).SetDetails(ctx, details)
	})
	return err
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (b *backupResumer) OnFailOrCancel(context.Context, interface{}) error {
	return nil
}

// OnSuccess is part of the jobs.Resumer interface.
func (b *backupResumer) OnSuccess(context.Context, *client.Txn) error { return nil }

// OnTerminal is part of the jobs.Resumer interface.
func (b *backupResumer) OnTerminal(
	ctx context.Context, status jobs.Status, resultsCh chan<- tree.Datums,
) {
	// Attempt to delete BACKUP-CHECKPOINT.
	if err := func() error {
		details := b.job.Details().(jobspb.BackupDetails)
		// For all backups, partitioned or not, the main BACKUP manifest is stored at
		// details.URI.
		conf, err := cloud.ExternalStorageConfFromURI(details.URI)
		if err != nil {
			return err
		}
		exportStore, err := b.makeExternalStorage(ctx, conf)
		if err != nil {
			return err
		}
		return exportStore.Delete(ctx, BackupManifestCheckpointName)
	}(); err != nil {
		log.Warningf(ctx, "unable to delete checkpointed backup descriptor: %+v", err)
	}

	if status == jobs.StatusSucceeded {
		// TODO(benesch): emit periodic progress updates.

		// TODO(mjibson): if a restore was resumed, then these counts will only have
		// the current coordinator's counts.

		resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(*b.job.ID())),
			tree.NewDString(string(jobs.StatusSucceeded)),
			tree.NewDFloat(tree.DFloat(1.0)),
			tree.NewDInt(tree.DInt(b.res.Rows)),
			tree.NewDInt(tree.DInt(b.res.IndexEntries)),
			tree.NewDInt(tree.DInt(b.res.DataSize)),
		}
	}
}

var _ jobs.Resumer = &backupResumer{}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeBackup,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &backupResumer{
				job:      job,
				settings: settings,
			}
		},
	)
}
