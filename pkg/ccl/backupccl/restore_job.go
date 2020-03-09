// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

type intervalSpan roachpb.Span

var _ interval.Interface = intervalSpan{}

// ID is part of `interval.Interface` but unused in makeImportSpans.
func (ie intervalSpan) ID() uintptr { return 0 }

// Range is part of `interval.Interface`.
func (ie intervalSpan) Range() interval.Range {
	return interval.Range{Start: []byte(ie.Key), End: []byte(ie.EndKey)}
}

type importEntryType int

const (
	backupSpan importEntryType = iota
	backupFile
	tableSpan
	completedSpan
	request
)

type importEntry struct {
	roachpb.Span
	entryType importEntryType

	// Only set if entryType is backupSpan
	start, end hlc.Timestamp

	// Only set if entryType is backupFile
	dir  roachpb.ExternalStorage
	file BackupManifest_File

	// Only set if entryType is request
	files []roachpb.ImportRequest_File

	// for progress tracking we assign the spans numbers as they can be executed
	// out-of-order based on splitAndScatter's scheduling.
	progressIdx int
}

// makeImportSpans pivots the backups, which are grouped by time, into
// spans for import, which are grouped by keyrange.
//
// The core logic of this is in OverlapCoveringMerge, which accepts sets of
// non-overlapping key ranges (aka coverings) each with a payload, and returns
// them aligned with the payloads in the same order as in the input.
//
// Example (input):
// - [A, C) backup t0 to t1 -> /file1
// - [C, D) backup t0 to t1 -> /file2
// - [A, B) backup t1 to t2 -> /file3
// - [B, C) backup t1 to t2 -> /file4
// - [C, D) backup t1 to t2 -> /file5
// - [B, D) requested table data to be restored
//
// Example (output):
// - [A, B) -> /file1, /file3
// - [B, C) -> /file1, /file4, requested (note that file1 was split into two ranges)
// - [C, D) -> /file2, /file5, requested
//
// This would be turned into two Import spans, one restoring [B, C) out of
// /file1 and /file4, the other restoring [C, D) out of /file2 and /file5.
// Nothing is restored out of /file3 and only part of /file1 is used.
//
// NB: All grouping operates in the pre-rewrite keyspace, meaning the keyranges
// as they were backed up, not as they're being restored.
//
// If a span is not covered, the onMissing function is called with the span and
// time missing to determine what error, if any, should be returned.
func makeImportSpans(
	tableSpans []roachpb.Span,
	backups []BackupManifest,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	lowWaterMark roachpb.Key,
	onMissing func(span covering.Range, start, end hlc.Timestamp) error,
) ([]importEntry, hlc.Timestamp, error) {
	// Put the covering for the already-completed spans into the
	// OverlapCoveringMerge input first. Payloads are returned in the same order
	// that they appear in the input; putting the completedSpan first means we'll
	// see it first when iterating over the output of OverlapCoveringMerge and
	// avoid doing unnecessary work.
	completedCovering := covering.Covering{
		{
			Start:   []byte(keys.MinKey),
			End:     []byte(lowWaterMark),
			Payload: importEntry{entryType: completedSpan},
		},
	}

	// Put the merged table data covering into the OverlapCoveringMerge input
	// next.
	var tableSpanCovering covering.Covering
	for _, span := range tableSpans {
		tableSpanCovering = append(tableSpanCovering, covering.Range{
			Start: span.Key,
			End:   span.EndKey,
			Payload: importEntry{
				Span:      span,
				entryType: tableSpan,
			},
		})
	}

	backupCoverings := []covering.Covering{completedCovering, tableSpanCovering}

	// Iterate over backups creating two coverings for each. First the spans
	// that were backed up, then the files in the backup. The latter is a subset
	// when some of the keyranges in the former didn't change since the previous
	// backup. These alternate (backup1 spans, backup1 files, backup2 spans,
	// backup2 files) so they will retain that alternation in the output of
	// OverlapCoveringMerge.
	var maxEndTime hlc.Timestamp
	for i, b := range backups {
		if maxEndTime.Less(b.EndTime) {
			maxEndTime = b.EndTime
		}

		var backupNewSpanCovering covering.Covering
		for _, s := range b.IntroducedSpans {
			backupNewSpanCovering = append(backupNewSpanCovering, covering.Range{
				Start:   s.Key,
				End:     s.EndKey,
				Payload: importEntry{Span: s, entryType: backupSpan, start: hlc.Timestamp{}, end: b.StartTime},
			})
		}
		backupCoverings = append(backupCoverings, backupNewSpanCovering)

		var backupSpanCovering covering.Covering
		for _, s := range b.Spans {
			backupSpanCovering = append(backupSpanCovering, covering.Range{
				Start:   s.Key,
				End:     s.EndKey,
				Payload: importEntry{Span: s, entryType: backupSpan, start: b.StartTime, end: b.EndTime},
			})
		}
		backupCoverings = append(backupCoverings, backupSpanCovering)
		var backupFileCovering covering.Covering

		var storesByLocalityKV map[string]roachpb.ExternalStorage
		if backupLocalityInfo != nil && backupLocalityInfo[i].URIsByOriginalLocalityKV != nil {
			storesByLocalityKV = make(map[string]roachpb.ExternalStorage)
			for kv, uri := range backupLocalityInfo[i].URIsByOriginalLocalityKV {
				conf, err := cloud.ExternalStorageConfFromURI(uri)
				if err != nil {
					return nil, hlc.Timestamp{}, err
				}
				storesByLocalityKV[kv] = conf
			}
		}
		for _, f := range b.Files {
			dir := b.Dir
			if storesByLocalityKV != nil {
				if newDir, ok := storesByLocalityKV[f.LocalityKV]; ok {
					dir = newDir
				}
			}
			backupFileCovering = append(backupFileCovering, covering.Range{
				Start: f.Span.Key,
				End:   f.Span.EndKey,
				Payload: importEntry{
					Span:      f.Span,
					entryType: backupFile,
					dir:       dir,
					file:      f,
				},
			})
		}
		backupCoverings = append(backupCoverings, backupFileCovering)
	}

	// Group ranges covered by backups with ones needed to restore the selected
	// tables. Note that this breaks intervals up as necessary to align them.
	// See the function godoc for details.
	importRanges := covering.OverlapCoveringMerge(backupCoverings)

	// Translate the output of OverlapCoveringMerge into requests.
	var requestEntries []importEntry
rangeLoop:
	for _, importRange := range importRanges {
		needed := false
		var ts hlc.Timestamp
		var files []roachpb.ImportRequest_File
		payloads := importRange.Payload.([]interface{})
		for _, p := range payloads {
			ie := p.(importEntry)
			switch ie.entryType {
			case completedSpan:
				continue rangeLoop
			case tableSpan:
				needed = true
			case backupSpan:
				if ts != ie.start {
					return nil, hlc.Timestamp{}, errors.Errorf(
						"no backup covers time [%s,%s) for range [%s,%s) or backups listed out of order (mismatched start time)",
						ts, ie.start,
						roachpb.Key(importRange.Start), roachpb.Key(importRange.End))
				}
				ts = ie.end
			case backupFile:
				if len(ie.file.Path) > 0 {
					files = append(files, roachpb.ImportRequest_File{
						Dir:    ie.dir,
						Path:   ie.file.Path,
						Sha512: ie.file.Sha512,
					})
				}
			}
		}
		if needed {
			if ts != maxEndTime {
				if err := onMissing(importRange, ts, maxEndTime); err != nil {
					return nil, hlc.Timestamp{}, err
				}
			}
			// If needed is false, we have data backed up that is not necessary
			// for this restore. Skip it.
			requestEntries = append(requestEntries, importEntry{
				Span:      roachpb.Span{Key: importRange.Start, EndKey: importRange.End},
				entryType: request,
				files:     files,
			})
		}
	}
	return requestEntries, maxEndTime, nil
}

// splitAndScatter creates new ranges for importSpans and scatters replicas and
// leaseholders to be as evenly balanced as possible. It does this with some
// amount of parallelism but also staying as close to the order in importSpans
// as possible (the more out of order, the more work is done if a RESTORE job
// loses its lease and has to be restarted).
//
// At a high level, this is accomplished by splitting and scattering large
// "chunks" from the front of importEntries in one goroutine, each of which are
// in turn passed to one of many worker goroutines that split and scatter the
// individual entries.
//
// importEntries are sent to readyForImportCh as they are scattered, so letting
// that channel send block can be used for backpressure on the splits and
// scatters.
//
// TODO(dan): This logic is largely tuned by running BenchmarkRestore2TB. See if
// there's some way to test it without running an O(hour) long benchmark.
func splitAndScatter(
	restoreCtx context.Context,
	settings *cluster.Settings,
	db *kv.DB,
	kr *storageccl.KeyRewriter,
	numClusterNodes int,
	importSpans []importEntry,
	readyForImportCh chan<- importEntry,
) error {
	var span opentracing.Span
	ctx, span := tracing.ChildSpan(restoreCtx, "presplit-scatter")
	defer tracing.FinishSpan(span)

	g := ctxgroup.WithContext(ctx)

	// TODO(dan): This not super principled. I just wanted something that wasn't
	// a constant and grew slower than linear with the length of importSpans. It
	// seems to be working well for BenchmarkRestore2TB but worth revisiting.
	chunkSize := int(math.Sqrt(float64(len(importSpans))))
	importSpanChunks := make([][]importEntry, 0, len(importSpans)/chunkSize)
	for start := 0; start < len(importSpans); {
		importSpanChunk := importSpans[start:]
		end := start + chunkSize
		if end < len(importSpans) {
			importSpanChunk = importSpans[start:end]
		}
		importSpanChunks = append(importSpanChunks, importSpanChunk)
		start = end
	}

	importSpanChunksCh := make(chan []importEntry)
	expirationTime := db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	g.GoCtx(func(ctx context.Context) error {
		defer close(importSpanChunksCh)
		for idx, importSpanChunk := range importSpanChunks {
			// TODO(dan): The structure between this and the below are very
			// similar. Dedup.
			chunkKey, err := rewriteBackupSpanKey(kr, importSpanChunk[0].Key)
			if err != nil {
				return err
			}

			// TODO(dan): Really, this should be splitting the Key of the first
			// entry in the _next_ chunk.
			log.VEventf(restoreCtx, 1, "presplitting chunk %d of %d", idx, len(importSpanChunks))
			if err := db.AdminSplit(ctx, chunkKey, chunkKey, expirationTime); err != nil {
				return err
			}

			log.VEventf(restoreCtx, 1, "scattering chunk %d of %d", idx, len(importSpanChunks))
			scatterReq := &roachpb.AdminScatterRequest{
				RequestHeader: roachpb.RequestHeaderFromSpan(roachpb.Span{
					Key:    chunkKey,
					EndKey: chunkKey.Next(),
				}),
				// TODO(dan): This is a bit of a hack, but it seems to be an effective
				// one (see the PR that added it for graphs). As of the commit that
				// added this, scatter is not very good at actually balancing leases.
				// This is likely for two reasons: 1) there's almost certainly some
				// regression in scatter's behavior, it used to work much better and 2)
				// scatter has to operate by balancing leases for all ranges in a
				// cluster, but in RESTORE, we really just want it to be balancing the
				// span being restored into.
				RandomizeLeases: true,
			}
			if _, pErr := kv.SendWrapped(ctx, db.NonTransactionalSender(), scatterReq); pErr != nil {
				// TODO(dan): Unfortunately, Scatter is still too unreliable to
				// fail the RESTORE when Scatter fails. I'm uncomfortable that
				// this could break entirely and not start failing the tests,
				// but on the bright side, it doesn't affect correctness, only
				// throughput.
				log.Errorf(ctx, "failed to scatter chunk %d: %s", idx, pErr.GoError())
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case importSpanChunksCh <- importSpanChunk:
			}
		}
		return nil
	})

	// TODO(dan): This tries to cover for a bad scatter by having 2 * the number
	// of nodes in the cluster. Is it necessary?
	splitScatterWorkers := numClusterNodes * 2
	var splitScatterStarted uint64 // Only access using atomic.
	for worker := 0; worker < splitScatterWorkers; worker++ {
		g.GoCtx(func(ctx context.Context) error {
			for importSpanChunk := range importSpanChunksCh {
				for _, importSpan := range importSpanChunk {
					idx := atomic.AddUint64(&splitScatterStarted, 1)

					newSpanKey, err := rewriteBackupSpanKey(kr, importSpan.Span.Key)
					if err != nil {
						return err
					}

					// TODO(dan): Really, this should be splitting the Key of
					// the _next_ entry.
					log.VEventf(restoreCtx, 1, "presplitting %d of %d", idx, len(importSpans))
					if err := db.AdminSplit(ctx, newSpanKey, newSpanKey, expirationTime); err != nil {
						return err
					}

					log.VEventf(restoreCtx, 1, "scattering %d of %d", idx, len(importSpans))
					scatterReq := &roachpb.AdminScatterRequest{
						RequestHeader: roachpb.RequestHeaderFromSpan(roachpb.Span{Key: newSpanKey, EndKey: newSpanKey.Next()}),
					}
					if _, pErr := kv.SendWrapped(ctx, db.NonTransactionalSender(), scatterReq); pErr != nil {
						// TODO(dan): Unfortunately, Scatter is still too unreliable to
						// fail the RESTORE when Scatter fails. I'm uncomfortable that
						// this could break entirely and not start failing the tests,
						// but on the bright side, it doesn't affect correctness, only
						// throughput.
						log.Errorf(ctx, "failed to scatter %d: %s", idx, pErr.GoError())
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case readyForImportCh <- importSpan:
					}
				}
			}
			return nil
		})
	}

	return g.Wait()
}

// WriteTableDescs writes all the the new descriptors: First the ID ->
// TableDescriptor for the new table, then flip (or initialize) the name -> ID
// entry so any new queries will use the new one. The tables are assigned the
// permissions of their parent database and the user must have CREATE permission
// on that database at the time this function is called.
func WriteTableDescs(
	ctx context.Context,
	txn *kv.Txn,
	databases []*sqlbase.DatabaseDescriptor,
	tables []*sqlbase.TableDescriptor,
	descCoverage tree.DescriptorCoverage,
	user string,
	settings *cluster.Settings,
	extra []roachpb.KeyValue,
) error {
	ctx, span := tracing.ChildSpan(ctx, "WriteTableDescs")
	defer tracing.FinishSpan(span)
	err := func() error {
		b := txn.NewBatch()
		wroteDBs := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor)
		for _, desc := range databases {
			// If the restore is not a full cluster restore we cannot know that
			// the users on the restoring cluster match the ones that were on the
			// cluster that was backed up. So we wipe the priviledges on the database.
			if descCoverage != tree.AllDescriptors {
				desc.Privileges = sqlbase.NewDefaultPrivilegeDescriptor()
			}
			wroteDBs[desc.ID] = desc
			if err := sql.WriteNewDescToBatch(ctx, false /* kvTrace */, settings, b, desc.ID, desc); err != nil {
				return err
			}
			// Depending on which cluster version we are restoring to, we decide which
			// namespace table to write the descriptor into. This may cause wrong
			// behavior if the cluster version is bumped DURING a restore.
			dKey := sqlbase.MakeDatabaseNameKey(ctx, settings, desc.Name)
			b.CPut(dKey.Key(), desc.ID, nil)
		}
		for i := range tables {
			// For full cluster restore, keep privileges as they were.
			if wrote, ok := wroteDBs[tables[i].ParentID]; ok {
				// Leave the privileges of the temp system tables as
				// the default.
				if descCoverage != tree.AllDescriptors || wrote.Name == restoreTempSystemDB {
					tables[i].Privileges = wrote.GetPrivileges()
				}
			} else {
				parentDB, err := sqlbase.GetDatabaseDescFromID(ctx, txn, tables[i].ParentID)
				if err != nil {
					return errors.Wrapf(err,
						"failed to lookup parent DB %d", errors.Safe(tables[i].ParentID))
				}
				// We don't check priv's here since we checked them during job planning.

				// On full cluster restore, keep the privs as they are in the backup.
				if descCoverage != tree.AllDescriptors {
					// Default is to copy privs from restoring parent db, like CREATE TABLE.
					// TODO(dt): Make this more configurable.
					tables[i].Privileges = parentDB.GetPrivileges()
				}
			}
			if err := sql.WriteNewDescToBatch(ctx, false /* kvTrace */, settings, b, tables[i].ID, tables[i]); err != nil {
				return err
			}
			// Depending on which cluster version we are restoring to, we decide which
			// namespace table to write the descriptor into. This may cause wrong
			// behavior if the cluster version is bumped DURING a restore.
			tkey := sqlbase.MakePublicTableNameKey(ctx, settings, tables[i].ParentID, tables[i].Name)
			b.CPut(tkey.Key(), tables[i].ID, nil)
		}
		for _, kv := range extra {
			b.InitPut(kv.Key, &kv.Value, false)
		}
		if err := txn.Run(ctx, b); err != nil {
			if _, ok := errors.UnwrapAll(err).(*roachpb.ConditionFailedError); ok {
				return pgerror.Newf(pgcode.DuplicateObject, "table already exists")
			}
			return err
		}

		for _, table := range tables {
			if err := table.Validate(ctx, txn); err != nil {
				return errors.Wrapf(err,
					"validate table %d", errors.Safe(table.ID))
			}
		}
		return nil
	}()
	return errors.Wrapf(err, "restoring table desc and namespace entries")
}

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
// Note that the actual restore process (i.e. inside ImportRequest) does not use
// these keys -- they are only used to split the key space and distribute those
// requests, thus truncation is fine. In the rare case where multiple backup
// spans are truncated to the same prefix (i.e. entire spans resided under the
// same interleave parent row) we'll generate some no-op splits and route the
// work to the same range, but the actual imported data is unaffected.
func rewriteBackupSpanKey(kr *storageccl.KeyRewriter, key roachpb.Key) (roachpb.Key, error) {
	newKey, rewritten, err := kr.RewriteKey(append([]byte(nil), key...), true /* isFromSpan */)
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"could not rewrite span start key: %s", key)
	}
	if !rewritten && bytes.Equal(newKey, key) {
		// if nothing was changed, we didn't match the top-level key at all.
		return nil, errors.AssertionFailedf(
			"no rewrite for span start key: %s", key)
	}
	// Modify all spans that begin at the primary index to instead begin at the
	// start of the table. That is, change a span start key from /Table/51/1 to
	// /Table/51. Otherwise a permanently empty span at /Table/51-/Table/51/1
	// will be created.
	if b, id, idx, err := sqlbase.DecodeTableIDIndexID(newKey); err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"could not rewrite span start key: %s", key)
	} else if idx == 1 && len(b) == 0 {
		newKey = keys.MakeTablePrefix(uint32(id))
	}
	return newKey, nil
}

// restore imports a SQL table (or tables) from sets of non-overlapping sstable
// files.
func restore(
	restoreCtx context.Context,
	db *kv.DB,
	gossip *gossip.Gossip,
	settings *cluster.Settings,
	backupManifests []BackupManifest,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
	tables []*sqlbase.TableDescriptor,
	oldTableIDs []sqlbase.ID,
	spans []roachpb.Span,
	job *jobs.Job,
	encryption *roachpb.FileEncryptionOptions,
) (RowCount, error) {
	// A note about contexts and spans in this method: the top-level context
	// `restoreCtx` is used for orchestration logging. All operations that carry
	// out work get their individual contexts.

	mu := struct {
		syncutil.Mutex
		res               RowCount
		requestsCompleted []bool
		highWaterMark     int
	}{
		highWaterMark: -1,
	}

	// Get TableRekeys to use when importing raw data.
	var rekeys []roachpb.ImportRequest_TableRekey
	for i := range tables {
		tableToSerialize := tables[i]
		newDescBytes, err := protoutil.Marshal(sqlbase.WrapDescriptor(tableToSerialize))
		if err != nil {
			return mu.res, errors.NewAssertionErrorWithWrappedErrf(err,
				"marshaling descriptor")
		}
		rekeys = append(rekeys, roachpb.ImportRequest_TableRekey{
			OldID:   uint32(oldTableIDs[i]),
			NewDesc: newDescBytes,
		})
	}
	kr, err := storageccl.MakeKeyRewriterFromRekeys(rekeys)
	if err != nil {
		return mu.res, err
	}

	// Pivot the backups, which are grouped by time, into requests for import,
	// which are grouped by keyrange.
	highWaterMark := job.Progress().Details.(*jobspb.Progress_Restore).Restore.HighWater
	importSpans, _, err := makeImportSpans(spans, backupManifests, backupLocalityInfo, highWaterMark, errOnMissingRange)
	if err != nil {
		return mu.res, errors.Wrapf(err, "making import requests for %d backups", len(backupManifests))
	}

	for i := range importSpans {
		importSpans[i].progressIdx = i
	}
	mu.requestsCompleted = make([]bool, len(importSpans))

	progressLogger := jobs.NewChunkProgressLogger(job, len(importSpans), job.FractionCompleted(),
		func(progressedCtx context.Context, details jobspb.ProgressDetails) {
			switch d := details.(type) {
			case *jobspb.Progress_Restore:
				mu.Lock()
				if mu.highWaterMark >= 0 {
					d.Restore.HighWater = importSpans[mu.highWaterMark].Key
				}
				mu.Unlock()
			default:
				log.Errorf(progressedCtx, "job payload had unexpected type %T", d)
			}
		})

	pkIDs := make(map[uint64]struct{})
	for _, tbl := range tables {
		pkIDs[roachpb.BulkOpSummaryID(uint64(tbl.ID), uint64(tbl.PrimaryIndex.ID))] = struct{}{}
	}

	// We're already limiting these on the server-side, but sending all the
	// Import requests at once would fill up distsender/grpc/something and cause
	// all sorts of badness (node liveness timeouts leading to mass leaseholder
	// transfers, poor performance on SQL workloads, etc) as well as log spam
	// about slow distsender requests. Rate limit them here, too.
	//
	// Use the number of cpus across all nodes in the cluster as the number of
	// outstanding Import requests for the rate limiting. Note that this assumes
	// all nodes in the cluster have the same number of cpus, but it's okay if
	// that's wrong.
	//
	// TODO(dan): Make this limiting per node.
	numClusterNodes := clusterNodeCount(gossip)
	maxConcurrentImports := numClusterNodes * runtime.NumCPU()
	importsSem := make(chan struct{}, maxConcurrentImports)

	g := ctxgroup.WithContext(restoreCtx)

	// The Import (and resulting AddSSTable) requests made below run on
	// leaseholders, so presplit and scatter the ranges to balance the work
	// among many nodes.
	//
	// We're about to start off some goroutines that presplit & scatter each
	// import span. Once split and scattered, the span is submitted to
	// readyForImportCh to indicate it's ready for Import. Since import is so
	// much slower, we buffer the channel to keep the split/scatter work from
	// getting too far ahead. This both naturally rate limits the split/scatters
	// and bounds the number of empty ranges created if the RESTORE fails (or is
	// canceled).
	const presplitLeadLimit = 10
	readyForImportCh := make(chan importEntry, presplitLeadLimit)
	g.GoCtx(func(ctx context.Context) error {
		defer close(readyForImportCh)
		return splitAndScatter(ctx, settings, db, kr, numClusterNodes, importSpans, readyForImportCh)
	})

	requestFinishedCh := make(chan struct{}, len(importSpans)) // enough buffer to never block
	g.GoCtx(func(ctx context.Context) error {
		ctx, progressSpan := tracing.ChildSpan(ctx, "progress-log")
		defer tracing.FinishSpan(progressSpan)
		return progressLogger.Loop(ctx, requestFinishedCh)
	})
	g.GoCtx(func(ctx context.Context) error {
		log.Eventf(restoreCtx, "commencing import of data with concurrency %d", maxConcurrentImports)
		for readyForImportSpan := range readyForImportCh {
			newSpanKey, err := rewriteBackupSpanKey(kr, readyForImportSpan.Span.Key)
			if err != nil {
				return err
			}
			idx := readyForImportSpan.progressIdx

			importRequest := &roachpb.ImportRequest{
				// Import is a point request because we don't want DistSender to split
				// it. Assume (but don't require) the entire post-rewrite span is on the
				// same range.
				RequestHeader: roachpb.RequestHeader{Key: newSpanKey},
				DataSpan:      readyForImportSpan.Span,
				Files:         readyForImportSpan.files,
				EndTime:       endTime,
				Rekeys:        rekeys,
				Encryption:    encryption,
			}

			log.VEventf(restoreCtx, 1, "importing %d of %d", idx, len(importSpans))

			select {
			case importsSem <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}

			g.GoCtx(func(ctx context.Context) error {
				ctx, importSpan := tracing.ChildSpan(ctx, "import")
				log.Event(ctx, "acquired semaphore")
				defer tracing.FinishSpan(importSpan)
				defer func() { <-importsSem }()

				importRes, pErr := kv.SendWrapped(ctx, db.NonTransactionalSender(), importRequest)
				if pErr != nil {
					return errors.Wrapf(pErr.GoError(), "importing span %v", importRequest.DataSpan)

				}

				mu.Lock()
				mu.res.add(countRows(importRes.(*roachpb.ImportResponse).Imported, pkIDs))

				// Assert that we're actually marking the correct span done. See #23977.
				if !importSpans[idx].Key.Equal(importRequest.DataSpan.Key) {
					mu.Unlock()
					return errors.Newf("request %d for span %v (to %v) does not match import span for same idx: %v",
						idx, importRequest.DataSpan, newSpanKey, importSpans[idx],
					)
				}
				mu.requestsCompleted[idx] = true
				for j := mu.highWaterMark + 1; j < len(mu.requestsCompleted) && mu.requestsCompleted[j]; j++ {
					mu.highWaterMark = j
				}
				mu.Unlock()

				requestFinishedCh <- struct{}{}
				return nil
			})
		}
		log.Event(restoreCtx, "wait for outstanding imports to finish")
		return nil
	})

	if err := g.Wait(); err != nil {
		// This leaves the data that did get imported in case the user wants to
		// retry.
		// TODO(dan): Build tooling to allow a user to restart a failed restore.
		return mu.res, errors.Wrapf(err, "importing %d ranges", len(importSpans))
	}

	return mu.res, nil
}

// loadBackupSQLDescs extracts the backup descriptors, the latest backup
// descriptor, and all the Descriptors for a backup to be restored. It upgrades
// the table descriptors to the new FK representation if necessary. FKs that
// can't be restored because the necessary tables are missing are omitted; if
// skip_missing_foreign_keys was set, we should have aborted the RESTORE and
// returned an error prior to this.
func loadBackupSQLDescs(
	ctx context.Context,
	details jobspb.RestoreDetails,
	makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
	encryption *roachpb.FileEncryptionOptions,
) ([]BackupManifest, BackupManifest, []sqlbase.Descriptor, error) {
	backupManifests, err := loadBackupManifests(ctx, details.URIs, makeExternalStorageFromURI, encryption)
	if err != nil {
		return nil, BackupManifest{}, nil, err
	}

	// Upgrade the table descriptors to use the new FK representation.
	// TODO(lucy, jordan): This should become unnecessary in 20.1 when we stop
	// writing old-style descs in RestoreDetails (unless a job persists across
	// an upgrade?).
	if err := maybeUpgradeTableDescsInBackupManifests(ctx, backupManifests, true /* skipFKsWithNoMatchingTable */); err != nil {
		return nil, BackupManifest{}, nil, err
	}

	allDescs, latestBackupManifest := loadSQLDescsFromBackupsAtTime(backupManifests, details.EndTime)

	var sqlDescs []sqlbase.Descriptor
	for _, desc := range allDescs {
		if _, ok := details.TableRewrites[desc.GetID()]; ok {
			sqlDescs = append(sqlDescs, desc)
		}
	}
	return backupManifests, latestBackupManifest, sqlDescs, nil
}

type restoreResumer struct {
	job                *jobs.Job
	settings           *cluster.Settings
	res                RowCount
	databases          []*sqlbase.DatabaseDescriptor
	tables             []*sqlbase.TableDescriptor
	descriptorCoverage tree.DescriptorCoverage
	latestStats        []*stats.TableStatisticProto
	execCfg            *sql.ExecutorConfig
}

// remapRelevantStatistics changes the table ID references in the stats
// from those they had in the backed up database to what they should be
// in the restored database.
// It also selects only the statistics which belong to one of the tables
// being restored. If the tableRewrites can re-write the table ID, then that
// table is being restored.
func remapRelevantStatistics(
	backup BackupManifest, tableRewrites TableRewriteMap,
) []*stats.TableStatisticProto {
	relevantTableStatistics := make([]*stats.TableStatisticProto, 0, len(backup.Statistics))

	for i := range backup.Statistics {
		stat := backup.Statistics[i]
		tableRewrite, ok := tableRewrites[stat.TableID]
		if !ok {
			// Table re-write not present, so statistic should not be imported.
			continue
		}
		stat.TableID = tableRewrite.TableID
		relevantTableStatistics = append(relevantTableStatistics, stat)
	}

	return relevantTableStatistics
}

// isDatabaseEmpty checks if there exists any tables in the given database.
// It pretends that the `ignoredTables` do not exist for the purposes of
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
	db *kv.DB,
	dbDesc *sql.DatabaseDescriptor,
	ignoredTables map[sqlbase.ID]struct{},
) (bool, error) {
	var allDescs []sqlbase.Descriptor
	if err := db.Txn(
		ctx,
		func(ctx context.Context, txn *kv.Txn) error {
			var err error
			allDescs, err = allSQLDescriptors(ctx, txn)
			return err
		}); err != nil {
		return false, err
	}

	for _, desc := range allDescs {
		if t := desc.Table(hlc.Timestamp{}); t != nil {
			if _, ok := ignoredTables[t.GetID()]; ok {
				continue
			}
			if t.GetParentID() == dbDesc.ID {
				return false, nil
			}
		}
	}
	return true, nil
}

// createImportingTables create the tables that we will restore into. It also
// fetches the information from the old tables that we need for the restore.
func createImportingTables(
	ctx context.Context, p sql.PlanHookState, sqlDescs []sqlbase.Descriptor, r *restoreResumer,
) (
	[]*sqlbase.DatabaseDescriptor,
	[]*sqlbase.TableDescriptor,
	[]sqlbase.ID,
	[]roachpb.Span,
	error,
) {
	details := r.job.Details().(jobspb.RestoreDetails)

	var databases []*sqlbase.DatabaseDescriptor
	var tables []*sqlbase.TableDescriptor
	var oldTableIDs []sqlbase.ID
	for _, desc := range sqlDescs {
		if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			tables = append(tables, tableDesc)
			oldTableIDs = append(oldTableIDs, tableDesc.ID)
		}
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			if rewrite, ok := details.TableRewrites[dbDesc.ID]; ok {
				dbDesc.ID = rewrite.TableID
				databases = append(databases, dbDesc)
			}
		}
	}
	var tempSystemDBID sqlbase.ID
	for id := range details.TableRewrites {
		if uint32(id) > uint32(tempSystemDBID) {
			tempSystemDBID = id
		}
	}
	if details.DescriptorCoverage == tree.AllDescriptors {
		databases = append(databases, &sqlbase.DatabaseDescriptor{
			ID:         tempSystemDBID,
			Name:       restoreTempSystemDB,
			Privileges: sqlbase.NewDefaultPrivilegeDescriptor(),
		})
	}

	// We get the spans of the restoring tables _as they appear in the backup_,
	// that is, in the 'old' keyspace, before we reassign the table IDs.
	spans := spansForAllTableIndexes(tables, nil)

	log.Eventf(ctx, "starting restore for %d tables", len(tables))

	// Assign new IDs and privileges to the tables, and update all references to
	// use the new IDs.
	if err := RewriteTableDescs(tables, details.TableRewrites, details.OverrideDB); err != nil {
		return nil, nil, nil, nil, err
	}

	for _, desc := range tables {
		desc.Version++
		desc.State = sqlbase.TableDescriptor_OFFLINE
		desc.OfflineReason = "restoring"
	}

	if !details.PrepareCompleted {
		err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Write the new TableDescriptors which are set in the OFFLINE state.
			if err := WriteTableDescs(ctx, txn, databases, tables, details.DescriptorCoverage, r.job.Payload().Username, r.settings, nil /* extra */); err != nil {
				return errors.Wrapf(err, "restoring %d TableDescriptors from %d databases", len(r.tables), len(databases))
			}

			details.PrepareCompleted = true
			details.TableDescs = tables

			// Update the job once all descs have been prepared for ingestion.
			err := r.job.WithTxn(txn).SetDetails(ctx, details)

			return err
		})
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}

	return databases, tables, oldTableIDs, spans, nil
}

// Resume is part of the jobs.Resumer interface.
func (r *restoreResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)
	p := phs.(sql.PlanHookState)

	backupManifests, latestBackupManifest, sqlDescs, err := loadBackupSQLDescs(
		ctx, details, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, details.Encryption,
	)
	if err != nil {
		return err
	}

	databases, tables, oldTableIDs, spans, err := createImportingTables(ctx, p, sqlDescs, r)
	if err != nil {
		return err
	}
	r.tables = tables
	r.descriptorCoverage = details.DescriptorCoverage
	r.databases = databases
	r.execCfg = p.ExecCfg()
	r.latestStats = remapRelevantStatistics(latestBackupManifest, details.TableRewrites)

	if len(r.tables) == 0 {
		// We have no tables to restore (we are restoring an empty DB).
		// Since we have already created any new databases that we needed,
		// we can return without importing any data.
		log.Warning(ctx, "no tables to restore")
		return nil
	}

	res, err := restore(
		ctx,
		p.ExecCfg().DB,
		p.ExecCfg().Gossip,
		p.ExecCfg().Settings,
		backupManifests,
		details.BackupLocalityInfo,
		details.EndTime,
		tables,
		oldTableIDs,
		spans,
		r.job,
		details.Encryption,
	)
	r.res = res
	if err != nil {
		return err
	}

	if err := r.insertStats(ctx); err != nil {
		return errors.Wrap(err, "inserting table statistics")
	}

	if err := r.publishTables(ctx); err != nil {
		return err
	}

	if r.descriptorCoverage == tree.AllDescriptors {
		if err := r.restoreSystemTables(ctx); err != nil {
			return err
		}
	}

	resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(*r.job.ID())),
		tree.NewDString(string(jobs.StatusSucceeded)),
		tree.NewDFloat(tree.DFloat(1.0)),
		tree.NewDInt(tree.DInt(r.res.Rows)),
		tree.NewDInt(tree.DInt(r.res.IndexEntries)),
		tree.NewDInt(tree.DInt(r.res.DataSize)),
	}

	return nil
}

// Insert stats re-inserts the table statistics stored in the backup manifest.
func (r *restoreResumer) insertStats(ctx context.Context) error {
	details := r.job.Details().(jobspb.RestoreDetails)
	if details.StatsInserted {
		return nil
	}

	err := r.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := stats.InsertNewStats(ctx, r.execCfg.InternalExecutor, txn, r.latestStats); err != nil {
			return errors.Wrapf(err, "inserting stats from backup")
		}
		details.StatsInserted = true
		if err := r.job.WithTxn(txn).SetDetails(ctx, details); err != nil {
			return errors.Wrapf(err, "updating job marking stats insertion complete")
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// publishTables updates the RESTORED tables status from OFFLINE to PUBLIC.
func (r *restoreResumer) publishTables(ctx context.Context) error {
	details := r.job.Details().(jobspb.RestoreDetails)
	if details.TablesPublished {
		return nil
	}
	log.Event(ctx, "making tables live")

	err := r.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Write the new TableDescriptors and flip state over to public so they can be
		// accessed.
		b := txn.NewBatch()
		for _, tbl := range r.tables {
			tableDesc := *tbl
			tableDesc.Version++
			tableDesc.State = sqlbase.TableDescriptor_PUBLIC
			existingDescVal, err := sqlbase.ConditionalGetTableDescFromTxn(ctx, txn, tbl)
			if err != nil {
				return errors.Wrap(err, "validating table descriptor has not changed")
			}
			b.CPut(
				sqlbase.MakeDescMetadataKey(tableDesc.ID),
				sqlbase.WrapDescriptor(&tableDesc),
				existingDescVal,
			)
		}

		if err := txn.Run(ctx, b); err != nil {
			return errors.Wrap(err, "publishing tables")
		}

		// Update and persist the state of the job.
		details.TablesPublished = true
		if err := r.job.WithTxn(txn).SetDetails(ctx, details); err != nil {
			return errors.Wrap(err, "updating job details after publishing tables")
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Initiate a run of CREATE STATISTICS. We don't know the actual number of
	// rows affected per table, so we use a large number because we want to make
	// sure that stats always get created/refreshed here.
	for i := range r.tables {
		r.execCfg.StatsRefresher.NotifyMutation(r.tables[i].ID, math.MaxInt32 /* rowsAffected */)
	}

	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes KV data that
// has been committed from a restore that has failed or been canceled. It does
// this by adding the table descriptors in DROP state, which causes the schema
// change stuff to delete the keys in the background.
func (r *restoreResumer) OnFailOrCancel(ctx context.Context, phs interface{}) error {
	return phs.(sql.PlanHookState).ExecCfg().DB.Txn(ctx, r.dropTables)
}

// dropTables implements the OnFailOrCancel logic.
func (r *restoreResumer) dropTables(ctx context.Context, txn *kv.Txn) error {
	details := r.job.Details().(jobspb.RestoreDetails)

	// No need to mark the tables as dropped if they were not even created in the
	// first place.
	if !details.PrepareCompleted {
		return nil
	}

	// Needed to trigger the schema change manager.
	if err := txn.SetSystemConfigTrigger(); err != nil {
		return err
	}

	b := txn.NewBatch()
	// Drop the table descriptors that were created at the start of the restore.
	for _, tbl := range details.TableDescs {
		tableDesc := *tbl
		tableDesc.Version++
		tableDesc.State = sqlbase.TableDescriptor_DROP
		err := sqlbase.RemovePublicTableNamespaceEntry(ctx, txn, tbl.ParentID, tbl.Name)
		if err != nil {
			return errors.Wrap(err, "dropping tables caused by restore fail/cancel from public namespace")
		}
		existingDescVal, err := sqlbase.ConditionalGetTableDescFromTxn(ctx, txn, tbl)
		if err != nil {
			return errors.Wrap(err, "dropping tables caused by restore fail/cancel")
		}
		b.CPut(
			sqlbase.MakeDescMetadataKey(tableDesc.ID),
			sqlbase.WrapDescriptor(&tableDesc),
			existingDescVal,
		)
	}

	// Drop the database descriptors that were created at the start of the
	// restore if they are now empty (i.e. no user created a table in this
	// database during the restore).
	var isDBEmpty bool
	var err error
	ignoredTables := make(map[sqlbase.ID]struct{})
	for _, table := range details.TableDescs {
		ignoredTables[table.ID] = struct{}{}
	}
	for _, dbDesc := range r.databases {
		// We need to ignore details.TableDescs since we haven't committed the txn that deletes these.
		isDBEmpty, err = isDatabaseEmpty(ctx, r.execCfg.DB, dbDesc, ignoredTables)
		if err != nil {
			return errors.Wrapf(err, "checking if database %s is empty during restore cleanup", dbDesc.Name)
		}

		if isDBEmpty {
			descKey := sqlbase.MakeDescMetadataKey(dbDesc.ID)
			b.Del(descKey)
			b.Del(sqlbase.NewDatabaseKey(dbDesc.Name).Key())
		}
	}
	if err := txn.Run(ctx, b); err != nil {
		return errors.Wrap(err, "dropping tables created at the start of restore caused by fail/cancel")
	}

	return nil
}

// restoreSystemTables atomically replaces the contents of the system tables
// with the data from the restored system tables.
func (r *restoreResumer) restoreSystemTables(ctx context.Context) error {
	executor := r.execCfg.InternalExecutor
	var err error
	for _, systemTable := range fullClusterSystemTables {
		systemTxn := r.execCfg.DB.NewTxn(ctx, "system-restore-txn")
		txnDebugName := fmt.Sprintf("restore-system-systemTable-%s", systemTable)
		// Don't clear the jobs table as to not delete the jobs that are performing
		// the restore.
		if systemTable != sqlbase.JobsTable.Name {
			deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true;", systemTable)
			_, err = executor.Exec(ctx, txnDebugName+"-data-deletion", systemTxn, deleteQuery)
			if err != nil {
				return errors.Wrapf(err, "restoring system.%s", systemTable)
			}
		}
		restoreQuery := fmt.Sprintf("INSERT INTO system.%s (SELECT * FROM %s.%s);", systemTable, restoreTempSystemDB, systemTable)
		_, err = executor.Exec(ctx, txnDebugName+"-data-insert", systemTxn, restoreQuery)
		if err != nil {
			return errors.Wrap(err, "restoring system tables")
		}
		err = systemTxn.Commit(ctx)
		if err != nil {
			return errors.Wrap(err, "committing system systemTable restoration")
		}
	}

	// After restoring the system tables, drop the temporary database holding the
	// system tables.
	dropTableQuery := fmt.Sprintf("DROP DATABASE %s CASCADE", restoreTempSystemDB)
	_, err = executor.Exec(ctx, "drop-temp-system-db" /* opName */, nil /* txn */, dropTableQuery)
	if err != nil {
		return errors.Wrap(err, "dropping temporary system db")
	}

	return nil
}

var _ jobs.Resumer = &restoreResumer{}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeRestore,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &restoreResumer{
				job:      job,
				settings: settings,
			}
		},
	)
}
