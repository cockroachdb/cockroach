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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	user string,
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
				conf, err := cloudimpl.ExternalStorageConfFromURI(uri, user)
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
			if err := db.AdminSplit(ctx, chunkKey, expirationTime); err != nil {
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
					if err := db.AdminSplit(ctx, newSpanKey, expirationTime); err != nil {
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

// WriteDescriptors writes all the the new descriptors: First the ID ->
// TableDescriptor for the new table, then flip (or initialize) the name -> ID
// entry so any new queries will use the new one. The tables are assigned the
// permissions of their parent database and the user must have CREATE permission
// on that database at the time this function is called.
func WriteDescriptors(
	ctx context.Context,
	txn *kv.Txn,
	databases []*sqlbase.ImmutableDatabaseDescriptor,
	tables []sqlbase.TableDescriptorInterface,
	types []sqlbase.TypeDescriptorInterface,
	descCoverage tree.DescriptorCoverage,
	settings *cluster.Settings,
	extra []roachpb.KeyValue,
) error {
	ctx, span := tracing.ChildSpan(ctx, "WriteDescriptors")
	defer tracing.FinishSpan(span)
	err := func() error {
		b := txn.NewBatch()
		wroteDBs := make(map[sqlbase.ID]*sqlbase.ImmutableDatabaseDescriptor)
		for _, desc := range databases {
			// If the restore is not a full cluster restore we cannot know that
			// the users on the restoring cluster match the ones that were on the
			// cluster that was backed up. So we wipe the privileges on the database.
			if descCoverage != tree.AllDescriptors {
				desc.Privileges = sqlbase.NewDefaultPrivilegeDescriptor()
			}
			wroteDBs[desc.GetID()] = desc
			if err := catalogkv.WriteNewDescToBatch(ctx, false /* kvTrace */, settings, b, keys.SystemSQLCodec, desc.GetID(), desc); err != nil {
				return err
			}
			// Depending on which cluster version we are restoring to, we decide which
			// namespace table to write the descriptor into. This may cause wrong
			// behavior if the cluster version is bumped DURING a restore.
			dKey := sqlbase.MakeDatabaseNameKey(ctx, settings, desc.GetName())
			b.CPut(dKey.Key(keys.SystemSQLCodec), desc.GetID(), nil)
		}
		for i := range tables {
			table := tables[i].TableDesc()
			// For full cluster restore, keep privileges as they were.
			if wrote, ok := wroteDBs[table.ParentID]; ok {
				// Leave the privileges of the temp system tables as
				// the default.
				if descCoverage != tree.AllDescriptors || wrote.GetName() == restoreTempSystemDB {
					table.Privileges = wrote.GetPrivileges()
				}
			} else {
				parentDB, err := sqlbase.GetDatabaseDescFromID(ctx, txn, keys.SystemSQLCodec, table.ParentID)
				if err != nil {
					return errors.Wrapf(err,
						"failed to lookup parent DB %d", errors.Safe(table.ParentID))
				}
				// We don't check priv's here since we checked them during job planning.

				// On full cluster restore, keep the privs as they are in the backup.
				if descCoverage != tree.AllDescriptors {
					// Default is to copy privs from restoring parent db, like CREATE TABLE.
					// TODO(dt): Make this more configurable.
					table.Privileges = parentDB.GetPrivileges()
				}
			}
			if err := catalogkv.WriteNewDescToBatch(ctx, false /* kvTrace */, settings, b, keys.SystemSQLCodec, table.ID, tables[i]); err != nil {
				return err
			}
			// Depending on which cluster version we are restoring to, we decide which
			// namespace table to write the descriptor into. This may cause wrong
			// behavior if the cluster version is bumped DURING a restore.
			tkey := sqlbase.MakePublicTableNameKey(ctx, settings, table.ParentID, table.Name)
			b.CPut(tkey.Key(keys.SystemSQLCodec), table.ID, nil)
		}

		// Write all type descriptors -- create namespace entries and write to
		// the system.descriptor table.
		for i := range types {
			typ := types[i].TypeDesc()
			if err := catalogkv.WriteNewDescToBatch(
				ctx,
				false, /* kvTrace */
				settings,
				b,
				keys.SystemSQLCodec,
				typ.ID,
				types[i],
			); err != nil {
				return err
			}
			tkey := sqlbase.MakePublicTableNameKey(ctx, settings, typ.ParentID, typ.Name)
			b.CPut(tkey.Key(keys.SystemSQLCodec), typ.ID, nil)
		}

		for _, kv := range extra {
			b.InitPut(kv.Key, &kv.Value, false)
		}
		if err := txn.Run(ctx, b); err != nil {
			if errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
				return pgerror.Newf(pgcode.DuplicateObject, "table already exists")
			}
			return err
		}

		for _, table := range tables {
			if err := table.TableDesc().Validate(ctx, txn, keys.SystemSQLCodec); err != nil {
				return errors.Wrapf(err,
					"validate table %d", errors.Safe(table.GetID()))
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
	if b, id, idx, err := keys.TODOSQLCodec.DecodeIndexPrefix(newKey); err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"could not rewrite span start key: %s", key)
	} else if idx == 1 && len(b) == 0 {
		newKey = keys.TODOSQLCodec.TablePrefix(id)
	}
	return newKey, nil
}

// restore imports a SQL table (or tables) from sets of non-overlapping sstable
// files.
func restore(
	restoreCtx context.Context,
	db *kv.DB,
	numClusterNodes int,
	settings *cluster.Settings,
	backupManifests []BackupManifest,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
	tables []sqlbase.TableDescriptorInterface,
	oldTableIDs []sqlbase.ID,
	spans []roachpb.Span,
	job *jobs.Job,
	encryption *roachpb.FileEncryptionOptions,
	user string,
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
		newDescBytes, err := protoutil.Marshal(tableToSerialize.DescriptorProto())
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
	importSpans, _, err := makeImportSpans(spans, backupManifests, backupLocalityInfo,
		highWaterMark, user, errOnMissingRange)
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

	pkIDs := make(map[uint64]bool)
	for _, tbl := range tables {
		pkIDs[roachpb.BulkOpSummaryID(uint64(tbl.GetID()), uint64(tbl.TableDesc().PrimaryIndex.ID))] = true
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
// TODO(anzoteh96): this method returns two things: backup manifests
// and the descriptors of the relevant manifests. Ideally, this should
// be broken down into two methods.
func loadBackupSQLDescs(
	ctx context.Context,
	p sql.PlanHookState,
	details jobspb.RestoreDetails,
	encryption *roachpb.FileEncryptionOptions,
) ([]BackupManifest, BackupManifest, []sqlbase.Descriptor, error) {
	backupManifests, err := loadBackupManifests(ctx, details.URIs,
		p.User(), p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, encryption)
	if err != nil {
		return nil, BackupManifest{}, nil, err
	}

	// Upgrade the table descriptors to use the new FK representation.
	// TODO(lucy, jordan): This should become unnecessary in 20.1 when we stop
	// writing old-style descs in RestoreDetails (unless a job persists across
	// an upgrade?).
	if err := maybeUpgradeTableDescsInBackupManifests(ctx, backupManifests, p.ExecCfg().Codec, true /* skipFKsWithNoMatchingTable */); err != nil {
		return nil, BackupManifest{}, nil, err
	}

	allDescs, latestBackupManifest := loadSQLDescsFromBackupsAtTime(backupManifests, details.EndTime)

	var sqlDescs []sqlbase.Descriptor
	for _, desc := range allDescs {
		if _, ok := details.DescriptorRewrites[desc.GetID()]; ok {
			sqlDescs = append(sqlDescs, desc)
		}
	}
	return backupManifests, latestBackupManifest, sqlDescs, nil
}

type restoreResumer struct {
	job                *jobs.Job
	settings           *cluster.Settings
	databases          []*sqlbase.ImmutableDatabaseDescriptor
	tables             []sqlbase.TableDescriptorInterface
	descriptorCoverage tree.DescriptorCoverage
	latestStats        []*stats.TableStatisticProto
	execCfg            *sql.ExecutorConfig

	testingKnobs struct {
		// duringSystemTableResotration is called once for every system table we
		// restore. It is used to simulate any errors that we may face at this point
		// of the restore.
		duringSystemTableRestoration func() error
	}
}

// getStatisticsFromBackup retrieves Statistics from backup manifest,
// either through the Statistics field or from the files.
func getStatisticsFromBackup(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	encryption *roachpb.FileEncryptionOptions,
	backup BackupManifest,
) ([]*stats.TableStatisticProto, error) {
	// This part deals with pre-20.2 stats format where backup statistics
	// are stored as a field in backup manifests instead of in their
	// individual files.
	if backup.DeprecatedStatistics != nil {
		return backup.DeprecatedStatistics, nil
	}
	tableStatistics := make([]*stats.TableStatisticProto, 0, len(backup.StatisticsFilenames))
	uniqueFileNames := make(map[string]struct{})
	for _, fname := range backup.StatisticsFilenames {
		if _, exists := uniqueFileNames[fname]; !exists {
			uniqueFileNames[fname] = struct{}{}
			myStatsTable, err := readTableStatistics(ctx, exportStore, fname, encryption)
			if err != nil {
				return tableStatistics, err
			}
			tableStatistics = append(tableStatistics, myStatsTable.Statistics...)
		}
	}

	return tableStatistics, nil
}

// remapRelevantStatistics changes the table ID references in the stats
// from those they had in the backed up database to what they should be
// in the restored database.
// It also selects only the statistics which belong to one of the tables
// being restored. If the descriptorRewrites can re-write the table ID, then that
// table is being restored.
func remapRelevantStatistics(
	tableStatistics []*stats.TableStatisticProto, descriptorRewrites DescRewriteMap,
) []*stats.TableStatisticProto {
	relevantTableStatistics := make([]*stats.TableStatisticProto, 0, len(tableStatistics))

	for _, stat := range tableStatistics {
		if tableRewrite, ok := descriptorRewrites[stat.TableID]; ok {
			// Statistics imported only when table re-write is present.
			stat.TableID = tableRewrite.ID
			relevantTableStatistics = append(relevantTableStatistics, stat)
		}
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
	dbDesc *sqlbase.ImmutableDatabaseDescriptor,
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
			if t.GetParentID() == dbDesc.GetID() {
				return false, nil
			}
		}
	}
	return true, nil
}

// createImportingDescriptors create the tables that we will restore into. It also
// fetches the information from the old tables that we need for the restore.
func createImportingDescriptors(
	ctx context.Context, p sql.PlanHookState, sqlDescs []sqlbase.Descriptor, r *restoreResumer,
) (
	[]*sqlbase.ImmutableDatabaseDescriptor,
	[]sqlbase.TableDescriptorInterface,
	[]sqlbase.ID,
	[]roachpb.Span,
	error,
) {
	details := r.job.Details().(jobspb.RestoreDetails)

	var databases []*sqlbase.ImmutableDatabaseDescriptor
	var tables []sqlbase.TableDescriptorInterface
	var types []sqlbase.TypeDescriptorInterface
	var oldTableIDs []sqlbase.ID
	for _, desc := range sqlDescs {
		if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			table := sqlbase.NewMutableCreatedTableDescriptor(*tableDesc)
			tables = append(tables, table)
			oldTableIDs = append(oldTableIDs, tableDesc.ID)
		}
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			if rewrite, ok := details.DescriptorRewrites[dbDesc.GetID()]; ok {
				rewriteDesc := sqlbase.NewInitialDatabaseDescriptorWithPrivileges(
					rewrite.ID, dbDesc.GetName(), dbDesc.Privileges)
				databases = append(databases, rewriteDesc)
			}
		}
		if typDesc := desc.GetType(); typDesc != nil {
			types = append(types, sqlbase.NewMutableCreatedTypeDescriptor(*typDesc))
		}
	}
	tempSystemDBID := keys.MinNonPredefinedUserDescID
	for id := range details.DescriptorRewrites {
		if int(id) > tempSystemDBID {
			tempSystemDBID = int(id)
		}
	}
	if details.DescriptorCoverage == tree.AllDescriptors {
		databases = append(databases, sqlbase.NewInitialDatabaseDescriptor(
			sqlbase.ID(tempSystemDBID), restoreTempSystemDB))
	}

	// We get the spans of the restoring tables _as they appear in the backup_,
	// that is, in the 'old' keyspace, before we reassign the table IDs.
	spans := spansForAllTableIndexes(p.ExecCfg().Codec, tables, nil)

	log.Eventf(ctx, "starting restore for %d tables", len(tables))

	// Assign new IDs and privileges to the tables, and update all references to
	// use the new IDs.
	tableDescs := make([]*sqlbase.TableDescriptor, len(tables))
	for i, table := range tables {
		tableDescs[i] = table.TableDesc()
	}
	if err := RewriteTableDescs(tableDescs, details.DescriptorRewrites, details.OverrideDB); err != nil {
		return nil, nil, nil, nil, err
	}

	// We might be remapping some types to existing types in the cluster. In that
	// case, we don't want to create namespace and descriptor entries for those
	// types. So collect only the types that we need to write here.
	var typesToWrite []sqlbase.TypeDescriptorInterface
	for i := range types {
		typ := types[i]
		if !details.DescriptorRewrites[typ.GetID()].ToExisting {
			typesToWrite = append(typesToWrite, typ)
		}
	}

	// Assign new IDs to all of the type descriptors that need to be written.
	typDescs := make([]*sqlbase.TypeDescriptor, len(typesToWrite))
	for i, typ := range typesToWrite {
		typDescs[i] = typ.TypeDesc()
	}
	if err := rewriteTypeDescs(typDescs, details.DescriptorRewrites); err != nil {
		return nil, nil, nil, nil, err
	}

	for _, desc := range tableDescs {
		desc.Version++
		desc.State = sqlbase.TableDescriptor_OFFLINE
		desc.OfflineReason = "restoring"
	}

	if !details.PrepareCompleted {
		err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Write the new TableDescriptors which are set in the OFFLINE state.
			if err := WriteDescriptors(ctx, txn, databases, tables, typesToWrite, details.DescriptorCoverage, r.settings, nil /* extra */); err != nil {
				return errors.Wrapf(err, "restoring %d TableDescriptors from %d databases", len(r.tables), len(databases))
			}

			details.PrepareCompleted = true
			details.TableDescs = tableDescs

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
		ctx, p, details, details.Encryption,
	)
	if err != nil {
		return err
	}
	lastBackupIndex, err := getBackupIndexAtTime(backupManifests, details.EndTime)
	if err != nil {
		return err
	}
	defaultConf, err := cloudimpl.ExternalStorageConfFromURI(details.URIs[lastBackupIndex], p.User())
	if err != nil {
		return errors.Wrapf(err, "creating external store configuration")
	}
	defaultStore, err := p.ExecCfg().DistSQLSrv.ExternalStorage(ctx, defaultConf)
	if err != nil {
		return err
	}

	databases, tables, oldTableIDs, spans, err := createImportingDescriptors(ctx, p, sqlDescs, r)
	if err != nil {
		return err
	}
	r.tables = tables
	r.descriptorCoverage = details.DescriptorCoverage
	r.databases = databases
	r.execCfg = p.ExecCfg()
	backupStats, err := getStatisticsFromBackup(ctx, defaultStore, details.Encryption, latestBackupManifest)
	if err != nil {
		return err
	}
	r.latestStats = remapRelevantStatistics(backupStats, details.DescriptorRewrites)

	if len(r.tables) == 0 {
		// We have no tables to restore (we are restoring an empty DB).
		// Since we have already created any new databases that we needed,
		// we can return without importing any data.
		log.Warning(ctx, "no tables to restore")
		return nil
	}

	numClusterNodes, err := clusterNodeCount(p.ExecCfg().Gossip)
	if err != nil {
		return err
	}

	res, err := restore(
		ctx,
		p.ExecCfg().DB,
		numClusterNodes,
		p.ExecCfg().Settings,
		backupManifests,
		details.BackupLocalityInfo,
		details.EndTime,
		tables,
		oldTableIDs,
		spans,
		r.job,
		details.Encryption,
		p.User(),
	)
	if err != nil {
		return err
	}

	if err := r.insertStats(ctx); err != nil {
		return errors.Wrap(err, "inserting table statistics")
	}

	if err := r.publishTables(ctx); err != nil {
		return err
	}

	// TODO(pbardea): This was part of the original design where full cluster
	// restores were a special case, but really we should be making only the
	// temporary system tables public before we restore all the system table data.
	if r.descriptorCoverage == tree.AllDescriptors {
		if err := r.restoreSystemTables(ctx, p.ExecCfg().DB); err != nil {
			return err
		}
	}

	resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(*r.job.ID())),
		tree.NewDString(string(jobs.StatusSucceeded)),
		tree.NewDFloat(tree.DFloat(1.0)),
		tree.NewDInt(tree.DInt(res.Rows)),
		tree.NewDInt(tree.DInt(res.IndexEntries)),
		tree.NewDInt(tree.DInt(res.DataSize)),
	}

	// Collect telemetry.
	{
		telemetry.Count("restore.total.succeeded")
		const mb = 1 << 20
		sizeMb := res.DataSize / mb
		sec := int64(timeutil.Since(timeutil.FromUnixMicros(r.job.Payload().StartedMicros)).Seconds())
		var mbps int64
		if sec > 0 {
			mbps = mb / sec
		}
		telemetry.CountBucketed("restore.duration-sec.succeeded", sec)
		telemetry.CountBucketed("restore.size-mb.full", sizeMb)
		telemetry.CountBucketed("restore.speed-mbps.total", mbps)
		telemetry.CountBucketed("restore.speed-mbps.per-node", mbps/int64(numClusterNodes))
		// Tiny restores may skew throughput numbers due to overhead.
		if sizeMb > 10 {
			telemetry.CountBucketed("restore.speed-mbps.over10mb", mbps)
			telemetry.CountBucketed("restore.speed-mbps.over10mb.per-node", mbps/int64(numClusterNodes))
		}
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

	newSchemaChangeJobs := make([]*jobs.StartableJob, 0)
	err := r.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Write the new TableDescriptors and flip state over to public so they can be
		// accessed.
		b := txn.NewBatch()
		newTables := make([]*sqlbase.TableDescriptor, 0, len(details.TableDescs))
		for _, tbl := range r.tables {
			newTableDesc := sqlbase.NewMutableExistingTableDescriptor(*tbl.TableDesc())
			newTableDesc.Version++
			newTableDesc.State = sqlbase.TableDescriptor_PUBLIC
			// Convert any mutations that were in progress on the table descriptor
			// when the backup was taken, and convert them to schema change jobs.
			newJobs, err := createSchemaChangeJobsFromMutations(ctx, r.execCfg.JobRegistry, r.execCfg.Codec, txn, r.job.Payload().Username, newTableDesc.TableDesc())
			if err != nil {
				return err
			}
			newSchemaChangeJobs = append(newSchemaChangeJobs, newJobs...)
			existingDescVal, err := sqlbase.ConditionalGetTableDescFromTxn(ctx, txn, r.execCfg.Codec, tbl.TableDesc())
			if err != nil {
				return errors.Wrap(err, "validating table descriptor has not changed")
			}
			b.CPut(
				sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, newTableDesc.ID),
				newTableDesc.DescriptorProto(),
				existingDescVal,
			)
			newTables = append(newTables, newTableDesc.TableDesc())
		}

		if err := txn.Run(ctx, b); err != nil {
			return errors.Wrap(err, "publishing tables")
		}

		// Update and persist the state of the job.
		details.TablesPublished = true
		details.TableDescs = newTables
		if err := r.job.WithTxn(txn).SetDetails(ctx, details); err != nil {
			for _, newJob := range newSchemaChangeJobs {
				if cleanupErr := newJob.CleanupOnRollback(ctx); cleanupErr != nil {
					log.Warningf(ctx, "failed to clean up job %d: %v", newJob.ID(), cleanupErr)
				}
			}
			return errors.Wrap(err, "updating job details after publishing tables")
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Start the schema change jobs we created.
	for _, newJob := range newSchemaChangeJobs {
		if _, err := newJob.Start(ctx); err != nil {
			return err
		}
	}

	// Initiate a run of CREATE STATISTICS. We don't know the actual number of
	// rows affected per table, so we use a large number because we want to make
	// sure that stats always get created/refreshed here.
	for i := range r.tables {
		r.execCfg.StatsRefresher.NotifyMutation(r.tables[i].GetID(), math.MaxInt32 /* rowsAffected */)
	}

	// TODO (rohany): Once types have type schema change jobs, we need to create
	//  jobs for pending type changes here.

	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes KV data that
// has been committed from a restore that has failed or been canceled. It does
// this by adding the table descriptors in DROP state, which causes the schema
// change stuff to delete the keys in the background.
func (r *restoreResumer) OnFailOrCancel(ctx context.Context, phs interface{}) error {
	telemetry.Count("restore.total.failed")
	telemetry.CountBucketed("restore.duration-sec.failed",
		int64(timeutil.Since(timeutil.FromUnixMicros(r.job.Payload().StartedMicros)).Seconds()))

	execCfg := phs.(sql.PlanHookState).ExecCfg()
	return execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return r.dropTables(ctx, execCfg.JobRegistry, txn)
	})
}

// dropTables implements the OnFailOrCancel logic.
// TODO (rohany): Needs to be updated for user defined types.
func (r *restoreResumer) dropTables(ctx context.Context, jr *jobs.Registry, txn *kv.Txn) error {
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
	tablesToGC := make([]sqlbase.ID, 0, len(details.TableDescs))
	for _, tbl := range details.TableDescs {
		tablesToGC = append(tablesToGC, tbl.ID)
		tableToDrop := sqlbase.NewMutableExistingTableDescriptor(*tbl)
		tableToDrop.Version++
		tableToDrop.State = sqlbase.TableDescriptor_DROP
		err := sqlbase.RemovePublicTableNamespaceEntry(ctx, txn, keys.SystemSQLCodec, tbl.ParentID, tbl.Name)
		if err != nil {
			return errors.Wrap(err, "dropping tables caused by restore fail/cancel from public namespace")
		}
		existingDescVal, err := sqlbase.ConditionalGetTableDescFromTxn(ctx, txn, r.execCfg.Codec, tbl)
		if err != nil {
			return errors.Wrap(err, "dropping tables caused by restore fail/cancel")
		}
		b.CPut(
			sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, tableToDrop.ID),
			tableToDrop.DescriptorProto(),
			existingDescVal,
		)
	}

	// Queue a GC job.
	// Set the drop time as 1 (ns in Unix time), so that the table gets GC'd
	// immediately.
	dropTime := int64(1)
	gcDetails := jobspb.SchemaChangeGCDetails{}
	for _, tableID := range tablesToGC {
		gcDetails.Tables = append(gcDetails.Tables, jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       tableID,
			DropTime: dropTime,
		})
	}
	gcJobRecord := jobs.Record{
		Description:   fmt.Sprintf("GC for %s", r.job.Payload().Description),
		Username:      r.job.Payload().Username,
		DescriptorIDs: tablesToGC,
		Details:       gcDetails,
		Progress:      jobspb.SchemaChangeGCProgress{},
		NonCancelable: true,
	}
	if _, err := jr.CreateJobWithTxn(ctx, gcJobRecord, txn); err != nil {
		return err
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
			return errors.Wrapf(err, "checking if database %s is empty during restore cleanup", dbDesc.GetName())
		}

		if isDBEmpty {
			descKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, dbDesc.GetID())
			b.Del(descKey)
			b.Del(sqlbase.NewDatabaseKey(dbDesc.GetName()).Key(keys.SystemSQLCodec))
		}
	}
	if err := txn.Run(ctx, b); err != nil {
		return errors.Wrap(err, "dropping tables created at the start of restore caused by fail/cancel")
	}

	return nil
}

// restoreSystemTables atomically replaces the contents of the system tables
// with the data from the restored system tables.
func (r *restoreResumer) restoreSystemTables(ctx context.Context, db *kv.DB) error {
	executor := r.execCfg.InternalExecutor
	var err error
	for _, systemTable := range fullClusterSystemTables {
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			txn.SetDebugName("system-restore-txn")
			stmtDebugName := fmt.Sprintf("restore-system-systemTable-%s", systemTable)
			// Don't clear the jobs table as to not delete the jobs that are performing
			// the restore.
			if systemTable != sqlbase.JobsTable.Name {
				deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true;", systemTable)
				_, err = executor.Exec(ctx, stmtDebugName+"-data-deletion", txn, deleteQuery)
				if err != nil {
					return errors.Wrapf(err, "deleting data from system.%s", systemTable)
				}
			}
			restoreQuery := fmt.Sprintf("INSERT INTO system.%s (SELECT * FROM %s.%s);", systemTable, restoreTempSystemDB, systemTable)
			if _, err := executor.Exec(ctx, stmtDebugName+"-data-insert", txn, restoreQuery); err != nil {
				return errors.Wrapf(err, "inserting data to system.%s", systemTable)
			}
			return nil
		}); err != nil {
			return err
		}

		if fn := r.testingKnobs.duringSystemTableRestoration; fn != nil {
			if err := fn(); err != nil {
				return err
			}
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
