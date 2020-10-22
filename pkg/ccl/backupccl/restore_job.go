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

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
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
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
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
)

type importEntry struct {
	roachpb.Span
	entryType importEntryType

	// Only set if entryType is backupSpan
	start, end hlc.Timestamp

	// Only set if entryType is backupFile
	dir  roachpb.ExternalStorage
	file BackupManifest_File
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
	user security.SQLUsername,
	onMissing func(span covering.Range, start, end hlc.Timestamp) error,
) ([]execinfrapb.RestoreSpanEntry, hlc.Timestamp, error) {
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
	var requestEntries []execinfrapb.RestoreSpanEntry
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
			requestEntries = append(requestEntries, execinfrapb.RestoreSpanEntry{
				Span:  roachpb.Span{Key: importRange.Start, EndKey: importRange.End},
				Files: files,
			})
		}
	}
	return requestEntries, maxEndTime, nil
}

// WriteDescriptors writes all the new descriptors: First the ID ->
// TableDescriptor for the new table, then flip (or initialize) the name -> ID
// entry so any new queries will use the new one. The tables are assigned the
// permissions of their parent database and the user must have CREATE permission
// on that database at the time this function is called.
func WriteDescriptors(
	ctx context.Context,
	codec keys.SQLCodec,
	txn *kv.Txn,
	user security.SQLUsername,
	descsCol *descs.Collection,
	databases []catalog.DatabaseDescriptor,
	schemas []catalog.SchemaDescriptor,
	tables []catalog.TableDescriptor,
	types []catalog.TypeDescriptor,
	descCoverage tree.DescriptorCoverage,
	settings *cluster.Settings,
	extra []roachpb.KeyValue,
) error {
	ctx, span := tracing.ChildSpan(ctx, "WriteDescriptors")
	defer span.Finish()
	err := func() error {
		b := txn.NewBatch()
		wroteDBs := make(map[descpb.ID]catalog.DatabaseDescriptor)
		for i := range databases {
			desc := databases[i]
			updatedPrivileges, err := getRestoringPrivileges(ctx, codec, txn, desc, user, wroteDBs, descCoverage)
			if err != nil {
				return err
			}
			if updatedPrivileges != nil {
				if mut, ok := desc.(*dbdesc.Mutable); ok {
					mut.Privileges = updatedPrivileges
				} else {
					log.Fatalf(ctx, "wrong type for database %d, %T, expected Mutable",
						desc.GetID(), desc)
				}
			}
			wroteDBs[desc.GetID()] = desc
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, desc.(catalog.MutableDescriptor), b,
			); err != nil {
				return err
			}
			// Depending on which cluster version we are restoring to, we decide which
			// namespace table to write the descriptor into. This may cause wrong
			// behavior if the cluster version is bumped DURING a restore.
			dKey := catalogkv.MakeDatabaseNameKey(ctx, settings, desc.GetName())
			b.CPut(dKey.Key(codec), desc.GetID(), nil)
		}

		// Write namespace and descriptor entries for each schema.
		for i := range schemas {
			sc := schemas[i]
			updatedPrivileges, err := getRestoringPrivileges(ctx, codec, txn, sc, user, wroteDBs, descCoverage)
			if err != nil {
				return err
			}
			if updatedPrivileges != nil {
				if mut, ok := sc.(*schemadesc.Mutable); ok {
					mut.Privileges = updatedPrivileges
				} else {
					log.Fatalf(ctx, "wrong type for schema %d, %T, expected Mutable",
						sc.GetID(), sc)
				}
			}
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, sc.(catalog.MutableDescriptor), b,
			); err != nil {
				return err
			}
			skey := catalogkeys.NewSchemaKey(sc.GetParentID(), sc.GetName())
			b.CPut(skey.Key(codec), sc.GetID(), nil)
		}

		for i := range tables {
			table := tables[i]
			updatedPrivileges, err := getRestoringPrivileges(ctx, codec, txn, table, user, wroteDBs, descCoverage)
			if err != nil {
				return err
			}
			if updatedPrivileges != nil {
				if mut, ok := table.(*tabledesc.Mutable); ok {
					mut.Privileges = updatedPrivileges
				} else {
					log.Fatalf(ctx, "wrong type for table %d, %T, expected Mutable",
						table.GetID(), table)
				}
			}
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, tables[i].(catalog.MutableDescriptor), b,
			); err != nil {
				return err
			}
			// Depending on which cluster version we are restoring to, we decide which
			// namespace table to write the descriptor into. This may cause wrong
			// behavior if the cluster version is bumped DURING a restore.
			tkey := catalogkv.MakeObjectNameKey(
				ctx,
				settings,
				table.GetParentID(),
				table.GetParentSchemaID(),
				table.GetName(),
			)
			b.CPut(tkey.Key(codec), table.GetID(), nil)
		}

		// Write all type descriptors -- create namespace entries and write to
		// the system.descriptor table.
		for i := range types {
			typ := types[i]
			updatedPrivileges, err := getRestoringPrivileges(ctx, codec, txn, typ, user, wroteDBs, descCoverage)
			if err != nil {
				return err
			}
			if updatedPrivileges != nil {
				if mut, ok := typ.(*typedesc.Mutable); ok {
					mut.Privileges = updatedPrivileges
				} else {
					log.Fatalf(ctx, "wrong type for type %d, %T, expected Mutable",
						typ.GetID(), typ)
				}
			}
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, typ.(catalog.MutableDescriptor), b,
			); err != nil {
				return err
			}
			tkey := catalogkv.MakeObjectNameKey(ctx, settings, typ.GetParentID(), typ.GetParentSchemaID(), typ.GetName())
			b.CPut(tkey.Key(codec), typ.GetID(), nil)
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
		// TODO(ajwerner): Utilize validation inside of the descs.Collection
		// rather than reaching into the store.
		dg := catalogkv.NewOneLevelUncachedDescGetter(txn, codec)
		for _, table := range tables {
			if err := table.Validate(ctx, dg); err != nil {
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
// Note that the actual restore process (i.e the restore processor method which
// writes the KVs) does not use these keys -- they are only used to split the
// key space and distribute those requests, thus truncation is fine. In the rare
// case where multiple backup spans are truncated to the same prefix (i.e.
// entire spans resided under the same interleave parent row) we'll generate
// some no-op splits and route the work to the same range, but the actual
// imported data is unaffected.
func rewriteBackupSpanKey(
	codec keys.SQLCodec, kr *storageccl.KeyRewriter, key roachpb.Key,
) (roachpb.Key, error) {
	// TODO(dt): support rewriting tenant keys.
	if bytes.HasPrefix(key, keys.TenantPrefix) {
		return key, nil
	}

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
	if b, id, idx, err := codec.DecodeIndexPrefix(newKey); err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"could not rewrite span start key: %s", key)
	} else if idx == 1 && len(b) == 0 {
		newKey = codec.TablePrefix(id)
	}
	return newKey, nil
}

// restore imports a SQL table (or tables) from sets of non-overlapping sstable
// files.
func restore(
	restoreCtx context.Context,
	execCtx sql.JobExecContext,
	numClusterNodes int,
	backupManifests []BackupManifest,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
	tables []catalog.TableDescriptor,
	oldTableIDs []descpb.ID,
	spans []roachpb.Span,
	job *jobs.Job,
	encryption *jobspb.BackupEncryptionOptions,
) (RowCount, error) {
	user := execCtx.User()
	// A note about contexts and spans in this method: the top-level context
	// `restoreCtx` is used for orchestration logging. All operations that carry
	// out work get their individual contexts.
	emptyRowCount := RowCount{}

	// If there weren't any spans requested, then return early.
	if len(spans) == 0 {
		return emptyRowCount, nil
	}

	mu := struct {
		syncutil.Mutex
		highWaterMark     int
		res               RowCount
		requestsCompleted []bool
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

	// Pivot the backups, which are grouped by time, into requests for import,
	// which are grouped by keyrange.
	highWaterMark := job.Progress().Details.(*jobspb.Progress_Restore).Restore.HighWater
	importSpans, _, err := makeImportSpans(spans, backupManifests, backupLocalityInfo,
		highWaterMark, user, errOnMissingRange)
	if err != nil {
		return emptyRowCount, errors.Wrapf(err, "making import requests for %d backups", len(backupManifests))
	}

	for i := range importSpans {
		importSpans[i].ProgressIdx = int64(i)
	}
	mu.requestsCompleted = make([]bool, len(importSpans))

	progressLogger := jobs.NewChunkProgressLogger(job, len(importSpans), job.FractionCompleted(),
		func(progressedCtx context.Context, details jobspb.ProgressDetails) {
			switch d := details.(type) {
			case *jobspb.Progress_Restore:
				mu.Lock()
				if mu.highWaterMark >= 0 {
					d.Restore.HighWater = importSpans[mu.highWaterMark].Span.Key
				}
				mu.Unlock()
			default:
				log.Errorf(progressedCtx, "job payload had unexpected type %T", d)
			}
		})

	pkIDs := make(map[uint64]bool)
	for _, tbl := range tables {
		pkIDs[roachpb.BulkOpSummaryID(uint64(tbl.GetID()), uint64(tbl.GetPrimaryIndexID()))] = true
	}

	g := ctxgroup.WithContext(restoreCtx)

	// TODO(dan): This not super principled. I just wanted something that wasn't
	// a constant and grew slower than linear with the length of importSpans. It
	// seems to be working well for BenchmarkRestore2TB but worth revisiting.
	chunkSize := int(math.Sqrt(float64(len(importSpans))))
	importSpanChunks := make([][]execinfrapb.RestoreSpanEntry, 0, len(importSpans)/chunkSize)
	for start := 0; start < len(importSpans); {
		importSpanChunk := importSpans[start:]
		end := start + chunkSize
		if end < len(importSpans) {
			importSpanChunk = importSpans[start:end]
		}
		importSpanChunks = append(importSpanChunks, importSpanChunk)
		start = end
	}

	requestFinishedCh := make(chan struct{}, len(importSpans)) // enough buffer to never block
	g.GoCtx(func(ctx context.Context) error {
		ctx, progressSpan := tracing.ChildSpan(ctx, "progress-log")
		defer progressSpan.Finish()
		return progressLogger.Loop(ctx, requestFinishedCh)
	})

	progCh := make(chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)

	g.GoCtx(func(ctx context.Context) error {
		// When a processor is done importing a span, it will send a progress update
		// to progCh.
		for progress := range progCh {
			mu.Lock()
			var progDetails RestoreProgress
			if err := types.UnmarshalAny(&progress.ProgressDetails, &progDetails); err != nil {
				log.Errorf(ctx, "unable to unmarshal restore progress details: %+v", err)
			}

			mu.res.add(progDetails.Summary)
			idx := progDetails.ProgressIdx

			// Assert that we're actually marking the correct span done. See #23977.
			if !importSpans[progDetails.ProgressIdx].Span.Key.Equal(progDetails.DataSpan.Key) {
				mu.Unlock()
				return errors.Newf("request %d for span %v does not match import span for same idx: %v",
					idx, progDetails.DataSpan, importSpans[idx],
				)
			}
			mu.requestsCompleted[idx] = true
			for j := mu.highWaterMark + 1; j < len(mu.requestsCompleted) && mu.requestsCompleted[j]; j++ {
				mu.highWaterMark = j
			}
			mu.Unlock()

			// Signal that the processor has finished importing a span, to update job
			// progress.
			requestFinishedCh <- struct{}{}
		}
		return nil
	})

	// TODO(pbardea): Improve logging in processors.
	if err := distRestore(
		restoreCtx,
		execCtx,
		importSpanChunks,
		pkIDs,
		encryption,
		rekeys,
		endTime,
		progCh,
	); err != nil {
		return emptyRowCount, err
	}

	if err := g.Wait(); err != nil {
		// This leaves the data that did get imported in case the user wants to
		// retry.
		// TODO(dan): Build tooling to allow a user to restart a failed restore.
		return emptyRowCount, errors.Wrapf(err, "importing %d ranges", len(importSpans))
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
	p sql.JobExecContext,
	details jobspb.RestoreDetails,
	encryption *jobspb.BackupEncryptionOptions,
) ([]BackupManifest, BackupManifest, []catalog.Descriptor, error) {
	backupManifests, err := loadBackupManifests(ctx, details.URIs,
		p.User(), p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, encryption)
	if err != nil {
		return nil, BackupManifest{}, nil, err
	}

	// Upgrade the table descriptors to use the new FK representation.
	// TODO(lucy, jordan): This should become unnecessary in 20.1 when we stop
	// writing old-style descs in RestoreDetails (unless a job persists across
	// an upgrade?).
	if err := maybeUpgradeTableDescsInBackupManifests(ctx, backupManifests, true); err != nil {
		return nil, BackupManifest{}, nil, err
	}

	allDescs, latestBackupManifest := loadSQLDescsFromBackupsAtTime(backupManifests, details.EndTime)

	var sqlDescs []catalog.Descriptor
	for _, desc := range allDescs {
		id := desc.GetID()
		if _, ok := details.DescriptorRewrites[id]; ok {
			sqlDescs = append(sqlDescs, desc)
		}
	}
	return backupManifests, latestBackupManifest, sqlDescs, nil
}

// restoreResumer should only store a reference to the job it's running. State
// should not be stored here, but rather in the job details.
type restoreResumer struct {
	job *jobs.Job

	settings *cluster.Settings
	execCfg  *sql.ExecutorConfig

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
		duringSystemTableRestoration func() error
		// afterOfflineTableCreation is called after creating the OFFLINE table
		// descriptors we're ingesting. If an error is returned, we fail the
		// restore.
		afterOfflineTableCreation func() error
	}
}

// getStatisticsFromBackup retrieves Statistics from backup manifest,
// either through the Statistics field or from the files.
func getStatisticsFromBackup(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	encryption *jobspb.BackupEncryptionOptions,
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

func getTempSystemDBID(details jobspb.RestoreDetails) descpb.ID {
	tempSystemDBID := keys.MinNonPredefinedUserDescID
	for id := range details.DescriptorRewrites {
		if int(id) > tempSystemDBID {
			tempSystemDBID = int(id)
		}
	}

	return descpb.ID(tempSystemDBID)
}

// spansForAllRestoreTableIndexes returns non-overlapping spans for every index
// and table passed in. They would normally overlap if any of them are
// interleaved.
func spansForAllRestoreTableIndexes(
	codec keys.SQLCodec, tables []catalog.TableDescriptor, revs []BackupManifest_DescriptorRevision,
) []roachpb.Span {

	added := make(map[tableAndIndex]bool, len(tables))
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	for _, table := range tables {
		for _, index := range table.NonDropIndexes() {
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
		rawTbl := descpb.TableFromDescriptor(rev.Desc, hlc.Timestamp{})
		if rawTbl != nil && rawTbl.State != descpb.DescriptorState_DROP {
			tbl := tabledesc.NewImmutable(*rawTbl)
			for _, idx := range tbl.NonDropIndexes() {
				key := tableAndIndex{tableID: tbl.ID, indexID: idx.GetID()}
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
	return spans
}

// createImportingDescriptors create the tables that we will restore into. It also
// fetches the information from the old tables that we need for the restore.
func createImportingDescriptors(
	ctx context.Context, p sql.JobExecContext, sqlDescs []catalog.Descriptor, r *restoreResumer,
) (tables []catalog.TableDescriptor, oldTableIDs []descpb.ID, spans []roachpb.Span, err error) {
	details := r.job.Details().(jobspb.RestoreDetails)

	var databases []catalog.DatabaseDescriptor
	var writtenTypes []catalog.TypeDescriptor
	var schemas []*schemadesc.Mutable
	var types []*typedesc.Mutable
	// Store the tables as both the concrete mutable structs and the interface
	// to deal with the lack of slice covariance in go. We want the slice of
	// mutable descriptors for rewriting but ultimately want to return the
	// tables as the slice of interfaces.
	var mutableTables []*tabledesc.Mutable
	var mutableDatabases []*dbdesc.Mutable

	for _, desc := range sqlDescs {
		switch desc := desc.(type) {
		case catalog.TableDescriptor:
			mut := tabledesc.NewCreatedMutable(*desc.TableDesc())
			tables = append(tables, mut)
			mutableTables = append(mutableTables, mut)
			oldTableIDs = append(oldTableIDs, mut.GetID())
		case catalog.DatabaseDescriptor:
			if _, ok := details.DescriptorRewrites[desc.GetID()]; ok {
				mut := dbdesc.NewCreatedMutable(*desc.DatabaseDesc())
				databases = append(databases, mut)
				mutableDatabases = append(mutableDatabases, mut)
			}
		case catalog.SchemaDescriptor:
			mut := schemadesc.NewCreatedMutable(*desc.SchemaDesc())
			schemas = append(schemas, mut)
		case catalog.TypeDescriptor:
			mut := typedesc.NewCreatedMutable(*desc.TypeDesc())
			types = append(types, mut)
		}
	}

	if details.DescriptorCoverage == tree.AllDescriptors {
		tempSystemDBID := getTempSystemDBID(details)
		databases = append(databases, dbdesc.NewInitial(tempSystemDBID, restoreTempSystemDB,
			security.AdminRoleName()))
	}

	// We get the spans of the restoring tables _as they appear in the backup_,
	// that is, in the 'old' keyspace, before we reassign the table IDs.
	spans = spansForAllRestoreTableIndexes(p.ExecCfg().Codec, tables, nil)

	log.Eventf(ctx, "starting restore for %d tables", len(mutableTables))

	// Assign new IDs to the database descriptors.
	if err := rewriteDatabaseDescs(mutableDatabases, details.DescriptorRewrites); err != nil {
		return nil, nil, nil, err
	}
	databaseDescs := make([]*descpb.DatabaseDescriptor, len(mutableDatabases))
	for i, database := range mutableDatabases {
		databaseDescs[i] = database.DatabaseDesc()
	}

	// Assign new IDs and privileges to the tables, and update all references to
	// use the new IDs.
	if err := RewriteTableDescs(
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

	// Assign new IDs to all of the type descriptors that need to be written.
	if err := rewriteTypeDescs(typesToWrite, details.DescriptorRewrites); err != nil {
		return nil, nil, nil, err
	}

	// Collect all schemas that are going to be restored.
	var schemasToWrite []*schemadesc.Mutable
	var writtenSchemas []catalog.SchemaDescriptor
	for i := range schemas {
		sc := schemas[i]
		rw := details.DescriptorRewrites[sc.ID]
		if !rw.ToExisting {
			schemasToWrite = append(schemasToWrite, sc)
			writtenSchemas = append(writtenSchemas, sc)
		}
	}

	if err := rewriteSchemaDescs(schemasToWrite, details.DescriptorRewrites); err != nil {
		return nil, nil, nil, err
	}

	// Set the new descriptors' states to offline.
	for _, desc := range mutableTables {
		desc.SetOffline("restoring")
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

	// Collect all types after they have had their ID's rewritten.
	typesByID := make(map[descpb.ID]catalog.TypeDescriptor)
	for i := range types {
		typesByID[types[i].GetID()] = types[i]
	}

	// Collect all databases, for doing lookups of whether a database is new when
	// updating schema references later on.
	dbsByID := make(map[descpb.ID]catalog.DatabaseDescriptor)
	for i := range databases {
		dbsByID[databases[i].GetID()] = databases[i]
	}

	if !details.PrepareCompleted {
		err := descs.Txn(
			ctx, p.ExecCfg().Settings, p.ExecCfg().LeaseManager,
			p.ExecCfg().InternalExecutor, p.ExecCfg().DB, func(
				ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
			) error {
				// Write the new descriptors which are set in the OFFLINE state.
				if err := WriteDescriptors(
					ctx, p.ExecCfg().Codec, txn, p.User(), descsCol, databases, writtenSchemas, tables, writtenTypes,
					details.DescriptorCoverage, r.settings, nil, /* extra */
				); err != nil {
					return errors.Wrapf(err, "restoring %d TableDescriptors from %d databases", len(tables), len(databases))
				}

				b := txn.NewBatch()

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
					desc, err := descsCol.GetMutableDescriptorByID(ctx, dbID, txn)
					if err != nil {
						return err
					}
					db := desc.(*dbdesc.Mutable)
					if db.Schemas == nil {
						db.Schemas = make(map[string]descpb.DatabaseDescriptor_SchemaInfo)
					}
					for _, sc := range schemas {
						db.Schemas[sc.GetName()] = descpb.DatabaseDescriptor_SchemaInfo{ID: sc.GetID()}
					}
					if err := descsCol.WriteDescToBatch(
						ctx, false /* kvTrace */, db, b,
					); err != nil {
						return err
					}
				}

				// We could be restoring tables that point to existing types. We need to
				// ensure that those existing types are updated with back references pointing
				// to the new tables being restored.
				for _, table := range mutableTables {
					// Collect all types used by this table.
					typeIDs, err := table.GetAllReferencedTypeIDs(func(id descpb.ID) (catalog.TypeDescriptor, error) {
						return typesByID[id], nil
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
						typDesc, err := descsCol.GetMutableTypeVersionByID(ctx, txn, id)
						if err != nil {
							return err
						}
						typDesc.AddReferencingDescriptorID(table.GetID())
						if err := descsCol.WriteDescToBatch(
							ctx, false /* kvTrace */, typDesc, b,
						); err != nil {
							return err
						}
					}
				}
				if err := txn.Run(ctx, b); err != nil {
					return err
				}

				for _, tenant := range details.Tenants {
					// Mark the tenant info as adding.
					tenant.State = descpb.TenantInfo_ADD
					if err := sql.CreateTenantRecord(ctx, p.ExecCfg(), txn, &tenant); err != nil {
						return err
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

				// Update the job once all descs have been prepared for ingestion.
				err := r.job.WithTxn(txn).SetDetails(ctx, details)

				return err
			})
		if err != nil {
			return nil, nil, nil, err
		}

		// Wait for one version on any existing changed types.
		for existing := range existingTypeIDs {
			if err := sql.WaitToUpdateLeases(ctx, p.ExecCfg().LeaseManager, existing); err != nil {
				return nil, nil, nil, err
			}
		}
	}

	return tables, oldTableIDs, spans, nil
}

// Resume is part of the jobs.Resumer interface.
func (r *restoreResumer) Resume(
	ctx context.Context, execCtx interface{}, resultsCh chan<- tree.Datums,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)
	p := execCtx.(sql.JobExecContext)

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

	tables, oldTableIDs, spans, err := createImportingDescriptors(ctx, p, sqlDescs, r)
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
	r.execCfg = p.ExecCfg()
	backupStats, err := getStatisticsFromBackup(ctx, defaultStore, details.Encryption, latestBackupManifest)
	if err != nil {
		return err
	}
	latestStats := remapRelevantStatistics(backupStats, details.DescriptorRewrites)

	if len(details.TableDescs) == 0 && len(details.Tenants) == 0 && len(details.TypeDescs) == 0 {
		// We have no tables to restore (we are restoring an empty DB).
		// Since we have already created any new databases that we needed,
		// we can return without importing any data.
		log.Warning(ctx, "nothing to restore")
		// The database was created in the offline state and needs to be made
		// public.
		// TODO (lucy): Ideally we'd just create the database in the public state in
		// the first place, as a special case.
		var newDescriptorChangeJobs []*jobs.StartableJob
		publishDescriptors := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) (err error) {
			newDescriptorChangeJobs, err = r.publishDescriptors(ctx, txn, descsCol, details)
			return err
		}
		if err := descs.Txn(
			ctx, r.execCfg.Settings, r.execCfg.LeaseManager, r.execCfg.InternalExecutor,
			r.execCfg.DB, publishDescriptors,
		); err != nil {
			return err
		}
		// Start the schema change jobs we created.
		for _, newJob := range newDescriptorChangeJobs {
			if _, err := newJob.Start(ctx); err != nil {
				return err
			}
		}
		if fn := r.testingKnobs.afterPublishingDescriptors; fn != nil {
			if err := fn(); err != nil {
				return err
			}
		}
		return nil
	}

	numClusterNodes, err := clusterNodeCount(p.ExecCfg().Gossip)
	if err != nil {
		if !build.IsRelease() {
			return err
		}
		log.Warningf(ctx, "unable to determine cluster node count: %v", err)
		numClusterNodes = 1
	}

	for _, tenant := range details.Tenants {
		prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(tenant.ID))
		spans = append(spans, roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()})
	}

	res, err := restore(
		ctx,
		p,
		numClusterNodes,
		backupManifests,
		details.BackupLocalityInfo,
		details.EndTime,
		tables,
		oldTableIDs,
		spans,
		r.job,
		details.Encryption,
	)
	if err != nil {
		return err
	}

	if err := insertStats(ctx, r.job, p.ExecCfg(), latestStats); err != nil {
		return errors.Wrap(err, "inserting table statistics")
	}
	var newDescriptorChangeJobs []*jobs.StartableJob
	publishDescriptors := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) (err error) {
		newDescriptorChangeJobs, err = r.publishDescriptors(ctx, txn, descsCol, details)
		return err
	}
	if err := descs.Txn(
		ctx, r.execCfg.Settings, r.execCfg.LeaseManager, r.execCfg.InternalExecutor,
		r.execCfg.DB, publishDescriptors,
	); err != nil {
		return err
	}
	// Reload the details as we may have updated the job.
	details = r.job.Details().(jobspb.RestoreDetails)

	// Start the schema change jobs we created.
	for _, newJob := range newDescriptorChangeJobs {
		if _, err := newJob.Start(ctx); err != nil {
			return err
		}
	}
	if fn := r.testingKnobs.afterPublishingDescriptors; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}

	r.notifyStatsRefresherOfNewTables()

	// TODO(pbardea): This was part of the original design where full cluster
	// restores were a special case, but really we should be making only the
	// temporary system tables public before we restore all the system table data.
	if details.DescriptorCoverage == tree.AllDescriptors {
		if err := r.restoreSystemTables(ctx, p.ExecCfg().DB, details, tables); err != nil {
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

// Initiate a run of CREATE STATISTICS. We don't know the actual number of
// rows affected per table, so we use a large number because we want to make
// sure that stats always get created/refreshed here.
func (r *restoreResumer) notifyStatsRefresherOfNewTables() {
	details := r.job.Details().(jobspb.RestoreDetails)
	for i := range details.TableDescs {
		r.execCfg.StatsRefresher.NotifyMutation(details.TableDescs[i].GetID(), math.MaxInt32 /* rowsAffected */)
	}
}

// Insert stats re-inserts the table statistics stored in the backup manifest.
func insertStats(
	ctx context.Context,
	job *jobs.Job,
	execCfg *sql.ExecutorConfig,
	latestStats []*stats.TableStatisticProto,
) error {
	details := job.Details().(jobspb.RestoreDetails)
	if details.StatsInserted {
		return nil
	}

	err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := stats.InsertNewStats(ctx, execCfg.InternalExecutor, txn, latestStats); err != nil {
			return errors.Wrapf(err, "inserting stats from backup")
		}
		details.StatsInserted = true
		if err := job.WithTxn(txn).SetDetails(ctx, details); err != nil {
			return errors.Wrapf(err, "updating job marking stats insertion complete")
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// publishDescriptors updates the RESTORED descriptors' status from OFFLINE to
// PUBLIC. The schema change jobs are returned to be started after the
// transaction commits. The details struct is passed in rather than loaded
// from r.job as the call to r.job.SetDetails will overwrite the job details
// with a new value even if this transaction does not commit.
func (r *restoreResumer) publishDescriptors(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, details jobspb.RestoreDetails,
) (newDescriptorChangeJobs []*jobs.StartableJob, err error) {
	defer func() {
		if err == nil {
			return
		}
		for _, j := range newDescriptorChangeJobs {
			if cleanupErr := j.CleanupOnRollback(ctx); cleanupErr != nil {
				log.Warningf(ctx, "failed to clean up job %d: %v", j.ID(), cleanupErr)
			}
		}
		newDescriptorChangeJobs = nil
	}()
	if details.DescriptorsPublished {
		return nil, nil
	}
	if fn := r.testingKnobs.beforePublishingDescriptors; fn != nil {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	log.VEventf(ctx, 1, "making tables live")

	// Write the new descriptors and flip state over to public so they can be
	// accessed.
	allMutDescs := make([]catalog.MutableDescriptor, 0,
		len(details.TableDescs)+len(details.TypeDescs)+len(details.SchemaDescs)+len(details.DatabaseDescs))
	// Create slices of raw descriptors for the restore job details.
	newTables := make([]*descpb.TableDescriptor, 0, len(details.TableDescs))
	newTypes := make([]*descpb.TypeDescriptor, 0, len(details.TypeDescs))
	newSchemas := make([]*descpb.SchemaDescriptor, 0, len(details.SchemaDescs))
	newDBs := make([]*descpb.DatabaseDescriptor, 0, len(details.DatabaseDescs))
	checkVersion := func(read catalog.Descriptor, exp descpb.DescriptorVersion) error {
		if read.GetVersion() == exp {
			return nil
		}
		return errors.Errorf("version mismatch for descriptor %d, expected version %d, got %v",
			read.GetID(), read.GetVersion(), exp)
	}

	// Write the new TableDescriptors and flip state over to public so they can be
	// accessed.
	for _, tbl := range details.TableDescs {
		mutTable, err := descsCol.GetMutableTableVersionByID(ctx, tbl.GetID(), txn)
		if err != nil {
			return newDescriptorChangeJobs, err
		}
		if err := checkVersion(mutTable, tbl.Version); err != nil {
			return newDescriptorChangeJobs, err
		}
		allMutDescs = append(allMutDescs, mutTable)
		newTables = append(newTables, mutTable.TableDesc())
		// For cluster restores, all the jobs are restored directly from the jobs
		// table, so there is no need to re-create ongoing schema change jobs,
		// otherwise we'll create duplicate jobs.
		if details.DescriptorCoverage != tree.AllDescriptors {
			// Convert any mutations that were in progress on the table descriptor
			// when the backup was taken, and convert them to schema change jobs.
			newJobs, err := createSchemaChangeJobsFromMutations(ctx,
				r.execCfg.JobRegistry, r.execCfg.Codec, txn, r.job.Payload().UsernameProto.Decode(), mutTable)
			if err != nil {
				return newDescriptorChangeJobs, err
			}
			newDescriptorChangeJobs = append(newDescriptorChangeJobs, newJobs...)
		}
	}
	// For all of the newly created types, make type schema change jobs for any
	// type descriptors that were backed up in the middle of a type schema change.
	for _, typDesc := range details.TypeDescs {
		typ, err := descsCol.GetMutableTypeVersionByID(ctx, txn, typDesc.GetID())
		if err != nil {
			return newDescriptorChangeJobs, err
		}
		if err := checkVersion(typ, typDesc.Version); err != nil {
			return newDescriptorChangeJobs, err
		}
		allMutDescs = append(allMutDescs, typ)
		newTypes = append(newTypes, typ.TypeDesc())
		if typ.HasPendingSchemaChanges() && details.DescriptorCoverage != tree.AllDescriptors {
			typJob, err := createTypeChangeJobFromDesc(ctx, r.execCfg.JobRegistry, r.execCfg.Codec, txn, r.job.Payload().UsernameProto.Decode(), typ)
			if err != nil {
				return newDescriptorChangeJobs, err
			}
			newDescriptorChangeJobs = append(newDescriptorChangeJobs, typJob)
		}
	}
	for _, sc := range details.SchemaDescs {
		mutDesc, err := descsCol.GetMutableDescriptorByID(ctx, sc.ID, txn)
		if err != nil {
			return newDescriptorChangeJobs, err
		}
		if err := checkVersion(mutDesc, sc.Version); err != nil {
			return newDescriptorChangeJobs, err
		}
		mutSchema := mutDesc.(*schemadesc.Mutable)
		allMutDescs = append(allMutDescs, mutSchema)
		newSchemas = append(newSchemas, mutSchema.SchemaDesc())
	}
	for _, dbDesc := range details.DatabaseDescs {
		// Jobs started before 20.2 upgrade finalization don't put databases in
		// an offline state.
		// TODO(lucy): Should we make this more explicit with a format version
		// field in the details?
		mutDesc, err := descsCol.GetMutableDescriptorByID(ctx, dbDesc.ID, txn)
		if err != nil {
			return newDescriptorChangeJobs, err
		}
		if err := checkVersion(mutDesc, dbDesc.Version); err != nil {
			return newDescriptorChangeJobs, err
		}
		mutDB := mutDesc.(*dbdesc.Mutable)
		// TODO(lucy,ajwerner): Remove this in 21.1.
		if mutDB.GetState() != descpb.DescriptorState_OFFLINE {
			newDBs = append(newDBs, dbDesc)
		} else {
			allMutDescs = append(allMutDescs, mutDB)
			newDBs = append(newDBs, mutDB.DatabaseDesc())
		}
	}
	b := txn.NewBatch()
	for _, desc := range allMutDescs {
		desc.SetPublic()
		if err := descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, desc, b,
		); err != nil {
			return newDescriptorChangeJobs, err
		}
	}

	if err := txn.Run(ctx, b); err != nil {
		return newDescriptorChangeJobs, errors.Wrap(err, "publishing tables")
	}

	for _, tenant := range details.Tenants {
		if err := sql.ActivateTenant(ctx, r.execCfg, txn, tenant.ID); err != nil {
			return newDescriptorChangeJobs, err
		}
	}

	// Update and persist the state of the job.
	details.DescriptorsPublished = true
	details.TableDescs = newTables
	details.TypeDescs = newTypes
	details.SchemaDescs = newSchemas
	details.DatabaseDescs = newDBs
	if err := r.job.WithTxn(txn).SetDetails(ctx, details); err != nil {
		return newDescriptorChangeJobs, errors.Wrap(err,
			"updating job details after publishing tables")
	}
	r.job.WithTxn(nil)

	return newDescriptorChangeJobs, nil
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes KV data that
// has been committed from a restore that has failed or been canceled. It does
// this by adding the table descriptors in DROP state, which causes the schema
// change stuff to delete the keys in the background.
func (r *restoreResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	telemetry.Count("restore.total.failed")
	telemetry.CountBucketed("restore.duration-sec.failed",
		int64(timeutil.Since(timeutil.FromUnixMicros(r.job.Payload().StartedMicros)).Seconds()))

	details := r.job.Details().(jobspb.RestoreDetails)

	execCfg := execCtx.(sql.JobExecContext).ExecCfg()
	return descs.Txn(ctx, execCfg.Settings, execCfg.LeaseManager, execCfg.InternalExecutor,
		execCfg.DB, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			for _, tenant := range details.Tenants {
				tenant.State = descpb.TenantInfo_DROP
				// This is already a job so no need to spin up a gc job for the tenant;
				// instead just GC the data eagerly.
				if err := sql.GCTenantSync(ctx, execCfg, &tenant); err != nil {
					return err
				}
			}
			return r.dropDescriptors(ctx, execCfg.JobRegistry, execCfg.Codec, txn, descsCol)
		})
}

// dropDescriptors implements the OnFailOrCancel logic.
// TODO (lucy): If the descriptors have already been published, we need to queue
// drop jobs for all the descriptors.
func (r *restoreResumer) dropDescriptors(
	ctx context.Context,
	jr *jobs.Registry,
	codec keys.SQLCodec,
	txn *kv.Txn,
	descsCol *descs.Collection,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)

	// No need to mark the tables as dropped if they were not even created in the
	// first place.
	if !details.PrepareCompleted {
		return nil
	}

	b := txn.NewBatch()

	// Collect the tables into mutable versions.
	mutableTables := make([]*tabledesc.Mutable, len(details.TableDescs))
	for i := range details.TableDescs {
		var err error
		mutableTables[i], err = descsCol.GetMutableTableVersionByID(ctx, details.TableDescs[i].ID, txn)
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
					"drop: got %d, expected %d", mutableTables[i].GetVersion(), got, exp)
			}
		}

	}

	// Remove any back references installed from existing types to tables being restored.
	if err := r.removeExistingTypeBackReferences(
		ctx, txn, descsCol, b, mutableTables, &details,
	); err != nil {
		return err
	}

	// Drop the table descriptors that were created at the start of the restore.
	tablesToGC := make([]descpb.ID, 0, len(details.TableDescs))
	for i := range mutableTables {
		tableToDrop := mutableTables[i]
		tablesToGC = append(tablesToGC, tableToDrop.ID)
		tableToDrop.State = descpb.DescriptorState_DROP
		catalogkv.WriteObjectNamespaceEntryRemovalToBatch(
			ctx,
			b,
			codec,
			tableToDrop.ParentID,
			tableToDrop.GetParentSchemaID(),
			tableToDrop.Name,
			false, /* kvTrace */
		)
		if err := descsCol.WriteDescToBatch(ctx, false /* kvTrace */, tableToDrop, b); err != nil {
			return errors.Wrap(err, "writing dropping table to batch")
		}
	}

	// Drop the type descriptors that this restore created.
	for i := range details.TypeDescs {
		// TypeDescriptors don't have a GC job process, so we can just write them
		// as dropped here.
		typDesc := details.TypeDescs[i]
		catalogkv.WriteObjectNamespaceEntryRemovalToBatch(
			ctx,
			b,
			codec,
			typDesc.ParentID,
			typDesc.ParentSchemaID,
			typDesc.Name,
			false, /* kvTrace */
		)
		b.Del(catalogkeys.MakeDescMetadataKey(codec, typDesc.ID))
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
		Username:      r.job.Payload().UsernameProto.Decode(),
		DescriptorIDs: tablesToGC,
		Details:       gcDetails,
		Progress:      jobspb.SchemaChangeGCProgress{},
		NonCancelable: true,
	}
	if _, err := jr.CreateJobWithTxn(ctx, gcJobRecord, txn); err != nil {
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
	allDescs, err := descsCol.GetAllDescriptors(ctx, txn)
	if err != nil {
		return err
	}

	// Delete any schema descriptors that this restore created. Also collect the
	// descriptors so we can update their parent databases later.
	dbsWithDeletedSchemas := make(map[descpb.ID][]*descpb.SchemaDescriptor)
	for _, schemaDesc := range details.SchemaDescs {
		sc := schemadesc.NewMutableExisting(*schemaDesc)
		// We need to ignore descriptors we just added since we haven't committed the txn that deletes these.
		isSchemaEmpty, err := isSchemaEmpty(ctx, txn, sc.GetID(), allDescs, ignoredChildDescIDs)
		if err != nil {
			return errors.Wrapf(err, "checking if schema %s is empty during restore cleanup", sc.GetName())
		}

		if !isSchemaEmpty {
			log.Warningf(ctx, "preserving schema %s on restore failure because it contains new child objects", sc.GetName())
			continue
		}
		catalogkv.WriteObjectNamespaceEntryRemovalToBatch(
			ctx,
			b,
			codec,
			sc.ParentID,
			keys.RootNamespaceID,
			sc.Name,
			false, /* kvTrace */
		)
		b.Del(catalogkeys.MakeDescMetadataKey(codec, sc.ID))
		dbsWithDeletedSchemas[sc.GetParentID()] = append(dbsWithDeletedSchemas[sc.GetParentID()], sc.SchemaDesc())
	}

	// Delete the database descriptors.
	deletedDBs := make(map[descpb.ID]struct{})
	for _, dbDesc := range details.DatabaseDescs {
		db := dbdesc.NewExistingMutable(*dbDesc)
		// We need to ignore descriptors we just added since we haven't committed the txn that deletes these.
		isDBEmpty, err := isDatabaseEmpty(ctx, txn, db.GetID(), allDescs, ignoredChildDescIDs)
		if err != nil {
			return errors.Wrapf(err, "checking if database %s is empty during restore cleanup", db.GetName())
		}

		if !isDBEmpty {
			log.Warningf(ctx, "preserving database %s on restore failure because it contains new child objects or schemas", db.GetName())
			continue
		}
		descKey := catalogkeys.MakeDescMetadataKey(codec, db.GetID())
		b.Del(descKey)
		b.Del(catalogkeys.NewDatabaseKey(db.GetName()).Key(codec))
		deletedDBs[db.GetID()] = struct{}{}
	}

	// For each database that had a child schema deleted (regardless of whether
	// the db was created in the restore job), if it wasn't deleted just now,
	// delete the now-deleted child schema from its schema map.
	for dbID, schemas := range dbsWithDeletedSchemas {
		log.Infof(ctx, "deleting %d schema entries from database %d", len(schemas), dbID)
		desc, err := descsCol.GetMutableDescriptorByID(ctx, dbID, txn)
		if err != nil {
			return err
		}
		db := desc.(*dbdesc.Mutable)
		for _, sc := range schemas {
			if schemaInfo, ok := db.Schemas[sc.GetName()]; !ok {
				log.Warningf(ctx, "unexpected missing schema entry for %s from db %d; skipping deletion",
					sc.GetName(), dbID)
			} else if schemaInfo.ID != sc.GetID() {
				log.Warningf(ctx, "unexpected schema entry %d for %s from db %d, expecting %d; skipping deletion",
					schemaInfo.ID, sc.GetName(), dbID, sc.GetID())
			} else {
				delete(db.Schemas, sc.GetName())
			}
		}
		if err := descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, db, b,
		); err != nil {
			return err
		}
	}

	if err := txn.Run(ctx, b); err != nil {
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
	restoredTypes := make(map[descpb.ID]*typedesc.Immutable)
	existingTypes := make(map[descpb.ID]*typedesc.Mutable)
	for i := range details.TypeDescs {
		typ := details.TypeDescs[i]
		restoredTypes[typ.ID] = typedesc.NewImmutable(*typ)
	}
	for _, tbl := range restoredTables {
		lookup := func(id descpb.ID) (catalog.TypeDescriptor, error) {
			// First see if the type was restored.
			restored, ok := restoredTypes[id]
			if ok {
				return restored, nil
			}
			// Finally, look it up using the transaction.
			typ, err := descsCol.GetMutableTypeVersionByID(ctx, txn, id)
			if err != nil {
				return nil, err
			}
			existingTypes[typ.GetID()] = typ
			return typ, nil
		}

		// Get all types that this descriptor references.
		referencedTypes, err := tbl.GetAllReferencedTypeIDs(lookup)
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

func getRestoringPrivileges(
	ctx context.Context,
	codec keys.SQLCodec,
	txn *kv.Txn,
	desc catalog.Descriptor,
	user security.SQLUsername,
	wroteDBs map[descpb.ID]catalog.DatabaseDescriptor,
	descCoverage tree.DescriptorCoverage,
) (*descpb.PrivilegeDescriptor, error) {
	// Don't update the privileges of descriptors if we're doing a cluster
	// restore.
	if descCoverage == tree.AllDescriptors {
		return nil, nil
	}

	var updatedPrivileges *descpb.PrivilegeDescriptor

	switch desc := desc.(type) {
	case catalog.TableDescriptor, catalog.SchemaDescriptor:
		if wrote, ok := wroteDBs[desc.GetParentID()]; ok {
			// If we're creating a new database in this restore, the privileges of the
			// table and schema should be that of the parent DB.
			//
			// Leave the privileges of the temp system tables as the default too.
			if descCoverage != tree.AllDescriptors || wrote.GetName() == restoreTempSystemDB {
				updatedPrivileges = wrote.GetPrivileges()
			}
		} else {
			parentDB, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, codec, desc.GetParentID())
			if err != nil {
				return nil, errors.Wrapf(err,
					"failed to lookup parent DB %d", errors.Safe(desc.GetParentID()))
			}

			// Default is to copy privs from restoring parent db, like CREATE {TABLE,
			// SCHEMA}. But also like CREATE {TABLE,SCHEMA}, we set the owner to the
			// user creating the table (the one running the restore).
			// TODO(dt): Make this more configurable.
			updatedPrivileges = sql.CreateInheritedPrivilegesFromDBDesc(parentDB, user)
		}
	case catalog.TypeDescriptor, catalog.DatabaseDescriptor:
		// If the restore is not a cluster restore we cannot know that the users on
		// the restoring cluster match the ones that were on the cluster that was
		// backed up. So we wipe the privileges on the type/database.
		updatedPrivileges = descpb.NewDefaultPrivilegeDescriptor(user)
	}
	return updatedPrivileges, nil
}

// restoreSystemTables atomically replaces the contents of the system tables
// with the data from the restored system tables.
func (r *restoreResumer) restoreSystemTables(
	ctx context.Context,
	db *kv.DB,
	restoreDetails jobspb.RestoreDetails,
	tables []catalog.TableDescriptor,
) error {
	tempSystemDBID := getTempSystemDBID(restoreDetails)

	executor := r.execCfg.InternalExecutor
	var err error

	// Iterate through all the tables that we're restoring, and if it was restored
	// to the temporary system DB then copy it's data over to the real system
	// table.
	for _, table := range tables {
		if table.GetParentID() != tempSystemDBID {
			continue
		}
		systemTableName := table.GetName()

		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			txn.SetDebugName("system-restore-txn")
			config, ok := systemTableBackupConfiguration[systemTableName]
			if !ok {
				log.Warningf(ctx, "no configuration specified for table %s... skipping restoration",
					systemTableName)
			}

			restoreFunc := defaultSystemTableRestoreFunc
			if config.customRestoreFunc != nil {
				restoreFunc = config.customRestoreFunc
				log.Eventf(ctx, "using custom restore function for table %s", systemTableName)
			}

			log.Eventf(ctx, "restoring system table %s", systemTableName)
			err := restoreFunc(ctx, r.execCfg, txn, systemTableName, restoreTempSystemDB+"."+systemTableName)
			if err != nil {
				return errors.Wrapf(err, "restoring system table %s", systemTableName)
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
