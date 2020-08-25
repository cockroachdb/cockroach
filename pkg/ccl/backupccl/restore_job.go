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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
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
	user string,
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

// WriteDescriptors writes all the the new descriptors: First the ID ->
// TableDescriptor for the new table, then flip (or initialize) the name -> ID
// entry so any new queries will use the new one. The tables are assigned the
// permissions of their parent database and the user must have CREATE permission
// on that database at the time this function is called.
func WriteDescriptors(
	ctx context.Context,
	txn *kv.Txn,
	databases []catalog.DatabaseDescriptor,
	schemas []catalog.SchemaDescriptor,
	tables []catalog.TableDescriptor,
	types []catalog.TypeDescriptor,
	descCoverage tree.DescriptorCoverage,
	settings *cluster.Settings,
	extra []roachpb.KeyValue,
) error {
	ctx, span := tracing.ChildSpan(ctx, "WriteDescriptors")
	defer tracing.FinishSpan(span)
	err := func() error {
		b := txn.NewBatch()
		wroteDBs := make(map[descpb.ID]catalog.DatabaseDescriptor)
		for i := range databases {
			desc := databases[i]
			// If the restore is not a full cluster restore we cannot know that
			// the users on the restoring cluster match the ones that were on the
			// cluster that was backed up. So we wipe the privileges on the database.
			if descCoverage != tree.AllDescriptors {
				if mut, ok := desc.(*dbdesc.Mutable); ok {
					mut.Privileges = descpb.NewDefaultPrivilegeDescriptor(security.AdminRole)
				} else {
					log.Fatalf(ctx, "wrong type for table %d, %T, expected Mutable",
						desc.GetID(), desc)
				}
			}
			wroteDBs[desc.GetID()] = desc
			if err := catalogkv.WriteNewDescToBatch(ctx, false /* kvTrace */, settings, b, keys.SystemSQLCodec, desc.GetID(), desc); err != nil {
				return err
			}
			// Depending on which cluster version we are restoring to, we decide which
			// namespace table to write the descriptor into. This may cause wrong
			// behavior if the cluster version is bumped DURING a restore.
			dKey := catalogkv.MakeDatabaseNameKey(ctx, settings, desc.GetName())
			b.CPut(dKey.Key(keys.SystemSQLCodec), desc.GetID(), nil)
		}

		// Write namespace and descriptor entries for each schema.
		for i := range schemas {
			sc := schemas[i]
			if err := catalogkv.WriteNewDescToBatch(
				ctx,
				false, /* kvTrace */
				settings,
				b,
				keys.SystemSQLCodec,
				sc.GetID(),
				schemas[i],
			); err != nil {
				return err
			}
			skey := catalogkeys.NewSchemaKey(sc.GetParentID(), sc.GetName())
			b.CPut(skey.Key(keys.SystemSQLCodec), sc.GetID(), nil)
		}

		for i := range tables {
			table := tables[i]
			// For full cluster restore, keep privileges as they were.
			var updatedPrivileges *descpb.PrivilegeDescriptor
			if wrote, ok := wroteDBs[table.GetParentID()]; ok {
				// Leave the privileges of the temp system tables as
				// the default.
				if descCoverage != tree.AllDescriptors || wrote.GetName() == restoreTempSystemDB {
					updatedPrivileges = wrote.GetPrivileges()
				}
			} else {
				parentDB, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, keys.SystemSQLCodec, table.GetParentID())
				if err != nil {
					return errors.Wrapf(err,
						"failed to lookup parent DB %d", errors.Safe(table.GetParentID()))
				}
				// We don't check priv's here since we checked them during job planning.

				// On full cluster restore, keep the privs as they are in the backup.
				if descCoverage != tree.AllDescriptors {
					// Default is to copy privs from restoring parent db, like CREATE TABLE.
					// TODO(dt): Make this more configurable.
					updatedPrivileges = parentDB.GetPrivileges()
				}
			}
			if updatedPrivileges != nil {
				if mut, ok := table.(*tabledesc.Mutable); ok {
					mut.Privileges = updatedPrivileges
				} else {
					log.Fatalf(ctx, "wrong type for table %d, %T, expected Mutable",
						table.GetID(), table)
				}
			}
			if err := catalogkv.WriteNewDescToBatch(
				ctx, false /* kvTrace */, settings, b, keys.SystemSQLCodec, table.GetID(), tables[i],
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
			b.CPut(tkey.Key(keys.SystemSQLCodec), table.GetID(), nil)
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
			tkey := catalogkv.MakeObjectNameKey(ctx, settings, typ.GetParentID(), typ.GetParentSchemaID(), typ.GetName())
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
		dg := catalogkv.NewOneLevelUncachedDescGetter(txn, keys.SystemSQLCodec)
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
// Note that the actual restore process (i.e. inside ImportRequest) does not use
// these keys -- they are only used to split the key space and distribute those
// requests, thus truncation is fine. In the rare case where multiple backup
// spans are truncated to the same prefix (i.e. entire spans resided under the
// same interleave parent row) we'll generate some no-op splits and route the
// work to the same range, but the actual imported data is unaffected.
func rewriteBackupSpanKey(kr *storageccl.KeyRewriter, key roachpb.Key) (roachpb.Key, error) {
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
	phs sql.PlanHookState,
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
	user := phs.User()
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
		defer tracing.FinishSpan(progressSpan)
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

			// Signal that an ImportRequest finished to update job progress.
			requestFinishedCh <- struct{}{}
		}
		return nil
	})

	// TODO(pbardea): Improve logging in processors.
	if err := distRestore(
		restoreCtx,
		phs,
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
	p sql.PlanHookState,
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

type restoreResumer struct {
	job       *jobs.Job
	settings  *cluster.Settings
	databases []catalog.DatabaseDescriptor
	tables    []catalog.TableDescriptor
	// writtenTypes is the set of types that are restored from the backup into
	// the database. Note that this is not always the set of types within the
	// backup, as some types might be remapped to existing types in the database.
	writtenTypes       []catalog.TypeDescriptor
	descriptorCoverage tree.DescriptorCoverage
	latestStats        []*stats.TableStatisticProto
	execCfg            *sql.ExecutorConfig

	testingKnobs struct {
		// duringSystemTableRestoration is called once for every system table we
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
	dbDesc catalog.DatabaseDescriptor,
	ignoredTables map[descpb.ID]struct{},
) (bool, error) {
	var allDescs []catalog.Descriptor
	if err := db.Txn(
		ctx,
		func(ctx context.Context, txn *kv.Txn) error {
			var err error
			allDescs, err = catalogkv.GetAllDescriptors(ctx, txn, keys.SystemSQLCodec)
			return err
		}); err != nil {
		return false, err
	}

	for _, desc := range allDescs {
		if _, ok := ignoredTables[desc.GetID()]; ok {
			continue
		}
		if desc.GetParentID() == dbDesc.GetID() {
			return false, nil
		}
	}
	return true, nil
}

// createImportingDescriptors create the tables that we will restore into. It also
// fetches the information from the old tables that we need for the restore.
func createImportingDescriptors(
	ctx context.Context, p sql.PlanHookState, sqlDescs []catalog.Descriptor, r *restoreResumer,
) (
	databases []catalog.DatabaseDescriptor,
	tables []catalog.TableDescriptor,
	oldTableIDs []descpb.ID,
	writtenTypes []catalog.TypeDescriptor,
	spans []roachpb.Span,
	err error,
) {
	details := r.job.Details().(jobspb.RestoreDetails)

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
			schemas = append(schemas, schemadesc.NewMutableCreatedSchemaDescriptor(*desc.SchemaDesc()))
		case catalog.TypeDescriptor:
			types = append(types, typedesc.NewCreatedMutable(*desc.TypeDesc()))
		}
	}
	tempSystemDBID := keys.MinNonPredefinedUserDescID
	for id := range details.DescriptorRewrites {
		if int(id) > tempSystemDBID {
			tempSystemDBID = int(id)
		}
	}
	if details.DescriptorCoverage == tree.AllDescriptors {
		databases = append(databases, dbdesc.NewInitial(
			descpb.ID(tempSystemDBID), restoreTempSystemDB, security.AdminRole))
	}

	// We get the spans of the restoring tables _as they appear in the backup_,
	// that is, in the 'old' keyspace, before we reassign the table IDs.
	spans = spansForAllTableIndexes(p.ExecCfg().Codec, tables, nil)

	log.Eventf(ctx, "starting restore for %d tables", len(mutableTables))

	// Assign new IDs to the database descriptors.
	if err := rewriteDatabaseDescs(mutableDatabases, details.DescriptorRewrites); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// Assign new IDs and privileges to the tables, and update all references to
	// use the new IDs.
	tableDescs := make([]*descpb.TableDescriptor, len(mutableTables))
	for i, table := range mutableTables {
		tableDescs[i] = table.TableDesc()
	}
	if err := RewriteTableDescs(mutableTables, details.DescriptorRewrites, details.OverrideDB); err != nil {
		return nil, nil, nil, nil, nil, err
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
		return nil, nil, nil, nil, nil, err
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
		return nil, nil, nil, nil, nil, err
	}

	for _, desc := range tableDescs {
		desc.Version++
		desc.State = descpb.TableDescriptor_OFFLINE
		desc.OfflineReason = "restoring"
	}

	// Collect all types after they have had their ID's rewritten.
	typesByID := make(map[descpb.ID]catalog.TypeDescriptor)
	for i := range types {
		typesByID[types[i].GetID()] = types[i]
	}

	if !details.PrepareCompleted {
		err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if len(details.Tenants) > 0 {
				// TODO(dt): we need to set the system config trigger as the loop below
				// will make a batch that anchors the txn at '/Table/...', and setting the trigger
				// later (as we will have to) would fail.
				if err := txn.SetSystemConfigTrigger(p.ExecCfg().Codec.ForSystemTenant()); err != nil {
					return err
				}
			}
			// Write the new TableDescriptors which are set in the OFFLINE state.
			if err := WriteDescriptors(ctx, txn, databases, writtenSchemas, tables, writtenTypes, details.DescriptorCoverage, r.settings, nil /* extra */); err != nil {
				return errors.Wrapf(err, "restoring %d TableDescriptors from %d databases", len(r.tables), len(databases))
			}

			b := txn.NewBatch()

			// For new schemas with existing parent databases, the schema map on the
			// database descriptor needs to be updated.
			existingDBsWithNewSchemas := make(map[descpb.ID][]catalog.SchemaDescriptor)
			for _, sc := range writtenSchemas {
				parentID := sc.GetParentID()
				if _, ok := details.DescriptorRewrites[parentID]; !ok {
					existingDBsWithNewSchemas[parentID] = append(existingDBsWithNewSchemas[parentID], sc)
				}
			}
			// Write the updated databases.
			for dbID, schemas := range existingDBsWithNewSchemas {
				log.Infof(ctx, "writing %d schema entries to database %d", len(schemas), dbID)
				// TODO (lucy): Replace these direct descriptor reads from the store
				// with some interface backed by a descs.Collection.
				desc, err := catalogkv.GetDescriptorByID(ctx, txn, keys.SystemSQLCodec,
					dbID, catalogkv.Mutable, catalogkv.DatabaseDescriptorKind, true /* required */)
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
				// Note that since we're reading and writing these descriptors straight
				// from/to the store every time, MaybeIncrementVersion doesn't provide
				// any guarantees about incrementing the version exactly once in the
				// transaction.
				db.MaybeIncrementVersion()
				if err := catalogkv.WriteDescToBatch(
					ctx,
					false, /* kvTrace */
					p.ExecCfg().Settings,
					b,
					keys.SystemSQLCodec,
					db.ID,
					db,
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
					desc, err := catalogkv.GetDescriptorByID(ctx, txn, keys.SystemSQLCodec,
						id, catalogkv.Mutable, catalogkv.TypeDescriptorKind, true /* required */)
					if err != nil {
						return err
					}
					typDesc := desc.(*typedesc.Mutable)
					typDesc.AddReferencingDescriptorID(table.GetID())
					// TODO (lucy): I think we should be incrementing the version for the
					// types, but first we have to ensure that it only happens once per
					// transaction since we're going outside the normal mutable descriptor
					// resolution path. Also see the comment above in the case of
					// updated databases.
					if err := catalogkv.WriteDescToBatch(
						ctx,
						false, /* kvTrace */
						p.ExecCfg().Settings,
						b,
						keys.SystemSQLCodec,
						typDesc.ID,
						typDesc,
					); err != nil {
						return err
					}
				}
			}
			if err := txn.Run(ctx, b); err != nil {
				return err
			}

			for _, tenant := range details.Tenants {
				const inactive = false
				if err := sql.CreateTenantRecord(ctx, p.ExecCfg(), txn, tenant.ID, inactive, tenant.Info); err != nil {
					return err
				}
			}

			details.PrepareCompleted = true
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
			return nil, nil, nil, nil, nil, err
		}

		// Wait for one version on any existing changed types.
		for existing := range existingTypeIDs {
			if err := sql.WaitToUpdateLeases(ctx, p.ExecCfg().LeaseManager, existing); err != nil {
				return nil, nil, nil, nil, nil, err
			}
		}
	}

	return databases, tables, oldTableIDs, writtenTypes, spans, nil
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

	databases, tables, oldTableIDs, writtenTypes, spans, err := createImportingDescriptors(ctx, p, sqlDescs, r)
	if err != nil {
		return err
	}
	r.tables = tables
	r.writtenTypes = writtenTypes
	r.descriptorCoverage = details.DescriptorCoverage
	r.databases = databases
	r.execCfg = p.ExecCfg()
	backupStats, err := getStatisticsFromBackup(ctx, defaultStore, details.Encryption, latestBackupManifest)
	if err != nil {
		return err
	}
	r.latestStats = remapRelevantStatistics(backupStats, details.DescriptorRewrites)

	if len(r.tables) == 0 && len(details.Tenants) == 0 && len(r.writtenTypes) == 0 {
		// We have no tables to restore (we are restoring an empty DB).
		// Since we have already created any new databases that we needed,
		// we can return without importing any data.
		log.Warning(ctx, "nothing to restore")
		return nil
	}

	numClusterNodes, err := clusterNodeCount(p.ExecCfg().Gossip)
	if err != nil {
		return err
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

	if err := r.insertStats(ctx); err != nil {
		return errors.Wrap(err, "inserting table statistics")
	}

	if err := r.publishDescriptors(ctx); err != nil {
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

// publishDescriptors updates the RESTORED tables status from OFFLINE to PUBLIC.
func (r *restoreResumer) publishDescriptors(ctx context.Context) error {
	details := r.job.Details().(jobspb.RestoreDetails)
	if details.TablesPublished {
		return nil
	}
	log.Event(ctx, "making tables live")

	newDescriptorChangeJobs := make([]*jobs.StartableJob, 0)
	err := r.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if len(details.Tenants) > 0 {
			// TODO(dt): we need to set the system config trigger as the loop below
			// will make a batch that anchors the txn at '/Table/...', and setting the trigger
			// later (as we will have to) would fail.
			if err := txn.SetSystemConfigTrigger(r.execCfg.Codec.ForSystemTenant()); err != nil {
				return err
			}
		}
		// Write the new TableDescriptors and flip state over to public so they can be
		// accessed.
		b := txn.NewBatch()
		newTables := make([]*descpb.TableDescriptor, 0, len(details.TableDescs))
		for _, tbl := range r.tables {
			newTableDesc := tabledesc.NewExistingMutable(*tbl.TableDesc())
			newTableDesc.Version++
			newTableDesc.State = descpb.TableDescriptor_PUBLIC
			newTableDesc.OfflineReason = ""
			// Convert any mutations that were in progress on the table descriptor
			// when the backup was taken, and convert them to schema change jobs.
			newJobs, err := createSchemaChangeJobsFromMutations(ctx,
				r.execCfg.JobRegistry, r.execCfg.Codec, txn, r.job.Payload().Username, newTableDesc)
			if err != nil {
				return err
			}
			newDescriptorChangeJobs = append(newDescriptorChangeJobs, newJobs...)
			existingDescVal, err := catalogkv.ConditionalGetTableDescFromTxn(ctx, txn, r.execCfg.Codec, tbl)
			if err != nil {
				return errors.Wrap(err, "validating table descriptor has not changed")
			}
			b.CPut(
				catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, newTableDesc.ID),
				newTableDesc.DescriptorProto(),
				existingDescVal,
			)
			newTables = append(newTables, newTableDesc.TableDesc())
		}

		if err := txn.Run(ctx, b); err != nil {
			return errors.Wrap(err, "publishing tables")
		}

		// For all of the newly created types, make type schema change jobs for any
		// type descriptors that were backed up in the middle of a type schema change.
		for _, typ := range r.writtenTypes {
			if typ.HasPendingSchemaChanges() {
				typJob, err := createTypeChangeJobFromDesc(ctx, r.execCfg.JobRegistry, r.execCfg.Codec, txn, r.job.Payload().Username, typ)
				if err != nil {
					return err
				}
				newDescriptorChangeJobs = append(newDescriptorChangeJobs, typJob)
			}
		}

		for _, tenant := range details.Tenants {
			if err := sql.ActivateTenant(ctx, r.execCfg, txn, tenant.ID); err != nil {
				return err
			}
		}

		// Update and persist the state of the job.
		details.TablesPublished = true
		details.TableDescs = newTables
		if err := r.job.WithTxn(txn).SetDetails(ctx, details); err != nil {
			for _, newJob := range newDescriptorChangeJobs {
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
	for _, newJob := range newDescriptorChangeJobs {
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

	details := r.job.Details().(jobspb.RestoreDetails)

	execCfg := phs.(sql.PlanHookState).ExecCfg()
	return execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		for _, tenant := range details.Tenants {
			// TODO(dt): this is a noop since the tenant is already active=false but
			// that should be fixed in DestroyTenant.
			if err := sql.DestroyTenant(ctx, execCfg, txn, tenant.ID); err != nil {
				return err
			}
		}
		return r.dropDescriptors(ctx, execCfg.JobRegistry, txn)
	})
}

// dropDescriptors implements the OnFailOrCancel logic.
func (r *restoreResumer) dropDescriptors(
	ctx context.Context, jr *jobs.Registry, txn *kv.Txn,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)

	// No need to mark the tables as dropped if they were not even created in the
	// first place.
	if !details.PrepareCompleted {
		return nil
	}

	// Needed to trigger the schema change manager.
	if err := txn.SetSystemConfigTrigger(r.execCfg.Codec.ForSystemTenant()); err != nil {
		return err
	}

	b := txn.NewBatch()
	// Drop the table descriptors that were created at the start of the restore.
	tablesToGC := make([]descpb.ID, 0, len(details.TableDescs))
	for _, tbl := range details.TableDescs {
		tablesToGC = append(tablesToGC, tbl.ID)
		tableToDrop := tabledesc.NewExistingMutable(*tbl)
		prev := tableToDrop.ImmutableCopy().(catalog.TableDescriptor)
		tableToDrop.Version++
		tableToDrop.State = descpb.TableDescriptor_DROP
		err := catalogkv.RemovePublicTableNamespaceEntry(ctx, txn, keys.SystemSQLCodec, tbl.ParentID, tbl.Name)
		if err != nil {
			return errors.Wrap(err, "dropping tables caused by restore fail/cancel from public namespace")
		}
		existingDescVal, err := catalogkv.ConditionalGetTableDescFromTxn(ctx, txn, r.execCfg.Codec, prev)
		if err != nil {
			return errors.Wrap(err, "dropping tables caused by restore fail/cancel")
		}
		b.CPut(
			catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableToDrop.ID),
			tableToDrop.DescriptorProto(),
			existingDescVal,
		)
	}

	// Drop the type descriptors that this restore created.
	for i := range details.TypeDescs {
		// TypeDescriptors don't have a GC job process, so we can just write them
		// as dropped here.
		typDesc := details.TypeDescs[i]
		if err := catalogkv.RemoveObjectNamespaceEntry(
			ctx,
			txn,
			keys.SystemSQLCodec,
			typDesc.ParentID,
			typDesc.ParentSchemaID,
			typDesc.Name,
			false, /* kvTrace */
		); err != nil {
			return err
		}
		b.Del(catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, typDesc.ID))
	}

	// Drop any schema descriptors that this restore created. Also collect the
	// descriptors so we can update their parent databases later.
	dbsWithDeletedSchemas := make(map[descpb.ID][]*descpb.SchemaDescriptor)
	for i := range details.SchemaDescs {
		sc := details.SchemaDescs[i]
		if err := catalogkv.RemoveObjectNamespaceEntry(
			ctx,
			txn,
			keys.SystemSQLCodec,
			sc.ParentID,
			keys.RootNamespaceID,
			sc.Name,
			false, /* kvTrace */
		); err != nil {
			return err
		}
		b.Del(catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, sc.ID))
		dbsWithDeletedSchemas[sc.GetParentID()] = append(dbsWithDeletedSchemas[sc.GetParentID()], sc)
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
	ignoredTables := make(map[descpb.ID]struct{})
	for _, table := range details.TableDescs {
		ignoredTables[table.ID] = struct{}{}
	}
	deletedDBs := make(map[descpb.ID]struct{})
	for _, dbDesc := range r.databases {
		// We need to ignore details.TableDescs since we haven't committed the txn that deletes these.
		isDBEmpty, err = isDatabaseEmpty(ctx, r.execCfg.DB, dbDesc, ignoredTables)
		if err != nil {
			return errors.Wrapf(err, "checking if database %s is empty during restore cleanup", dbDesc.GetName())
		}

		if isDBEmpty {
			descKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, dbDesc.GetID())
			b.Del(descKey)
			b.Del(catalogkeys.NewDatabaseKey(dbDesc.GetName()).Key(keys.SystemSQLCodec))
			deletedDBs[dbDesc.GetID()] = struct{}{}
		}
	}

	// For each database that had a child schema deleted (regardless of whether
	// the db was created in the restore job), if it wasn't deleted just now,
	// delete the now-deleted child schema from its schema map.
	for dbID, schemas := range dbsWithDeletedSchemas {
		log.Infof(ctx, "deleting %d schema entries from database %d", len(schemas), dbID)
		// TODO (lucy): Replace these direct descriptor reads from the store
		// with some interface backed by a descs.Collection.
		desc, err := catalogkv.GetDescriptorByID(ctx, txn, keys.SystemSQLCodec,
			dbID, catalogkv.Mutable, catalogkv.DatabaseDescriptorKind, true /* required */)
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
		db.MaybeIncrementVersion()
		if err := catalogkv.WriteDescToBatch(
			ctx,
			false, /* kvTrace */
			r.settings,
			b,
			keys.SystemSQLCodec,
			db.ID,
			db,
		); err != nil {
			return err
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
			if systemTable != systemschema.JobsTable.Name {
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
