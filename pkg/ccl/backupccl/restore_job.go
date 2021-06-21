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
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
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
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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
	backupLocalityMap map[int]storeByLocalityKV,
	lowWaterMark roachpb.Key,
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
		if storesByLocalityKVMap, ok := backupLocalityMap[i]; ok {
			storesByLocalityKV = storesByLocalityKVMap
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
		var latestCoveredTime hlc.Timestamp
		var files []execinfrapb.RestoreFileSpec
		payloads := importRange.Payload.([]interface{})
		for _, p := range payloads {
			ie := p.(importEntry)
			switch ie.entryType {
			case completedSpan:
				continue rangeLoop
			case tableSpan:
				needed = true
			case backupSpan:
				// The latest time we've backed up this span may be ahead of the start
				// time of this entry. This is because some spans can be
				// "re-introduced", meaning that they were previously backed up but
				// still appear in introducedSpans. Spans are re-introduced when they
				// were taken OFFLINE (and therefore processed non-transactional writes)
				// and brought back online (PUBLIC). For more information see #62564.
				if latestCoveredTime.Less(ie.start) {
					return nil, hlc.Timestamp{}, errors.Errorf(
						"no backup covers time [%s,%s) for range [%s,%s) or backups listed out of order (mismatched start time)",
						latestCoveredTime, ie.start,
						roachpb.Key(importRange.Start), roachpb.Key(importRange.End))
				}
				if !ie.end.Less(latestCoveredTime) {
					latestCoveredTime = ie.end
				}
			case backupFile:
				if len(ie.file.Path) > 0 {
					files = append(files, execinfrapb.RestoreFileSpec{
						Dir:  ie.dir,
						Path: ie.file.Path,
					})
				}
			}
		}
		if needed {
			if latestCoveredTime != maxEndTime {
				if err := onMissing(importRange, latestCoveredTime, maxEndTime); err != nil {
					return nil, hlc.Timestamp{}, err
				}
			}
			if len(files) == 0 {
				// There may be import entries that refer to no data, and hence
				// no files. These are caused because file spans start at a
				// specific key. E.g. consider the first file backing up data
				// from table 51. It will cover span ‹/Table/51/1/0/0› -
				// ‹/Table/51/1/3273›. When merged with the backup span:
				// ‹/Table/51› - ‹/Table/52›, we get an empty span with no
				// files: ‹/Table/51› - ‹/Table/51/1/0/0›. We should ignore
				// these to avoid thrashing during restore's split and scatter.
				continue
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
			privilegeDesc := desc.GetPrivileges()
			descpb.MaybeFixUsagePrivForTablesAndDBs(&privilegeDesc)
			wroteDBs[desc.GetID()] = desc
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, desc.(catalog.MutableDescriptor), b,
			); err != nil {
				return err
			}
			b.CPut(catalogkeys.EncodeNameKey(codec, desc), desc.GetID(), nil)
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
			b.CPut(catalogkeys.EncodeNameKey(codec, sc), sc.GetID(), nil)
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
			privilegeDesc := table.GetPrivileges()
			descpb.MaybeFixUsagePrivForTablesAndDBs(&privilegeDesc)
			// If the table descriptor is being written to a multi-region database and
			// the table does not have a locality config setup, set one up here. The
			// table's locality config will be set to the default locality - REGIONAL
			// BY TABLE IN PRIMARY REGION.
			_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
				ctx, txn, table.GetParentID(), tree.DatabaseLookupFlags{
					Required:       true,
					AvoidCached:    true,
					IncludeOffline: true,
				})
			if err != nil {
				return err
			}
			if dbDesc.IsMultiRegion() {
				if table.GetLocalityConfig() == nil {
					table.(*tabledesc.Mutable).SetTableLocalityRegionalByTable(tree.PrimaryRegionNotSpecifiedName)
				}
			} else {
				// If the database is not multi-region enabled, ensure that we don't
				// write any multi-region table descriptors into it.
				if table.GetLocalityConfig() != nil {
					return pgerror.Newf(pgcode.FeatureNotSupported,
						"cannot write descriptor for multi-region table %s into non-multi-region database %s",
						table.GetName(),
						dbDesc.GetName(),
					)
				}
			}

			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, tables[i].(catalog.MutableDescriptor), b,
			); err != nil {
				return err
			}
			b.CPut(catalogkeys.EncodeNameKey(codec, table), table.GetID(), nil)
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
			b.CPut(catalogkeys.EncodeNameKey(codec, typ), typ.GetID(), nil)
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
	codec keys.SQLCodec, kr *KeyRewriter, key roachpb.Key,
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

func restoreWithRetry(
	restoreCtx context.Context,
	execCtx sql.JobExecContext,
	numNodes int,
	backupManifests []BackupManifest,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
	dataToRestore restorationData,
	job *jobs.Job,
	encryption *jobspb.BackupEncryptionOptions,
) (RowCount, error) {
	// We retry on pretty generic failures -- any rpc error. If a worker node were
	// to restart, it would produce this kind of error, but there may be other
	// errors that are also rpc errors. Don't retry to aggressively.
	retryOpts := retry.Options{
		MaxBackoff: 1 * time.Second,
		MaxRetries: 5,
	}

	// We want to retry a restore if there are transient failures (i.e. worker nodes
	// dying), so if we receive a retryable error, re-plan and retry the backup.
	var res RowCount
	var err error
	for r := retry.StartWithCtx(restoreCtx, retryOpts); r.Next(); {
		res, err = restore(
			restoreCtx,
			execCtx,
			numNodes,
			backupManifests,
			backupLocalityInfo,
			endTime,
			dataToRestore,
			job,
			encryption,
		)
		if err == nil {
			break
		}

		if !utilccl.IsDistSQLRetryableError(err) {
			return RowCount{}, err
		}

		log.Warningf(restoreCtx, `encountered retryable error: %+v`, err)
	}

	if err != nil {
		return RowCount{}, errors.Wrap(err, "exhausted retries")
	}
	return res, nil
}

type storeByLocalityKV map[string]roachpb.ExternalStorage

func makeBackupLocalityMap(
	backupLocalityInfos []jobspb.RestoreDetails_BackupLocalityInfo, user security.SQLUsername,
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
	numNodes int,
	backupManifests []BackupManifest,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
	dataToRestore restorationData,
	job *jobs.Job,
	encryption *jobspb.BackupEncryptionOptions,
) (RowCount, error) {
	user := execCtx.User()
	// A note about contexts and spans in this method: the top-level context
	// `restoreCtx` is used for orchestration logging. All operations that carry
	// out work get their individual contexts.
	emptyRowCount := RowCount{}

	// If there isn't any data to restore, then return early.
	if dataToRestore.isEmpty() {
		return emptyRowCount, nil
	}

	// If we've already migrated some of the system tables we're about to
	// restore, this implies that a previous attempt restored all of this data.
	// We want to avoid restoring again since we'll be shadowing migrated keys.
	details := job.Details().(jobspb.RestoreDetails)
	if alreadyMigrated := checkForMigratedData(details, dataToRestore); alreadyMigrated {
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

	backupLocalityMap, err := makeBackupLocalityMap(backupLocalityInfo, user)
	if err != nil {
		return emptyRowCount, errors.Wrap(err, "resolving locality locations")
	}

	// Pivot the backups, which are grouped by time, into requests for import,
	// which are grouped by keyrange.
	highWaterMark := job.Progress().Details.(*jobspb.Progress_Restore).Restore.HighWater
	importSpans, _, err := makeImportSpans(dataToRestore.getSpans(), backupManifests, backupLocalityMap,
		highWaterMark, errOnMissingRange)
	if err != nil {
		return emptyRowCount, errors.Wrapf(err, "making import requests for %d backups", len(backupManifests))
	}
	if len(importSpans) == 0 {
		// There are no files to restore.
		return emptyRowCount, nil
	}

	for i := range importSpans {
		importSpans[i].ProgressIdx = int64(i)
	}
	mu.requestsCompleted = make([]bool, len(importSpans))

	// TODO(pbardea): This not super principled. I just wanted something that
	// wasn't a constant and grew slower than linear with the length of
	// importSpans. It seems to be working well for BenchmarkRestore2TB but
	// worth revisiting.
	// It tries to take the cluster size into account so that larger clusters
	// distribute more chunks amongst them so that after scattering there isn't
	// a large varience in the distribution of entries.
	chunkSize := int(math.Sqrt(float64(len(importSpans)))) / numNodes
	if chunkSize == 0 {
		chunkSize = 1
	}
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
	progCh := make(chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)

	// tasks are the concurrent tasks that are run during the restore.
	var tasks []func(ctx context.Context) error
	if dataToRestore.isMainBundle() {
		// Only update the job progress on the main data bundle. This should account
		// for the bulk of the data to restore. Other data (e.g. zone configs in
		// cluster restores) may be restored first. When restoring that data, we
		// don't want to update the high-water mark key, so instead progress is just
		// defined on the main data bundle (of which there should only be one).
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

		jobProgressLoop := func(ctx context.Context) error {
			ctx, progressSpan := tracing.ChildSpan(ctx, "progress-log")
			defer progressSpan.Finish()
			return progressLogger.Loop(ctx, requestFinishedCh)
		}
		tasks = append(tasks, jobProgressLoop)
	}

	jobCheckpointLoop := func(ctx context.Context) error {
		defer close(requestFinishedCh)
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
	}
	tasks = append(tasks, jobCheckpointLoop)

	runRestore := func(ctx context.Context) error {
		return distRestore(
			ctx,
			execCtx,
			importSpanChunks,
			dataToRestore.getPKIDs(),
			encryption,
			dataToRestore.getRekeys(),
			endTime,
			progCh,
		)
	}
	tasks = append(tasks, runRestore)

	if err := ctxgroup.GoAndWait(restoreCtx, tasks...); err != nil {
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

	allDescs, latestBackupManifest := loadSQLDescsFromBackupsAtTime(backupManifests, details.EndTime)

	var sqlDescs []catalog.Descriptor
	for _, desc := range allDescs {
		id := desc.GetID()
		if _, ok := details.DescriptorRewrites[id]; ok {
			sqlDescs = append(sqlDescs, desc)
		}
	}

	if err := maybeUpgradeDescriptors(ctx, sqlDescs, true /* skipFKsWithNoMatchingTable */); err != nil {
		return nil, BackupManifest{}, nil, err
	}
	return backupManifests, latestBackupManifest, sqlDescs, nil
}

// restoreResumer should only store a reference to the job it's running. State
// should not be stored here, but rather in the job details.
type restoreResumer struct {
	job *jobs.Job

	settings     *cluster.Settings
	execCfg      *sql.ExecutorConfig
	restoreStats RowCount

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
	ctx context.Context,
	tableStatistics []*stats.TableStatisticProto,
	descriptorRewrites DescRewriteMap,
	tableDescs []*descpb.TableDescriptor,
) []*stats.TableStatisticProto {
	relevantTableStatistics := make([]*stats.TableStatisticProto, 0, len(tableStatistics))

	tableHasStatsInBackup := make(map[descpb.ID]struct{})
	for _, stat := range tableStatistics {
		tableHasStatsInBackup[stat.TableID] = struct{}{}
		if tableRewrite, ok := descriptorRewrites[stat.TableID]; ok {
			// Statistics imported only when table re-write is present.
			stat.TableID = tableRewrite.ID
			relevantTableStatistics = append(relevantTableStatistics, stat)
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
		// We only import spans for physical tables.
		if !table.IsPhysicalTable() {
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
		rawTbl, _, _, _ := descpb.FromDescriptor(rev.Desc)
		if rawTbl != nil && !rawTbl.Dropped() {
			tbl := tabledesc.NewBuilder(rawTbl).BuildImmutableTable()
			// We only import spans for physical tables.
			if !tbl.IsPhysicalTable() {
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
	return spans
}

func shouldPreRestore(table *tabledesc.Mutable) bool {
	if table.GetParentID() != keys.SystemDatabaseID {
		return false
	}
	tablesToPreRestore := getSystemTablesToRestoreBeforeData()
	_, ok := tablesToPreRestore[table.GetName()]
	return ok
}

// createImportingDescriptors create the tables that we will restore into. It also
// fetches the information from the old tables that we need for the restore.
func createImportingDescriptors(
	ctx context.Context,
	p sql.JobExecContext,
	backupCodec keys.SQLCodec,
	sqlDescs []catalog.Descriptor,
	r *restoreResumer,
) (*restorationDataBase, *mainRestorationData, error) {
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

	oldTableIDs := make([]descpb.ID, 0)

	tables := make([]catalog.TableDescriptor, 0)
	postRestoreTables := make([]catalog.TableDescriptor, 0)

	preRestoreTables := make([]catalog.TableDescriptor, 0)

	for _, desc := range sqlDescs {
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
			oldTableIDs = append(oldTableIDs, mut.GetID())
		case catalog.DatabaseDescriptor:
			if _, ok := details.DescriptorRewrites[desc.GetID()]; ok {
				mut := dbdesc.NewBuilder(desc.DatabaseDesc()).BuildCreatedMutableDatabase()
				databases = append(databases, mut)
				mutableDatabases = append(mutableDatabases, mut)
			}
		case catalog.SchemaDescriptor:
			mut := schemadesc.NewBuilder(desc.SchemaDesc()).BuildCreatedMutableSchema()
			schemas = append(schemas, mut)
		case catalog.TypeDescriptor:
			mut := typedesc.NewBuilder(desc.TypeDesc()).BuildCreatedMutableType()
			types = append(types, mut)
		}
	}

	tempSystemDBID := descpb.InvalidID
	if details.DescriptorCoverage == tree.AllDescriptors {
		tempSystemDBID = getTempSystemDBID(details)
		databases = append(databases, dbdesc.NewInitial(tempSystemDBID, restoreTempSystemDB,
			security.AdminRoleName()))
	}

	// We get the spans of the restoring tables _as they appear in the backup_,
	// that is, in the 'old' keyspace, before we reassign the table IDs.
	preRestoreSpans := spansForAllRestoreTableIndexes(backupCodec, preRestoreTables, nil)
	postRestoreSpans := spansForAllRestoreTableIndexes(backupCodec, postRestoreTables, nil)

	log.Eventf(ctx, "starting restore for %d tables", len(mutableTables))

	// Assign new IDs to the database descriptors.
	if err := rewriteDatabaseDescs(mutableDatabases, details.DescriptorRewrites); err != nil {
		return nil, nil, err
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
		return nil, nil, err
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
		return nil, nil, err
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
		return nil, nil, err
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

	if tempSystemDBID != descpb.InvalidID {
		for _, desc := range mutableTables {
			if desc.GetParentID() == tempSystemDBID {
				desc.SetPublic()
			}
		}
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
				// A couple of pieces of cleanup are required for multi-region databases.
				// First, we need to find all of the MULTIREGION_ENUMs types and remap the
				// IDs stored in the corresponding database descriptors to match the type's
				// new ID. Secondly, we need to rebuild the zone configuration for each
				// multi-region database. We don't perform the zone configuration rebuild on
				// cluster restores, as they will have the zone configurations restored as
				// as the system tables are restored.
				mrEnumsFound := make(map[descpb.ID]descpb.ID)
				for _, t := range typesByID {
					typeDesc := typedesc.NewBuilder(t.TypeDesc()).BuildImmutableType()
					if typeDesc.GetKind() == descpb.TypeDescriptor_MULTIREGION_ENUM {
						// Check to see if we've found more than one multi-region enum on any
						// given database.
						if id, ok := mrEnumsFound[typeDesc.GetParentID()]; ok {
							return errors.AssertionFailedf(
								"unexpectedly found more than one MULTIREGION_ENUM (IDs = %d, %d) "+
									"on database %d during restore", id, typeDesc.GetID(), typeDesc.GetParentID())
						}
						mrEnumsFound[typeDesc.GetParentID()] = typeDesc.GetID()

						if db, ok := dbsByID[typeDesc.GetParentID()]; ok {
							desc := db.DatabaseDesc()
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
								regionNames, err := typeDesc.RegionNames()
								if err != nil {
									return err
								}
								regionConfig := multiregion.MakeRegionConfig(
									regionNames,
									desc.RegionConfig.PrimaryRegion,
									desc.RegionConfig.SurvivalGoal,
									desc.RegionConfig.RegionEnumID,
								)
								if err := sql.ApplyZoneConfigFromDatabaseRegionConfig(
									ctx,
									desc.GetID(),
									regionConfig,
									txn,
									p.ExecCfg(),
								); err != nil {
									return err
								}
							}
						}
					}
				}

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
					_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
						ctx, txn, table.GetParentID(), tree.DatabaseLookupFlags{
							Required:       true,
							AvoidCached:    true,
							IncludeOffline: true,
						})
					if err != nil {
						return err
					}
					typeIDs, err := table.GetAllReferencedTypeIDs(dbDesc, func(id descpb.ID) (catalog.TypeDescriptor, error) {
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

				// Now that all of the descriptors have been written to disk, rebuild
				// the zone configurations for any multi-region tables. We only do this
				// in cases where this is not a full cluster restore, because in cluster
				// restore cases, the zone configurations will be restored when the
				// system tables are restored.
				if details.DescriptorCoverage != tree.AllDescriptors {
					for _, table := range tableDescs {
						if lc := table.GetLocalityConfig(); lc != nil {
							_, desc, err := descsCol.GetImmutableDatabaseByID(
								ctx,
								txn,
								table.ParentID,
								tree.DatabaseLookupFlags{
									Required:       true,
									AvoidCached:    true,
									IncludeOffline: true,
								},
							)
							if err != nil {
								return err
							}
							if desc.GetRegionConfig() == nil {
								return errors.AssertionFailedf(
									"found multi-region table %d in non-multi-region database %d",
									table.ID, table.ParentID)
							}

							mutTable, err := descsCol.GetMutableTableVersionByID(ctx, table.GetID(), txn)
							if err != nil {
								return err
							}

							regionConfig, err := sql.SynthesizeRegionConfig(
								ctx,
								txn,
								desc.GetID(),
								descsCol,
								sql.SynthesizeRegionConfigOptionIncludeOffline,
							)
							if err != nil {
								return err
							}
							if err := sql.ApplyZoneConfigForMultiRegionTable(
								ctx,
								txn,
								p.ExecCfg(),
								regionConfig,
								mutTable,
								sql.ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
							); err != nil {
								return err
							}
						}
					}
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
				err := r.job.SetDetails(ctx, txn, details)

				// Emit to the event log now that the job has finished preparing descs.
				emitRestoreJobEvent(ctx, p, jobs.StatusRunning, r.job)

				return err
			})
		if err != nil {
			return nil, nil, err
		}
	}

	// Get TableRekeys to use when importing raw data.
	var rekeys []execinfrapb.TableRekey
	for i := range tables {
		tableToSerialize := tables[i]
		newDescBytes, err := protoutil.Marshal(tableToSerialize.DescriptorProto())
		if err != nil {
			return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"marshaling descriptor")
		}
		rekeys = append(rekeys, execinfrapb.TableRekey{
			OldID:   uint32(oldTableIDs[i]),
			NewDesc: newDescBytes,
		})
	}

	pkIDs := make(map[uint64]bool)
	for _, tbl := range tables {
		pkIDs[roachpb.BulkOpSummaryID(uint64(tbl.GetID()), uint64(tbl.GetPrimaryIndexID()))] = true
	}

	dataToPreRestore := &restorationDataBase{
		spans:  preRestoreSpans,
		rekeys: rekeys,
		pkIDs:  pkIDs,
	}

	dataToRestore := &mainRestorationData{
		restorationDataBase{
			spans:  postRestoreSpans,
			rekeys: rekeys,
			pkIDs:  pkIDs,
		},
	}

	if tempSystemDBID != descpb.InvalidID {
		for _, table := range preRestoreTables {
			if table.GetParentID() == tempSystemDBID {
				dataToPreRestore.systemTables = append(dataToPreRestore.systemTables, table)
			}
		}
		for _, table := range postRestoreTables {
			if table.GetParentID() == tempSystemDBID {
				dataToRestore.systemTables = append(dataToRestore.systemTables, table)
			}
		}
	}
	return dataToPreRestore, dataToRestore, nil
}

// Resume is part of the jobs.Resumer interface.
func (r *restoreResumer) Resume(ctx context.Context, execCtx interface{}) error {
	details := r.job.Details().(jobspb.RestoreDetails)
	p := execCtx.(sql.JobExecContext)

	backupManifests, latestBackupManifest, sqlDescs, err := loadBackupSQLDescs(
		ctx, p, details, details.Encryption,
	)
	if err != nil {
		return err
	}
	// backupCodec is the codec that was used to encode the keys in the backup. It
	// is the tenant in which the backup was taken.
	backupCodec := keys.SystemSQLCodec
	if len(sqlDescs) != 0 {
		if len(latestBackupManifest.Spans) != 0 && len(latestBackupManifest.Tenants) == 0 {
			// If there are no tenant targets, then the entire keyspace covered by
			// Spans must lie in 1 tenant.
			_, backupTenantID, err := keys.DecodeTenantPrefix(latestBackupManifest.Spans[0].Key)
			if err != nil {
				return err
			}
			backupCodec = keys.MakeSQLCodec(backupTenantID)
			// Disallow cluster restores, unless the tenant IDs match.
			if details.DescriptorCoverage == tree.AllDescriptors {
				if !backupCodec.TenantPrefix().Equal(p.ExecCfg().Codec.TenantPrefix()) {
					return unimplemented.NewWithIssuef(62277,
						"cannot cluster RESTORE backups taken from different tenant: %s",
						backupTenantID.String())
				}
			}
			if backupTenantID != roachpb.SystemTenantID && p.ExecCfg().Codec.ForSystemTenant() {
				// TODO(pbardea): This is unsupported for now because the key-rewriter
				// cannot distinguish between RESTORE TENANT and table restore from a
				// backup taken in a tenant, into the system tenant.
				return errors.New("cannot restore tenant backups into system tenant")
			}
		}
	}

	lastBackupIndex, err := getBackupIndexAtTime(backupManifests, details.EndTime)
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

	preData, mainData, err := createImportingDescriptors(ctx, p, backupCodec, sqlDescs, r)
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
	var remappedStats []*stats.TableStatisticProto
	backupStats, err := getStatisticsFromBackup(ctx, defaultStore, details.Encryption,
		latestBackupManifest)
	if err == nil {
		remappedStats = remapRelevantStatistics(ctx, backupStats, details.DescriptorRewrites,
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
		publishDescriptors := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) (err error) {
			return r.publishDescriptors(ctx, txn, descsCol, details, nil)
		}
		if err := descs.Txn(
			ctx, r.execCfg.Settings, r.execCfg.LeaseManager, r.execCfg.InternalExecutor,
			r.execCfg.DB, publishDescriptors,
		); err != nil {
			return err
		}
		if err := p.ExecCfg().JobRegistry.NotifyToAdoptJobs(ctx); err != nil {
			return err
		}
		if fn := r.testingKnobs.afterPublishingDescriptors; fn != nil {
			if err := fn(); err != nil {
				return err
			}
		}
		emitRestoreJobEvent(ctx, p, jobs.StatusSucceeded, r.job)
		return nil
	}

	for _, tenant := range details.Tenants {
		mainData.addTenant(roachpb.MakeTenantID(tenant.ID))
	}

	numNodes, err := clusterNodeCount(p.ExecCfg().Gossip)
	if err != nil {
		if !build.IsRelease() && p.ExecCfg().Codec.ForSystemTenant() {
			return err
		}
		log.Warningf(ctx, "unable to determine cluster node count: %v", err)
		numNodes = 1
	}

	var resTotal RowCount
	if !preData.isEmpty() {
		res, err := restoreWithRetry(
			ctx,
			p,
			numNodes,
			backupManifests,
			details.BackupLocalityInfo,
			details.EndTime,
			preData,
			r.job,
			details.Encryption,
		)
		if err != nil {
			return err
		}

		resTotal.add(res)

		if details.DescriptorCoverage == tree.AllDescriptors {
			if err := r.restoreSystemTables(ctx, p.ExecCfg().DB, details, preData.systemTables); err != nil {
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
	}

	{
		// Restore the main data bundle. We notably only restore the system tables
		// later.
		res, err := restoreWithRetry(
			ctx,
			p,
			numNodes,
			backupManifests,
			details.BackupLocalityInfo,
			details.EndTime,
			mainData,
			r.job,
			details.Encryption,
		)
		if err != nil {
			return err
		}

		resTotal.add(res)
	}

	if err := insertStats(ctx, r.job, p.ExecCfg(), remappedStats); err != nil {
		return errors.Wrap(err, "inserting table statistics")
	}

	var devalidateIndexes map[descpb.ID][]descpb.IndexID
	if toValidate := len(details.RevalidateIndexes); toValidate > 0 {
		if err := r.job.RunningStatus(ctx, nil /* txn */, func(_ context.Context, _ jobspb.Details) (jobs.RunningStatus, error) {
			return jobs.RunningStatus(fmt.Sprintf("re-validating %d indexes", toValidate)), nil
		}); err != nil {
			return errors.Wrapf(err, "failed to update running status of job %d", errors.Safe(r.job.ID()))
		}
		bad, err := revalidateIndexes(ctx, p.ExecCfg(), r.job, details.TableDescs, details.RevalidateIndexes)
		if err != nil {
			return err
		}
		devalidateIndexes = bad
	}

	publishDescriptors := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) (err error) {
		err = r.publishDescriptors(ctx, txn, descsCol, details, devalidateIndexes)
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
	if err := p.ExecCfg().JobRegistry.NotifyToAdoptJobs(ctx); err != nil {
		return err
	}

	if details.DescriptorCoverage == tree.AllDescriptors {
		// We restore the system tables from the main data bundle so late because it
		// includes the jobs that are being restored. As soon as we restore these
		// jobs, they become accessible to the user, and may start executing. We
		// need this to happen after the descriptors have been marked public.
		if err := r.restoreSystemTables(ctx, p.ExecCfg().DB, details, mainData.systemTables); err != nil {
			return err
		}
		// Reload the details as we may have updated the job.
		details = r.job.Details().(jobspb.RestoreDetails)

		if err := r.cleanupTempSystemTables(ctx); err != nil {
			return err
		}
	}

	if fn := r.testingKnobs.afterPublishingDescriptors; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}

	r.notifyStatsRefresherOfNewTables()

	r.restoreStats = resTotal

	// Emit an event now that the restore job has completed.
	emitRestoreJobEvent(ctx, p, jobs.StatusSucceeded, r.job)

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
	}
	return nil
}

func revalidateIndexes(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	job *jobs.Job,
	tables []*descpb.TableDescriptor,
	indexIDs []jobspb.RestoreDetails_RevalidateIndex,
) (map[descpb.ID][]descpb.IndexID, error) {
	indexIDsByTable := make(map[descpb.ID]map[descpb.IndexID]struct{})
	for _, idx := range indexIDs {
		if indexIDsByTable[idx.TableID] == nil {
			indexIDsByTable[idx.TableID] = make(map[descpb.IndexID]struct{})
		}
		indexIDsByTable[idx.TableID][idx.IndexID] = struct{}{}
	}

	// We don't actually need the 'historical' read the way the schema change does
	// since our table is offline.
	var runner sql.HistoricalInternalExecTxnRunner = func(ctx context.Context, fn sql.InternalExecFn) error {
		return execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			ie := job.MakeSessionBoundInternalExecutor(ctx, sql.NewFakeSessionData(execCfg.SV())).(*sql.InternalExecutor)
			return fn(ctx, txn, ie)
		})
	}

	invalidIndexes := make(map[descpb.ID][]descpb.IndexID)

	for _, tbl := range tables {
		indexes := indexIDsByTable[tbl.ID]
		if len(indexes) == 0 {
			continue
		}
		tableDesc := tabledesc.NewBuilder(tbl).BuildExistingMutableTable()

		var forward, inverted []catalog.Index
		for _, idx := range tableDesc.AllIndexes() {
			if _, ok := indexes[idx.GetID()]; ok {
				switch idx.GetType() {
				case descpb.IndexDescriptor_FORWARD:
					forward = append(forward, idx)
				case descpb.IndexDescriptor_INVERTED:
					inverted = append(inverted, idx)
				}
			}
		}
		if len(forward) > 0 {
			if err := sql.ValidateForwardIndexes(ctx, tableDesc.MakePublic(), forward, runner, false, true); err != nil {
				if invalid := (sql.InvalidIndexesError{}); errors.As(err, &invalid) {
					invalidIndexes[tableDesc.ID] = invalid.Indexes
				} else {
					return nil, err
				}
			}
		}
		if len(inverted) > 0 {
			if err := sql.ValidateInvertedIndexes(ctx, execCfg.Codec, tableDesc.MakePublic(), inverted, runner, true); err != nil {
				if invalid := (sql.InvalidIndexesError{}); errors.As(err, &invalid) {
					invalidIndexes[tableDesc.ID] = append(invalidIndexes[tableDesc.ID], invalid.Indexes...)
				} else {
					return nil, err
				}
			}
		}
	}
	return invalidIndexes, nil
}

// ReportResults implements JobResultsReporter interface.
func (r *restoreResumer) ReportResults(ctx context.Context, resultsCh chan<- tree.Datums) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(r.job.ID())),
		tree.NewDString(string(jobs.StatusSucceeded)),
		tree.NewDFloat(tree.DFloat(1.0)),
		tree.NewDInt(tree.DInt(r.restoreStats.Rows)),
		tree.NewDInt(tree.DInt(r.restoreStats.IndexEntries)),
		tree.NewDInt(tree.DInt(r.restoreStats.DataSize)),
	}:
		return nil
	}
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

	if latestStats == nil {
		return nil
	}

	err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := stats.InsertNewStats(ctx, execCfg.InternalExecutor, txn, latestStats); err != nil {
			return errors.Wrapf(err, "inserting stats from backup")
		}
		details.StatsInserted = true
		if err := job.SetDetails(ctx, txn, details); err != nil {
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
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	details jobspb.RestoreDetails,
	devalidateIndexes map[descpb.ID][]descpb.IndexID,
) (err error) {
	if details.DescriptorsPublished {
		return nil
	}
	if fn := r.testingKnobs.beforePublishingDescriptors; fn != nil {
		if err := fn(); err != nil {
			return err
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
			return err
		}
		if err := checkVersion(mutTable, tbl.Version); err != nil {
			return err
		}
		badIndexes := devalidateIndexes[mutTable.ID]
		for _, badIdx := range badIndexes {
			found, err := mutTable.FindIndexWithID(badIdx)
			if err != nil {
				return err
			}
			newIdx := found.IndexDescDeepCopy()
			mutTable.RemovePublicNonPrimaryIndex(found.Ordinal())
			if err := mutTable.AddIndexMutation(&newIdx, descpb.DescriptorMutation_ADD); err != nil {
				return err
			}
		}
		allMutDescs = append(allMutDescs, mutTable)
		newTables = append(newTables, mutTable.TableDesc())
		// For cluster restores, all the jobs are restored directly from the jobs
		// table, so there is no need to re-create ongoing schema change jobs,
		// otherwise we'll create duplicate jobs.
		if details.DescriptorCoverage != tree.AllDescriptors || len(badIndexes) > 0 {
			// Convert any mutations that were in progress on the table descriptor
			// when the backup was taken, and convert them to schema change jobs.
			if err := createSchemaChangeJobsFromMutations(ctx,
				r.execCfg.JobRegistry, r.execCfg.Codec, txn, r.job.Payload().UsernameProto.Decode(), mutTable,
			); err != nil {
				return err
			}
		}
	}
	// For all of the newly created types, make type schema change jobs for any
	// type descriptors that were backed up in the middle of a type schema change.
	for _, typDesc := range details.TypeDescs {
		typ, err := descsCol.GetMutableTypeVersionByID(ctx, txn, typDesc.GetID())
		if err != nil {
			return err
		}
		if err := checkVersion(typ, typDesc.Version); err != nil {
			return err
		}
		allMutDescs = append(allMutDescs, typ)
		newTypes = append(newTypes, typ.TypeDesc())
		if typ.HasPendingSchemaChanges() && details.DescriptorCoverage != tree.AllDescriptors {
			if err := createTypeChangeJobFromDesc(
				ctx, r.execCfg.JobRegistry, r.execCfg.Codec, txn, r.job.Payload().UsernameProto.Decode(), typ,
			); err != nil {
				return err
			}
		}
	}
	for _, sc := range details.SchemaDescs {
		mutDesc, err := descsCol.GetMutableDescriptorByID(ctx, sc.ID, txn)
		if err != nil {
			return err
		}
		if err := checkVersion(mutDesc, sc.Version); err != nil {
			return err
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
			return err
		}
		if err := checkVersion(mutDesc, dbDesc.Version); err != nil {
			return err
		}
		mutDB := mutDesc.(*dbdesc.Mutable)
		// TODO(lucy,ajwerner): Remove this in 21.1.
		if !mutDB.Offline() {
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
			return err
		}
	}

	if err := txn.Run(ctx, b); err != nil {
		return errors.Wrap(err, "publishing tables")
	}

	for _, tenant := range details.Tenants {
		if err := sql.ActivateTenant(ctx, r.execCfg, txn, tenant.ID); err != nil {
			return err
		}
	}

	// Update and persist the state of the job.
	details.DescriptorsPublished = true
	details.TableDescs = newTables
	details.TypeDescs = newTypes
	details.SchemaDescs = newSchemas
	details.DatabaseDescs = newDBs
	if err := r.job.SetDetails(ctx, txn, details); err != nil {
		return errors.Wrap(err,
			"updating job details after publishing tables")
	}
	return nil
}

func emitRestoreJobEvent(
	ctx context.Context, p sql.JobExecContext, status jobs.Status, job *jobs.Job,
) {
	// Emit to the event log now that we have completed the prepare step.
	var restoreEvent eventpb.Restore
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return sql.LogEventForJobs(ctx, p.ExecCfg(), txn, &restoreEvent, int64(job.ID()),
			job.Payload(), p.User(), status)
	}); err != nil {
		log.Warningf(ctx, "failed to log event: %v", err)
	}
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes KV data that
// has been committed from a restore that has failed or been canceled. It does
// this by adding the table descriptors in DROP state, which causes the schema
// change stuff to delete the keys in the background.
func (r *restoreResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	// Emit to the event log that the job has started reverting.
	emitRestoreJobEvent(ctx, p, jobs.StatusReverting, r.job)

	telemetry.Count("restore.total.failed")
	telemetry.CountBucketed("restore.duration-sec.failed",
		int64(timeutil.Since(timeutil.FromUnixMicros(r.job.Payload().StartedMicros)).Seconds()))

	details := r.job.Details().(jobspb.RestoreDetails)

	execCfg := execCtx.(sql.JobExecContext).ExecCfg()
	err := descs.Txn(ctx, execCfg.Settings, execCfg.LeaseManager, execCfg.InternalExecutor,
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
	if err != nil {
		return err
	}

	// Emit to the event log that the job has completed reverting.
	emitRestoreJobEvent(ctx, p, jobs.StatusFailed, r.job)
	return nil
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
		tableToDrop.SetDropped()
		b.Del(catalogkeys.EncodeNameKey(codec, tableToDrop))
		descsCol.AddDeletedDescriptor(tableToDrop)
		if err := descsCol.WriteDescToBatch(ctx, false /* kvTrace */, tableToDrop, b); err != nil {
			return errors.Wrap(err, "writing dropping table to batch")
		}
	}

	// Drop the type descriptors that this restore created.
	for i := range details.TypeDescs {
		// TypeDescriptors don't have a GC job process, so we can just write them
		// as dropped here.
		typDesc := details.TypeDescs[i]
		mutType, err := descsCol.GetMutableTypeByID(ctx, txn, typDesc.ID, tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				AvoidCached:    true,
				IncludeOffline: true,
			},
		})
		if err != nil {
			return err
		}

		b.Del(catalogkeys.EncodeNameKey(codec, typDesc))
		mutType.SetDropped()
		if err := descsCol.WriteDescToBatch(ctx, false /* kvTrace */, mutType, b); err != nil {
			return errors.Wrap(err, "writing dropping type to batch")
		}
		// Remove the system.descriptor entry.
		b.Del(catalogkeys.MakeDescMetadataKey(codec, typDesc.ID))
		descsCol.AddDeletedDescriptor(mutType)
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
	allDescs, err := descsCol.GetAllDescriptors(ctx, txn)
	if err != nil {
		return err
	}

	// Delete any schema descriptors that this restore created. Also collect the
	// descriptors so we can update their parent databases later.
	dbsWithDeletedSchemas := make(map[descpb.ID][]catalog.SchemaDescriptor)
	for _, schemaDesc := range details.SchemaDescs {
		sc := schemadesc.NewBuilder(schemaDesc).BuildImmutableSchema()
		// We need to ignore descriptors we just added since we haven't committed the txn that deletes these.
		isSchemaEmpty, err := isSchemaEmpty(ctx, txn, sc.GetID(), allDescs, ignoredChildDescIDs)
		if err != nil {
			return errors.Wrapf(err, "checking if schema %s is empty during restore cleanup", sc.GetName())
		}

		if !isSchemaEmpty {
			log.Warningf(ctx, "preserving schema %s on restore failure because it contains new child objects", sc.GetName())
			continue
		}

		b.Del(catalogkeys.EncodeNameKey(codec, sc))
		b.Del(catalogkeys.MakeDescMetadataKey(codec, sc.GetID()))
		descsCol.AddDeletedDescriptor(sc)
		dbsWithDeletedSchemas[sc.GetParentID()] = append(dbsWithDeletedSchemas[sc.GetParentID()], sc)
	}

	// Delete the database descriptors.
	deletedDBs := make(map[descpb.ID]struct{})
	for _, dbDesc := range details.DatabaseDescs {

		// We need to ignore descriptors we just added since we haven't committed the txn that deletes these.
		isDBEmpty, err := isDatabaseEmpty(ctx, txn, dbDesc.GetID(), allDescs, ignoredChildDescIDs)
		if err != nil {
			return errors.Wrapf(err, "checking if database %s is empty during restore cleanup", dbDesc.GetName())
		}
		if !isDBEmpty {
			log.Warningf(ctx, "preserving database %s on restore failure because it contains new child objects or schemas", dbDesc.GetName())
			continue
		}

		db, err := descsCol.GetMutableDescriptorByID(ctx, dbDesc.GetID(), txn)
		if err != nil {
			return err
		}

		// Mark db as dropped and add uncommitted version to pass pre-txn
		// descriptor validation.
		db.SetDropped()
		db.MaybeIncrementVersion()
		if err := descsCol.AddUncommittedDescriptor(db); err != nil {
			return err
		}

		descKey := catalogkeys.MakeDescMetadataKey(codec, db.GetID())
		b.Del(descKey)
		nameKey := catalogkeys.MakeDatabaseNameKey(codec, db.GetName())
		b.Del(nameKey)
		descsCol.AddDeletedDescriptor(db)
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
			typ, err := descsCol.GetMutableTypeVersionByID(ctx, txn, id)
			if err != nil {
				return nil, err
			}
			existingTypes[typ.GetID()] = typ
			return typ, nil
		}

		_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
			ctx, txn, tbl.GetParentID(), tree.DatabaseLookupFlags{
				Required:       true,
				AvoidCached:    true,
				IncludeOffline: true,
			})
		if err != nil {
			return err
		}

		// Get all types that this descriptor references.
		referencedTypes, err := tbl.GetAllReferencedTypeIDs(dbDesc, lookup)
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
) (updatedPrivileges *descpb.PrivilegeDescriptor, err error) {
	switch desc := desc.(type) {
	case catalog.TableDescriptor, catalog.SchemaDescriptor:
		if wrote, ok := wroteDBs[desc.GetParentID()]; ok {
			// If we're creating a new database in this restore, the privileges of the
			// table and schema should be that of the parent DB.
			//
			// Leave the privileges of the temp system tables as the default too.
			if descCoverage == tree.RequestedDescriptors || wrote.GetName() == restoreTempSystemDB {
				updatedPrivileges = wrote.GetPrivileges()
			}
		} else if descCoverage == tree.RequestedDescriptors {
			parentDB, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, codec, desc.GetParentID())
			if err != nil {
				return nil, errors.Wrapf(err, "failed to lookup parent DB %d", errors.Safe(desc.GetParentID()))
			}

			// Default is to copy privs from restoring parent db, like CREATE {TABLE,
			// SCHEMA}. But also like CREATE {TABLE,SCHEMA}, we set the owner to the
			// user creating the table (the one running the restore).
			// TODO(dt): Make this more configurable.
			updatedPrivileges = sql.CreateInheritedPrivilegesFromDBDesc(parentDB, user)
		}
	case catalog.TypeDescriptor, catalog.DatabaseDescriptor:
		if descCoverage == tree.RequestedDescriptors {
			// If the restore is not a cluster restore we cannot know that the users on
			// the restoring cluster match the ones that were on the cluster that was
			// backed up. So we wipe the privileges on the type/database.
			updatedPrivileges = descpb.NewDefaultPrivilegeDescriptor(user)
		}
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
	details := r.job.Details().(jobspb.RestoreDetails)
	if details.SystemTablesMigrated == nil {
		details.SystemTablesMigrated = make(map[string]bool)
	}

	// Iterate through all the tables that we're restoring, and if it was restored
	// to the temporary system DB then copy it's data over to the real system
	// table.
	for _, table := range tables {
		if table.GetParentID() != tempSystemDBID {
			continue
		}
		systemTableName := table.GetName()
		stagingTableName := restoreTempSystemDB + "." + systemTableName

		config, ok := systemTableBackupConfiguration[systemTableName]
		if !ok {
			log.Warningf(ctx, "no configuration specified for table %s... skipping restoration",
				systemTableName)
		}

		if config.migrationFunc != nil {
			if details.SystemTablesMigrated[systemTableName] {
				continue
			}

			if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				if err := config.migrationFunc(ctx, r.execCfg, txn, stagingTableName); err != nil {
					return err
				}

				// Keep track of which system tables we've migrated so that future job
				// restarts don't try to import data over our migrated data. This would
				// fail since the restored data would shadow the migrated keys.
				details.SystemTablesMigrated[systemTableName] = true
				return r.job.SetDetails(ctx, txn, details)
			}); err != nil {
				return err
			}
		}

		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			txn.SetDebugName("system-restore-txn")

			restoreFunc := defaultSystemTableRestoreFunc
			if config.customRestoreFunc != nil {
				restoreFunc = config.customRestoreFunc
				log.Eventf(ctx, "using custom restore function for table %s", systemTableName)
			}

			log.Eventf(ctx, "restoring system table %s", systemTableName)
			err := restoreFunc(ctx, r.execCfg, txn, systemTableName, stagingTableName)
			if err != nil {
				return errors.Wrapf(err, "restoring system table %s", systemTableName)
			}
			return nil
		}); err != nil {
			return err
		}

		if fn := r.testingKnobs.duringSystemTableRestoration; fn != nil {
			if err := fn(systemTableName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *restoreResumer) cleanupTempSystemTables(ctx context.Context) error {
	executor := r.execCfg.InternalExecutor
	// After restoring the system tables, drop the temporary database holding the
	// system tables.
	dropTableQuery := fmt.Sprintf("DROP DATABASE %s CASCADE", restoreTempSystemDB)
	_, err := executor.Exec(ctx, "drop-temp-system-db" /* opName */, nil /* txn */, dropTableQuery)
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
