// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const (
	restoreOptIntoDB         = "into_db"
	restoreOptSkipMissingFKs = "skip_missing_foreign_keys"
)

// Import loads some data in sstables into an empty range. Only the keys between
// startKey and endKey are loaded. Every row's key is rewritten to be for
// newTableID.
func Import(
	ctx context.Context,
	db client.DB,
	startKey, endKey roachpb.Key,
	files []roachpb.ImportRequest_File,
	kr storageccl.KeyRewriter,
) (*roachpb.ImportResponse, error) {
	var newStartKey, newEndKey roachpb.Key
	{
		var ok bool
		newStartKey, ok = kr.RewriteKey(append([]byte(nil), startKey...))
		if !ok {
			return nil, errors.Errorf("could not rewrite key: %s", newStartKey)
		}
		newEndKey, ok = kr.RewriteKey(append([]byte(nil), endKey...))
		if !ok {
			return nil, errors.Errorf("could not rewrite key: %s", newEndKey)
		}
	}

	if log.V(1) {
		log.Infof(ctx, "import [%s,%s) (%d files)", newStartKey, newEndKey, len(files))
	}
	if len(files) == 0 {
		return &roachpb.ImportResponse{}, nil
	}

	req := &roachpb.ImportRequest{
		// Import is a point request because we don't want DistSender to split
		// it. Assume (but don't require) the entire post-rewrite span is on the
		// same range.
		Span: roachpb.Span{Key: newStartKey},
		DataSpan: roachpb.Span{
			Key:    startKey,
			EndKey: endKey,
		},
		Files:       files,
		KeyRewrites: kr,
	}
	res, pErr := client.SendWrapped(ctx, db.GetSender(), req)
	if pErr != nil {
		return nil, pErr.GoError()
	}

	return res.(*roachpb.ImportResponse), nil
}

func loadBackupDescs(ctx context.Context, uris []string) ([]BackupDescriptor, error) {
	backupDescs := make([]BackupDescriptor, len(uris))

	for i, uri := range uris {
		desc, err := readBackupDescriptor(ctx, uri)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read backup descriptor")
		}
		backupDescs[i] = desc
	}
	if len(backupDescs) == 0 {
		return nil, errors.Errorf("no backups found")
	}
	return backupDescs, nil
}

func reassignParentIDs(
	ctx context.Context,
	txn *client.Txn,
	p sql.PlanHookState,
	databasesByID map[sqlbase.ID]*sqlbase.DatabaseDescriptor,
	tables []*sqlbase.TableDescriptor,
	opt parser.KVOptions,
) error {
	for _, table := range tables {
		// Update the parentID to point to the named DB in the new cluster.
		{
			var targetDB string
			if override, ok := opt.Get(restoreOptIntoDB); ok {
				targetDB = override
			} else {
				database, ok := databasesByID[table.ParentID]
				if !ok {
					return errors.Errorf("no database with ID %d in backup for table %q", table.ParentID, table.Name)
				}
				targetDB = database.Name
			}

			// Make sure the target DB exists.
			existingDatabaseID, err := txn.Get(ctx, sqlbase.MakeNameMetadataKey(0, targetDB))
			if err != nil {
				return err
			}
			if existingDatabaseID.Value == nil {
				return errors.Errorf("a database named %q needs to exist to restore table %q",
					targetDB, table.Name)
			}
			newParentID, err := existingDatabaseID.Value.GetInt()
			if err != nil {
				return err
			}
			table.ParentID = sqlbase.ID(newParentID)
		}
		// Check that the table name is _not_ in use.
		// This would fail the CPut later anyway, but this yields a prettier error.
		{
			nameKey := table.GetNameMetadataKey()
			res, err := txn.Get(ctx, nameKey)
			if err != nil {
				return err
			}
			if res.Exists() {
				return sqlbase.NewRelationAlreadyExistsError(table.Name)
			}
		}

		// Check and set privileges.
		{
			parentDB, err := sqlbase.GetDatabaseDescFromID(ctx, txn, table.ParentID)
			if err != nil {
				return errors.Wrapf(err, "failed to lookup parent DB %d", table.ParentID)
			}

			if err := p.CheckPrivilege(parentDB, privilege.CREATE); err != nil {
				return err
			}

			// Default is to copy privs from restoring parent db, like CREATE TABLE.
			// TODO(dt): Make this more configurable.
			{
				table.Privileges = parentDB.GetPrivileges()
			}
		}
	}
	return nil
}

// reassignTableIDs updates the tables being restored with new TableIDs reserved
// in the restoring cluster, as well as fixing cross-table references to use the
// new IDs. It returns a KeyRewriter that can be used to transform KV data to
// reflect the ID remapping it has done in the descriptors.
func reassignTableIDs(
	ctx context.Context, db client.DB, tables []*sqlbase.TableDescriptor, opt parser.KVOptions,
) (storageccl.KeyRewriter, map[sqlbase.ID]sqlbase.ID, error) {
	var newTableIDs map[sqlbase.ID]sqlbase.ID
	var kr storageccl.KeyRewriter

	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		newTableIDs = make(map[sqlbase.ID]sqlbase.ID, len(tables))
		for _, table := range tables {
			newTableID, err := sql.GenerateUniqueDescID(ctx, txn)
			if err != nil {
				return err
			}
			kr = append(kr, MakeKeyRewriterForNewTableID(table, newTableID)...)
			newTableIDs[table.ID] = newTableID
			table.ID = newTableID
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}

	if err := reassignReferencedTables(tables, newTableIDs, opt); err != nil {
		return nil, nil, err
	}

	return kr, newTableIDs, nil
}

func reassignReferencedTables(
	tables []*sqlbase.TableDescriptor, newTableIDs map[sqlbase.ID]sqlbase.ID, opt parser.KVOptions,
) error {
	for _, table := range tables {
		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			// Verify that for any interleaved index being restored, the interleave
			// parent is also being restored. Otherwise, the interleave entries in the
			// restored IndexDescriptors won't have anything to point to.
			// TODO(dan): It seems like this restriction could be lifted by restoring
			// stub TableDescriptors for the missing interleave parents.
			for j, a := range index.Interleave.Ancestors {
				ancestorID, ok := newTableIDs[a.TableID]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave parent %d", table.Name, a.TableID,
					)
				}
				index.Interleave.Ancestors[j].TableID = ancestorID
			}
			for j, c := range index.InterleavedBy {
				childID, ok := newTableIDs[c.Table]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave child table %d", table.Name, c.Table,
					)
				}
				index.InterleavedBy[j].Table = childID
			}

			if index.ForeignKey.IsSet() {
				to := index.ForeignKey.Table
				if newID, ok := newTableIDs[to]; ok {
					index.ForeignKey.Table = newID
				} else {
					if empty, ok := opt.Get(restoreOptSkipMissingFKs); ok {
						if empty != "" {
							return errors.Errorf("option %q does not take a value", restoreOptSkipMissingFKs)
						}
						index.ForeignKey = sqlbase.ForeignKeyReference{}
					} else {
						return errors.Errorf(
							"cannot restore table %q without referenced table %d (or %q option)",
							table.Name, to, restoreOptSkipMissingFKs,
						)
					}

					// TODO(dt): if there is an existing (i.e. non-restoring) table with
					// a db and name matching the one the FK pointed to at backup, should
					// we update the FK to point to it?
				}
			}

			origRefs := index.ReferencedBy
			index.ReferencedBy = nil
			for _, ref := range origRefs {
				if newID, ok := newTableIDs[ref.Table]; ok {
					ref.Table = newID
					index.ReferencedBy = append(index.ReferencedBy, ref)
				}
			}
			return nil
		}); err != nil {
			return err
		}

		if _, renaming := opt.Get(restoreOptIntoDB); renaming && table.IsView() {
			return errors.Errorf("cannot restore view when using %q option", restoreOptIntoDB)
		}

		for i, dest := range table.DependsOn {
			if newID, ok := newTableIDs[dest]; ok {
				table.DependsOn[i] = newID
			} else {
				return errors.Errorf(
					"cannot restore %q without restoring referenced table %d in same operation",
					table.Name, dest)
			}
		}
		origRefs := table.DependedOnBy
		table.DependedOnBy = nil
		for _, ref := range origRefs {
			if newID, ok := newTableIDs[ref.ID]; ok {
				ref.ID = newID
				table.DependedOnBy = append(table.DependedOnBy, ref)
			}
		}

		// since this is a "new" table in eyes of new cluster, any leftover change
		// lease is obviously bogus (plus the nodeID is relative to backup cluster).
		table.Lease = nil
	}
	return nil
}

type intervalSpan roachpb.Span

var _ interval.Interface = intervalSpan{}

// ID is part of `interval.Interface` but unused in makeImportRequests.
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
	request
)

type importEntry struct {
	roachpb.Span
	entryType importEntryType

	// Only set if entryType is backupSpan
	backup BackupDescriptor

	// Only set if entryType is backupFile
	dir  roachpb.ExportStorage
	file BackupDescriptor_File

	// Only set if entryType is request
	files []roachpb.ImportRequest_File
}

// makeImportRequests pivots the backups, which are grouped by time, into
// requests for import, which are grouped by keyrange.
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
// This would be turned into two Import requests, one restoring [B, C) out of
// /file1 and /file3, the other restoring [C, D) out of /file2 and /file5.
// Nothing is restored out of /file3 and only part of /file1 is used.
//
// NB: All grouping operates in the pre-rewrite keyspace, meaning the keyranges
// as they were backed up, not as they're being restored.
func makeImportRequests(
	tableSpans []roachpb.Span, backups []BackupDescriptor,
) ([]importEntry, hlc.Timestamp, error) {
	// Put the merged table data covering first into the OverlapCoveringMerge
	// input.
	var tableSpanCovering intervalccl.Covering
	for _, span := range tableSpans {
		tableSpanCovering = append(tableSpanCovering, intervalccl.Range{
			Start: span.Key,
			End:   span.EndKey,
			Payload: importEntry{
				Span:      span,
				entryType: tableSpan,
			},
		})
	}
	backupCoverings := []intervalccl.Covering{tableSpanCovering}

	// Iterate over backups creating two coverings for each. First the spans
	// that were backed up, then the files in the backup. The latter is a subset
	// when some of the keyranges in the former didn't change since the previous
	// backup. These alternate (backup1 spans, backup1 files, backup2 spans,
	// backup2 files) so they will retain that alternation in the output of
	// OverlapCoveringMerge.
	var maxEndTime hlc.Timestamp
	for _, b := range backups {
		if maxEndTime.Less(b.EndTime) {
			maxEndTime = b.EndTime
		}

		var backupSpanCovering intervalccl.Covering
		for _, s := range b.Spans {
			backupSpanCovering = append(backupSpanCovering, intervalccl.Range{
				Start:   s.Key,
				End:     s.EndKey,
				Payload: importEntry{Span: s, entryType: backupSpan, backup: b},
			})
		}
		backupCoverings = append(backupCoverings, backupSpanCovering)
		var backupFileCovering intervalccl.Covering
		for _, f := range b.Files {
			backupFileCovering = append(backupFileCovering, intervalccl.Range{
				Start: f.Span.Key,
				End:   f.Span.EndKey,
				Payload: importEntry{
					Span:      f.Span,
					entryType: backupFile,
					dir:       b.Dir,
					file:      f,
				},
			})
		}
		backupCoverings = append(backupCoverings, backupFileCovering)
	}

	// Group ranges covered by backups with ones needed to restore the selected
	// tables. Note that this breaks intervals up as necessary to align them.
	// See the function godoc for details.
	importRanges := intervalccl.OverlapCoveringMerge(backupCoverings)

	// Translate the output of OverlapCoveringMerge into requests.
	var requestEntries []importEntry
	for _, importRange := range importRanges {
		needed := false
		var ts hlc.Timestamp
		var files []roachpb.ImportRequest_File
		payloads := importRange.Payload.([]interface{})
		for _, p := range payloads {
			ie := p.(importEntry)
			switch ie.entryType {
			case tableSpan:
				needed = true
			case backupSpan:
				if ts != ie.backup.StartTime {
					return nil, hlc.Timestamp{}, errors.Errorf(
						"no backup covers time [%s,%s) for range [%s,%s) (or backups out of order)",
						ts, ie.backup.StartTime,
						roachpb.Key(importRange.Start), roachpb.Key(importRange.End))
				}
				ts = ie.backup.EndTime
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
		if ts != maxEndTime {
			return nil, hlc.Timestamp{}, errors.Errorf(
				"no backup covers time [%s,%s) for range [%s,%s) (or backups out of order)",
				ts, maxEndTime, roachpb.Key(importRange.Start), roachpb.Key(importRange.End))
		}
		if needed {
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

// presplitRanges concurrently creates the splits described by `input`. It does
// this by finding the middle key, splitting and recursively presplitting the
// resulting left and right hand ranges. NB: The split code assumes that the LHS
// of the resulting ranges is the smaller, so normally you'd split from the
// left, but this method should only be called on empty keyranges, so it's okay.
//
// The `input` parameter expected to be sorted.
func presplitRanges(baseCtx context.Context, db client.DB, input []roachpb.Key) error {
	// TODO(dan): This implementation does nothing to control the maximum
	// parallelization or number of goroutines spawned. Revisit (possibly via a
	// semaphore) if this becomes a problem in practice.

	ctx, span := tracing.ChildSpan(baseCtx, "presplitRanges")
	defer tracing.FinishSpan(span)
	log.Infof(ctx, "presplitting %d ranges", len(input))

	if len(input) == 0 {
		return nil
	}

	// 100 was picked because it's small enough to work with on a 3-node cluster
	// on my laptop and large enough that it only takes a couple minutes to
	// presplit for a ~16000 range dataset.
	// TODO(dan): See if there's some better solution #14798.
	const splitsPerSecond, splitsBurst = 100, 1
	limiter := rate.NewLimiter(splitsPerSecond, splitsBurst)

	g, ctx := errgroup.WithContext(ctx)
	var splitFn func([]roachpb.Key) error
	splitFn = func(splitPoints []roachpb.Key) error {
		if err := limiter.Wait(ctx); err != nil {
			return err
		}

		// Pick the index such that it's 0 if len(splitPoints) == 1.
		splitIdx := len(splitPoints) / 2
		// AdminSplit requires that the key be a valid table key, which means
		// the last byte is a uvarint indicating how much of the end of the key
		// needs to be stripped off to get the key's row prefix. The start keys
		// input to restore don't have this suffix, so make them row sentinels,
		// which means nothing should be stripped (aka appends 0). See
		// EnsureSafeSplitKey for more context.
		splitKey := append([]byte(nil), splitPoints[splitIdx]...)
		splitKey = keys.MakeRowSentinelKey(splitKey)
		if err := db.AdminSplit(ctx, splitKey); err != nil {
			return err
		}

		splitPointsLeft, splitPointsRight := splitPoints[:splitIdx], splitPoints[splitIdx+1:]
		if len(splitPointsLeft) > 0 {
			g.Go(func() error {
				return splitFn(splitPointsLeft)
			})
		}
		if len(splitPointsRight) > 0 {
			// Save a few goroutines by reusing this one.
			return splitFn(splitPointsRight)
		}
		return nil
	}

	g.Go(func() error {
		return splitFn(input)
	})
	return g.Wait()
}

// Write the new descriptors. First the ID -> TableDescriptor for the new table,
// then flip (or initialize) the name -> ID entry so any new queries will use
// the new one.
func restoreTableDescs(ctx context.Context, db client.DB, tables []*sqlbase.TableDescriptor) error {
	ctx, span := tracing.ChildSpan(ctx, "restoreTableDescs")
	defer tracing.FinishSpan(span)
	err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		for _, table := range tables {
			b.CPut(table.GetDescMetadataKey(), sqlbase.WrapDescriptor(table), nil)
			b.CPut(table.GetNameMetadataKey(), table.ID, nil)
		}
		if err := txn.Run(ctx, b); err != nil {
			return err
		}

		for _, table := range tables {
			if err := table.Validate(ctx, txn); err != nil {
				return err
			}
		}
		return nil
	})
	return errors.Wrap(err, "restoring table desc and namespace entries")
}

func restoreJobDescription(restore *parser.Restore, from []string) (string, error) {
	r := parser.Restore{
		AsOf:    restore.AsOf,
		Options: restore.Options,
		Targets: restore.Targets,
		From:    make(parser.Exprs, len(restore.From)),
	}

	for i, f := range from {
		sf, err := storageccl.SanitizeExportStorageURI(f)
		if err != nil {
			return "", err
		}
		r.From[i] = parser.NewDString(sf)
	}

	return r.String(), nil
}

// Restore imports a SQL table (or tables) from sets of non-overlapping sstable
// files.
func Restore(
	ctx context.Context,
	p sql.PlanHookState,
	uris []string,
	targets parser.TargetList,
	opt parser.KVOptions,
	jobLogger *sql.JobLogger,
) (dataSize int64, err error) {

	db := *p.ExecCfg().DB

	if len(targets.Databases) > 0 {
		return 0, errors.Errorf("RESTORE DATABASE is not yet supported " +
			"(but you can use 'RESTORE somedb.*' to restore all backed up tables for a given DB).")
	}

	backupDescs, err := loadBackupDescs(ctx, uris)
	if err != nil {
		return 0, err
	}
	lastBackupDesc := backupDescs[len(backupDescs)-1]

	databasesByID := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor)
	var tables []*sqlbase.TableDescriptor
	{
		// TODO(dan): Plumb the session database down.
		sessionDatabase := ""
		sqlDescs := lastBackupDesc.Descriptors
		var err error
		if sqlDescs, err = descriptorsMatchingTargets(sessionDatabase, sqlDescs, targets); err != nil {
			return 0, err
		}
		for _, desc := range sqlDescs {
			if dbDesc := desc.GetDatabase(); dbDesc != nil {
				databasesByID[dbDesc.ID] = dbDesc
			} else if tableDesc := desc.GetTable(); tableDesc != nil {
				tables = append(tables, tableDesc)
			}
		}
		if len(tables) == 0 {
			return 0, errors.Errorf("no tables found: %s", parser.AsString(targets))
		}
	}

	// Fail fast if the necessary databases don't exist since the below logic
	// leaks table IDs when Restore fails.
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		return reassignParentIDs(ctx, txn, p, databasesByID, tables, opt)
	}); err != nil {
		return 0, err
	}

	// We get the spans of the restoring tables _as they appear in the backup_,
	// that is, in the 'old' keyspace, before we reassign the table IDs.
	spans := spansForAllTableIndexes(tables)

	// Assign new IDs to the tables and update all references to use the new IDs,
	// and get a KeyRewriter to use when importing their raw data.
	//
	// NB: we do this in a standalone transaction, not one that covers the entire
	// restore since restarts would be terrible (and our bulk import primitive
	// are non-transactional), but this does mean if something fails during Import,
	// we've "leaked" the IDs, in that the generator will have been incremented.
	kr, newTableIDs, err := reassignTableIDs(ctx, db, tables, opt)
	if err != nil {
		// We expect user-facing usage errors here, so don't wrapf.
		return 0, err
	}

	// Pivot the backups, which are grouped by time, into requests for import,
	// which are grouped by keyrange.
	importRequests, _, err := makeImportRequests(spans, backupDescs)
	if err != nil {
		return 0, errors.Wrapf(err, "making import requests for %d backups", len(backupDescs))
	}

	for _, desc := range newTableIDs {
		jobLogger.Job.DescriptorIDs = append(jobLogger.Job.DescriptorIDs, desc)
	}
	if err := jobLogger.Created(ctx); err != nil {
		return 0, err
	}
	if err := jobLogger.Started(ctx); err != nil {
		return 0, err
	}

	progressLogger := jobProgressLogger{
		jobLogger:   jobLogger,
		totalChunks: len(importRequests),
	}

	// The Import (and resulting WriteBatch) requests made below run on
	// leaseholders, so presplit the ranges to balance the work among many
	// nodes
	splitKeys := make([]roachpb.Key, len(importRequests))
	for i, r := range importRequests {
		var ok bool
		splitKeys[i], ok = kr.RewriteKey(append([]byte(nil), r.Key...))
		if !ok {
			return 0, errors.Errorf("failed to rewrite key: %s", r.Key)
		}
	}
	if err := presplitRanges(ctx, db, splitKeys); err != nil {
		return 0, errors.Wrapf(err, "presplitting %d ranges", len(importRequests))
	}
	{
		newSpans := spansForAllTableIndexes(tables)
		g, gCtx := errgroup.WithContext(ctx)
		for i := range newSpans {
			span := newSpans[i]
			g.Go(func() error {
				req := &roachpb.AdminScatterRequest{
					Span: roachpb.Span{Key: span.Key, EndKey: span.EndKey},
				}
				res, pErr := client.SendWrapped(gCtx, db.GetSender(), req)
				if pErr != nil {
					return pErr.GoError()
				}
				// Scatter is best-effort, so log why any individual ranges
				// didn't get scattered.
				for _, r := range res.(*roachpb.AdminScatterResponse).Ranges {
					if r.Error != nil {
						log.Warningf(ctx, "error scattering range [%s,%s): %+v",
							r.Span.Key, r.Span.EndKey, r.Error.GoError())
					}
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return 0, errors.Wrapf(err, "scattering %d ranges", len(importRequests))
		}
	}

	// We're already limiting these on the server-side, but sending all the
	// Import requests at once would fill up distsender/grpc/something and cause
	// all sorts of badness (node liveness timeouts leading to mass leaseholder
	// transfers, poor performance on SQL workloads, etc) as well as log spam
	// about slow distsender requests. Rate limit them here, too.
	//
	// Use the number of nodes in the cluster as the number of outstanding
	// Import requests for the rate limiting. TODO(dan): This is very
	// conservative, see if we can bump it back up by rate limiting WriteBatch.
	//
	// TODO(dan): Make this limiting per node.
	//
	// TODO(dan): See if there's some better solution than rate-limiting #14798.
	maxConcurrentImports := clusterNodeCount(p.ExecCfg().Gossip)
	importsSem := make(chan struct{}, maxConcurrentImports)

	mu := struct {
		syncutil.Mutex
		dataSize int64
	}{}

	g, gCtx := errgroup.WithContext(ctx)
	for i := range importRequests {
		select {
		case importsSem <- struct{}{}:
		case <-ctx.Done():
			return 0, ctx.Err()
		}

		ir := importRequests[i]
		g.Go(func() error {
			defer func() { <-importsSem }()

			res, err := Import(gCtx, db, ir.Key, ir.EndKey, ir.files, kr)
			if err != nil {
				return err
			}
			mu.Lock()
			mu.dataSize += res.DataSize
			mu.Unlock()
			if err := progressLogger.chunkFinished(gCtx); err != nil {
				// Errors while updating progress are not important enough to merit
				// failing the entire restore.
				log.Errorf(ctx, "RESTORE ignoring error while updating progress on job %d (%s): %+v",
					jobLogger.JobID(), jobLogger.Job.Description, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		// This leaves the data that did get imported in case the user wants to
		// retry.
		// TODO(dan): Build tooling to allow a user to restart a failed restore.
		return 0, errors.Wrapf(err, "importing %d ranges", len(importRequests))
	}

	// Write the new TableDescriptors and flip the namespace entries over to
	// them. After this call, any queries on a table will be served by the newly
	// restored data.
	if err := restoreTableDescs(ctx, db, tables); err != nil {
		return 0, errors.Wrapf(err, "restoring %d TableDescriptors", len(tables))
	}

	// TODO(dan): Delete any old table data here. The first version of restore
	// assumes that it's operating on a new cluster. If it's not empty,
	// everything works but the table data is left abandoned.

	return mu.dataSize, nil
}

func restorePlanHook(
	baseCtx context.Context, stmt parser.Statement, p sql.PlanHookState,
) (func() ([]parser.Datums, error), sql.ResultColumns, error) {
	restore, ok := stmt.(*parser.Restore)
	if !ok {
		return nil, nil, nil
	}
	if err := utilccl.CheckEnterpriseEnabled("RESTORE"); err != nil {
		return nil, nil, err
	}

	if err := p.RequireSuperUser("RESTORE"); err != nil {
		return nil, nil, err
	}

	fromFn, err := p.TypeAsStringArray(&restore.From)
	if err != nil {
		return nil, nil, err
	}

	header := sql.ResultColumns{
		{Name: "job_id", Typ: parser.TypeInt},
		{Name: "status", Typ: parser.TypeString},
		{Name: "fraction_completed", Typ: parser.TypeFloat},
		{Name: "bytes", Typ: parser.TypeInt},
	}
	fn := func() ([]parser.Datums, error) {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(baseCtx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		from := fromFn()
		description, err := restoreJobDescription(restore, from)
		if err != nil {
			return nil, err
		}
		jobLogger := sql.NewJobLogger(p.ExecCfg().DB, p.LeaseMgr(), sql.JobRecord{
			Description: description,
			Username:    p.User(),
			Details:     sql.RestoreJobDetails{},
		})
		dataSize, err := Restore(
			ctx,
			p,
			from,
			restore.Targets,
			restore.Options,
			&jobLogger,
		)
		if err != nil {
			jobLogger.Failed(ctx, err)
			return nil, err
		}
		if err := jobLogger.Succeeded(ctx); err != nil {
			// An error while marking the job as successful is not important enough to
			// merit failing the entire restore.
			log.Errorf(ctx, "RESTORE ignoring error while marking job %d (%s) as successful: %+v",
				jobLogger.JobID(), description, err)
		}
		// TODO(benesch): emit periodic progress updates once we have the
		// infrastructure to stream responses.
		ret := []parser.Datums{{
			parser.NewDInt(parser.DInt(*jobLogger.JobID())),
			parser.NewDString(string(sql.JobStatusSucceeded)),
			parser.NewDFloat(parser.DFloat(1.0)),
			parser.NewDInt(parser.DInt(dataSize)),
		}}
		return ret, nil
	}
	return fn, header, nil
}

func init() {
	sql.AddPlanHook(restorePlanHook)
}
