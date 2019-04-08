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
	"math"
	"runtime"
	"sort"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/ccl/gossipccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// TableRewriteMap maps old table IDs to new table and parent IDs.
type TableRewriteMap map[sqlbase.ID]*jobspb.RestoreDetails_TableRewrite

const (
	restoreOptIntoDB               = "into_db"
	restoreOptSkipMissingFKs       = "skip_missing_foreign_keys"
	restoreOptSkipMissingSequences = "skip_missing_sequences"
)

var restoreOptionExpectValues = map[string]sql.KVStringOptValidate{
	restoreOptIntoDB:               sql.KVStringOptRequireValue,
	restoreOptSkipMissingFKs:       sql.KVStringOptRequireNoValue,
	restoreOptSkipMissingSequences: sql.KVStringOptRequireNoValue,
}

func loadBackupDescs(
	ctx context.Context, uris []string, settings *cluster.Settings,
) ([]BackupDescriptor, error) {
	backupDescs := make([]BackupDescriptor, len(uris))

	for i, uri := range uris {
		desc, err := ReadBackupDescriptorFromURI(ctx, uri, settings)
		if err != nil {
			return nil, pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
				"failed to read backup descriptor")
		}
		backupDescs[i] = desc
	}
	if len(backupDescs) == 0 {
		return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError, "no backups found")
	}
	return backupDescs, nil
}

func loadSQLDescsFromBackupsAtTime(
	backupDescs []BackupDescriptor, asOf hlc.Timestamp,
) ([]sqlbase.Descriptor, BackupDescriptor) {
	lastBackupDesc := backupDescs[len(backupDescs)-1]

	if asOf.IsEmpty() {
		return lastBackupDesc.Descriptors, lastBackupDesc
	}

	for _, b := range backupDescs {
		if asOf.Less(b.StartTime) {
			break
		}
		lastBackupDesc = b
	}
	if len(lastBackupDesc.DescriptorChanges) == 0 {
		return lastBackupDesc.Descriptors, lastBackupDesc
	}

	byID := make(map[sqlbase.ID]*sqlbase.Descriptor, len(lastBackupDesc.Descriptors))
	for _, rev := range lastBackupDesc.DescriptorChanges {
		if asOf.Less(rev.Time) {
			break
		}
		if rev.Desc == nil {
			delete(byID, rev.ID)
		} else {
			byID[rev.ID] = rev.Desc
		}
	}

	allDescs := make([]sqlbase.Descriptor, 0, len(byID))
	for _, desc := range byID {
		if t := desc.GetTable(); t != nil {
			// A table revisions may have been captured before it was in a DB that is
			// backed up -- if the DB is missing, filter the table.
			if byID[t.ParentID] == nil {
				continue
			}
		}
		allDescs = append(allDescs, *desc)
	}
	return allDescs, lastBackupDesc
}

func selectTargets(
	ctx context.Context,
	p sql.PlanHookState,
	backupDescs []BackupDescriptor,
	targets tree.TargetList,
	asOf hlc.Timestamp,
) ([]sqlbase.Descriptor, []*sqlbase.DatabaseDescriptor, error) {
	allDescs, lastBackupDesc := loadSQLDescsFromBackupsAtTime(backupDescs, asOf)
	matched, err := descriptorsMatchingTargets(ctx,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, targets)
	if err != nil {
		return nil, nil, err
	}

	seenTable := false
	for _, desc := range matched.descs {
		if desc.GetTable() != nil {
			seenTable = true
			break
		}
	}
	if !seenTable {
		return nil, nil, errors.Errorf("no tables found: %s", tree.ErrString(&targets))
	}

	if lastBackupDesc.FormatVersion >= BackupFormatDescriptorTrackingVersion {
		if err := matched.checkExpansions(lastBackupDesc.CompleteDbs); err != nil {
			return nil, nil, err
		}
	}

	return matched.descs, matched.requestedDBs, nil
}

// rewriteViewQueryDBNames rewrites the passed table's ViewQuery replacing all
// non-empty db qualifiers with `newDB`.
//
// TODO: this AST traversal misses tables named in strings (#24556).
func rewriteViewQueryDBNames(table *sqlbase.TableDescriptor, newDB string) error {
	stmt, err := parser.ParseOne(table.ViewQuery)
	if err != nil {
		return pgerror.Wrapf(err, pgerror.CodeSyntaxError,
			"failed to parse underlying query from view %q", table.Name)
	}
	// Re-format to change all DB names to `newDB`.
	f := tree.NewFmtCtx(tree.FmtParsable)
	f.SetReformatTableNames(func(ctx *tree.FmtCtx, tn *tree.TableName) {
		// empty catalog e.g. ``"".information_schema.tables` should stay empty.
		if tn.CatalogName != "" {
			tn.CatalogName = tree.Name(newDB)
		}
		ctx.WithReformatTableNames(nil, func() {
			ctx.FormatNode(tn)
		})
	})
	f.FormatNode(stmt.AST)
	table.ViewQuery = f.CloseAndGetString()
	return nil
}

// allocateTableRewrites determines the new ID and parentID (a "TableRewrite")
// for each table in sqlDescs and returns a mapping from old ID to said
// TableRewrite. It first validates that the provided sqlDescs can be restored
// into their original database (or the database specified in opst) to avoid
// leaking table IDs if we can be sure the restore would fail.
func allocateTableRewrites(
	ctx context.Context,
	p sql.PlanHookState,
	sqlDescs []sqlbase.Descriptor,
	restoreDBs []*sqlbase.DatabaseDescriptor,
	opts map[string]string,
) (TableRewriteMap, error) {
	tableRewrites := make(TableRewriteMap)
	overrideDB, renaming := opts[restoreOptIntoDB]

	restoreDBNames := make(map[string]*sqlbase.DatabaseDescriptor, len(restoreDBs))
	for _, db := range restoreDBs {
		restoreDBNames[db.Name] = db
	}

	if len(restoreDBNames) > 0 && renaming {
		return nil, errors.Errorf("cannot use %q option when restoring database(s)", restoreOptIntoDB)
	}

	databasesByID := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor)
	tablesByID := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	for _, desc := range sqlDescs {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			databasesByID[dbDesc.ID] = dbDesc
		} else if tableDesc := desc.GetTable(); tableDesc != nil {
			tablesByID[tableDesc.ID] = tableDesc
		}
	}

	// The logic at the end of this function leaks table IDs, so fail fast if
	// we can be certain the restore will fail.

	// Fail fast if the tables to restore are incompatible with the specified
	// options.
	// Check that foreign key targets exist.
	for _, table := range tablesByID {
		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			if index.ForeignKey.IsSet() {
				to := index.ForeignKey.Table
				if _, ok := tablesByID[to]; !ok {
					if _, ok := opts[restoreOptSkipMissingFKs]; !ok {
						return errors.Errorf(
							"cannot restore table %q without referenced table %d (or %q option)",
							table.Name, to, restoreOptSkipMissingFKs,
						)
					}
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}

		// Check that referenced sequences exist.
		for i := range table.Columns {
			col := &table.Columns[i]
			for _, seqID := range col.UsesSequenceIds {
				if _, ok := tablesByID[seqID]; !ok {
					if _, ok := opts[restoreOptSkipMissingSequences]; !ok {
						return nil, errors.Errorf(
							"cannot restore table %q without referenced sequence %d (or %q option)",
							table.Name, seqID, restoreOptSkipMissingSequences,
						)
					}
				}
			}
		}
	}

	needsNewParentIDs := make(map[string][]sqlbase.ID)

	// Fail fast if the necessary databases don't exist or are otherwise
	// incompatible with this restore.
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Check that any DBs being restored do _not_ exist.
		for name := range restoreDBNames {
			existingDatabaseID, err := txn.Get(ctx, sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, name))
			if err != nil {
				return err
			}
			if existingDatabaseID.Value != nil {
				return errors.Errorf("database %q already exists", name)
			}
		}

		for _, table := range tablesByID {
			var targetDB string
			if renaming {
				targetDB = overrideDB
			} else {
				database, ok := databasesByID[table.ParentID]
				if !ok {
					return errors.Errorf("no database with ID %d in backup for table %q",
						table.ParentID, table.Name)
				}
				targetDB = database.Name
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], table.ID)
			} else {
				var parentID sqlbase.ID
				{
					existingDatabaseID, err := txn.Get(ctx, sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, targetDB))
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
					parentID = sqlbase.ID(newParentID)
				}

				// Check that the table name is _not_ in use.
				// This would fail the CPut later anyway, but this yields a prettier error.
				if err := CheckTableExists(ctx, txn, parentID, table.Name); err != nil {
					return err
				}

				// Check privileges. These will be checked again in the transaction
				// that actually writes the new table descriptors.
				{
					parentDB, err := sqlbase.GetDatabaseDescFromID(ctx, txn, parentID)
					if err != nil {
						return pgerror.NewAssertionErrorWithWrappedErrf(err,
							"failed to lookup parent DB %d", log.Safe(parentID))
					}

					if err := p.CheckPrivilege(ctx, parentDB, privilege.CREATE); err != nil {
						return err
					}
				}
				// Create the table rewrite with the new parent ID. We've done all the
				// up-front validation that we can.
				tableRewrites[table.ID] = &jobspb.RestoreDetails_TableRewrite{ParentID: parentID}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Allocate new IDs for each database and table.
	//
	// NB: we do this in a standalone transaction, not one that covers the
	// entire restore since restarts would be terrible (and our bulk import
	// primitive are non-transactional), but this does mean if something fails
	// during restore we've "leaked" the IDs, in that the generator will have
	// been incremented.
	//
	// NB: The ordering of the new IDs must be the same as the old ones,
	// otherwise the keys may sort differently after they're rekeyed. We could
	// handle this by chunking the AddSSTable calls more finely in Import, but
	// it would be a big performance hit.

	for _, db := range restoreDBs {
		newID, err := sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
		if err != nil {
			return nil, err
		}
		tableRewrites[db.ID] = &jobspb.RestoreDetails_TableRewrite{TableID: newID}
		for _, tableID := range needsNewParentIDs[db.Name] {
			tableRewrites[tableID] = &jobspb.RestoreDetails_TableRewrite{ParentID: newID}
		}
	}

	tables := make([]*sqlbase.TableDescriptor, 0, len(tablesByID))
	for _, table := range tablesByID {
		tables = append(tables, table)
	}
	sort.Sort(sqlbase.TableDescriptors(tables))
	for _, table := range tables {
		newTableID, err := sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
		if err != nil {
			return nil, err
		}
		tableRewrites[table.ID].TableID = newTableID
	}

	return tableRewrites, nil
}

// CheckTableExists returns an error if a table already exists with given
// parent and name.
func CheckTableExists(
	ctx context.Context, txn *client.Txn, parentID sqlbase.ID, name string,
) error {
	nameKey := sqlbase.MakeNameMetadataKey(parentID, name)
	res, err := txn.Get(ctx, nameKey)
	if err != nil {
		return err
	}
	if res.Exists() {
		return sqlbase.NewRelationAlreadyExistsError(name)
	}
	return nil
}

// RewriteTableDescs mutates tables to match the ID and privilege specified
// in tableRewrites, as well as adjusting cross-table references to use the
// new IDs. overrideDB can be specified to set database names in views.
func RewriteTableDescs(
	tables []*sqlbase.TableDescriptor, tableRewrites TableRewriteMap, overrideDB string,
) error {
	for _, table := range tables {
		tableRewrite, ok := tableRewrites[table.ID]
		if !ok {
			return errors.Errorf("missing table rewrite for table %d", table.ID)
		}
		if table.IsView() && overrideDB != "" {
			// restore checks that all dependencies are also being restored, but if
			// the restore is overriding the destination database, qualifiers in the
			// view query string may be wrong. Since the destination override is
			// applied to everything being restored, anything the view query
			// references will be in the override DB post-restore, so all database
			// qualifiers in the view query should be replaced with overrideDB.
			if err := rewriteViewQueryDBNames(table, overrideDB); err != nil {
				return err
			}
		}

		table.ID = tableRewrite.TableID
		table.ParentID = tableRewrite.ParentID

		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			// Verify that for any interleaved index being restored, the interleave
			// parent is also being restored. Otherwise, the interleave entries in the
			// restored IndexDescriptors won't have anything to point to.
			// TODO(dan): It seems like this restriction could be lifted by restoring
			// stub TableDescriptors for the missing interleave parents.
			for j, a := range index.Interleave.Ancestors {
				ancestorRewrite, ok := tableRewrites[a.TableID]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave parent %d", table.Name, a.TableID,
					)
				}
				index.Interleave.Ancestors[j].TableID = ancestorRewrite.TableID
			}
			for j, c := range index.InterleavedBy {
				childRewrite, ok := tableRewrites[c.Table]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave child table %d", table.Name, c.Table,
					)
				}
				index.InterleavedBy[j].Table = childRewrite.TableID
			}

			if index.ForeignKey.IsSet() {
				to := index.ForeignKey.Table
				if indexRewrite, ok := tableRewrites[to]; ok {
					index.ForeignKey.Table = indexRewrite.TableID
				} else {
					// If indexRewrite doesn't exist, the user has specified
					// restoreOptSkipMissingFKs. Error checking in the case the user hasn't has
					// already been done in allocateTableRewrites.
					index.ForeignKey = sqlbase.ForeignKeyReference{}
				}

				// TODO(dt): if there is an existing (i.e. non-restoring) table with
				// a db and name matching the one the FK pointed to at backup, should
				// we update the FK to point to it?
			}

			origRefs := index.ReferencedBy
			index.ReferencedBy = nil
			for _, ref := range origRefs {
				if refRewrite, ok := tableRewrites[ref.Table]; ok {
					ref.Table = refRewrite.TableID
					index.ReferencedBy = append(index.ReferencedBy, ref)
				}
			}
			return nil
		}); err != nil {
			return err
		}

		for i, dest := range table.DependsOn {
			if depRewrite, ok := tableRewrites[dest]; ok {
				table.DependsOn[i] = depRewrite.TableID
			} else {
				return errors.Errorf(
					"cannot restore %q without restoring referenced table %d in same operation",
					table.Name, dest)
			}
		}
		origRefs := table.DependedOnBy
		table.DependedOnBy = nil
		for _, ref := range origRefs {
			if refRewrite, ok := tableRewrites[ref.ID]; ok {
				ref.ID = refRewrite.TableID
				table.DependedOnBy = append(table.DependedOnBy, ref)
			}
		}

		// Rewrite sequence references in column descriptors.
		for idx := range table.Columns {
			var newSeqRefs []sqlbase.ID
			col := &table.Columns[idx]
			for _, seqID := range col.UsesSequenceIds {
				if rewrite, ok := tableRewrites[seqID]; ok {
					newSeqRefs = append(newSeqRefs, rewrite.TableID)
				} else {
					// The referenced sequence isn't being restored.
					// Strip the DEFAULT expression and sequence references.
					// To get here, the user must have specified 'skip_missing_sequences' --
					// otherwise, would have errored out in allocateTableRewrites.
					newSeqRefs = []sqlbase.ID{}
					col.DefaultExpr = nil
					break
				}
			}
			col.UsesSequenceIds = newSeqRefs
		}

		// since this is a "new" table in eyes of new cluster, any leftover change
		// lease is obviously bogus (plus the nodeID is relative to backup cluster).
		table.Lease = nil
	}
	return nil
}

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
	dir  roachpb.ExportStorage
	file BackupDescriptor_File

	// Only set if entryType is request
	files []roachpb.ImportRequest_File

	// for progress tracking we assign the spans numbers as they can be executed
	// out-of-order based on splitAndScatter's scheduling.
	progressIdx int
}

func errOnMissingRange(span intervalccl.Range, start, end hlc.Timestamp) error {
	return errors.Errorf(
		"no backup covers time [%s,%s) for range [%s,%s) (or backups out of order)",
		start, end, roachpb.Key(span.Start), roachpb.Key(span.End),
	)
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
// /file1 and /file3, the other restoring [C, D) out of /file2 and /file5.
// Nothing is restored out of /file3 and only part of /file1 is used.
//
// NB: All grouping operates in the pre-rewrite keyspace, meaning the keyranges
// as they were backed up, not as they're being restored.
//
// If a span is not covered, the onMissing function is called with the span and
// time missing to determine what error, if any, should be returned.
func makeImportSpans(
	tableSpans []roachpb.Span,
	backups []BackupDescriptor,
	lowWaterMark roachpb.Key,
	onMissing func(span intervalccl.Range, start, end hlc.Timestamp) error,
) ([]importEntry, hlc.Timestamp, error) {
	// Put the covering for the already-completed spans into the
	// OverlapCoveringMerge input first. Payloads are returned in the same order
	// that they appear in the input; putting the completedSpan first means we'll
	// see it first when iterating over the output of OverlapCoveringMerge and
	// avoid doing unnecessary work.
	completedCovering := intervalccl.Covering{
		{
			Start:   []byte(keys.MinKey),
			End:     []byte(lowWaterMark),
			Payload: importEntry{entryType: completedSpan},
		},
	}

	// Put the merged table data covering into the OverlapCoveringMerge input
	// next.
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

	backupCoverings := []intervalccl.Covering{completedCovering, tableSpanCovering}

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

		var backupNewSpanCovering intervalccl.Covering
		for _, s := range b.IntroducedSpans {
			backupNewSpanCovering = append(backupNewSpanCovering, intervalccl.Range{
				Start:   s.Key,
				End:     s.EndKey,
				Payload: importEntry{Span: s, entryType: backupSpan, start: hlc.Timestamp{}, end: b.StartTime},
			})
		}
		backupCoverings = append(backupCoverings, backupNewSpanCovering)

		var backupSpanCovering intervalccl.Covering
		for _, s := range b.Spans {
			backupSpanCovering = append(backupSpanCovering, intervalccl.Range{
				Start:   s.Key,
				End:     s.EndKey,
				Payload: importEntry{Span: s, entryType: backupSpan, start: b.StartTime, end: b.EndTime},
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
	db *client.DB,
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
			if err := db.AdminSplit(ctx, chunkKey, chunkKey); err != nil {
				return err
			}

			log.VEventf(restoreCtx, 1, "scattering chunk %d of %d", idx, len(importSpanChunks))
			scatterReq := &roachpb.AdminScatterRequest{
				RequestHeader: roachpb.RequestHeaderFromSpan(roachpb.Span{Key: chunkKey, EndKey: chunkKey.Next()}),
			}
			if _, pErr := client.SendWrapped(ctx, db.NonTransactionalSender(), scatterReq); pErr != nil {
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
					if err := db.AdminSplit(ctx, newSpanKey, newSpanKey); err != nil {
						return err
					}

					log.VEventf(restoreCtx, 1, "scattering %d of %d", idx, len(importSpans))
					scatterReq := &roachpb.AdminScatterRequest{
						RequestHeader: roachpb.RequestHeaderFromSpan(roachpb.Span{Key: newSpanKey, EndKey: newSpanKey.Next()}),
					}
					if _, pErr := client.SendWrapped(ctx, db.NonTransactionalSender(), scatterReq); pErr != nil {
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
	txn *client.Txn,
	databases []*sqlbase.DatabaseDescriptor,
	tables []*sqlbase.TableDescriptor,
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
			// TODO(dt): support restoring privs.
			desc.Privileges = sqlbase.NewDefaultPrivilegeDescriptor()
			wroteDBs[desc.ID] = desc
			b.CPut(sqlbase.MakeDescMetadataKey(desc.ID), sqlbase.WrapDescriptor(desc), nil)
			b.CPut(sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, desc.Name), desc.ID, nil)
		}
		for _, table := range tables {
			if wrote, ok := wroteDBs[table.ParentID]; ok {
				table.Privileges = wrote.GetPrivileges()
			} else {
				parentDB, err := sqlbase.GetDatabaseDescFromID(ctx, txn, table.ParentID)
				if err != nil {
					return pgerror.NewAssertionErrorWithWrappedErrf(err,
						"failed to lookup parent DB %d", log.Safe(table.ParentID))
				}
				// TODO(mberhault): CheckPrivilege wants a planner.
				if err := sql.CheckPrivilegeForUser(ctx, user, parentDB, privilege.CREATE); err != nil {
					return err
				}
				// Default is to copy privs from restoring parent db, like CREATE TABLE.
				// TODO(dt): Make this more configurable.
				table.Privileges = parentDB.GetPrivileges()
			}
			b.CPut(table.GetDescMetadataKey(), sqlbase.WrapDescriptor(table), nil)
			b.CPut(table.GetNameMetadataKey(), table.ID, nil)
		}
		for _, kv := range extra {
			b.InitPut(kv.Key, &kv.Value, false)
		}
		if err := txn.Run(ctx, b); err != nil {
			if _, ok := errors.Cause(err).(*roachpb.ConditionFailedError); ok {
				return pgerror.NewErrorf(pgerror.CodeDuplicateObjectError, "table already exists")
			}
			return err
		}

		for _, table := range tables {
			if err := table.Validate(ctx, txn, settings); err != nil {
				return pgerror.NewAssertionErrorWithWrappedErrf(err,
					"validate table %d", log.Safe(table.ID))
			}
		}
		return nil
	}()
	return pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
		"restoring table desc and namespace entries")
}

func restoreJobDescription(
	restore *tree.Restore, from []string, opts map[string]string,
) (string, error) {
	r := &tree.Restore{
		AsOf:    restore.AsOf,
		Options: optsToKVOptions(opts),
		Targets: restore.Targets,
		From:    make(tree.Exprs, len(restore.From)),
	}

	for i, f := range from {
		sf, err := storageccl.SanitizeExportStorageURI(f)
		if err != nil {
			return "", err
		}
		r.From[i] = tree.NewDString(sf)
	}

	return tree.AsStringWithFlags(r, tree.FmtAlwaysQualifyTableNames), nil
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
	newKey, rewritten, err := kr.RewriteKey(append([]byte(nil), key...))
	if err != nil {
		return nil, pgerror.NewAssertionErrorWithWrappedErrf(err,
			"could not rewrite span start key: %s", key)
	}
	if !rewritten && bytes.Equal(newKey, key) {
		// if nothing was changed, we didn't match the top-level key at all.
		return nil, pgerror.NewAssertionErrorf(
			"no rewrite for span start key: %s", key)
	}
	// Modify all spans that begin at the primary index to instead begin at the
	// start of the table. That is, change a span start key from /Table/51/1 to
	// /Table/51. Otherwise a permanently empty span at /Table/51-/Table/51/1
	// will be created.
	if b, id, idx, err := sqlbase.DecodeTableIDIndexID(newKey); err != nil {
		return nil, pgerror.NewAssertionErrorWithWrappedErrf(err,
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
	db *client.DB,
	gossip *gossip.Gossip,
	backupDescs []BackupDescriptor,
	endTime hlc.Timestamp,
	sqlDescs []sqlbase.Descriptor,
	tableRewrites TableRewriteMap,
	overrideDB string,
	job *jobs.Job,
	resultsCh chan<- tree.Datums,
) (roachpb.BulkOpSummary, []*sqlbase.DatabaseDescriptor, []*sqlbase.TableDescriptor, error) {
	// A note about contexts and spans in this method: the top-level context
	// `restoreCtx` is used for orchestration logging. All operations that carry
	// out work get their individual contexts.

	mu := struct {
		syncutil.Mutex
		res               roachpb.BulkOpSummary
		requestsCompleted []bool
		highWaterMark     int
	}{
		highWaterMark: -1,
	}

	var databases []*sqlbase.DatabaseDescriptor
	var tables []*sqlbase.TableDescriptor
	var oldTableIDs []sqlbase.ID
	for _, desc := range sqlDescs {
		if tableDesc := desc.GetTable(); tableDesc != nil {
			tables = append(tables, tableDesc)
			oldTableIDs = append(oldTableIDs, tableDesc.ID)
		}
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			if rewrite, ok := tableRewrites[dbDesc.ID]; ok {
				dbDesc.ID = rewrite.TableID
				databases = append(databases, dbDesc)
			}
		}
	}

	log.Eventf(restoreCtx, "starting restore for %d tables", len(tables))

	// We get the spans of the restoring tables _as they appear in the backup_,
	// that is, in the 'old' keyspace, before we reassign the table IDs.
	spans := spansForAllTableIndexes(tables, nil)

	// Assign new IDs and privileges to the tables, and update all references to
	// use the new IDs.
	if err := RewriteTableDescs(tables, tableRewrites, overrideDB); err != nil {
		return mu.res, nil, nil, err
	}

	{
		// Disable merging for the table IDs being restored into. We don't want the
		// merge queue undoing the splits performed during RESTORE.
		tableIDs := make([]uint32, 0, len(tables))
		for _, t := range tables {
			tableIDs = append(tableIDs, uint32(t.ID))
		}
		disableCtx, cancel := context.WithCancel(restoreCtx)
		defer cancel()
		gossipccl.DisableMerges(disableCtx, gossip, tableIDs)
	}

	// Get TableRekeys to use when importing raw data.
	var rekeys []roachpb.ImportRequest_TableRekey
	for i := range tables {
		newDescBytes, err := protoutil.Marshal(sqlbase.WrapDescriptor(tables[i]))
		if err != nil {
			return mu.res, nil, nil, pgerror.NewAssertionErrorWithWrappedErrf(err,
				"marshaling descriptor")
		}
		rekeys = append(rekeys, roachpb.ImportRequest_TableRekey{
			OldID:   uint32(oldTableIDs[i]),
			NewDesc: newDescBytes,
		})
	}
	kr, err := storageccl.MakeKeyRewriterFromRekeys(rekeys)
	if err != nil {
		return mu.res, nil, nil, err
	}

	// Pivot the backups, which are grouped by time, into requests for import,
	// which are grouped by keyrange.
	highWaterMark := job.Progress().Details.(*jobspb.Progress_Restore).Restore.HighWater
	importSpans, _, err := makeImportSpans(spans, backupDescs, highWaterMark, errOnMissingRange)
	if err != nil {
		return mu.res, nil, nil, pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
			"making import requests for %d backups", len(backupDescs))
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
	// and bounds the number of empty ranges crated if the RESTORE fails (or is
	// canceled).
	const presplitLeadLimit = 10
	readyForImportCh := make(chan importEntry, presplitLeadLimit)
	g.GoCtx(func(ctx context.Context) error {
		defer close(readyForImportCh)
		return splitAndScatter(ctx, db, kr, numClusterNodes, importSpans, readyForImportCh)
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

				importRes, pErr := client.SendWrapped(ctx, db.NonTransactionalSender(), importRequest)
				if pErr != nil {
					return pgerror.Wrapf(pErr.GoError(), pgerror.CodeDataExceptionError,
						"importing span %v", importRequest.DataSpan)

				}

				mu.Lock()
				mu.res.Add(importRes.(*roachpb.ImportResponse).Imported)

				// Assert that we're actually marking the correct span done. See #23977.
				if !importSpans[idx].Key.Equal(importRequest.DataSpan.Key) {
					mu.Unlock()
					return pgerror.NewErrorf(pgerror.CodeDataExceptionError,
						"request %d for span %v (to %v) does not match import span for same idx: %v",
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
		return mu.res, nil, nil, pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
			"importing %d ranges", len(importSpans))
	}

	return mu.res, databases, tables, nil
}

// RestoreHeader is the header for RESTORE stmt results.
var RestoreHeader = sqlbase.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "status", Typ: types.String},
	{Name: "fraction_completed", Typ: types.Float},
	{Name: "rows", Typ: types.Int},
	{Name: "index_entries", Typ: types.Int},
	{Name: "system_records", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
}

// restorePlanHook implements sql.PlanHookFn.
func restorePlanHook(
	_ context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	restoreStmt, ok := stmt.(*tree.Restore)
	if !ok {
		return nil, nil, nil, false, nil
	}

	fromFn, err := p.TypeAsStringArray(restoreStmt.From, "RESTORE")
	if err != nil {
		return nil, nil, nil, false, err
	}

	optsFn, err := p.TypeAsStringOpts(restoreStmt.Options, restoreOptionExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "RESTORE",
		); err != nil {
			return err
		}

		if err := p.RequireSuperUser(ctx, "RESTORE"); err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("RESTORE cannot be used inside a transaction")
		}

		// Older nodes don't know about many new fields and flags, e.g. as-of-time,
		// and our testing does not comprehensively cover mixed-version clusters.
		// Refusing to initiate RESTOREs on a new node while old nodes may evaluate
		// the RPCs it issues or even try to resume the RESTORE job and mishandle it
		// avoid any potential unexepcted behavior. This errs on the side of being too
		// restrictive, but an operator can still send the job to the remaining 1.x
		// nodes if needed. VersionClearRange was introduced after many of these
		// fields and the refactors to how jobs were saved and resumed, though we may
		// want to bump this to 2.0 for simplicity.
		if !p.ExecCfg().Settings.Version.IsActive(cluster.VersionClearRange) {
			return errors.Errorf(
				"running RESTORE on a 2.x node requires cluster version >= %s",
				cluster.VersionByKey(cluster.VersionClearRange).String(),
			)
		}

		from, err := fromFn()
		if err != nil {
			return err
		}
		var endTime hlc.Timestamp
		if restoreStmt.AsOf.Expr != nil {
			var err error
			endTime, err = p.EvalAsOfTimestamp(restoreStmt.AsOf)
			if err != nil {
				return err
			}
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}
		return doRestorePlan(ctx, restoreStmt, p, from, endTime, opts, resultsCh)
	}
	return fn, RestoreHeader, nil, false, nil
}

func doRestorePlan(
	ctx context.Context,
	restoreStmt *tree.Restore,
	p sql.PlanHookState,
	from []string,
	endTime hlc.Timestamp,
	opts map[string]string,
	resultsCh chan<- tree.Datums,
) error {
	backupDescs, err := loadBackupDescs(ctx, from, p.ExecCfg().Settings)
	if err != nil {
		return err
	}

	if !endTime.IsEmpty() {
		ok := false
		for _, b := range backupDescs {
			// Find the backup that covers the requested time.
			if b.StartTime.Less(endTime) && !b.EndTime.Less(endTime) {
				ok = true
				// Ensure that the backup actually has revision history.
				if b.MVCCFilter != MVCCFilter_All {
					return errors.Errorf(
						"incompatible RESTORE timestamp: BACKUP for requested time  needs option '%s'", backupOptRevisionHistory,
					)
				}
				// Ensure that the revision history actually covers the requested time -
				// while the BACKUP's start and end might contain the requested time for
				// example if start time is 0 (full backup), the revision history was
				// only captured since the GC window. Note that the RevisionStartTime is
				// the latest for ranges backed up.
				if !b.RevisionStartTime.Less(endTime) {
					return errors.Errorf(
						"incompatible RESTORE timestamp: BACKUP for requested time only has revision history from %v", b.RevisionStartTime,
					)
				}
			}
		}
		if !ok {
			return errors.Errorf(
				"incompatible RESTORE timestamp: supplied backups do not cover requested time",
			)
		}
	}

	sqlDescs, restoreDBs, err := selectTargets(ctx, p, backupDescs, restoreStmt.Targets, endTime)
	if err != nil {
		return err
	}

	tableRewrites, err := allocateTableRewrites(ctx, p, sqlDescs, restoreDBs, opts)
	if err != nil {
		return err
	}
	description, err := restoreJobDescription(restoreStmt, from, opts)
	if err != nil {
		return err
	}

	var tables []*sqlbase.TableDescriptor
	for _, desc := range sqlDescs {
		if tableDesc := desc.GetTable(); tableDesc != nil {
			tables = append(tables, tableDesc)
		}
	}
	if err := RewriteTableDescs(tables, tableRewrites, opts[restoreOptIntoDB]); err != nil {
		return err
	}

	_, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, resultsCh, jobs.Record{
		Description: description,
		Username:    p.User(),
		DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
			for _, tableRewrite := range tableRewrites {
				sqlDescIDs = append(sqlDescIDs, tableRewrite.TableID)
			}
			return sqlDescIDs
		}(),
		Details: jobspb.RestoreDetails{
			EndTime:       endTime,
			TableRewrites: tableRewrites,
			URIs:          from,
			TableDescs:    tables,
			OverrideDB:    opts[restoreOptIntoDB],
		},
		Progress: jobspb.RestoreProgress{},
	})
	if err != nil {
		return err
	}
	return <-errCh
}

func loadBackupSQLDescs(
	ctx context.Context, details jobspb.RestoreDetails, settings *cluster.Settings,
) ([]BackupDescriptor, []sqlbase.Descriptor, error) {
	backupDescs, err := loadBackupDescs(ctx, details.URIs, settings)
	if err != nil {
		return nil, nil, err
	}

	allDescs, _ := loadSQLDescsFromBackupsAtTime(backupDescs, details.EndTime)

	var sqlDescs []sqlbase.Descriptor
	for _, desc := range allDescs {
		if _, ok := details.TableRewrites[desc.GetID()]; ok {
			sqlDescs = append(sqlDescs, desc)
		}
	}
	return backupDescs, sqlDescs, nil
}

type restoreResumer struct {
	job            *jobs.Job
	settings       *cluster.Settings
	res            roachpb.BulkOpSummary
	databases      []*sqlbase.DatabaseDescriptor
	tables         []*sqlbase.TableDescriptor
	statsRefresher *stats.Refresher
}

// Resume is part of the jobs.Resumer interface.
func (r *restoreResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)
	p := phs.(sql.PlanHookState)

	backupDescs, sqlDescs, err := loadBackupSQLDescs(ctx, details, r.settings)
	if err != nil {
		return err
	}

	res, databases, tables, err := restore(
		ctx,
		p.ExecCfg().DB,
		p.ExecCfg().Gossip,
		backupDescs,
		details.EndTime,
		sqlDescs,
		details.TableRewrites,
		details.OverrideDB,
		r.job,
		resultsCh,
	)
	r.res = res
	r.databases = databases
	r.tables = tables
	r.statsRefresher = p.ExecCfg().StatsRefresher
	return err
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes KV data that
// has been committed from a restore that has failed or been canceled. It does
// this by adding the table descriptors in DROP state, which causes the schema
// change stuff to delete the keys in the background.
func (r *restoreResumer) OnFailOrCancel(ctx context.Context, txn *client.Txn) error {
	details := r.job.Details().(jobspb.RestoreDetails)

	// Needed to trigger the schema change manager.
	if err := txn.SetSystemConfigTrigger(); err != nil {
		return err
	}
	b := txn.NewBatch()
	for _, tableDesc := range details.TableDescs {
		tableDesc.State = sqlbase.TableDescriptor_DROP
		b.CPut(sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc), nil)
	}
	return txn.Run(ctx, b)
}

// OnSuccess is part of the jobs.Resumer interface.
func (r *restoreResumer) OnSuccess(ctx context.Context, txn *client.Txn) error {
	log.Event(ctx, "making tables live")

	// Write the new TableDescriptors and flip the namespace entries over to
	// them. After this call, any queries on a table will be served by the newly
	// restored data.
	if err := WriteTableDescs(ctx, txn, r.databases, r.tables, r.job.Payload().Username, r.settings, nil); err != nil {
		return pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
			"restoring %d TableDescriptors", len(r.tables))
	}

	// Initiate a run of CREATE STATISTICS. We don't know the actual number of
	// rows affected per table, so we use a large number because we want to make
	// sure that stats always get created/refreshed here.
	for i := range r.tables {
		r.statsRefresher.NotifyMutation(r.tables[i].ID, math.MaxInt32 /* rowsAffected */)
	}

	return nil
}

// OnTerminal is part of the jobs.Resumer interface.
func (r *restoreResumer) OnTerminal(
	ctx context.Context, status jobs.Status, resultsCh chan<- tree.Datums,
) {
	if status == jobs.StatusSucceeded {
		// TODO(benesch): emit periodic progress updates.

		// TODO(mjibson): if a restore was resumed, then these counts will only have
		// the current coordinator's counts.

		resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(*r.job.ID())),
			tree.NewDString(string(jobs.StatusSucceeded)),
			tree.NewDFloat(tree.DFloat(1.0)),
			tree.NewDInt(tree.DInt(r.res.Rows)),
			tree.NewDInt(tree.DInt(r.res.IndexEntries)),
			tree.NewDInt(tree.DInt(r.res.SystemRecords)),
			tree.NewDInt(tree.DInt(r.res.DataSize)),
		}
	}
}

var _ jobs.Resumer = &restoreResumer{}

func init() {
	sql.AddPlanHook(restorePlanHook)
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
