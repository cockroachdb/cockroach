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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// TableRewriteMap maps old table IDs to new table and parent IDs.
type TableRewriteMap map[sqlbase.ID]*jobspb.RestoreDetails_TableRewrite

const (
	restoreOptIntoDB               = "into_db"
	restoreOptSkipMissingFKs       = "skip_missing_foreign_keys"
	restoreOptSkipMissingSequences = "skip_missing_sequences"
	restoreOptSkipMissingViews     = "skip_missing_views"

	// The temporary database system tables will be restored into for full
	// cluster backups.
	restoreTempSystemDB = "crdb_temp_system"
)

var restoreOptionExpectValues = map[string]sql.KVStringOptValidate{
	restoreOptIntoDB:               sql.KVStringOptRequireValue,
	restoreOptSkipMissingFKs:       sql.KVStringOptRequireNoValue,
	restoreOptSkipMissingSequences: sql.KVStringOptRequireNoValue,
	restoreOptSkipMissingViews:     sql.KVStringOptRequireNoValue,
	backupOptEncPassphrase:         sql.KVStringOptRequireValue,
}

// rewriteViewQueryDBNames rewrites the passed table's ViewQuery replacing all
// non-empty db qualifiers with `newDB`.
//
// TODO: this AST traversal misses tables named in strings (#24556).
func rewriteViewQueryDBNames(table *sqlbase.TableDescriptor, newDB string) error {
	stmt, err := parser.ParseOne(table.ViewQuery)
	if err != nil {
		return pgerror.Wrapf(err, pgcode.Syntax,
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

// maybeFilterMissingViews filters the set of tables to restore to exclude views
// whose dependencies are either missing or are themselves unrestorable due to
// missing dependencies, and returns the resulting set of tables. If the
// restoreOptSkipMissingViews option is not set, an error is returned if any
// unrestorable views are found.
func maybeFilterMissingViews(
	tablesByID map[sqlbase.ID]*sqlbase.TableDescriptor, opts map[string]string,
) (map[sqlbase.ID]*sqlbase.TableDescriptor, error) {
	// Function that recursively determines whether a given table, if it is a
	// view, has valid dependencies. Dependencies are looked up in tablesByID.
	var hasValidViewDependencies func(*sqlbase.TableDescriptor) bool
	hasValidViewDependencies = func(desc *sqlbase.TableDescriptor) bool {
		if !desc.IsView() {
			return true
		}
		for _, id := range desc.DependsOn {
			if desc, ok := tablesByID[id]; !ok || !hasValidViewDependencies(desc) {
				return false
			}
		}
		return true
	}

	filteredTablesByID := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	for id, table := range tablesByID {
		if hasValidViewDependencies(table) {
			filteredTablesByID[id] = table
		} else {
			if _, ok := opts[restoreOptSkipMissingViews]; !ok {
				return nil, errors.Errorf(
					"cannot restore view %q without restoring referenced table (or %q option)",
					table.Name, restoreOptSkipMissingViews,
				)
			}
		}
	}
	return filteredTablesByID, nil
}

// allocateTableRewrites determines the new ID and parentID (a "TableRewrite")
// for each table in sqlDescs and returns a mapping from old ID to said
// TableRewrite. It first validates that the provided sqlDescs can be restored
// into their original database (or the database specified in opts) to avoid
// leaking table IDs if we can be sure the restore would fail.
func allocateTableRewrites(
	ctx context.Context,
	p sql.PlanHookState,
	databasesByID map[sqlbase.ID]*sql.DatabaseDescriptor,
	tablesByID map[sqlbase.ID]*sql.TableDescriptor,
	restoreDBs []*sqlbase.DatabaseDescriptor,
	descriptorCoverage tree.DescriptorCoverage,
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

	// The logic at the end of this function leaks table IDs, so fail fast if
	// we can be certain the restore will fail.

	// Fail fast if the tables to restore are incompatible with the specified
	// options.
	maxDescIDInBackup := uint32(0)
	for _, table := range tablesByID {
		if uint32(table.ID) > maxDescIDInBackup {
			maxDescIDInBackup = uint32(table.ID)
		}
		// Check that foreign key targets exist.
		for i := range table.OutboundFKs {
			fk := &table.OutboundFKs[i]
			if _, ok := tablesByID[fk.ReferencedTableID]; !ok {
				if _, ok := opts[restoreOptSkipMissingFKs]; !ok {
					return nil, errors.Errorf(
						"cannot restore table %q without referenced table %d (or %q option)",
						table.Name, fk.ReferencedTableID, restoreOptSkipMissingFKs,
					)
				}
			}
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

	// Increment the DescIDGenerator so that it is higher than the max desc ID in
	// the backup. This generator keeps produced the next descriptor ID.
	var tempSysDBID sqlbase.ID
	if descriptorCoverage == tree.AllDescriptors {
		var err error
		numberOfIncrements := maxDescIDInBackup - uint32(sql.MaxDefaultDescriptorID)
		// We need to increment this key this many times rather than settings
		// it since the interface does not expect it to be set. See client.Inc()
		// for more information.
		// TODO(pbardea): Follow up too see if there is a way to just set this
		//   since for clusters with many descrirptors we'd want to avoid
		//   incrementing it 10,000+ times.
		for i := uint32(0); i <= numberOfIncrements; i++ {
			_, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
			if err != nil {
				return nil, err
			}
		}

		// Generate one more desc ID for the ID of the temporary system db.
		tempSysDBID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
		if err != nil {
			return nil, err
		}
		tableRewrites[tempSysDBID] = &jobspb.RestoreDetails_TableRewrite{TableID: tempSysDBID}
	}

	// Fail fast if the necessary databases don't exist or are otherwise
	// incompatible with this restore.
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		maxExpectedDB := keys.MinUserDescID + sql.MaxDefaultDescriptorID
		// Check that any DBs being restored do _not_ exist.
		for name := range restoreDBNames {
			found, foundID, err := sqlbase.LookupDatabaseID(ctx, txn, name)
			if err != nil {
				return err
			}
			if found && descriptorCoverage == tree.AllDescriptors {
				if foundID > maxExpectedDB {
					return errors.Errorf("database %q already exists", name)
				}
			}
		}

		for _, table := range tablesByID {
			var targetDB string
			if renaming {
				targetDB = overrideDB
			} else if descriptorCoverage == tree.AllDescriptors && table.ParentID < sql.MaxDefaultDescriptorID {
				// This is a table that is in a database that already existed at
				// cluster creation time.
				defaultDBID, err := lookupDatabaseID(ctx, txn, sessiondata.DefaultDatabaseName)
				if err != nil {
					return err
				}
				postgresDBID, err := lookupDatabaseID(ctx, txn, sessiondata.PgDatabaseName)
				if err != nil {
					return err
				}

				if table.ParentID == sqlbase.SystemDB.ID {
					// For full cluster backups, put the system tables in the temporary
					// system table.
					targetDB = restoreTempSystemDB
					tableRewrites[table.ID] = &jobspb.RestoreDetails_TableRewrite{ParentID: tempSysDBID}
				} else if table.ParentID == defaultDBID {
					targetDB = sessiondata.DefaultDatabaseName
				} else if table.ParentID == postgresDBID {
					targetDB = sessiondata.PgDatabaseName
				}
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
			} else if descriptorCoverage == tree.AllDescriptors {
				// Set the remapped ID to the original parent ID, except for system tables which
				// should be RESTOREd to the temporary system database.
				if targetDB != restoreTempSystemDB {
					tableRewrites[table.ID] = &jobspb.RestoreDetails_TableRewrite{ParentID: table.ParentID}
				}
			} else {
				var parentID sqlbase.ID
				{
					found, newParentID, err := sqlbase.LookupDatabaseID(ctx, txn, targetDB)
					if err != nil {
						return err
					}
					if !found {
						return errors.Errorf("a database named %q needs to exist to restore table %q",
							targetDB, table.Name)
					}
					parentID = newParentID
				}
				// Check that the table name is _not_ in use.
				// This would fail the CPut later anyway, but this yields a prettier error.
				if err := CheckTableExists(ctx, txn, parentID, table.Name); err != nil {
					return err
				}

				// Check privileges.
				{
					parentDB, err := sqlbase.GetDatabaseDescFromID(ctx, txn, parentID)
					if err != nil {
						return errors.Wrapf(err,
							"failed to lookup parent DB %d", errors.Safe(parentID))
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
		var newID sqlbase.ID
		var err error
		if descriptorCoverage == tree.AllDescriptors {
			newID = db.ID
		} else {
			newID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
			if err != nil {
				return nil, err
			}
		}

		tableRewrites[db.ID] = &jobspb.RestoreDetails_TableRewrite{TableID: newID}
		for _, tableID := range needsNewParentIDs[db.Name] {
			tableRewrites[tableID] = &jobspb.RestoreDetails_TableRewrite{ParentID: newID}
		}
	}

	// tablesToRemap usually contains all tables that are being restored. In a
	// full cluster restore this should only include the system tables that need
	// to be remapped to the temporary table. All other tables in a full cluster
	// backup should have the same ID as they do in the backup.
	tablesToRemap := make([]*sqlbase.TableDescriptor, 0, len(tablesByID))
	for _, table := range tablesByID {
		if descriptorCoverage == tree.AllDescriptors {
			if table.ParentID == sqlbase.SystemDB.ID {
				// This is a system table that should be marked for descriptor creation.
				tablesToRemap = append(tablesToRemap, table)
			} else {
				// This table does not need to be remapped.
				tableRewrites[table.ID].TableID = table.ID
			}
		} else {
			tablesToRemap = append(tablesToRemap, table)
		}
	}
	sort.Sort(sqlbase.TableDescriptors(tablesToRemap))

	// Generate new IDs for the tables that need to be remapped.
	for _, table := range tablesToRemap {
		newTableID, err := sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
		if err != nil {
			return nil, err
		}
		tableRewrites[table.ID].TableID = newTableID
	}

	return tableRewrites, nil
}

// maybeUpgradeTableDescsInBackupManifests updates the backup descriptors'
// table descriptors to use the newer 19.2-style foreign key representation,
// if they are not already upgraded. This requires resolving cross-table FK
// references, which is done by looking up all table descriptors across all
// backup descriptors provided. if skipFKsWithNoMatchingTable is set, FKs whose
// "other" table is missing from the set provided are omitted during the
// upgrade, instead of causing an error to be returned.
func maybeUpgradeTableDescsInBackupManifests(
	ctx context.Context, backupManifests []BackupManifest, skipFKsWithNoMatchingTable bool,
) error {
	protoGetter := sqlbase.MapProtoGetter{
		Protos: make(map[interface{}]protoutil.Message),
	}
	// Populate the protoGetter with all table descriptors in all backup
	// descriptors so that they can be looked up.
	for _, backupManifest := range backupManifests {
		for _, desc := range backupManifest.Descriptors {
			if table := desc.Table(hlc.Timestamp{}); table != nil {
				protoGetter.Protos[string(sqlbase.MakeDescMetadataKey(table.ID))] =
					sqlbase.WrapDescriptor(protoutil.Clone(table).(*sqlbase.TableDescriptor))
			}
		}
	}

	for i := range backupManifests {
		backupManifest := &backupManifests[i]
		for j := range backupManifest.Descriptors {
			if table := backupManifest.Descriptors[j].Table(hlc.Timestamp{}); table != nil {
				if _, err := table.MaybeUpgradeForeignKeyRepresentation(ctx, protoGetter, skipFKsWithNoMatchingTable); err != nil {
					return err
				}
				// TODO(lucy): Is this necessary?
				backupManifest.Descriptors[j] = *sqlbase.WrapDescriptor(table)
			}
		}
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
			return nil
		}); err != nil {
			return err
		}

		// TODO(lucy): deal with outbound foreign key mutations here as well.
		origFKs := table.OutboundFKs
		table.OutboundFKs = nil
		for i := range origFKs {
			fk := &origFKs[i]
			to := fk.ReferencedTableID
			if indexRewrite, ok := tableRewrites[to]; ok {
				fk.ReferencedTableID = indexRewrite.TableID
				fk.OriginTableID = tableRewrite.TableID
			} else {
				// If indexRewrite doesn't exist, the user has specified
				// restoreOptSkipMissingFKs. Error checking in the case the user hasn't has
				// already been done in allocateTableRewrites.
				continue
			}

			// TODO(dt): if there is an existing (i.e. non-restoring) table with
			// a db and name matching the one the FK pointed to at backup, should
			// we update the FK to point to it?
			table.OutboundFKs = append(table.OutboundFKs, *fk)
		}

		origInboundFks := table.InboundFKs
		table.InboundFKs = nil
		for i := range origInboundFks {
			ref := &origInboundFks[i]
			if refRewrite, ok := tableRewrites[ref.OriginTableID]; ok {
				ref.ReferencedTableID = tableRewrite.TableID
				ref.OriginTableID = refRewrite.TableID
				table.InboundFKs = append(table.InboundFKs, *ref)
			}
		}

		for i, dest := range table.DependsOn {
			if depRewrite, ok := tableRewrites[dest]; ok {
				table.DependsOn[i] = depRewrite.TableID
			} else {
				// Views with missing dependencies should have been filtered out
				// or have caused an error in maybeFilterMissingViews().
				return errors.AssertionFailedf(
					"cannot restore %q because referenced table %d was not found",
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

func errOnMissingRange(span covering.Range, start, end hlc.Timestamp) error {
	return errors.Errorf(
		"no backup covers time [%s,%s) for range [%s,%s) (or backups out of order)",
		start, end, roachpb.Key(span.Start), roachpb.Key(span.End),
	)
}

func restoreJobDescription(
	p sql.PlanHookState, restore *tree.Restore, from [][]string, opts map[string]string,
) (string, error) {
	r := &tree.Restore{
		AsOf:    restore.AsOf,
		Options: optsToKVOptions(opts),
		Targets: restore.Targets,
		From:    make([]tree.PartitionedBackup, len(restore.From)),
	}

	for i, backup := range from {
		r.From[i] = make(tree.PartitionedBackup, len(backup))
		for j, uri := range backup {
			sf, err := cloud.SanitizeExternalStorageURI(uri, nil /* extraParams */)
			if err != nil {
				return "", err
			}
			r.From[i][j] = tree.NewDString(sf)
		}
	}

	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(r, ann), nil
}

// RestoreHeader is the header for RESTORE stmt results.
var RestoreHeader = sqlbase.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "status", Typ: types.String},
	{Name: "fraction_completed", Typ: types.Float},
	{Name: "rows", Typ: types.Int},
	{Name: "index_entries", Typ: types.Int},
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

	fromFns := make([]func() ([]string, error), len(restoreStmt.From))
	for i := range restoreStmt.From {
		fromFn, err := p.TypeAsStringArray(tree.Exprs(restoreStmt.From[i]), "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
		fromFns[i] = fromFn
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

		if err := p.RequireAdminRole(ctx, "RESTORE"); err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("RESTORE cannot be used inside a transaction")
		}

		from := make([][]string, len(fromFns))
		for i := range fromFns {
			from[i], err = fromFns[i]()
			if err != nil {
				return err
			}
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
	from [][]string,
	endTime hlc.Timestamp,
	opts map[string]string,
	resultsCh chan<- tree.Datums,
) error {
	if len(from) < 1 || len(from[0]) < 1 {
		return errors.New("invalid base backup specified")
	}
	baseStores := make([]cloud.ExternalStorage, len(from[0]))
	for i := range from[0] {
		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, from[0][i])
		if err != nil {
			return errors.Wrapf(err, "failed to open backup storage location")
		}
		defer store.Close()
		baseStores[i] = store
	}

	var encryption *roachpb.FileEncryptionOptions
	if passphrase, ok := opts[backupOptEncPassphrase]; ok {
		opts, err := readEncryptionOptions(ctx, baseStores[0])
		if err != nil {
			return err
		}
		encryptionKey := storageccl.GenerateKey([]byte(passphrase), opts.Salt)
		encryption = &roachpb.FileEncryptionOptions{Key: encryptionKey}
	}

	defaultURIs, mainBackupManifests, localityInfo, err := resolveBackupManifests(
		ctx, baseStores, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, from, endTime, encryption,
	)
	if err != nil {
		return err
	}

	// Validate that the table coverage of the backup matches that of the restore.
	// This prevents FULL CLUSTER backups to be restored as anything but full
	// cluster restores and vice-versa.
	if restoreStmt.DescriptorCoverage == tree.AllDescriptors && mainBackupManifests[0].DescriptorCoverage == tree.RequestedDescriptors {
		return errors.Errorf("full cluster RESTORE can only be used on full cluster BACKUP files")
	}

	// Ensure that no user table descriptors exist for a full cluster restore.
	txn := p.ExecCfg().DB.NewTxn(ctx, "count-user-descs")
	descCount, err := sql.CountUserDescriptors(ctx, txn)
	if err != nil {
		return errors.Wrap(err, "looking up user descriptors during restore")
	}
	if descCount != 0 && restoreStmt.DescriptorCoverage == tree.AllDescriptors {
		return errors.Errorf(
			"full cluster restore can only be run on a cluster with no tables or databases but found %d descriptors",
			descCount,
		)
	}

	_, skipMissingFKs := opts[restoreOptSkipMissingFKs]
	if err := maybeUpgradeTableDescsInBackupManifests(ctx, mainBackupManifests, skipMissingFKs); err != nil {
		return err
	}

	sqlDescs, restoreDBs, err := selectTargets(ctx, p, mainBackupManifests, restoreStmt.Targets, restoreStmt.DescriptorCoverage, endTime)
	if err != nil {
		return err
	}

	databasesByID := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor)
	tablesByID := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	for _, desc := range sqlDescs {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			databasesByID[dbDesc.ID] = dbDesc
		} else if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			tablesByID[tableDesc.ID] = tableDesc
		}
	}
	filteredTablesByID, err := maybeFilterMissingViews(tablesByID, opts)
	if err != nil {
		return err
	}
	tableRewrites, err := allocateTableRewrites(ctx, p, databasesByID, filteredTablesByID, restoreDBs, restoreStmt.DescriptorCoverage, opts)
	if err != nil {
		return err
	}
	description, err := restoreJobDescription(p, restoreStmt, from, opts)
	if err != nil {
		return err
	}

	var tables []*sqlbase.TableDescriptor
	for _, desc := range filteredTablesByID {
		tables = append(tables, desc)
	}
	if err := RewriteTableDescs(tables, tableRewrites, opts[restoreOptIntoDB]); err != nil {
		return err
	}

	_, errCh, err := p.ExecCfg().JobRegistry.CreateAndStartJob(ctx, resultsCh, jobs.Record{
		Description: description,
		Username:    p.User(),
		DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
			for _, tableRewrite := range tableRewrites {
				sqlDescIDs = append(sqlDescIDs, tableRewrite.TableID)
			}
			return sqlDescIDs
		}(),
		Details: jobspb.RestoreDetails{
			EndTime:            endTime,
			TableRewrites:      tableRewrites,
			URIs:               defaultURIs,
			BackupLocalityInfo: localityInfo,
			TableDescs:         tables,
			OverrideDB:         opts[restoreOptIntoDB],
			DescriptorCoverage: restoreStmt.DescriptorCoverage,
			Encryption:         encryption,
		},
		Progress: jobspb.RestoreProgress{},
	})
	if err != nil {
		return err
	}
	return <-errCh
}

func init() {
	sql.AddPlanHook(restorePlanHook)
}
