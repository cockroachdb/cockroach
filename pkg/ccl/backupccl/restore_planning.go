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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// TableRewriteMap maps old table IDs to new table and parent IDs.
type TableRewriteMap map[sqlbase.ID]*jobspb.RestoreDetails_TableRewrite

const (
	restoreOptIntoDB                    = "into_db"
	restoreOptSkipMissingFKs            = "skip_missing_foreign_keys"
	restoreOptSkipMissingSequences      = "skip_missing_sequences"
	restoreOptSkipMissingSequenceOwners = "skip_missing_sequence_owners"
	restoreOptSkipMissingViews          = "skip_missing_views"

	// The temporary database system tables will be restored into for full
	// cluster backups.
	restoreTempSystemDB = "crdb_temp_system"
)

var restoreOptionExpectValues = map[string]sql.KVStringOptValidate{
	restoreOptIntoDB:                    sql.KVStringOptRequireValue,
	restoreOptSkipMissingFKs:            sql.KVStringOptRequireNoValue,
	restoreOptSkipMissingSequences:      sql.KVStringOptRequireNoValue,
	restoreOptSkipMissingSequenceOwners: sql.KVStringOptRequireNoValue,
	restoreOptSkipMissingViews:          sql.KVStringOptRequireNoValue,
	backupOptEncPassphrase:              sql.KVStringOptRequireValue,
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

func synthesizePGTempSchema(
	ctx context.Context, p sql.PlanHookState, schemaName string,
) (sqlbase.ID, sqlbase.ID, error) {
	var synthesizedSchemaID sqlbase.ID
	var defaultDBID sqlbase.ID
	err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		defaultDBID, err = lookupDatabaseID(ctx, txn, sessiondata.DefaultDatabaseName)
		if err != nil {
			return err
		}

		sKey := sqlbase.NewSchemaKey(defaultDBID, schemaName)
		schemaID, err := sql.GetDescriptorID(ctx, txn, sKey)
		if err != nil {
			return err
		}
		if schemaID != sqlbase.InvalidID {
			return errors.Newf("attempted to synthesize temp schema during RESTORE but found"+
				" another schema already using the same schema key %s", sKey.Name())
		}
		synthesizedSchemaID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
		if err != nil {
			return err
		}
		return p.CreateSchemaWithID(ctx, sKey.Key(), synthesizedSchemaID)
	})

	return synthesizedSchemaID, defaultDBID, err
}

// dbSchemaKey is used when generating fake pg_temp schemas for the purpose of
// restoring temporary objects. Detailed comments can be found where it is being
// used.
type dbSchemaKey struct {
	parentID sqlbase.ID
	schemaID sqlbase.ID
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
	maxDescIDInBackup := int64(keys.MinNonPredefinedUserDescID)
	for _, table := range tablesByID {
		if int64(table.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(table.ID)
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
			for _, seqID := range col.OwnsSequenceIds {
				if _, ok := tablesByID[seqID]; !ok {
					if _, ok := opts[restoreOptSkipMissingSequenceOwners]; !ok {
						return nil, errors.Errorf(
							"cannot restore table %q without referenced sequence %d (or %q option)",
							table.Name, seqID, restoreOptSkipMissingSequenceOwners)
					}
				}
			}
		}

		// Handle sequence ownership dependencies.
		if table.IsSequence() && table.SequenceOpts.HasOwner() {
			if _, ok := tablesByID[table.SequenceOpts.SequenceOwner.OwnerTableID]; !ok {
				if _, ok := opts[restoreOptSkipMissingSequenceOwners]; !ok {
					return nil, errors.Errorf(
						"cannot restore sequence %q without referenced owner table %d (or %q option)",
						table.Name,
						table.SequenceOpts.SequenceOwner.OwnerTableID,
						restoreOptSkipMissingSequenceOwners,
					)
				}
			}
		}
	}

	// Include the database descriptors when calculating the max ID.
	for _, database := range databasesByID {
		if int64(database.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(database.ID)
		}
	}

	needsNewParentIDs := make(map[string][]sqlbase.ID)

	// Increment the DescIDGenerator so that it is higher than the max desc ID in
	// the backup. This generator keeps produced the next descriptor ID.
	var tempSysDBID sqlbase.ID
	if descriptorCoverage == tree.AllDescriptors {
		var err error
		// Restore the key which generates descriptor IDs.
		if err = p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			b := txn.NewBatch()
			// N.B. This key is usually mutated using the Inc command. That
			// command warns that if the key was every Put directly, Inc will
			// return an error. This is only to ensure that the type of the key
			// doesn't change. Here we just need to be very careful that we only
			// write int64 values.
			// The generator's value should be set to the value of the next ID
			// to generate.
			b.Put(keys.DescIDGenerator, maxDescIDInBackup+1)
			return txn.Run(ctx, b)
		}); err != nil {
			return nil, err
		}
		tempSysDBID, err = sql.GenerateUniqueDescID(ctx, p.ExecCfg().DB)
		if err != nil {
			return nil, err
		}
		tableRewrites[tempSysDBID] = &jobspb.RestoreDetails_TableRewrite{TableID: tempSysDBID}

		// When restoring a temporary object, the parent schema which the descriptor
		// is originally pointing to is never part of the BACKUP. This is because
		// the "pg_temp" schema in which temporary objects are created is not
		// represented as a descriptor and thus is not picked up during a full
		// cluster BACKUP.
		// To overcome this orphaned schema pointer problem, when restoring a
		// temporary object we create a "fake" pg_temp schema in defaultdb and add
		// it to the namespace table. We then remap the temporary object descriptors
		// to point to this schema. This allows us to piggy back on the temporary
		// reconciliation job which looks for "pg_temp" schemas linked to temporary
		// sessions and properly cleans up the temporary objects in it.
		haveSynthesizedTempSchema := make(map[dbSchemaKey]bool)
		var defaultDBID sqlbase.ID
		var synthesizedTempSchemaCount int
		for _, table := range tablesByID {
			if table.Temporary {
				// We generate a "fake" temporary schema for every unique
				// <dbID,schemaID> tuple of the backed-up temporary table descriptors.
				// This is important because post rewrite all the "fake" schemas and
				// consequently temp table objects are going to be in defaultdb. Placing
				// them under different "fake" schemas prevents name collisions if the
				// backed up tables had the same names but were in different temp
				// schemas/databases in the cluster which was backed up.
				dbSchemaIDKey := dbSchemaKey{parentID: table.GetParentID(),
					schemaID: table.GetParentSchemaID()}
				if _, ok := haveSynthesizedTempSchema[dbSchemaIDKey]; !ok {
					var synthesizedSchemaID sqlbase.ID
					var err error
					// NB: TemporarySchemaNameForRestorePrefix is a special value that has
					// been chosen to trick the reconciliation job into performing our
					// cleanup for us. The reconciliation job strips the "pg_temp" prefix
					// and parses the remainder of the string into a session ID. It then
					// checks the session ID against its active sessions, and performs
					// cleanup on the inactive ones.
					// We reserve the high bit to be 0 so as to never collide with an
					// actual session ID as normally the high bit is the hlc.Timestamp at
					// which the cluster was started.
					schemaName := sql.TemporarySchemaNameForRestorePrefix +
						strconv.Itoa(synthesizedTempSchemaCount)
					synthesizedSchemaID, defaultDBID, err = synthesizePGTempSchema(ctx, p, schemaName)
					if err != nil {
						return nil, err
					}
					// Write a schema descriptor rewrite so that we can remap all
					// temporary table descs which were under the original session
					// specific pg_temp schema to point to this synthesized schema when we
					// are performing the table rewrites.
					tableRewrites[table.GetParentSchemaID()] = &jobspb.RestoreDetails_TableRewrite{TableID: synthesizedSchemaID}
					haveSynthesizedTempSchema[dbSchemaIDKey] = true
					synthesizedTempSchemaCount++
				}

				// Remap the temp table descriptors to belong to the defaultdb where we
				// have synthesized the temp schema.
				tableRewrites[table.GetID()] = &jobspb.RestoreDetails_TableRewrite{ParentID: defaultDBID}
			}
		}
	}

	// Fail fast if the necessary databases don't exist or are otherwise
	// incompatible with this restore.
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Check that any DBs being restored do _not_ exist.
		for name := range restoreDBNames {
			found, _, err := sqlbase.LookupDatabaseID(ctx, txn, name)
			if err != nil {
				return err
			}
			if found {
				return errors.Errorf("database %q already exists", name)
			}
		}

		for _, table := range tablesByID {
			// If a descriptor has already been assigned a rewrite, then move on.
			if _, ok := tableRewrites[table.ID]; ok {
				continue
			}

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
				if err := CheckTableExists(
					ctx, p.ExecCfg().Settings, txn, parentID, table.Name,
				); err != nil {
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

// maybeUpgradeTableDescsInSlice updates the passed slice of table descriptors
// to use the newer 19.2-style foreign key representation, if they are not
// already upgraded. This requires resolving cross-table FK references, which is
// done by looking up all table descriptors in the slice provided.
//
// if skipFKsWithNoMatchingTable is set, FKs whose "other" table is missing from
// the set provided are omitted during the upgrade, instead of causing an error
// to be returned.
func maybeUpgradeTableDescsInSlice(
	ctx context.Context, descs []sqlbase.Descriptor, skipFKsWithNoMatchingTable bool,
) error {
	protoGetter := sqlbase.MapProtoGetter{
		Protos: make(map[interface{}]protoutil.Message),
	}
	// Populate the protoGetter with all table descriptors in all backup
	// descriptors so that they can be looked up.
	for _, desc := range descs {
		if table := desc.Table(hlc.Timestamp{}); table != nil {
			protoGetter.Protos[string(sqlbase.MakeDescMetadataKey(table.ID))] =
				sqlbase.WrapDescriptor(protoutil.Clone(table).(*sqlbase.TableDescriptor))
		}
	}

	for j := range descs {
		if table := descs[j].Table(hlc.Timestamp{}); table != nil {
			if _, err := table.MaybeUpgradeForeignKeyRepresentation(ctx, protoGetter, skipFKsWithNoMatchingTable); err != nil {
				return err
			}
			// TODO(lucy): Is this necessary?
			descs[j] = *sqlbase.WrapDescriptor(table)
		}
	}
	return nil
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

func maybeRewriteSchemaID(
	curSchemaID sqlbase.ID, descriptorRewrites TableRewriteMap, isTemporaryDesc bool,
) sqlbase.ID {
	// If the current schema is the public schema, then don't attempt to
	// do any rewriting.
	if curSchemaID == keys.PublicSchemaID && !isTemporaryDesc {
		return curSchemaID
	}
	rw, ok := descriptorRewrites[curSchemaID]
	if !ok {
		return curSchemaID
	}
	return rw.TableID
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
		table.UnexposedParentSchemaID = maybeRewriteSchemaID(table.GetParentSchemaID(),
			tableRewrites, table.Temporary)
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

		if table.IsSequence() && table.SequenceOpts.HasOwner() {
			if ownerRewrite, ok := tableRewrites[table.SequenceOpts.SequenceOwner.OwnerTableID]; ok {
				table.SequenceOpts.SequenceOwner.OwnerTableID = ownerRewrite.TableID
			} else {
				// The sequence's owner table is not being restored, thus we simply
				// remove the ownership dependency. To get here, the user must have
				// specified 'skip_missing_sequence_owners', otherwise we would have
				// errored out in allocateDescriptorRewrites.
				table.SequenceOpts.SequenceOwner = sqlbase.TableDescriptor_SequenceOpts_SequenceOwner{}
			}
		}

		// rewriteCol is a closure that performs the ID rewrite logic on a column.
		rewriteCol := func(col *sqlbase.ColumnDescriptor) error {
			var newUsedSeqRefs []sqlbase.ID
			for _, seqID := range col.UsesSequenceIds {
				if rewrite, ok := tableRewrites[seqID]; ok {
					newUsedSeqRefs = append(newUsedSeqRefs, rewrite.TableID)
				} else {
					// The referenced sequence isn't being restored.
					// Strip the DEFAULT expression and sequence references.
					// To get here, the user must have specified 'skip_missing_sequences' --
					// otherwise, would have errored out in allocateTableRewrites.
					newUsedSeqRefs = []sqlbase.ID{}
					col.DefaultExpr = nil
					break
				}
			}
			col.UsesSequenceIds = newUsedSeqRefs

			var newOwnedSeqRefs []sqlbase.ID
			for _, seqID := range col.OwnsSequenceIds {
				// We only add the sequence ownership dependency if the owned sequence
				// is being restored.
				// If the owned sequence is not being restored, the user must have
				// specified 'skip_missing_sequence_owners' to get here, otherwise
				// we would have errored out in allocateDescriptorRewrites.
				if rewrite, ok := tableRewrites[seqID]; ok {
					newOwnedSeqRefs = append(newOwnedSeqRefs, rewrite.TableID)
				}
			}
			col.OwnsSequenceIds = newOwnedSeqRefs

			return nil
		}

		// Rewrite sequence and type references in column descriptors.
		for idx := range table.Columns {
			if err := rewriteCol(&table.Columns[idx]); err != nil {
				return err
			}
		}
		for idx := range table.Mutations {
			if col := table.Mutations[idx].GetColumn(); col != nil {
				if err := rewriteCol(col); err != nil {
					return err
				}
			}
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

	sqlDescs, restoreDBs, err := selectTargets(ctx, p, mainBackupManifests, restoreStmt.Targets, restoreStmt.DescriptorCoverage, endTime)
	if err != nil {
		return err
	}

	if err := maybeUpgradeTableDescsInSlice(ctx, sqlDescs, skipMissingFKs); err != nil {
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

	// Collect telemetry.
	{
		telemetry.Count("restore.total.started")
		if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
			telemetry.Count("restore.full-cluster")
		}
	}

	jr := jobs.Record{
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
	}
	var sj *jobs.StartableJob
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		sj, err = p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, jr, txn, resultsCh)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		if sj != nil {
			if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
				log.Warningf(ctx, "failed to cleanup StartableJob: %v", cleanupErr)
			}
		}
		return err
	}

	return sj.Run(ctx)
}

func init() {
	sql.AddPlanHook(restorePlanHook)
}
