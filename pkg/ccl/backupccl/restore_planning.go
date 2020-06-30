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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// DescRewriteMap maps old descriptor IDs to new descriptor and parent IDs.
type DescRewriteMap map[sqlbase.ID]*jobspb.RestoreDetails_DescriptorRewrite

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

// rewriteTypesInExpr rewrites all explicit ID type references in the input
// expression string according to rewrites.
func rewriteTypesInExpr(expr string, rewrites DescRewriteMap) (string, error) {
	parsed, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}
	ctx := tree.NewFmtCtx(tree.FmtSerializable)
	ctx.SetIndexedTypeFormat(func(ctx *tree.FmtCtx, ref *tree.IDTypeReference) {
		newRef := ref
		if rw, ok := rewrites[sqlbase.ID(ref.ID)]; ok {
			newRef = &tree.IDTypeReference{ID: uint32(rw.ID)}
		}
		ctx.WriteString(newRef.SQLString())
	})
	ctx.FormatNode(parsed)
	return ctx.CloseAndGetString(), nil
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

// allocateDescriptorRewrites determines the new ID and parentID (a "TableRewrite")
// for each table in sqlDescs and returns a mapping from old ID to said
// TableRewrite. It first validates that the provided sqlDescs can be restored
// into their original database (or the database specified in opts) to avoid
// leaking table IDs if we can be sure the restore would fail.
func allocateDescriptorRewrites(
	ctx context.Context,
	p sql.PlanHookState,
	databasesByID map[sqlbase.ID]*sqlbase.ImmutableDatabaseDescriptor,
	tablesByID map[sqlbase.ID]*sql.TableDescriptor,
	typesByID map[sqlbase.ID]*sqlbase.TypeDescriptor,
	restoreDBs []*sqlbase.ImmutableDatabaseDescriptor,
	descriptorCoverage tree.DescriptorCoverage,
	opts map[string]string,
) (DescRewriteMap, error) {
	descriptorRewrites := make(DescRewriteMap)
	overrideDB, renaming := opts[restoreOptIntoDB]

	restoreDBNames := make(map[string]*sqlbase.ImmutableDatabaseDescriptor, len(restoreDBs))
	for _, db := range restoreDBs {
		restoreDBNames[db.GetName()] = db
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
			// Ensure that all referenced types are present.
			if col.Type.UserDefined() {
				// TODO (rohany): This can be turned into an option later.
				if _, ok := typesByID[sqlbase.ID(col.Type.StableTypeID())]; !ok {
					return nil, errors.Errorf(
						"cannot restore table %q without referenced type %d",
						table.Name,
						col.Type.StableTypeID(),
					)
				}
			}
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

	// Include the database descriptors when calculating the max ID.
	for _, database := range databasesByID {
		if int64(database.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(database.ID)
		}
	}

	// Include the type descriptors when calculating the max ID.
	for _, typ := range typesByID {
		if int64(typ.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(typ.ID)
		}
	}

	needsNewParentIDs := make(map[string][]sqlbase.ID)

	// Increment the DescIDSequenceKey so that it is higher than the max desc ID
	// in the backup. This generator keeps produced the next descriptor ID.
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
			b.Put(p.ExecCfg().Codec.DescIDSequenceKey(), maxDescIDInBackup+1)
			return txn.Run(ctx, b)
		}); err != nil {
			return nil, err
		}
		tempSysDBID, err = catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return nil, err
		}
		descriptorRewrites[tempSysDBID] = &jobspb.RestoreDetails_DescriptorRewrite{ID: tempSysDBID}
	}

	// Fail fast if the necessary databases don't exist or are otherwise
	// incompatible with this restore.
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Check that any DBs being restored do _not_ exist.
		for name := range restoreDBNames {
			found, _, err := sqlbase.LookupDatabaseID(ctx, txn, p.ExecCfg().Codec, name)
			if err != nil {
				return err
			}
			if found {
				return errors.Errorf("database %q already exists", name)
			}
		}

		for _, table := range tablesByID {
			var targetDB string
			if renaming {
				targetDB = overrideDB
			} else if descriptorCoverage == tree.AllDescriptors && table.ParentID < sqlbase.MaxDefaultDescriptorID {
				// This is a table that is in a database that already existed at
				// cluster creation time.
				defaultDBID, err := lookupDatabaseID(ctx, txn, p.ExecCfg().Codec, sqlbase.DefaultDatabaseName)
				if err != nil {
					return err
				}
				postgresDBID, err := lookupDatabaseID(ctx, txn, p.ExecCfg().Codec, sqlbase.PgDatabaseName)
				if err != nil {
					return err
				}

				if table.ParentID == sqlbase.SystemDB.GetID() {
					// For full cluster backups, put the system tables in the temporary
					// system table.
					targetDB = restoreTempSystemDB
					descriptorRewrites[table.ID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: tempSysDBID}
				} else if table.ParentID == defaultDBID {
					targetDB = sqlbase.DefaultDatabaseName
				} else if table.ParentID == postgresDBID {
					targetDB = sqlbase.PgDatabaseName
				}
			} else {
				database, ok := databasesByID[table.ParentID]
				if !ok {
					return errors.Errorf("no database with ID %d in backup for table %q",
						table.ParentID, table.Name)
				}
				targetDB = database.GetName()
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], table.ID)
			} else if descriptorCoverage == tree.AllDescriptors {
				// Set the remapped ID to the original parent ID, except for system tables which
				// should be RESTOREd to the temporary system database.
				if targetDB != restoreTempSystemDB {
					descriptorRewrites[table.ID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: table.ParentID}
				}
			} else {
				var parentID sqlbase.ID
				{
					found, newParentID, err := sqlbase.LookupDatabaseID(ctx, txn, p.ExecCfg().Codec, targetDB)
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
				// TODO (rohany): Use keys.PublicSchemaID for now, revisit this once we
				//  support user defined schemas.
				if err := CheckObjectExists(ctx, txn, p.ExecCfg().Codec, parentID, keys.PublicSchemaID, table.Name); err != nil {
					return err
				}

				// Check privileges.
				{
					parentDB, err := sqlbase.GetDatabaseDescFromID(ctx, txn, p.ExecCfg().Codec, parentID)
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
				descriptorRewrites[table.ID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: parentID}
			}
		}

		// Iterate through typesByID to construct a remapping entry for each type.
		for _, typ := range typesByID {
			// If a descriptor has already been assigned a rewrite, then move on.
			if _, ok := descriptorRewrites[typ.ID]; ok {
				continue
			}

			var targetDB string
			if renaming {
				targetDB = overrideDB
			} else {
				database, ok := databasesByID[typ.ParentID]
				if !ok {
					return errors.Errorf("no database with ID %d in backup for type %q",
						typ.ParentID, typ.Name)
				}
				targetDB = database.Name
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], typ.ID)
			} else {
				// The remapping logic for a type will perform the remapping for a type's
				// array type, so don't perform this logic for the array type itself.
				if typ.Kind == sqlbase.TypeDescriptor_ALIAS {
					continue
				}

				// Look up the parent database's ID.
				found, parentID, err := sqlbase.LookupDatabaseID(ctx, txn, p.ExecCfg().Codec, targetDB)
				if err != nil {
					return err
				}
				if !found {
					return errors.Errorf("a database named %q needs to exist to restore type %q",
						targetDB, typ.Name)
				}
				// Check privileges on the parent DB.
				parentDB, err := sqlbase.GetDatabaseDescFromID(ctx, txn, p.ExecCfg().Codec, parentID)
				if err != nil {
					return errors.Wrapf(err,
						"failed to lookup parent DB %d", errors.Safe(parentID))
				}

				// See if there is an existing type with the same name.
				found, id, err := sqlbase.LookupObjectID(ctx, txn, p.ExecCfg().Codec, parentID, keys.PublicSchemaID, typ.Name)
				if err != nil {
					return err
				}
				if !found {
					// If we didn't find a type with the same name, then mark that we
					// need to create the type.

					// Ensure that the user has the correct privilege to create types.
					if err := p.CheckPrivilege(ctx, parentDB, privilege.CREATE); err != nil {
						return err
					}

					// Create a rewrite entry for the type.
					descriptorRewrites[typ.ID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: parentID}

					// Ensure that there isn't a collision with the array type name.
					arrTyp := typesByID[typ.ArrayTypeID]
					if err := CheckObjectExists(ctx, txn, p.ExecCfg().Codec, parentID, keys.PublicSchemaID, arrTyp.Name); err != nil {
						return errors.Wrapf(err, "name collision for %q's array type", typ.Name)
					}
					// Create the rewrite entry for the array type as well.
					descriptorRewrites[arrTyp.ID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: parentID}
				} else {
					// If there was a name collision, we'll try to see if we can remap
					// this type to the type existing in the cluster.

					// See what kind of object we collided with.
					desc, err := catalogkv.GetDescriptorByID(ctx, txn, p.ExecCfg().Codec, id)
					if err != nil {
						return err
					}
					// If the collided object isn't a type, then error out.
					existingType := desc.TypeDesc()
					if existingType == nil {
						return sqlbase.MakeObjectAlreadyExistsError(desc.DescriptorProto(), typ.Name)
					}

					// Check if the collided type is compatible to be remapped to.
					if err := typ.IsCompatibleWith(existingType); err != nil {
						return errors.Wrapf(
							err,
							"%q is not compatible with type %q existing in cluster",
							existingType.Name,
							existingType.Name,
						)
					}

					// Remap both the type and its array type since they are compatible
					// with the type existing in the cluster.
					descriptorRewrites[typ.ID] = &jobspb.RestoreDetails_DescriptorRewrite{
						ParentID:   existingType.ParentID,
						ID:         existingType.ID,
						ToExisting: true,
					}
					descriptorRewrites[typ.ArrayTypeID] = &jobspb.RestoreDetails_DescriptorRewrite{
						ParentID:   existingType.ParentID,
						ID:         existingType.ArrayTypeID,
						ToExisting: true,
					}
				}
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
			newID = db.GetID()
		} else {
			newID, err = catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
			if err != nil {
				return nil, err
			}
		}

		descriptorRewrites[db.ID] = &jobspb.RestoreDetails_DescriptorRewrite{ID: newID}
		for _, tableID := range needsNewParentIDs[db.GetName()] {
			descriptorRewrites[tableID] = &jobspb.RestoreDetails_DescriptorRewrite{ParentID: newID}
		}
	}

	// descriptorsToRemap usually contains all tables that are being restored. In a
	// full cluster restore this should only include the system tables that need
	// to be remapped to the temporary table. All other tables in a full cluster
	// backup should have the same ID as they do in the backup.
	descriptorsToRemap := make([]sqlbase.BaseDescriptorInterface, 0, len(tablesByID))
	for _, table := range tablesByID {
		if descriptorCoverage == tree.AllDescriptors {
			if table.ParentID == sqlbase.SystemDB.GetID() {
				// This is a system table that should be marked for descriptor creation.
				descriptorsToRemap = append(descriptorsToRemap, table)
			} else {
				// This table does not need to be remapped.
				descriptorRewrites[table.ID].ID = table.ID
			}
		} else {
			descriptorsToRemap = append(descriptorsToRemap, table)
		}
	}

	// Update the remapping information for type descriptors.
	for _, typ := range typesByID {
		if descriptorCoverage == tree.AllDescriptors {
			// The type doesn't need to be remapped.
			descriptorRewrites[typ.ID].ID = typ.ID
		} else {
			// If the type is marked to be remapped to an existing type in the
			// cluster, then we don't want to generate an ID for it.
			if !descriptorRewrites[typ.ID].ToExisting {
				descriptorsToRemap = append(descriptorsToRemap, typ)
			}
		}
	}

	sort.Sort(sqlbase.BaseDescriptorInterfaces(descriptorsToRemap))

	// Generate new IDs for the tables that need to be remapped.
	for _, desc := range descriptorsToRemap {
		newTableID, err := catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return nil, err
		}
		descriptorRewrites[desc.GetID()].ID = newTableID
	}

	return descriptorRewrites, nil
}

// maybeUpgradeTableDescsInBackupManifests updates the backup descriptors'
// table descriptors to use the newer 19.2-style foreign key representation,
// if they are not already upgraded. This requires resolving cross-table FK
// references, which is done by looking up all table descriptors across all
// backup descriptors provided. if skipFKsWithNoMatchingTable is set, FKs whose
// "other" table is missing from the set provided are omitted during the
// upgrade, instead of causing an error to be returned.
func maybeUpgradeTableDescsInBackupManifests(
	ctx context.Context,
	backupManifests []BackupManifest,
	codec keys.SQLCodec,
	skipFKsWithNoMatchingTable bool,
) error {
	protoGetter := sqlbase.MapProtoGetter{
		Protos: make(map[interface{}]protoutil.Message),
	}
	// Populate the protoGetter with all table descriptors in all backup
	// descriptors so that they can be looked up.
	for _, backupManifest := range backupManifests {
		for _, desc := range backupManifest.Descriptors {
			if table := desc.Table(hlc.Timestamp{}); table != nil {
				protoGetter.Protos[string(sqlbase.MakeDescMetadataKey(codec, table.ID))] =
					sqlbase.NewImmutableTableDescriptor(*protoutil.Clone(table).(*sqlbase.TableDescriptor)).DescriptorProto()
			}
		}
	}

	for i := range backupManifests {
		backupManifest := &backupManifests[i]
		for j := range backupManifest.Descriptors {
			if table := backupManifest.Descriptors[j].Table(hlc.Timestamp{}); table != nil {
				if _, err := table.MaybeUpgradeForeignKeyRepresentation(ctx, protoGetter, codec, skipFKsWithNoMatchingTable); err != nil {
					return err
				}
				// TODO(lucy): Is this necessary?
				backupManifest.Descriptors[j] = *sqlbase.NewMutableExistingTableDescriptor(
					*table).DescriptorProto()
			}
		}
	}
	return nil
}

// rewriteIDsInTypesT rewrites all ID's in the input types.T using the input
// ID rewrite mapping.
func rewriteIDsInTypesT(typ *types.T, descriptorRewrites DescRewriteMap) {
	if !typ.UserDefined() {
		return
	}
	// TODO (rohany): Probably should expose some functions on the types.T to
	//  set this information, rather than reaching into the internal type.
	if rw, ok := descriptorRewrites[sqlbase.ID(typ.StableTypeID())]; ok {
		typ.InternalType.UDTMetadata.StableTypeID = uint32(rw.ID)
		typ.InternalType.Oid = types.StableTypeIDToOID(uint32(rw.ID))
	}

	if typ.Family() == types.ArrayFamily {
		rewriteIDsInTypesT(typ.ArrayContents(), descriptorRewrites)
	} else {
		// If the type is not an array, then we just need to updated the array
		// type ID in the type metadata.
		if rw, ok := descriptorRewrites[sqlbase.ID(typ.StableArrayTypeID())]; ok {
			typ.InternalType.UDTMetadata.StableArrayTypeID = uint32(rw.ID)
		}
	}
}

// rewriteTypeDescs rewrites all ID's in the input slice of TypeDescriptors
// using the input ID rewrite mapping.
func rewriteTypeDescs(types []*sqlbase.TypeDescriptor, descriptorRewrites DescRewriteMap) error {
	for _, typ := range types {
		rewrite, ok := descriptorRewrites[typ.ID]
		if !ok {
			return errors.Errorf("missing rewrite for type %d", typ.ID)
		}
		typ.ID = rewrite.ID
		typ.ParentID = rewrite.ParentID
		switch t := typ.Kind; t {
		case sqlbase.TypeDescriptor_ENUM:
			if rw, ok := descriptorRewrites[typ.ArrayTypeID]; ok {
				typ.ArrayTypeID = rw.ID
			}
		case sqlbase.TypeDescriptor_ALIAS:
			// We need to rewrite any ID's present in the aliased types.T.
			rewriteIDsInTypesT(typ.Alias, descriptorRewrites)
		default:
			return errors.AssertionFailedf("unknown type kind %s", t.String())
		}
	}
	return nil
}

// RewriteTableDescs mutates tables to match the ID and privilege specified
// in descriptorRewrites, as well as adjusting cross-table references to use the
// new IDs. overrideDB can be specified to set database names in views.
func RewriteTableDescs(
	tables []*sqlbase.TableDescriptor, descriptorRewrites DescRewriteMap, overrideDB string,
) error {
	for _, table := range tables {
		tableRewrite, ok := descriptorRewrites[table.ID]
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

		table.ID = tableRewrite.ID
		table.ParentID = tableRewrite.ParentID

		// Remap type IDs in all serialized expressions within the TableDescriptor.
		// TODO (rohany): This needs tests once partial indexes are ready.
		if err := sqlbase.ForEachExprStringInTableDesc(table, func(expr *string) error {
			newExpr, err := rewriteTypesInExpr(*expr, descriptorRewrites)
			if err != nil {
				return err
			}
			*expr = newExpr
			return nil
		}); err != nil {
			return err
		}

		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			// Verify that for any interleaved index being restored, the interleave
			// parent is also being restored. Otherwise, the interleave entries in the
			// restored IndexDescriptors won't have anything to point to.
			// TODO(dan): It seems like this restriction could be lifted by restoring
			// stub TableDescriptors for the missing interleave parents.
			for j, a := range index.Interleave.Ancestors {
				ancestorRewrite, ok := descriptorRewrites[a.TableID]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave parent %d", table.Name, a.TableID,
					)
				}
				index.Interleave.Ancestors[j].TableID = ancestorRewrite.ID
			}
			for j, c := range index.InterleavedBy {
				childRewrite, ok := descriptorRewrites[c.Table]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave child table %d", table.Name, c.Table,
					)
				}
				index.InterleavedBy[j].Table = childRewrite.ID
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
			if indexRewrite, ok := descriptorRewrites[to]; ok {
				fk.ReferencedTableID = indexRewrite.ID
				fk.OriginTableID = tableRewrite.ID
			} else {
				// If indexRewrite doesn't exist, the user has specified
				// restoreOptSkipMissingFKs. Error checking in the case the user hasn't has
				// already been done in allocateDescriptorRewrites.
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
			if refRewrite, ok := descriptorRewrites[ref.OriginTableID]; ok {
				ref.ReferencedTableID = tableRewrite.ID
				ref.OriginTableID = refRewrite.ID
				table.InboundFKs = append(table.InboundFKs, *ref)
			}
		}

		for i, dest := range table.DependsOn {
			if depRewrite, ok := descriptorRewrites[dest]; ok {
				table.DependsOn[i] = depRewrite.ID
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
			if refRewrite, ok := descriptorRewrites[ref.ID]; ok {
				ref.ID = refRewrite.ID
				table.DependedOnBy = append(table.DependedOnBy, ref)
			}
		}

		// rewriteCol is a closure that performs the ID rewrite logic on a column.
		rewriteCol := func(col *sqlbase.ColumnDescriptor) error {
			// Rewrite the types.T's IDs present in the column.
			rewriteIDsInTypesT(col.Type, descriptorRewrites)
			var newSeqRefs []sqlbase.ID
			for _, seqID := range col.UsesSequenceIds {
				if rewrite, ok := descriptorRewrites[seqID]; ok {
					newSeqRefs = append(newSeqRefs, rewrite.ID)
				} else {
					// The referenced sequence isn't being restored.
					// Strip the DEFAULT expression and sequence references.
					// To get here, the user must have specified 'skip_missing_sequences' --
					// otherwise, would have errored out in allocateDescriptorRewrites.
					newSeqRefs = []sqlbase.ID{}
					col.DefaultExpr = nil
					break
				}
			}
			col.UsesSequenceIds = newSeqRefs
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
			sf, err := cloudimpl.SanitizeExternalStorageURI(uri, nil /* extraParams */)
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
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	restoreStmt, ok := stmt.(*tree.Restore)
	if !ok {
		return nil, nil, nil, false, nil
	}

	fromFns := make([]func() ([]string, error), len(restoreStmt.From))
	for i := range restoreStmt.From {
		fromFn, err := p.TypeAsStringArray(ctx, tree.Exprs(restoreStmt.From[i]), "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
		fromFns[i] = fromFn
	}

	optsFn, err := p.TypeAsStringOpts(ctx, restoreStmt.Options, restoreOptionExpectValues)
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
			endTime, err = p.EvalAsOfTimestamp(ctx, restoreStmt.AsOf)
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
		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, from[0][i], p.User())
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
		p.User(),
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
	descCount, err := catalogkv.CountUserDescriptors(ctx, txn, p.ExecCfg().Codec)
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
	if err := maybeUpgradeTableDescsInBackupManifests(ctx, mainBackupManifests, p.ExecCfg().Codec, skipMissingFKs); err != nil {
		return err
	}

	sqlDescs, restoreDBs, err := selectTargets(ctx, p, mainBackupManifests, restoreStmt.Targets, restoreStmt.DescriptorCoverage, endTime)
	if err != nil {
		return err
	}

	databasesByID := make(map[sqlbase.ID]*sqlbase.ImmutableDatabaseDescriptor)
	tablesByID := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	typesByID := make(map[sqlbase.ID]*sqlbase.TypeDescriptor)
	for _, desc := range sqlDescs {
		// TODO(ajwerner): make sqlDescs into a []sqlbase.DescriptorInterface so
		// we don't need to do this duplicate construction of the unwrapped
		// descriptor.
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			dbDesc := sqlbase.NewImmutableDatabaseDescriptor(*dbDesc)
			databasesByID[dbDesc.GetID()] = dbDesc
		} else if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			tablesByID[tableDesc.ID] = tableDesc
		} else if typDesc := desc.GetType(); typDesc != nil {
			typesByID[typDesc.ID] = typDesc
		}
	}
	filteredTablesByID, err := maybeFilterMissingViews(tablesByID, opts)
	if err != nil {
		return err
	}
	descriptorRewrites, err := allocateDescriptorRewrites(
		ctx,
		p,
		databasesByID,
		filteredTablesByID,
		typesByID,
		restoreDBs,
		restoreStmt.DescriptorCoverage,
		opts,
	)
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
	var types []*sqlbase.TypeDescriptor
	for _, desc := range typesByID {
		types = append(types, desc)
	}

	// We attempt to rewrite ID's in the collected type and table descriptors
	// to catch errors during this process here, rather than in the job itself.
	if err := RewriteTableDescs(tables, descriptorRewrites, opts[restoreOptIntoDB]); err != nil {
		return err
	}
	if err := rewriteTypeDescs(types, descriptorRewrites); err != nil {
		return err
	}

	// Collect telemetry.
	{
		telemetry.Count("restore.total.started")
		if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
			telemetry.Count("restore.full-cluster")
		}
	}

	_, errCh, err := p.ExecCfg().JobRegistry.CreateAndStartJob(ctx, resultsCh, jobs.Record{
		Description: description,
		Username:    p.User(),
		DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
			for _, tableRewrite := range descriptorRewrites {
				sqlDescIDs = append(sqlDescIDs, tableRewrite.ID)
			}
			return sqlDescIDs
		}(),
		Details: jobspb.RestoreDetails{
			EndTime:            endTime,
			DescriptorRewrites: descriptorRewrites,
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
