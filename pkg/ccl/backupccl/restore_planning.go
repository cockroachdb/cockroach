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
	"fmt"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/rewrite"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

const (
	restoreOptIntoDB                    = "into_db"
	restoreOptSkipMissingFKs            = "skip_missing_foreign_keys"
	restoreOptSkipMissingSequences      = "skip_missing_sequences"
	restoreOptSkipMissingSequenceOwners = "skip_missing_sequence_owners"
	restoreOptSkipMissingViews          = "skip_missing_views"
	restoreOptSkipLocalitiesCheck       = "skip_localities_check"
	restoreOptDebugPauseOn              = "debug_pause_on"
	restoreOptAsTenant                  = "tenant"

	// The temporary database system tables will be restored into for full
	// cluster backups.
	restoreTempSystemDB = "crdb_temp_system"
)

var allowedDebugPauseOnValues = map[string]struct{}{
	"error": {},
}

// featureRestoreEnabled is used to enable and disable the RESTORE feature.
var featureRestoreEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"feature.restore.enabled",
	"set to true to enable restore, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
).WithPublic()

// maybeFilterMissingViews filters the set of tables to restore to exclude views
// whose dependencies are either missing or are themselves unrestorable due to
// missing dependencies, and returns the resulting set of tables. If the
// skipMissingViews option is not set, an error is returned if any
// unrestorable views are found.
func maybeFilterMissingViews(
	tablesByID map[descpb.ID]*tabledesc.Mutable,
	typesByID map[descpb.ID]*typedesc.Mutable,
	skipMissingViews bool,
) (map[descpb.ID]*tabledesc.Mutable, error) {
	// Function that recursively determines whether a given table, if it is a
	// view, has valid dependencies. Dependencies are looked up in tablesByID.
	var hasValidViewDependencies func(desc *tabledesc.Mutable) bool
	hasValidViewDependencies = func(desc *tabledesc.Mutable) bool {
		if !desc.IsView() {
			return true
		}
		for _, id := range desc.DependsOn {
			if depDesc, ok := tablesByID[id]; !ok || !hasValidViewDependencies(depDesc) {
				return false
			}
		}
		for _, id := range desc.DependsOnTypes {
			if _, ok := typesByID[id]; !ok {
				return false
			}
		}
		return true
	}

	filteredTablesByID := make(map[descpb.ID]*tabledesc.Mutable)
	for id, table := range tablesByID {
		if hasValidViewDependencies(table) {
			filteredTablesByID[id] = table
		} else {
			if !skipMissingViews {
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
	ctx context.Context, p sql.PlanHookState, schemaName string, dbID descpb.ID,
) (descpb.ID, error) {
	var synthesizedSchemaID descpb.ID
	err := sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		var err error
		schemaID, err := col.Direct().LookupSchemaID(ctx, txn, dbID, schemaName)
		if err != nil {
			return err
		}
		if schemaID != descpb.InvalidID {
			return errors.Newf("attempted to synthesize temp schema during RESTORE but found"+
				" another schema already using the same schema key %s", schemaName)
		}
		synthesizedSchemaID, err = descidgen.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return err
		}
		return p.CreateSchemaNamespaceEntry(ctx, catalogkeys.MakeSchemaNameKey(p.ExecCfg().Codec, dbID, schemaName), synthesizedSchemaID)
	})

	return synthesizedSchemaID, err
}

// allocateDescriptorRewrites determines the new ID and parentID (a "DescriptorRewrite")
// for each table in sqlDescs and returns a mapping from old ID to said
// DescriptorRewrite. It first validates that the provided sqlDescs can be restored
// into their original database (or the database specified in opts) to avoid
// leaking table IDs if we can be sure the restore would fail.
func allocateDescriptorRewrites(
	ctx context.Context,
	p sql.PlanHookState,
	databasesByID map[descpb.ID]*dbdesc.Mutable,
	schemasByID map[descpb.ID]*schemadesc.Mutable,
	tablesByID map[descpb.ID]*tabledesc.Mutable,
	typesByID map[descpb.ID]*typedesc.Mutable,
	restoreDBs []catalog.DatabaseDescriptor,
	descriptorCoverage tree.DescriptorCoverage,
	opts tree.RestoreOptions,
	intoDB string,
	newDBName string,
	restoreSystemUsers bool,
) (jobspb.DescRewriteMap, error) {
	descriptorRewrites := make(jobspb.DescRewriteMap)

	restoreDBNames := make(map[string]catalog.DatabaseDescriptor, len(restoreDBs))
	for _, db := range restoreDBs {
		restoreDBNames[db.GetName()] = db
	}

	if len(restoreDBNames) > 0 && intoDB != "" {
		return nil, errors.Errorf("cannot use %q option when restoring database(s)", restoreOptIntoDB)
	}

	// The logic at the end of this function leaks table IDs, so fail fast if
	// we can be certain the restore will fail.

	// Fail fast if the tables to restore are incompatible with the specified
	// options.
	var maxDescIDInBackup int64
	for _, table := range tablesByID {
		if int64(table.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(table.ID)
		}
		// Check that foreign key targets exist.
		for i := range table.OutboundFKs {
			fk := &table.OutboundFKs[i]
			if _, ok := tablesByID[fk.ReferencedTableID]; !ok {
				if !opts.SkipMissingFKs {
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
				id, err := typedesc.GetUserDefinedTypeDescID(col.Type)
				if err != nil {
					return nil, err
				}
				if _, ok := typesByID[id]; !ok {
					return nil, errors.Errorf(
						"cannot restore table %q without referenced type %d",
						table.Name,
						id,
					)
				}
			}
			for _, seqID := range col.UsesSequenceIds {
				if _, ok := tablesByID[seqID]; !ok {
					if !opts.SkipMissingSequences {
						return nil, errors.Errorf(
							"cannot restore table %q without referenced sequence %d (or %q option)",
							table.Name, seqID, restoreOptSkipMissingSequences,
						)
					}
				}
			}
			for _, seqID := range col.OwnsSequenceIds {
				if _, ok := tablesByID[seqID]; !ok {
					if !opts.SkipMissingSequenceOwners {
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
				if !opts.SkipMissingSequenceOwners {
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

	// Include the type descriptors when calculating the max ID.
	for _, typ := range typesByID {
		if int64(typ.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(typ.ID)
		}
	}

	// Include the schema descriptors when calculating the max ID.
	for _, sc := range schemasByID {
		if int64(sc.ID) > maxDescIDInBackup {
			maxDescIDInBackup = int64(sc.ID)
		}
	}

	needsNewParentIDs := make(map[string][]descpb.ID)

	// Increment the DescIDSequenceKey so that it is higher than both the max desc ID
	// in the backup and current max desc ID in the restoring cluster. This generator
	// keeps produced the next descriptor ID.
	var tempSysDBID descpb.ID
	if descriptorCoverage == tree.AllDescriptors || restoreSystemUsers {
		var err error
		if descriptorCoverage == tree.AllDescriptors {
			// Restore the key which generates descriptor IDs.
			if err = p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				v, err := txn.Get(ctx, p.ExecCfg().Codec.DescIDSequenceKey())
				if err != nil {
					return err
				}
				newValue := maxDescIDInBackup + 1
				if newValue <= v.ValueInt() {
					// This case may happen when restoring backups from older versions.
					newValue = v.ValueInt() + 1
				}
				b := txn.NewBatch()
				// N.B. This key is usually mutated using the Inc command. That
				// command warns that if the key was every Put directly, Inc will
				// return an error. This is only to ensure that the type of the key
				// doesn't change. Here we just need to be very careful that we only
				// write int64 values.
				// The generator's value should be set to the value of the next ID
				// to generate.
				b.Put(p.ExecCfg().Codec.DescIDSequenceKey(), newValue)
				return txn.Run(ctx, b)
			}); err != nil {
				return nil, err
			}
			tempSysDBID, err = descidgen.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
			if err != nil {
				return nil, err
			}
		} else if restoreSystemUsers {
			tempSysDBID, err = descidgen.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
			if err != nil {
				return nil, err
			}
		}
		// Remap all of the descriptor belonging to system tables to the temp system
		// DB.
		descriptorRewrites[tempSysDBID] = &jobspb.DescriptorRewrite{ID: tempSysDBID}
		for _, table := range tablesByID {
			if table.GetParentID() == systemschema.SystemDB.GetID() {
				descriptorRewrites[table.GetID()] = &jobspb.DescriptorRewrite{
					ParentID:       tempSysDBID,
					ParentSchemaID: keys.PublicSchemaIDForBackup,
				}
			}
		}
		for _, sc := range schemasByID {
			if sc.GetParentID() == systemschema.SystemDB.GetID() {
				descriptorRewrites[sc.GetID()] = &jobspb.DescriptorRewrite{ParentID: tempSysDBID}
			}
		}
		for _, typ := range typesByID {
			if typ.GetParentID() == systemschema.SystemDB.GetID() {
				descriptorRewrites[typ.GetID()] = &jobspb.DescriptorRewrite{
					ParentID:       tempSysDBID,
					ParentSchemaID: keys.PublicSchemaIDForBackup,
				}
			}
		}

		// When restoring a temporary object, the parent schema which the descriptor
		// is originally pointing to is never part of the BACKUP. This is because
		// the "pg_temp" schema in which temporary objects are created is not
		// represented as a descriptor and thus is not picked up during a full
		// cluster BACKUP.
		// To overcome this orphaned schema pointer problem, when restoring a
		// temporary object we create a "fake" pg_temp schema in temp table's db and
		// add it to the namespace table.
		// We then remap the temporary object descriptors to point to this schema.
		// This allows us to piggy back on the temporary
		// reconciliation job which looks for "pg_temp" schemas linked to temporary
		// sessions and properly cleans up the temporary objects in it.
		haveSynthesizedTempSchema := make(map[descpb.ID]bool)
		var synthesizedTempSchemaCount int
		for _, table := range tablesByID {
			if table.IsTemporary() {
				if _, ok := haveSynthesizedTempSchema[table.GetParentSchemaID()]; !ok {
					var synthesizedSchemaID descpb.ID
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
					synthesizedSchemaID, err = synthesizePGTempSchema(ctx, p, schemaName, table.GetParentID())
					if err != nil {
						return nil, err
					}
					// Write a schema descriptor rewrite so that we can remap all
					// temporary table descs which were under the original session
					// specific pg_temp schema to point to this synthesized schema when we
					// are performing the table rewrites.
					descriptorRewrites[table.GetParentSchemaID()] = &jobspb.DescriptorRewrite{ID: synthesizedSchemaID}
					haveSynthesizedTempSchema[table.GetParentSchemaID()] = true
					synthesizedTempSchemaCount++
				}
			}
		}
	}

	// Fail fast if the necessary databases don't exist or are otherwise
	// incompatible with this restore.
	if err := sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		// Check that any DBs being restored do _not_ exist.
		for name := range restoreDBNames {
			dbID, err := col.Direct().LookupDatabaseID(ctx, txn, name)
			if err != nil {
				return err
			}
			if dbID != descpb.InvalidID {
				return errors.Errorf("database %q already exists", name)
			}
		}

		// TODO (rohany, pbardea): These checks really need to be refactored.
		// Construct rewrites for any user defined schemas.
		for _, sc := range schemasByID {
			if _, ok := descriptorRewrites[sc.ID]; ok {
				continue
			}

			targetDB, err := resolveTargetDB(ctx, txn, p, databasesByID, intoDB, descriptorCoverage, sc)
			if err != nil {
				return err
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], sc.ID)
			} else {
				// Look up the parent database's ID.
				parentID, parentDB, err := getDatabaseIDAndDesc(ctx, txn, col, targetDB)
				if err != nil {
					return err
				}
				if err := p.CheckPrivilege(ctx, parentDB, privilege.CREATE); err != nil {
					return err
				}

				// See if there is an existing schema with the same name.
				id, err := col.Direct().LookupSchemaID(ctx, txn, parentID, sc.Name)
				if err != nil {
					return err
				}
				if id == descpb.InvalidID {
					// If we didn't find a matching schema, then we'll restore this schema.
					descriptorRewrites[sc.ID] = &jobspb.DescriptorRewrite{ParentID: parentID}
				} else {
					// If we found an existing schema, then we need to remap all references
					// to this schema to the existing one.
					desc, err := col.Direct().MustGetSchemaDescByID(ctx, txn, id)
					if err != nil {
						return err
					}
					descriptorRewrites[sc.ID] = &jobspb.DescriptorRewrite{
						ParentID:   desc.GetParentID(),
						ID:         desc.GetID(),
						ToExisting: true,
					}
				}
			}
		}

		for _, table := range tablesByID {
			// If a descriptor has already been assigned a rewrite, then move on.
			if _, ok := descriptorRewrites[table.ID]; ok {
				continue
			}

			targetDB, err := resolveTargetDB(ctx, txn, p, databasesByID, intoDB, descriptorCoverage,
				table)
			if err != nil {
				return err
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], table.ID)
			} else if descriptorCoverage == tree.AllDescriptors {
				// Set the remapped ID to the original parent ID, except for system tables which
				// should be RESTOREd to the temporary system database.
				if targetDB != restoreTempSystemDB {
					descriptorRewrites[table.ID] = &jobspb.DescriptorRewrite{ParentID: table.ParentID}
				}
			} else {
				var parentID descpb.ID
				{
					newParentID, err := col.Direct().LookupDatabaseID(ctx, txn, targetDB)
					if err != nil {
						return err
					}
					if newParentID == descpb.InvalidID {
						return errors.Errorf("a database named %q needs to exist to restore table %q",
							targetDB, table.Name)
					}
					parentID = newParentID
				}
				// Check that the table name is _not_ in use.
				// This would fail the CPut later anyway, but this yields a prettier error.
				tableName := tree.NewUnqualifiedTableName(tree.Name(table.GetName()))
				err := col.Direct().CheckObjectCollision(ctx, txn, parentID, table.GetParentSchemaID(), tableName)
				if err != nil {
					return err
				}

				// Check privileges.
				parentDB, err := col.Direct().MustGetDatabaseDescByID(ctx, txn, parentID)
				if err != nil {
					return errors.Wrapf(err,
						"failed to lookup parent DB %d", errors.Safe(parentID))
				}
				if err := p.CheckPrivilege(ctx, parentDB, privilege.CREATE); err != nil {
					return err
				}

				// We're restoring a table and not its parent database. We may block
				// restoring multi-region tables to multi-region databases since
				// regions may mismatch.
				if err := checkMultiRegionCompatible(ctx, txn, col, table, parentDB); err != nil {
					return pgerror.WithCandidateCode(err, pgcode.FeatureNotSupported)
				}

				// Create the table rewrite with the new parent ID. We've done all the
				// up-front validation that we can.
				descriptorRewrites[table.ID] = &jobspb.DescriptorRewrite{ParentID: parentID}

				// If we're restoring to a public schema of database that already exists
				// we can populate the rewrite ParentSchemaID field here since we
				// already have the database descriptor.
				if table.GetParentSchemaID() == keys.PublicSchemaIDForBackup ||
					table.GetParentSchemaID() == descpb.InvalidID {
					publicSchemaID := parentDB.GetSchemaID(tree.PublicSchema)
					descriptorRewrites[table.ID].ParentSchemaID = publicSchemaID
				}
			}
		}

		// Iterate through typesByID to construct a remapping entry for each type.
		for _, typ := range typesByID {
			// If a descriptor has already been assigned a rewrite, then move on.
			if _, ok := descriptorRewrites[typ.ID]; ok {
				continue
			}

			targetDB, err := resolveTargetDB(ctx, txn, p, databasesByID, intoDB, descriptorCoverage, typ)
			if err != nil {
				return err
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], typ.ID)
			} else {
				// The remapping logic for a type will perform the remapping for a type's
				// array type, so don't perform this logic for the array type itself.
				if typ.Kind == descpb.TypeDescriptor_ALIAS {
					continue
				}

				// Look up the parent database's ID.
				parentID, err := col.Direct().LookupDatabaseID(ctx, txn, targetDB)
				if err != nil {
					return err
				}
				if parentID == descpb.InvalidID {
					return errors.Errorf("a database named %q needs to exist to restore type %q",
						targetDB, typ.Name)
				}
				// Check privileges on the parent DB.
				parentDB, err := col.Direct().MustGetDatabaseDescByID(ctx, txn, parentID)
				if err != nil {
					return errors.Wrapf(err,
						"failed to lookup parent DB %d", errors.Safe(parentID))
				}

				// See if there is an existing type with the same name.
				getParentSchemaID := func(typ *typedesc.Mutable) (parentSchemaID descpb.ID) {
					parentSchemaID = typ.GetParentSchemaID()
					// If we find UDS with same name defined in the restoring DB, use its ID instead.
					if rewrite, ok := descriptorRewrites[parentSchemaID]; ok && rewrite.ID != 0 {
						parentSchemaID = rewrite.ID
					}
					return
				}
				desc, err := col.Direct().GetDescriptorCollidingWithObject(
					ctx,
					txn,
					parentID,
					getParentSchemaID(typ),
					typ.Name,
				)
				if err != nil {
					return err
				}
				if desc == nil {
					// If we didn't find a type with the same name, then mark that we
					// need to create the type.

					// Ensure that the user has the correct privilege to create types.
					if err := p.CheckPrivilege(ctx, parentDB, privilege.CREATE); err != nil {
						return err
					}

					// Create a rewrite entry for the type.
					descriptorRewrites[typ.ID] = &jobspb.DescriptorRewrite{ParentID: parentID}

					// Ensure that there isn't a collision with the array type name.
					arrTyp := typesByID[typ.ArrayTypeID]
					typeName := tree.NewUnqualifiedTypeName(arrTyp.GetName())
					err = col.Direct().CheckObjectCollision(ctx, txn, parentID, getParentSchemaID(typ), typeName)
					if err != nil {
						return errors.Wrapf(err, "name collision for %q's array type", typ.Name)
					}
					// Create the rewrite entry for the array type as well.
					descriptorRewrites[arrTyp.ID] = &jobspb.DescriptorRewrite{ParentID: parentID}
				} else {
					// If there was a name collision, we'll try to see if we can remap
					// this type to the type existing in the cluster.

					// If the collided object isn't a type, then error out.
					existingType, isType := desc.(catalog.TypeDescriptor)
					if !isType {
						return sqlerrors.MakeObjectAlreadyExistsError(desc.DescriptorProto(), typ.Name)
					}

					// Check if the collided type is compatible to be remapped to.
					if err := typ.IsCompatibleWith(existingType); err != nil {
						return errors.Wrapf(
							err,
							"%q is not compatible with type %q existing in cluster",
							existingType.GetName(),
							existingType.GetName(),
						)
					}

					// Remap both the type and its array type since they are compatible
					// with the type existing in the cluster.
					descriptorRewrites[typ.ID] = &jobspb.DescriptorRewrite{
						ParentID:   existingType.GetParentID(),
						ID:         existingType.GetID(),
						ToExisting: true,
					}
					descriptorRewrites[typ.ArrayTypeID] = &jobspb.DescriptorRewrite{
						ParentID:   existingType.GetParentID(),
						ID:         existingType.GetArrayTypeID(),
						ToExisting: true,
					}
				}
				// If we're restoring to a public schema of database that already exists
				// we can populate the rewrite ParentSchemaID field here since we
				// already have the database descriptor.
				if typ.GetParentSchemaID() == keys.PublicSchemaIDForBackup ||
					typ.GetParentSchemaID() == descpb.InvalidID {
					publicSchemaID := parentDB.GetSchemaID(tree.PublicSchema)
					descriptorRewrites[typ.ID].ParentSchemaID = publicSchemaID
					descriptorRewrites[typ.ArrayTypeID].ParentSchemaID = publicSchemaID
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
		var newID descpb.ID
		var err error
		if descriptorCoverage == tree.AllDescriptors {
			newID = db.GetID()
		} else {
			newID, err = descidgen.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
			if err != nil {
				return nil, err
			}
		}

		descriptorRewrites[db.GetID()] = &jobspb.DescriptorRewrite{ID: newID}

		// If a database restore has specified a new name for the restored database,
		// then populate the rewrite with the newDBName, else the restored database name is preserved.
		if newDBName != "" {
			descriptorRewrites[db.GetID()].NewDBName = newDBName
		}

		for _, objectID := range needsNewParentIDs[db.GetName()] {
			descriptorRewrites[objectID] = &jobspb.DescriptorRewrite{ParentID: newID}
		}
	}

	// descriptorsToRemap usually contains all tables that are being restored. In a
	// full cluster restore this should only include the system tables that need
	// to be remapped to the temporary table. All other tables in a full cluster
	// backup should have the same ID as they do in the backup.
	descriptorsToRemap := make([]catalog.Descriptor, 0, len(tablesByID))
	for _, table := range tablesByID {
		if descriptorCoverage == tree.AllDescriptors || restoreSystemUsers {
			if table.ParentID == systemschema.SystemDB.GetID() {
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

	// Update remapping information for schema descriptors.
	for _, sc := range schemasByID {
		if descriptorCoverage == tree.AllDescriptors {
			// The schema doesn't need to be remapped.
			descriptorRewrites[sc.ID].ID = sc.ID
		} else {
			// If this schema isn't being remapped to an existing schema, then
			// request to generate an ID for it.
			if !descriptorRewrites[sc.ID].ToExisting {
				descriptorsToRemap = append(descriptorsToRemap, sc)
			}
		}
	}

	sort.Sort(catalog.Descriptors(descriptorsToRemap))

	// Generate new IDs for the schemas, tables, and types that need to be
	// remapped.
	for _, desc := range descriptorsToRemap {
		id, err := descidgen.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return nil, err
		}
		descriptorRewrites[desc.GetID()].ID = id
	}

	// Now that the descriptorRewrites contains a complete rewrite entry for every
	// schema that is being restored, we can correctly populate the ParentSchemaID
	// of all tables and types.
	rewriteObject := func(desc catalog.Descriptor) {
		if descriptorRewrites[desc.GetID()].ParentSchemaID != descpb.InvalidID {
			// The rewrite is already populated for the Schema ID,
			// don't rewrite again.
			return
		}
		curSchemaID := desc.GetParentSchemaID()
		newSchemaID := curSchemaID
		if rw, ok := descriptorRewrites[curSchemaID]; ok {
			newSchemaID = rw.ID
		}
		descriptorRewrites[desc.GetID()].ParentSchemaID = newSchemaID
	}
	for _, table := range tablesByID {
		rewriteObject(table)
	}
	for _, typ := range typesByID {
		rewriteObject(typ)
	}

	return descriptorRewrites, nil
}

func getDatabaseIDAndDesc(
	ctx context.Context, txn *kv.Txn, col *descs.Collection, targetDB string,
) (dbID descpb.ID, dbDesc catalog.DatabaseDescriptor, err error) {
	dbID, err = col.Direct().LookupDatabaseID(ctx, txn, targetDB)
	if err != nil {
		return 0, nil, err
	}
	if dbID == descpb.InvalidID {
		return dbID, nil, errors.Errorf("a database named %q needs to exist", targetDB)
	}
	// Check privileges on the parent DB.
	dbDesc, err = col.Direct().MustGetDatabaseDescByID(ctx, txn, dbID)
	if err != nil {
		return 0, nil, errors.Wrapf(err,
			"failed to lookup parent DB %d", errors.Safe(dbID))
	}
	return dbID, dbDesc, nil
}

// If we're doing a full cluster restore - to treat defaultdb and postgres
// as regular databases, we drop them before restoring them again in the
// restore.
func dropDefaultUserDBs(ctx context.Context, execCfg *sql.ExecutorConfig) error {
	return sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		ie := execCfg.InternalExecutor
		_, err := ie.Exec(ctx, "drop-defaultdb", nil, "DROP DATABASE IF EXISTS defaultdb")
		if err != nil {
			return err
		}

		_, err = ie.Exec(ctx, "drop-postgres", nil, "DROP DATABASE IF EXISTS postgres")
		if err != nil {
			return err
		}
		return nil
	})
}

func resolveTargetDB(
	ctx context.Context,
	txn *kv.Txn,
	p sql.PlanHookState,
	databasesByID map[descpb.ID]*dbdesc.Mutable,
	intoDB string,
	descriptorCoverage tree.DescriptorCoverage,
	descriptor catalog.Descriptor,
) (string, error) {
	if intoDB != "" {
		return intoDB, nil
	}

	if descriptorCoverage == tree.AllDescriptors && catalog.IsSystemDescriptor(descriptor) {
		var targetDB string
		if descriptor.GetParentID() == systemschema.SystemDB.GetID() {
			// For full cluster backups, put the system tables in the temporary
			// system table.
			targetDB = restoreTempSystemDB
		}
		return targetDB, nil
	}

	database, ok := databasesByID[descriptor.GetParentID()]
	if !ok {
		return "", errors.Errorf("no database with ID %d in backup for object %q (%d)",
			descriptor.GetParentID(), descriptor.GetName(), descriptor.GetID())
	}
	return database.Name, nil
}

// maybeUpgradeDescriptors performs post-deserialization upgrades on the
// descriptors.
//
// This is done, for instance, to use the newer 19.2-style foreign key
// representation, if they are not already upgraded.
//
// if skipFKsWithNoMatchingTable is set, FKs whose "other" table is missing from
// the set provided are omitted during the upgrade, instead of causing an error
// to be returned.
func maybeUpgradeDescriptors(descs []catalog.Descriptor, skipFKsWithNoMatchingTable bool) error {
	for j, desc := range descs {
		var b catalog.DescriptorBuilder
		if tableDesc, isTable := desc.(catalog.TableDescriptor); isTable {
			b = tabledesc.NewBuilderForFKUpgrade(tableDesc.TableDesc(), skipFKsWithNoMatchingTable)
		} else {
			b = desc.NewBuilder()
		}
		b.RunPostDeserializationChanges()
		err := b.RunRestoreChanges(func(id descpb.ID) catalog.Descriptor {
			for _, d := range descs {
				if d.GetID() == id {
					return d
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		descs[j] = b.BuildExistingMutable()
	}
	return nil
}

// maybeUpgradeDescriptorsInBackupManifests updates the descriptors in the
// manifests. This is done in particular to use the newer 19.2-style foreign
// key representation, if they are not already upgraded.
// This requires resolving cross-table FK references, which is done by looking
// up all table descriptors across all backup descriptors provided.
// If skipFKsWithNoMatchingTable is set, FKs whose
// "other" table is missing from the set provided are omitted during the
// upgrade, instead of causing an error to be returned.
func maybeUpgradeDescriptorsInBackupManifests(
	backupManifests []BackupManifest, skipFKsWithNoMatchingTable bool,
) error {
	if len(backupManifests) == 0 {
		return nil
	}
	descriptors := make([]catalog.Descriptor, 0, len(backupManifests[0].Descriptors))
	for _, backupManifest := range backupManifests {
		for _, pb := range backupManifest.Descriptors {
			descriptors = append(descriptors, descbuilder.NewBuilder(&pb).BuildExistingMutable())
		}
	}

	err := maybeUpgradeDescriptors(descriptors, skipFKsWithNoMatchingTable)
	if err != nil {
		return err
	}

	k := 0
	for i := range backupManifests {
		manifest := &backupManifests[i]
		for j := range manifest.Descriptors {
			manifest.Descriptors[j] = *descriptors[k].DescriptorProto()
			k++
		}
	}
	return nil
}

// resolveOptionsForRestoreJobDescription creates a copy of
// the options specified during a restore, after processing
// them to be suitable for displaying in the jobs' description.
// This includes redacting secrets from external storage URIs.
func resolveOptionsForRestoreJobDescription(
	opts tree.RestoreOptions, intoDB string, newDBName string, kmsURIs []string, incFrom []string,
) (tree.RestoreOptions, error) {
	if opts.IsDefault() {
		return opts, nil
	}

	newOpts := tree.RestoreOptions{
		SkipMissingFKs:            opts.SkipMissingFKs,
		SkipMissingSequences:      opts.SkipMissingSequences,
		SkipMissingSequenceOwners: opts.SkipMissingSequenceOwners,
		SkipMissingViews:          opts.SkipMissingViews,
		Detached:                  opts.Detached,
	}

	if opts.EncryptionPassphrase != nil {
		newOpts.EncryptionPassphrase = tree.NewDString("redacted")
	}

	if opts.IntoDB != nil {
		newOpts.IntoDB = tree.NewDString(intoDB)
	}

	if opts.NewDBName != nil {
		newOpts.NewDBName = tree.NewDString(newDBName)
	}

	for _, uri := range kmsURIs {
		redactedURI, err := cloud.RedactKMSURI(uri)
		if err != nil {
			return tree.RestoreOptions{}, err
		}
		newOpts.DecryptionKMSURI = append(newOpts.DecryptionKMSURI, tree.NewDString(redactedURI))
	}

	if opts.IncrementalStorage != nil {
		var err error
		newOpts.IncrementalStorage, err = sanitizeURIList(incFrom)
		if err != nil {
			return tree.RestoreOptions{}, err
		}
	}

	return newOpts, nil
}

func restoreJobDescription(
	p sql.PlanHookState,
	restore *tree.Restore,
	from [][]string,
	incFrom []string,
	opts tree.RestoreOptions,
	intoDB string,
	newDBName string,
	kmsURIs []string,
) (string, error) {
	r := &tree.Restore{
		SystemUsers:        restore.SystemUsers,
		DescriptorCoverage: restore.DescriptorCoverage,
		AsOf:               restore.AsOf,
		Targets:            restore.Targets,
		From:               make([]tree.StringOrPlaceholderOptList, len(restore.From)),
	}

	var options tree.RestoreOptions
	var err error
	if options, err = resolveOptionsForRestoreJobDescription(opts, intoDB, newDBName,
		kmsURIs, incFrom); err != nil {
		return "", err
	}
	r.Options = options

	for i, backup := range from {
		r.From[i] = make(tree.StringOrPlaceholderOptList, len(backup))
		r.From[i], err = sanitizeURIList(backup)
		if err != nil {
			return "", err
		}
	}

	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(r, ann), nil
}

// restorePlanHook implements sql.PlanHookFn.
func restorePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	restoreStmt, ok := stmt.(*tree.Restore)
	if !ok {
		return nil, nil, nil, false, nil
	}

	if err := featureflag.CheckEnabled(
		ctx,
		p.ExecCfg(),
		featureRestoreEnabled,
		"RESTORE",
	); err != nil {
		return nil, nil, nil, false, err
	}

	fromFns := make([]func() ([]string, error), len(restoreStmt.From))
	for i := range restoreStmt.From {
		fromFn, err := p.TypeAsStringArray(ctx, tree.Exprs(restoreStmt.From[i]), "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
		fromFns[i] = fromFn
	}

	var pwFn func() (string, error)
	var err error
	if restoreStmt.Options.EncryptionPassphrase != nil {
		pwFn, err = p.TypeAsString(ctx, restoreStmt.Options.EncryptionPassphrase, "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var kmsFn func() ([]string, error)
	if restoreStmt.Options.DecryptionKMSURI != nil {
		if restoreStmt.Options.EncryptionPassphrase != nil {
			return nil, nil, nil, false, errors.New("cannot have both encryption_passphrase and kms option set")
		}
		kmsFn, err = p.TypeAsStringArray(ctx, tree.Exprs(restoreStmt.Options.DecryptionKMSURI),
			"RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var intoDBFn func() (string, error)
	if restoreStmt.Options.IntoDB != nil {
		if restoreStmt.SystemUsers {
			return nil, nil, nil, false, errors.New("cannot set into_db option when only restoring system users")
		}
		intoDBFn, err = p.TypeAsString(ctx, restoreStmt.Options.IntoDB, "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	subdirFn := func() (string, error) { return "", nil }
	if restoreStmt.Subdir != nil {
		subdirFn, err = p.TypeAsString(ctx, restoreStmt.Subdir, "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var incStorageFn func() ([]string, error)
	if restoreStmt.Options.IncrementalStorage != nil {
		if restoreStmt.Subdir == nil {
			err = errors.New("incremental_location can only be used with the following" +
				" syntax: 'RESTORE [target] FROM [subdirectory] IN [destination]'")
			return nil, nil, nil, false, err
		}
		incStorageFn, err = p.TypeAsStringArray(ctx, tree.Exprs(restoreStmt.Options.IncrementalStorage),
			"RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var newDBNameFn func() (string, error)
	if restoreStmt.Options.NewDBName != nil {
		if restoreStmt.DescriptorCoverage == tree.AllDescriptors || len(restoreStmt.Targets.Databases) != 1 {
			err = errors.New("new_db_name can only be used for RESTORE DATABASE with a single target" +
				" database")
			return nil, nil, nil, false, err
		}
		if restoreStmt.SystemUsers {
			return nil, nil, nil, false, errors.New("cannot set new_db_name option when only restoring system users")
		}
		newDBNameFn, err = p.TypeAsString(ctx, restoreStmt.Options.NewDBName, "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var newTenantIDFn func() (*roachpb.TenantID, error)
	if restoreStmt.Options.AsTenant != nil {
		if restoreStmt.DescriptorCoverage == tree.AllDescriptors || !restoreStmt.Targets.TenantID.IsSet() {
			err := errors.Errorf("%q can only be used when running RESTORE TENANT for a single tenant", restoreOptAsTenant)
			return nil, nil, nil, false, err
		}
		// TODO(dt): it'd be nice to have TypeAsInt or TypeAsTenantID and then in
		// sql.y an int_or_placeholder, but right now the hook view of planner only
		// has TypeAsString so we'll just atoi it.
		fn, err := p.TypeAsString(ctx, restoreStmt.Options.AsTenant, "RESTORE")
		if err != nil {
			return nil, nil, nil, false, err
		}
		newTenantIDFn = func() (*roachpb.TenantID, error) {
			s, err := fn()
			if err != nil {
				return nil, err
			}
			x, err := strconv.Atoi(s)
			if err != nil {
				return nil, err
			}
			if x < int(roachpb.MinTenantID.ToUint64()) {
				return nil, errors.New("invalid tenant ID value")
			}
			id := roachpb.MakeTenantID(uint64(x))
			return &id, nil
		}
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if !(p.ExtendedEvalContext().TxnImplicit || restoreStmt.Options.Detached) {
			return errors.Errorf("RESTORE cannot be used inside a transaction without DETACHED option")
		}

		subdir, err := subdirFn()
		if err != nil {
			return err
		}

		from := make([][]string, len(fromFns))
		for i := range fromFns {
			from[i], err = fromFns[i]()
			if err != nil {
				return err
			}
		}

		if err := checkPrivilegesForRestore(ctx, restoreStmt, p, from); err != nil {
			return err
		}

		var endTime hlc.Timestamp
		if restoreStmt.AsOf.Expr != nil {
			asOf, err := p.EvalAsOfTimestamp(ctx, restoreStmt.AsOf)
			if err != nil {
				return err
			}
			endTime = asOf.Timestamp
		}

		var passphrase string
		if pwFn != nil {
			passphrase, err = pwFn()
			if err != nil {
				return err
			}
		}

		var kms []string
		if kmsFn != nil {
			kms, err = kmsFn()
			if err != nil {
				return err
			}
		}

		var intoDB string
		if intoDBFn != nil {
			intoDB, err = intoDBFn()
			if err != nil {
				return err
			}
		}

		var newDBName string
		if newDBNameFn != nil {
			newDBName, err = newDBNameFn()
			if err != nil {
				return err
			}
		}
		var newTenantID *roachpb.TenantID
		if newTenantIDFn != nil {
			newTenantID, err = newTenantIDFn()
			if err != nil {
				return err
			}
		}

		// incFrom will contain the directory URIs for incremental backups (i.e.
		// <prefix>/<subdir>) iff len(From)==1, regardless of the
		// 'incremental_location' param. len(From)=1 implies that the user has not
		// explicitly passed incremental backups, so we'll have to look for any in
		// <prefix>/<subdir>. len(incFrom)>1 implies the incremental backups are
		// locality aware.
		var incFrom []string
		if incStorageFn != nil {
			incFrom, err = incStorageFn()
			if err != nil {
				return err
			}
		}
		if subdir != "" {
			if strings.EqualFold(subdir, "LATEST") {
				// set subdir to content of latest file
				latest, err := readLatestFile(ctx, from[0][0], p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, p.User())
				if err != nil {
					return err
				}
				subdir = latest
			}
			if len(from) != 1 {
				return errors.Errorf("RESTORE FROM ... IN can only by used against a single collection path (per-locality)")
			}

			appendPaths := func(uris []string, tailDir string) error {
				for i, uri := range uris {
					parsed, err := url.Parse(uri)
					if err != nil {
						return err
					}
					parsed.Path = path.Join(parsed.Path, tailDir)
					uris[i] = parsed.String()
				}
				return nil
			}

			if err = appendPaths(from[0][:], subdir); err != nil {
				return err
			}
			if len(incFrom) != 0 {
				if err = appendPaths(incFrom[:], subdir); err != nil {
					return err
				}
			}
		}

		return doRestorePlan(ctx, restoreStmt, p, from, incFrom, passphrase, kms, intoDB,
			newDBName, newTenantID, endTime, resultsCh)
	}

	if restoreStmt.Options.Detached {
		return fn, jobs.DetachedJobExecutionResultHeader, nil, false, nil
	}
	return fn, jobs.BulkJobExecutionResultHeader, nil, false, nil
}

func checkPrivilegesForRestore(
	ctx context.Context, restoreStmt *tree.Restore, p sql.PlanHookState, from [][]string,
) error {
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if hasAdmin {
		return nil
	}
	// Do not allow full cluster restores.
	if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"only users with the admin role are allowed to restore full cluster backups")
	}
	// Do not allow tenant restores.
	if restoreStmt.Targets.TenantID.IsSet() {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"only users with the admin role can perform RESTORE TENANT")
	}
	// Database restores require the CREATEDB privileges.
	if len(restoreStmt.Targets.Databases) > 0 {
		hasCreateDB, err := p.HasRoleOption(ctx, roleoption.CREATEDB)
		if err != nil {
			return err
		}
		if !hasCreateDB {
			return pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"only users with the CREATEDB privilege can restore databases")
		}
	}
	if p.ExecCfg().ExternalIODirConfig.EnableNonAdminImplicitAndArbitraryOutbound {
		return nil
	}
	// Check that none of the sources rely on implicit access.
	for i := range from {
		for j := range from[i] {
			conf, err := cloud.ExternalStorageConfFromURI(from[i][j], p.User())
			if err != nil {
				return err
			}
			if !conf.AccessIsWithExplicitAuth() {
				return pgerror.Newf(
					pgcode.InsufficientPrivilege,
					"only users with the admin role are allowed to RESTORE from the specified %s URI",
					conf.Provider.String())
			}
		}
	}
	return nil
}

func checkClusterRegions(
	ctx context.Context, p sql.PlanHookState, typesByID map[descpb.ID]*typedesc.Mutable,
) error {
	regionSet := make(map[catpb.RegionName]struct{})
	for _, typ := range typesByID {
		typeDesc := typedesc.NewBuilder(typ.TypeDesc()).BuildImmutableType()
		if typeDesc.GetKind() == descpb.TypeDescriptor_MULTIREGION_ENUM {
			regionNames, err := typeDesc.RegionNames()
			if err != nil {
				return err
			}
			for _, region := range regionNames {
				if _, ok := regionSet[region]; !ok {
					regionSet[region] = struct{}{}
				}
			}
		}
	}

	if len(regionSet) == 0 {
		return nil
	}

	l, err := sql.GetLiveClusterRegions(ctx, p)
	if err != nil {
		return err
	}

	missingRegions := make([]string, 0)
	for region := range regionSet {
		if !l.IsActive(region) {
			missingRegions = append(missingRegions, string(region))
		}
	}

	if len(missingRegions) > 0 {
		// Missing regions are sorted for predictable outputs in tests.
		sort.Strings(missingRegions)
		mismatchErr := errors.Newf("detected a mismatch in regions between the restore cluster and the backup cluster, "+
			"missing regions detected: %s.", strings.Join(missingRegions, ", "))
		hintsMsg := fmt.Sprintf("there are two ways you can resolve this issue: "+
			"1) update the cluster to which you're restoring to ensure that the regions present on the nodes' "+
			"--locality flags match those present in the backup image, or "+
			"2) restore with the %q option", restoreOptSkipLocalitiesCheck)
		return errors.WithHint(mismatchErr, hintsMsg)
	}

	return nil
}

func doRestorePlan(
	ctx context.Context,
	restoreStmt *tree.Restore,
	p sql.PlanHookState,
	from [][]string,
	incFrom []string,
	passphrase string,
	kms []string,
	intoDB string,
	newDBName string,
	newTenantID *roachpb.TenantID,
	endTime hlc.Timestamp,
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

	var encryption *jobspb.BackupEncryptionOptions
	if restoreStmt.Options.EncryptionPassphrase != nil {
		opts, err := readEncryptionOptions(ctx, baseStores[0])
		if err != nil {
			return err
		}
		encryptionKey := storageccl.GenerateKey([]byte(passphrase), opts[0].Salt)
		encryption = &jobspb.BackupEncryptionOptions{
			Mode: jobspb.EncryptionMode_Passphrase,
			Key:  encryptionKey,
		}
	} else if restoreStmt.Options.DecryptionKMSURI != nil {
		opts, err := readEncryptionOptions(ctx, baseStores[0])
		if err != nil {
			return err
		}
		ioConf := baseStores[0].ExternalIOConf()

		// A backup could have been encrypted with multiple KMS keys that
		// are stored across ENCRYPTION-INFO files. Iterate over all
		// ENCRYPTION-INFO files to check if the KMS passed in during
		// restore has been used to encrypt the backup at least once.
		var defaultKMSInfo *jobspb.BackupEncryptionOptions_KMSInfo
		for _, encFile := range opts {
			defaultKMSInfo, err = validateKMSURIsAgainstFullBackup(kms,
				newEncryptedDataKeyMapFromProtoMap(encFile.EncryptedDataKeyByKMSMasterKeyID), &backupKMSEnv{
					baseStores[0].Settings(),
					&ioConf,
				})
			if err == nil {
				break
			}
		}
		if err != nil {
			return err
		}
		encryption = &jobspb.BackupEncryptionOptions{
			Mode:    jobspb.EncryptionMode_KMS,
			KMSInfo: defaultKMSInfo,
		}
	}

	mem := p.ExecCfg().RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	defaultURIs, mainBackupManifests, localityInfo, memReserved, err := resolveBackupManifests(
		ctx, &mem, baseStores, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, from,
		incFrom, endTime, encryption, p.User(),
	)
	if err != nil {
		return err
	}
	defer func() {
		mem.Shrink(ctx, memReserved)
	}()

	currentVersion := p.ExecCfg().Settings.Version.ActiveVersion(ctx)
	for i := range mainBackupManifests {
		if v := mainBackupManifests[i].ClusterVersion; v.Major != 0 {
			// This is the "cluster" version that does not change between patches but
			// rather just tracks migrations run. If the backup is more migrated than
			// this cluster, then this cluster isn't ready to restore this backup.
			if currentVersion.Less(v) {
				return errors.Errorf("backup from version %s is newer than current version %s", v, currentVersion)
			}
		}
	}

	// Validate that the table coverage of the backup matches that of the restore.
	// This prevents FULL CLUSTER backups to be restored as anything but full
	// cluster restores and vice-versa.
	if restoreStmt.DescriptorCoverage == tree.AllDescriptors && mainBackupManifests[0].DescriptorCoverage == tree.RequestedDescriptors {
		return errors.Errorf("full cluster RESTORE can only be used on full cluster BACKUP files")
	}

	// Ensure that no user descriptors exist for a full cluster restore.
	if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
		var allDescs []catalog.Descriptor
		if err := sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn *kv.Txn, col *descs.Collection) (err error) {
			txn.SetDebugName("count-user-descs")
			all, err := col.GetAllDescriptors(ctx, txn)
			allDescs = all.OrderedDescriptors()
			return err
		}); err != nil {
			return errors.Wrap(err, "looking up user descriptors during restore")
		}
		if allUserDescs := filteredUserCreatedDescriptors(allDescs); len(allUserDescs) > 0 {
			userDescriptorNames := make([]string, 0, 20)
			for i, desc := range allUserDescs {
				if i == 20 {
					userDescriptorNames = append(userDescriptorNames, "...")
					break
				}
				userDescriptorNames = append(userDescriptorNames, desc.GetName())
			}
			return errors.Errorf(
				"full cluster restore can only be run on a cluster with no tables or databases but found %d descriptors: %s",
				len(allUserDescs), strings.Join(userDescriptorNames, ", "),
			)
		}
	}

	// wasOffline tracks which tables were in an offline or adding state at some
	// point in the incremental chain, meaning their spans would be seeing
	// non-transactional bulk-writes. If that backup exported those spans, then it
	// can't be trusted for that table/index since those bulk-writes can fail to
	// be caught by backups.
	wasOffline := make(map[tableAndIndex]hlc.Timestamp)

	for _, m := range mainBackupManifests {
		spans := roachpb.Spans(m.Spans)
		for i := range m.Descriptors {
			table, _, _, _ := descpb.FromDescriptor(&m.Descriptors[i])
			if table == nil {
				continue
			}
			index := table.GetPrimaryIndex()
			if len(index.Interleave.Ancestors) > 0 || len(index.InterleavedBy) > 0 {
				return errors.Errorf("restoring interleaved tables is no longer allowed. table %s was found to be interleaved", table.Name)
			}
			if err := catalog.ForEachNonDropIndex(
				tabledesc.NewBuilder(table).BuildImmutable().(catalog.TableDescriptor),
				func(index catalog.Index) error {
					if index.Adding() && spans.ContainsKey(p.ExecCfg().Codec.IndexPrefix(uint32(table.ID), uint32(index.GetID()))) {
						k := tableAndIndex{tableID: table.ID, indexID: index.GetID()}
						if _, ok := wasOffline[k]; !ok {
							wasOffline[k] = m.EndTime
						}
					}
					return nil
				}); err != nil {
				return err
			}
		}
	}

	sqlDescs, restoreDBs, tenants, err := selectTargets(
		ctx, p, mainBackupManifests, restoreStmt.Targets, restoreStmt.DescriptorCoverage, endTime, restoreStmt.SystemUsers,
	)
	if err != nil {
		return errors.Wrap(err,
			"failed to resolve targets in the BACKUP location specified by the RESTORE stmt, "+
				"use SHOW BACKUP to find correct targets")
	}

	var revalidateIndexes []jobspb.RestoreDetails_RevalidateIndex
	for _, desc := range sqlDescs {
		tbl, ok := desc.(catalog.TableDescriptor)
		if !ok {
			continue
		}
		for _, idx := range tbl.ActiveIndexes() {
			if _, ok := wasOffline[tableAndIndex{tableID: desc.GetID(), indexID: idx.GetID()}]; ok {
				revalidateIndexes = append(revalidateIndexes, jobspb.RestoreDetails_RevalidateIndex{
					TableID: desc.GetID(), IndexID: idx.GetID(),
				})
			}
		}
	}

	databaseModifiers, newTypeDescs, err := planDatabaseModifiersForRestore(ctx, p, sqlDescs, restoreDBs)
	if err != nil {
		return err
	}

	sqlDescs = append(sqlDescs, newTypeDescs...)

	if err := maybeUpgradeDescriptors(sqlDescs, restoreStmt.Options.SkipMissingFKs); err != nil {
		return err
	}

	if restoreStmt.Options.NewDBName != nil {
		if err := renameTargetDatabaseDescriptor(sqlDescs, restoreDBs, newDBName); err != nil {
			return err
		}
	}

	var oldTenantID *roachpb.TenantID
	if len(tenants) > 0 {
		if !p.ExecCfg().Codec.ForSystemTenant() {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "only the system tenant can restore other tenants")
		}
		if newTenantID != nil {
			if len(tenants) != 1 {
				return errors.Errorf("%q option can only be used when restoring a single tenant", restoreOptAsTenant)
			}
			res, err := p.ExecCfg().InternalExecutor.QueryRow(
				ctx, "restore-lookup-tenant", p.ExtendedEvalContext().Txn,
				`SELECT active FROM system.tenants WHERE id = $1`, newTenantID.ToUint64(),
			)
			if err != nil {
				return err
			}
			if res != nil {
				return errors.Errorf("tenant %s already exists", newTenantID)
			}
			old := roachpb.MakeTenantID(tenants[0].ID)
			tenants[0].ID = newTenantID.ToUint64()
			oldTenantID = &old
		} else {
			for _, i := range tenants {
				res, err := p.ExecCfg().InternalExecutor.QueryRow(
					ctx, "restore-lookup-tenant", p.ExtendedEvalContext().Txn,
					`SELECT active FROM system.tenants WHERE id = $1`, i.ID,
				)
				if err != nil {
					return err
				}
				if res != nil {
					return errors.Errorf("tenant %d already exists", i.ID)
				}
			}
		}
	}

	databasesByID := make(map[descpb.ID]*dbdesc.Mutable)
	schemasByID := make(map[descpb.ID]*schemadesc.Mutable)
	tablesByID := make(map[descpb.ID]*tabledesc.Mutable)
	typesByID := make(map[descpb.ID]*typedesc.Mutable)

	for _, desc := range sqlDescs {
		switch desc := desc.(type) {
		case *dbdesc.Mutable:
			databasesByID[desc.GetID()] = desc
		case *schemadesc.Mutable:
			schemasByID[desc.ID] = desc
		case *tabledesc.Mutable:
			tablesByID[desc.ID] = desc
		case *typedesc.Mutable:
			typesByID[desc.ID] = desc
		}
	}

	if !restoreStmt.Options.SkipLocalitiesCheck {
		if err := checkClusterRegions(ctx, p, typesByID); err != nil {
			return err
		}
	}

	var debugPauseOn string
	if restoreStmt.Options.DebugPauseOn != nil {
		pauseOnFn, err := p.TypeAsString(ctx, restoreStmt.Options.DebugPauseOn, "RESTORE")
		if err != nil {
			return err
		}

		debugPauseOn, err = pauseOnFn()
		if err != nil {
			return err
		}

		if _, ok := allowedDebugPauseOnValues[debugPauseOn]; len(debugPauseOn) > 0 && !ok {
			return errors.Newf("%s cannot be set with the value %s", restoreOptDebugPauseOn, debugPauseOn)
		}
	}

	filteredTablesByID, err := maybeFilterMissingViews(
		tablesByID,
		typesByID,
		restoreStmt.Options.SkipMissingViews)
	if err != nil {
		return err
	}

	// When running a full cluster restore, we drop the defaultdb and postgres
	// databases that are present in a new cluster.
	// This is done so that they can be restored the same way any other user
	// defined database would be restored from the backup.
	if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
		if err := dropDefaultUserDBs(ctx, p.ExecCfg()); err != nil {
			return err
		}
	}

	descriptorRewrites, err := allocateDescriptorRewrites(
		ctx,
		p,
		databasesByID,
		schemasByID,
		filteredTablesByID,
		typesByID,
		restoreDBs,
		restoreStmt.DescriptorCoverage,
		restoreStmt.Options,
		intoDB,
		newDBName,
		restoreStmt.SystemUsers)
	if err != nil {
		return err
	}
	description, err := restoreJobDescription(p, restoreStmt, from, incFrom, restoreStmt.Options,
		intoDB,
		newDBName, kms)
	if err != nil {
		return err
	}

	var databases []*dbdesc.Mutable
	for i := range databasesByID {
		if _, ok := descriptorRewrites[i]; ok {
			databases = append(databases, databasesByID[i])
		}
	}
	var schemas []*schemadesc.Mutable
	for i := range schemasByID {
		schemas = append(schemas, schemasByID[i])
	}
	var tables []*tabledesc.Mutable
	for _, desc := range filteredTablesByID {
		tables = append(tables, desc)
	}
	var types []*typedesc.Mutable
	for _, desc := range typesByID {
		types = append(types, desc)
	}

	// We attempt to rewrite ID's in the collected type and table descriptors
	// to catch errors during this process here, rather than in the job itself.
	if err := rewrite.TableDescs(tables, descriptorRewrites, intoDB); err != nil {
		return err
	}
	if err := rewrite.DatabaseDescs(databases, descriptorRewrites); err != nil {
		return err
	}
	if err := rewrite.SchemaDescs(schemas, descriptorRewrites); err != nil {
		return err
	}
	if err := rewrite.TypeDescs(types, descriptorRewrites); err != nil {
		return err
	}
	for i := range revalidateIndexes {
		revalidateIndexes[i].TableID = descriptorRewrites[revalidateIndexes[i].TableID].ID
	}

	// Collect telemetry.
	collectTelemetry := func() {
		telemetry.Count("restore.total.started")
		if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
			telemetry.Count("restore.full-cluster")
		}
	}

	encodedTables := make([]*descpb.TableDescriptor, len(tables))
	for i, table := range tables {
		encodedTables[i] = table.TableDesc()
	}

	jr := jobs.Record{
		Description: description,
		Username:    p.User(),
		DescriptorIDs: func() (sqlDescIDs []descpb.ID) {
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
			TableDescs:         encodedTables,
			Tenants:            tenants,
			OverrideDB:         intoDB,
			DescriptorCoverage: restoreStmt.DescriptorCoverage,
			Encryption:         encryption,
			RevalidateIndexes:  revalidateIndexes,
			DatabaseModifiers:  databaseModifiers,
			DebugPauseOn:       debugPauseOn,
			RestoreSystemUsers: restoreStmt.SystemUsers,
			PreRewriteTenantId: oldTenantID,
		},
		Progress: jobspb.RestoreProgress{},
	}

	if restoreStmt.Options.Detached {
		// When running in detached mode, we simply create the job record.
		// We do not wait for the job to finish.
		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		_, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
			ctx, jr, jobID, p.ExtendedEvalContext().Txn)
		if err != nil {
			return err
		}
		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jobID))}
		collectTelemetry()
		return nil
	}

	// We create the job record in the planner's transaction to ensure that
	// the job record creation happens transactionally.
	plannerTxn := p.ExtendedEvalContext().Txn

	// Construct the job and commit the transaction. Perform this work in a
	// closure to ensure that the job is cleaned up if an error occurs.
	var sj *jobs.StartableJob
	if err := func() (err error) {
		defer func() {
			if err == nil || sj == nil {
				return
			}
			if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
				log.Errorf(ctx, "failed to cleanup job: %v", cleanupErr)
			}
		}()
		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		if err := p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, &sj, jobID, plannerTxn, jr); err != nil {
			return err
		}

		// We commit the transaction here so that the job can be started. This is
		// safe because we're in an implicit transaction. If we were in an explicit
		// transaction the job would have to be created with the detached option and
		// would have been handled above.
		return plannerTxn.Commit(ctx)
	}(); err != nil {
		return err
	}
	collectTelemetry()
	if err := sj.Start(ctx); err != nil {
		return err
	}
	if err := sj.AwaitCompletion(ctx); err != nil {
		return err
	}
	return sj.ReportExecutionResults(ctx, resultsCh)
}

func filteredUserCreatedDescriptors(
	allDescs []catalog.Descriptor,
) (userDescs []catalog.Descriptor) {
	defaultDBs := make(map[descpb.ID]catalog.DatabaseDescriptor, len(catalogkeys.DefaultUserDBs))
	{
		names := make(map[string]struct{}, len(catalogkeys.DefaultUserDBs))
		for _, name := range catalogkeys.DefaultUserDBs {
			names[name] = struct{}{}
		}
		for _, desc := range allDescs {
			if db, ok := desc.(catalog.DatabaseDescriptor); ok {
				if _, found := names[desc.GetName()]; found {
					defaultDBs[db.GetID()] = db
				}
			}
		}
	}

	userDescs = make([]catalog.Descriptor, 0, len(allDescs))
	for _, desc := range allDescs {
		if catalog.IsSystemDescriptor(desc) {
			// Exclude system descriptors.
			continue
		}
		if _, found := defaultDBs[desc.GetID()]; found {
			// Exclude default databases.
			continue
		}
		if sc, ok := desc.(catalog.SchemaDescriptor); ok && sc.GetName() == catconstants.PublicSchemaName {
			if _, found := defaultDBs[sc.GetParentID()]; found {
				// Exclude public schemas of default databases.
				continue
			}
		}
		userDescs = append(userDescs, desc)
	}
	return userDescs
}

// renameTargetDatabaseDescriptor updates the name in the target database
// descriptor to the user specified new_db_name. We update the database
// descriptor in both sqlDescs that contains all the descriptors being restored,
// and restoreDBs that contains just the target database descriptor. Renaming
// the target descriptor ensures accurate name resolution during restore.
func renameTargetDatabaseDescriptor(
	sqlDescs []catalog.Descriptor, restoreDBs []catalog.DatabaseDescriptor, newDBName string,
) error {
	if len(restoreDBs) != 1 {
		return errors.NewAssertionErrorWithWrappedErrf(errors.Newf(
			"expected restoreDBs to have a single entry but found %d entries when renaming the target"+
				" database", len(restoreDBs)), "assertion failed")
	}

	for _, desc := range sqlDescs {
		// Only process database descriptors.
		db, isDB := desc.(*dbdesc.Mutable)
		if !isDB {
			continue
		}
		db.SetName(newDBName)
	}
	db, ok := restoreDBs[0].(*dbdesc.Mutable)
	if !ok {
		return errors.NewAssertionErrorWithWrappedErrf(errors.Newf(
			"expected db desc but found %T", db), "assertion failed")
	}
	db.SetName(newDBName)
	return nil
}

func planDatabaseModifiersForRestore(
	ctx context.Context,
	p sql.PlanHookState,
	sqlDescs []catalog.Descriptor,
	restoreDBs []catalog.DatabaseDescriptor,
) (map[descpb.ID]*jobspb.RestoreDetails_DatabaseModifier, []catalog.Descriptor, error) {
	databaseModifiers := make(map[descpb.ID]*jobspb.RestoreDetails_DatabaseModifier)
	defaultPrimaryRegion := catpb.RegionName(
		sql.DefaultPrimaryRegion.Get(&p.ExecCfg().Settings.SV),
	)
	if defaultPrimaryRegion == "" {
		return nil, nil, nil
	}
	if err := multiregionccl.CheckClusterSupportsMultiRegion(p.ExecCfg()); err != nil {
		return nil, nil, errors.WithHintf(
			err,
			"try disabling the default PRIMARY REGION by using RESET CLUSTER SETTING %s",
			sql.DefaultPrimaryRegionClusterSettingName,
		)
	}

	l, err := sql.GetLiveClusterRegions(ctx, p)
	if err != nil {
		return nil, nil, err
	}
	if err := sql.CheckClusterRegionIsLive(
		l,
		defaultPrimaryRegion,
	); err != nil {
		return nil, nil, errors.WithHintf(
			err,
			"set the default PRIMARY REGION to a region that exists (see SHOW REGIONS FROM CLUSTER) then using SET CLUSTER SETTING %s = 'region'",
			sql.DefaultPrimaryRegionClusterSettingName,
		)
	}

	var maxSeenID descpb.ID
	for _, desc := range sqlDescs {
		if desc.GetID() > maxSeenID {
			maxSeenID = desc.GetID()
		}
	}

	shouldRestoreDatabaseIDs := make(map[descpb.ID]struct{})
	for _, db := range restoreDBs {
		shouldRestoreDatabaseIDs[db.GetID()] = struct{}{}
	}

	var extraDescs []catalog.Descriptor
	// Do one pass through the sql descs to map the database to its public schema.
	dbToPublicSchema := make(map[descpb.ID]catalog.SchemaDescriptor)
	for _, desc := range sqlDescs {
		sc, isSc := desc.(*schemadesc.Mutable)
		if !isSc {
			continue
		}
		if sc.GetName() == tree.PublicSchema {
			dbToPublicSchema[sc.GetParentID()] = sc
		}
	}
	for _, desc := range sqlDescs {
		// Only process database descriptors.
		db, isDB := desc.(*dbdesc.Mutable)
		if !isDB {
			continue
		}
		// Check this database is actually being restored.
		if _, ok := shouldRestoreDatabaseIDs[db.GetID()]; !ok {
			continue
		}
		// We only need to change databases with no region defined.
		if db.IsMultiRegion() {
			continue
		}
		p.BufferClientNotice(
			ctx,
			errors.WithHintf(
				pgnotice.Newf(
					"setting the PRIMARY REGION as %s on database %s",
					defaultPrimaryRegion,
					db.GetName(),
				),
				"to change the default primary region, use SET CLUSTER SETTING %[1]s = 'region' "+
					"or use RESET CLUSTER SETTING %[1]s to disable this behavior",
				sql.DefaultPrimaryRegionClusterSettingName,
			),
		)

		// Allocate the region enum ID.
		regionEnumID := maxSeenID + 1
		regionEnumArrayID := maxSeenID + 2
		maxSeenID += 2

		// Assign the multi-region configuration to the database descriptor.
		sg, err := sql.TranslateSurvivalGoal(tree.SurvivalGoalDefault)
		if err != nil {
			return nil, nil, err
		}
		regionConfig := multiregion.MakeRegionConfig(
			[]catpb.RegionName{defaultPrimaryRegion},
			defaultPrimaryRegion,
			sg,
			regionEnumID,
			descpb.DataPlacement_DEFAULT,
		)
		if err := multiregion.ValidateRegionConfig(regionConfig); err != nil {
			return nil, nil, err
		}
		if err := db.SetInitialMultiRegionConfig(&regionConfig); err != nil {
			return nil, nil, err
		}

		// Create the multi-region enums.
		var sc catalog.SchemaDescriptor
		if db.HasPublicSchemaWithDescriptor() {
			sc = dbToPublicSchema[db.GetID()]
		} else {
			sc = schemadesc.GetPublicSchema()
		}
		regionEnum, regionArrayEnum, err := restoreCreateDefaultPrimaryRegionEnums(
			ctx,
			p,
			db,
			sc,
			regionConfig,
			regionEnumID,
			regionEnumArrayID,
		)
		if err != nil {
			return nil, nil, err
		}

		// Append the enums to sqlDescs.
		extraDescs = append(extraDescs, regionEnum, regionArrayEnum)
		databaseModifiers[db.GetID()] = &jobspb.RestoreDetails_DatabaseModifier{
			ExtraTypeDescs: []*descpb.TypeDescriptor{
				&regionEnum.TypeDescriptor,
				&regionArrayEnum.TypeDescriptor,
			},
			RegionConfig: db.GetRegionConfig(),
		}
	}
	return databaseModifiers, extraDescs, nil
}

func restoreCreateDefaultPrimaryRegionEnums(
	ctx context.Context,
	p sql.PlanHookState,
	db *dbdesc.Mutable,
	sc catalog.SchemaDescriptor,
	regionConfig multiregion.RegionConfig,
	regionEnumID descpb.ID,
	regionEnumArrayID descpb.ID,
) (*typedesc.Mutable, *typedesc.Mutable, error) {
	regionLabels := make(tree.EnumValueList, 0, len(regionConfig.Regions()))
	for _, regionName := range regionConfig.Regions() {
		regionLabels = append(regionLabels, tree.EnumValue(regionName))
	}
	regionEnum, err := sql.CreateEnumTypeDesc(
		p.RunParams(ctx),
		regionConfig.RegionEnumID(),
		regionLabels,
		db,
		sc,
		tree.NewQualifiedTypeName(db.GetName(), tree.PublicSchema, tree.RegionEnum),
		sql.EnumTypeMultiRegion,
	)
	if err != nil {
		return nil, nil, err
	}
	regionEnum.ArrayTypeID = regionEnumArrayID
	regionArrayEnum, err := sql.CreateEnumArrayTypeDesc(
		p.RunParams(ctx),
		regionEnum,
		db,
		sc.GetID(),
		regionEnumArrayID,
		"_"+tree.RegionEnum,
	)
	if err != nil {
		return nil, nil, err
	}
	return regionEnum, regionArrayEnum, nil
}

func init() {
	sql.AddPlanHook("restore", restorePlanHook)
}
