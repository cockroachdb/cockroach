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
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupdest"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuputils"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudprivilege"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/rewrite"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	restoreOptIntoDB                    = "into_db"
	restoreOptSkipMissingFKs            = "skip_missing_foreign_keys"
	restoreOptSkipMissingSequences      = "skip_missing_sequences"
	restoreOptSkipMissingSequenceOwners = "skip_missing_sequence_owners"
	restoreOptSkipMissingViews          = "skip_missing_views"
	restoreOptSkipLocalitiesCheck       = "skip_localities_check"
	restoreOptDebugPauseOn              = "debug_pause_on"
	restoreOptAsTenant                  = "tenant_name"
	restoreOptForceTenantID             = "tenant"

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
	functionsByID map[descpb.ID]*funcdesc.Mutable,
	restoreDBs []catalog.DatabaseDescriptor,
	descriptorCoverage tree.DescriptorCoverage,
	opts tree.RestoreOptions,
	intoDB string,
	newDBName string,
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
	for _, table := range tablesByID {
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
				id := typedesc.GetUserDefinedTypeDescID(col.Type)
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

	needsNewParentIDs := make(map[string][]descpb.ID)

	// Increment the DescIDSequenceKey so that it is higher than both the max desc ID
	// in the backup and current max desc ID in the restoring cluster. This generator
	// keeps produced the next descriptor ID.
	if descriptorCoverage == tree.AllDescriptors || descriptorCoverage == tree.SystemUsers {
		tempSysDBID, err := p.ExecCfg().DescIDGenerator.GenerateUniqueDescID(ctx)
		if err != nil {
			return nil, err
		}

		// Remap all of the descriptor belonging to system tables to the temp system
		// DB.
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
	}

	var shouldBufferDeprecatedPrivilegeNotice bool
	databasesWithDeprecatedPrivileges := make(map[string]struct{})

	// Fail fast if the necessary databases don't exist or are otherwise
	// incompatible with this restore.
	if err := func() error {
		txn := p.InternalSQLTxn()
		col := txn.Descriptors()
		// Check that any DBs being restored do _not_ exist.
		for name := range restoreDBNames {
			dbID, err := col.LookupDatabaseID(ctx, txn.KV(), name)
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

			targetDB, err := resolveTargetDB(ctx, databasesByID, intoDB, descriptorCoverage, sc)
			if err != nil {
				return err
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], sc.ID)
			} else {
				// Look up the parent database's ID.
				parentID, parentDB, err := getDatabaseIDAndDesc(ctx, txn.KV(), col, targetDB)
				if err != nil {
					return err
				}
				if usesDeprecatedPrivileges, err := checkRestorePrivilegesOnDatabase(ctx, p, parentDB); err != nil {
					return err
				} else if usesDeprecatedPrivileges {
					shouldBufferDeprecatedPrivilegeNotice = true
					databasesWithDeprecatedPrivileges[parentDB.GetName()] = struct{}{}
				}

				// See if there is an existing schema with the same name.
				id, err := col.LookupSchemaID(ctx, txn.KV(), parentID, sc.Name)
				if err != nil {
					return err
				}
				if id == descpb.InvalidID {
					// If we didn't find a matching schema, then we'll restore this schema.
					descriptorRewrites[sc.ID] = &jobspb.DescriptorRewrite{ParentID: parentID}
				} else {
					// If we found an existing schema, then we need to remap all references
					// to this schema to the existing one.
					desc, err := col.ByID(txn.KV()).Get().Schema(ctx, id)
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

			targetDB, err := resolveTargetDB(ctx, databasesByID, intoDB, descriptorCoverage, table)
			if err != nil {
				return err
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], table.ID)
			} else {
				var parentID descpb.ID
				{
					newParentID, err := col.LookupDatabaseID(ctx, txn.KV(), targetDB)
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
				err := descs.CheckObjectNameCollision(ctx, col, txn.KV(), parentID, table.GetParentSchemaID(), tableName)
				if err != nil {
					return err
				}

				// Check privileges.
				parentDB, err := col.ByID(txn.KV()).Get().Database(ctx, parentID)
				if err != nil {
					return errors.Wrapf(err,
						"failed to lookup parent DB %d", errors.Safe(parentID))
				}
				if usesDeprecatedPrivileges, err := checkRestorePrivilegesOnDatabase(ctx, p, parentDB); err != nil {
					return err
				} else if usesDeprecatedPrivileges {
					shouldBufferDeprecatedPrivilegeNotice = true
					databasesWithDeprecatedPrivileges[parentDB.GetName()] = struct{}{}
				}

				// We're restoring a table and not its parent database. We may block
				// restoring multi-region tables to multi-region databases since
				// regions may mismatch.
				if err := checkMultiRegionCompatible(ctx, txn.KV(), col, table, parentDB); err != nil {
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

			targetDB, err := resolveTargetDB(ctx, databasesByID, intoDB, descriptorCoverage, typ)
			if err != nil {
				return err
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], typ.ID)
			} else {
				// The remapping logic for a type will perform the remapping for a type's
				// array type, so don't perform this logic for the array type itself.
				if typ.AsAliasTypeDescriptor() != nil {
					continue
				}

				// Look up the parent database's ID.
				parentID, err := col.LookupDatabaseID(ctx, txn.KV(), targetDB)
				if err != nil {
					return err
				}
				if parentID == descpb.InvalidID {
					return errors.Errorf("a database named %q needs to exist to restore type %q",
						targetDB, typ.Name)
				}
				// Check privileges on the parent DB.
				parentDB, err := col.ByID(txn.KV()).Get().Database(ctx, parentID)
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
				desc, err := descs.GetDescriptorCollidingWithObjectName(
					ctx,
					col,
					txn.KV(),
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
					if usesDeprecatedPrivileges, err := checkRestorePrivilegesOnDatabase(ctx, p, parentDB); err != nil {
						return err
					} else if usesDeprecatedPrivileges {
						shouldBufferDeprecatedPrivilegeNotice = true
						databasesWithDeprecatedPrivileges[parentDB.GetName()] = struct{}{}
					}

					// Create a rewrite entry for the type.
					descriptorRewrites[typ.ID] = &jobspb.DescriptorRewrite{ParentID: parentID}

					// Ensure that there isn't a collision with the array type name.
					arrTyp := typesByID[typ.ArrayTypeID]
					typeName := tree.NewUnqualifiedTypeName(arrTyp.GetName())
					err = descs.CheckObjectNameCollision(ctx, col, txn.KV(), parentID, getParentSchemaID(typ), typeName)
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
						ID:         existingType.TypeDesc().ArrayTypeID,
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

		// TODO(chengxiong): we need to handle the cases of restoring tables when we
		// start supporting udf references from other objects. Namely, we need to do
		// collision checks similar to tables and types. However, there would be a
		// bit shift since there is not namespace entry for functions. That means we
		// need some function resolution for it.
		for _, function := range functionsByID {
			// User-defined functions are not allowed in tables, so restoring specific
			// tables shouldn't match any udf descriptors.
			if _, ok := descriptorRewrites[function.ID]; ok {
				return errors.AssertionFailedf("function descriptors seen when restoring tables")
			}

			targetDB, err := resolveTargetDB(ctx, databasesByID, intoDB, descriptorCoverage, function)
			if err != nil {
				return err
			}

			if _, ok := restoreDBNames[targetDB]; ok {
				needsNewParentIDs[targetDB] = append(needsNewParentIDs[targetDB], function.ID)
			} else {
				return errors.AssertionFailedf("function descriptor seen when restoring tables")
			}
		}
		return nil
	}(); err != nil {
		return nil, err
	}

	if shouldBufferDeprecatedPrivilegeNotice {
		dbNames := make([]string, 0, len(databasesWithDeprecatedPrivileges))
		for dbName := range databasesWithDeprecatedPrivileges {
			dbNames = append(dbNames, dbName)
		}
		p.BufferClientNotice(ctx, pgnotice.Newf("%s RESTORE TABLE, user %s will exclusively require the RESTORE privilege on databases %s",
			deprecatedPrivilegesRestorePreamble, p.User(), strings.Join(dbNames, ", ")))
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
		newID, err = p.ExecCfg().DescIDGenerator.GenerateUniqueDescID(ctx)
		if err != nil {
			return nil, err
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

	// descriptorsToRemap usually contains all tables that are being restored,
	// plus additional other descriptors.
	descriptorsToRemap := make([]catalog.Descriptor, 0, len(tablesByID))
	for _, table := range tablesByID {
		descriptorsToRemap = append(descriptorsToRemap, table)
	}

	// Update the remapping information for type descriptors.
	for _, typ := range typesByID {
		// If the type is marked to be remapped to an existing type in the
		// cluster, then we don't want to generate an ID for it.
		if !descriptorRewrites[typ.ID].ToExisting {
			descriptorsToRemap = append(descriptorsToRemap, typ)
		}
	}

	// Update remapping information for schema descriptors.
	for _, sc := range schemasByID {
		// If this schema isn't being remapped to an existing schema, then
		// request to generate an ID for it.
		if !descriptorRewrites[sc.ID].ToExisting {
			descriptorsToRemap = append(descriptorsToRemap, sc)
		}
	}

	// Remap function descriptor IDs.
	for _, fn := range functionsByID {
		descriptorsToRemap = append(descriptorsToRemap, fn)
	}

	sort.Sort(catalog.Descriptors(descriptorsToRemap))

	// Generate new IDs for the schemas, tables, and types that need to be
	// remapped.
	for _, desc := range descriptorsToRemap {
		id, err := p.ExecCfg().DescIDGenerator.GenerateUniqueDescID(ctx)
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
	for _, fn := range functionsByID {
		rewriteObject(fn)
	}

	return descriptorRewrites, nil
}

func getDatabaseIDAndDesc(
	ctx context.Context, txn *kv.Txn, col *descs.Collection, targetDB string,
) (dbID descpb.ID, dbDesc catalog.DatabaseDescriptor, err error) {
	dbID, err = col.LookupDatabaseID(ctx, txn, targetDB)
	if err != nil {
		return 0, nil, err
	}
	if dbID == descpb.InvalidID {
		return dbID, nil, errors.Errorf("a database named %q needs to exist", targetDB)
	}
	// Check privileges on the parent DB.
	dbDesc, err = col.ByID(txn).Get().Database(ctx, dbID)
	if err != nil {
		return 0, nil, errors.Wrapf(err,
			"failed to lookup parent DB %d", errors.Safe(dbID))
	}
	return dbID, dbDesc, nil
}

// If we're doing a full cluster restore - to treat defaultdb and postgres
// as regular databases, we drop them before restoring them again in the
// restore.
func dropDefaultUserDBs(ctx context.Context, txn isql.Txn) error {
	if _, err := txn.ExecEx(
		ctx, "drop-defaultdb", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		"DROP DATABASE IF EXISTS defaultdb",
	); err != nil {
		return err
	}
	_, err := txn.ExecEx(
		ctx, "drop-postgres", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		"DROP DATABASE IF EXISTS postgres",
	)
	return err
}

func resolveTargetDB(
	ctx context.Context,
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
	// A data structure for efficient descriptor lookup by ID or by name.
	descCatalog := &nstree.MutableCatalog{}
	for _, d := range descs {
		descCatalog.UpsertDescriptor(d)
	}

	for j, desc := range descs {
		var b catalog.DescriptorBuilder
		if tableDesc, isTable := desc.(catalog.TableDescriptor); isTable {
			b = tabledesc.NewBuilderForFKUpgrade(tableDesc.TableDesc(), skipFKsWithNoMatchingTable)
		} else {
			b = desc.NewBuilder()
		}
		if err := b.RunPostDeserializationChanges(); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "error during RunPostDeserializationChanges")
		}
		err := b.RunRestoreChanges(descCatalog.LookupDescriptor)
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
	backupManifests []backuppb.BackupManifest, skipFKsWithNoMatchingTable bool,
) error {
	if len(backupManifests) == 0 {
		return nil
	}

	descriptors := make([]catalog.Descriptor, 0, len(backupManifests[0].Descriptors))
	for i := range backupManifests {
		descs, err := backupinfo.BackupManifestDescriptors(&backupManifests[i])
		if err != nil {
			return err
		}
		descriptors = append(descriptors, descs...)
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
	ctx context.Context,
	opts tree.RestoreOptions,
	intoDB string,
	newDBName string,
	kmsURIs []string,
	incFrom []string,
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
		SchemaOnly:                opts.SchemaOnly,
		VerifyData:                opts.VerifyData,
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
		logSanitizedKmsURI(ctx, redactedURI)
	}

	if opts.IncrementalStorage != nil {
		var err error
		newOpts.IncrementalStorage, err = sanitizeURIList(incFrom)
		for _, uri := range newOpts.IncrementalStorage {
			logSanitizedRestoreDestination(ctx, uri.String())
		}
		if err != nil {
			return tree.RestoreOptions{}, err
		}
	}

	return newOpts, nil
}

func restoreJobDescription(
	ctx context.Context,
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
		DescriptorCoverage: restore.DescriptorCoverage,
		AsOf:               restore.AsOf,
		Targets:            restore.Targets,
		From:               make([]tree.StringOrPlaceholderOptList, len(restore.From)),
	}

	var options tree.RestoreOptions
	var err error
	if options, err = resolveOptionsForRestoreJobDescription(ctx, opts, intoDB, newDBName,
		kmsURIs, incFrom); err != nil {
		return "", err
	}
	r.Options = options

	for i, backup := range from {
		r.From[i] = make(tree.StringOrPlaceholderOptList, len(backup))
		r.From[i], err = sanitizeURIList(backup)
		for _, uri := range r.From[i] {
			logSanitizedRestoreDestination(ctx, uri.String())
		}
		if err != nil {
			return "", err
		}
	}

	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(r, ann), nil
}

func restoreTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	restoreStmt, ok := stmt.(*tree.Restore)
	if !ok {
		return false, nil, nil
	}
	if err := exprutil.TypeCheck(
		ctx, "RESTORE", p.SemaCtx(),
		append(
			exprutil.MakeStringArraysFromOptList(restoreStmt.From),
			tree.Exprs(restoreStmt.Options.DecryptionKMSURI),
			tree.Exprs(restoreStmt.Options.IncrementalStorage),
		),
		exprutil.Strings{
			restoreStmt.Subdir,
			restoreStmt.Options.EncryptionPassphrase,
			restoreStmt.Options.IntoDB,
			restoreStmt.Options.NewDBName,
			restoreStmt.Options.ForceTenantID,
			restoreStmt.Options.AsTenant,
			restoreStmt.Options.DebugPauseOn,
		},
	); err != nil {
		return false, nil, err
	}
	if restoreStmt.Options.Detached {
		header = jobs.DetachedJobExecutionResultHeader
	} else {
		header = jobs.BulkJobExecutionResultHeader
	}
	return true, header, nil
}

func logSanitizedKmsURI(ctx context.Context, kmsDestination string) {
	log.Ops.Infof(ctx, "restore planning to connect to KMS destination %v", redact.Safe(kmsDestination))
}

func logSanitizedRestoreDestination(ctx context.Context, restoreDestinations string) {
	log.Ops.Infof(ctx, "restore planning to connect to destination %v", redact.Safe(restoreDestinations))
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

	if restoreStmt.Options.SchemaOnly &&
		!p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.V22_2Start) {
		return nil, nil, nil, false,
			errors.New("cannot run RESTORE with schema_only until cluster has fully upgraded to 22.2")
	}
	if !restoreStmt.Options.SchemaOnly && restoreStmt.Options.VerifyData {
		return nil, nil, nil, false,
			errors.New("to set the verify_backup_table_data option, the schema_only option must be set")
	}

	exprEval := p.ExprEvaluator("RESTORE")

	from := make([][]string, len(restoreStmt.From))
	for i, expr := range restoreStmt.From {
		v, err := exprEval.StringArray(ctx, tree.Exprs(expr))
		if err != nil {
			return nil, nil, nil, false, err
		}
		from[i] = v
	}

	var pw string
	if restoreStmt.Options.EncryptionPassphrase != nil {
		var err error
		pw, err = exprEval.String(ctx, restoreStmt.Options.EncryptionPassphrase)
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var kms []string
	if restoreStmt.Options.DecryptionKMSURI != nil {
		if restoreStmt.Options.EncryptionPassphrase != nil {
			return nil, nil, nil, false, errors.New("cannot have both encryption_passphrase and kms option set")
		}
		var err error
		kms, err = exprEval.StringArray(
			ctx, tree.Exprs(restoreStmt.Options.DecryptionKMSURI),
		)
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var intoDB string
	if restoreStmt.Options.IntoDB != nil {
		if restoreStmt.DescriptorCoverage == tree.SystemUsers {
			return nil, nil, nil, false, errors.New("cannot set into_db option when only restoring system users")
		}
		var err error
		intoDB, err = exprEval.String(ctx, restoreStmt.Options.IntoDB)
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var subdir string
	if restoreStmt.Subdir != nil {
		var err error
		subdir, err = exprEval.String(ctx, restoreStmt.Subdir)
		if err != nil {
			return nil, nil, nil, false, err
		}
	} else {
		// Deprecation notice for non-colelction `RESTORE FROM` syntax. Remove this
		// once the syntax is deleted in 22.2.
		p.BufferClientNotice(ctx,
			pgnotice.Newf("The `RESTORE FROM <backup>` syntax will be removed in a future release, please"+
				" switch over to using `RESTORE FROM <backup> IN <collection>` to restore a particular backup from a collection: %s",
				"https://www.cockroachlabs.com/docs/stable/restore.html#view-the-backup-subdirectories"))
	}

	var incStorage []string
	if restoreStmt.Options.IncrementalStorage != nil {
		if restoreStmt.Subdir == nil {
			err := errors.New("incremental_location can only be used with the following" +
				" syntax: 'RESTORE [target] FROM [subdirectory] IN [destination]'")
			return nil, nil, nil, false, err
		}
		var err error
		incStorage, err = exprEval.StringArray(
			ctx, tree.Exprs(restoreStmt.Options.IncrementalStorage),
		)
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var newDBName string
	if restoreStmt.Options.NewDBName != nil {
		if restoreStmt.DescriptorCoverage == tree.AllDescriptors ||
			len(restoreStmt.Targets.Databases) != 1 {
			err := errors.New("new_db_name can only be used for RESTORE DATABASE with a single target" +
				" database")
			return nil, nil, nil, false, err
		}
		if restoreStmt.DescriptorCoverage == tree.SystemUsers {
			return nil, nil, nil, false, errors.New("cannot set new_db_name option when only restoring system users")
		}
		var err error
		newDBName, err = exprEval.String(ctx, restoreStmt.Options.NewDBName)
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var newTenantID *roachpb.TenantID
	var newTenantName *roachpb.TenantName
	if restoreStmt.Options.AsTenant != nil || restoreStmt.Options.ForceTenantID != nil {
		if restoreStmt.DescriptorCoverage == tree.AllDescriptors || !restoreStmt.Targets.TenantID.IsSet() {
			err := errors.Errorf("options %q/%q can only be used when running RESTORE TENANT for a single tenant",
				restoreOptAsTenant, restoreOptForceTenantID)
			return nil, nil, nil, false, err
		}

		if restoreStmt.Options.ForceTenantID != nil {
			var err error
			newTenantID, err = func() (*roachpb.TenantID, error) {
				s, err := exprEval.String(ctx, restoreStmt.Options.ForceTenantID)
				if err != nil {
					return nil, err
				}
				id, err := strconv.ParseUint(s, 10, 64)
				if err != nil {
					return nil, err
				}
				tid, err := roachpb.MakeTenantID(id)
				if err != nil {
					return nil, err
				}
				return &tid, err
			}()
			if err != nil {
				return nil, nil, nil, false, err
			}
		}
		if restoreStmt.Options.AsTenant != nil {
			var err error
			newTenantName, err = func() (*roachpb.TenantName, error) {
				s, err := exprEval.String(ctx, restoreStmt.Options.AsTenant)
				if err != nil {
					return nil, err
				}
				tn := roachpb.TenantName(s)
				if err := tn.IsValid(); err != nil {
					return nil, err
				}
				return &tn, nil
			}()
			if err != nil {
				return nil, nil, nil, false, err
			}
		}
		if newTenantID == nil && newTenantName != nil {
			id, err := p.GetAvailableTenantID(ctx, *newTenantName)
			if err != nil {
				return nil, nil, nil, false, err
			}
			newTenantID = &id
		}
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if !(p.ExtendedEvalContext().TxnIsSingleStmt || restoreStmt.Options.Detached) {
			return errors.Errorf("RESTORE cannot be used inside a multi-statement transaction without DETACHED option")
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

		// incFrom will contain the directory URIs for incremental backups (i.e.
		// <prefix>/<subdir>) iff len(From)==1, regardless of the
		// 'incremental_location' param. len(From)=1 implies that the user has not
		// explicitly passed incremental backups, so we'll have to look for any in
		// <prefix>/<subdir>. len(incFrom)>1 implies the incremental backups are
		// locality aware.

		return doRestorePlan(
			ctx, restoreStmt, &exprEval, p, from, incStorage, pw, kms, intoDB,
			newDBName, newTenantID, newTenantName, endTime, resultsCh, subdir,
		)
	}

	if restoreStmt.Options.Detached {
		return fn, jobs.DetachedJobExecutionResultHeader, nil, false, nil
	}
	return fn, jobs.BulkJobExecutionResultHeader, nil, false, nil
}

// checkRestoreDestinationPrivileges iterates over the External Storage URIs and
// ensures the user has adequate privileges to use each of them.
func checkRestoreDestinationPrivileges(
	ctx context.Context, p sql.PlanHookState, from [][]string,
) error {
	// Check destination specific privileges.
	for _, uris := range from {
		uris := uris
		if err := cloudprivilege.CheckDestinationPrivileges(ctx, p, uris); err != nil {
			return err
		}
	}

	return nil
}

// checkRestorePrivilegesOnDatabase check that the user has adequate privileges
// on the parent database to restore schema objects into the database. This is
// used to check the privileges required for a `RESTORE TABLE`.
func checkRestorePrivilegesOnDatabase(
	ctx context.Context, p sql.PlanHookState, parentDB catalog.DatabaseDescriptor,
) (shouldBufferNotice bool, err error) {
	if ok, err := p.HasPrivilege(ctx, parentDB, privilege.RESTORE, p.User()); err != nil {
		return false, err
	} else if ok {
		return false, nil
	}

	if err := p.CheckPrivilege(ctx, parentDB, privilege.CREATE); err != nil {
		notice := fmt.Sprintf("%s RESTORE TABLE, user %s will exclusively require the "+
			"RESTORE privilege on database %s.", deprecatedPrivilegesRestorePreamble, p.User().Normalized(), parentDB.GetName())
		p.BufferClientNotice(ctx, pgnotice.Newf("%s", notice))
		return false, errors.WithHint(err, notice)
	}

	return true, nil
}

// checkPrivilegesForRestore checks that the user has sufficient privileges to
// run a cluster or a database restore. A table restore requires us to know the
// parent database we will be writing to and so that happens at a later stage of
// restore planning.
//
// This method is also responsible for checking the privileges on the
// destination URIs the restore is reading from.
func checkPrivilegesForRestore(
	ctx context.Context, restoreStmt *tree.Restore, p sql.PlanHookState, from [][]string,
) error {
	// If the user is admin no further checks need to be performed.
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if hasAdmin {
		return nil
	}

	{
		// Cluster and tenant restores require the `RESTORE` system privilege for
		// non-admin users.
		requiresRestoreSystemPrivilege := restoreStmt.DescriptorCoverage == tree.AllDescriptors ||
			restoreStmt.Targets.TenantID.IsSet()

		if requiresRestoreSystemPrivilege {
			if err := p.CheckPrivilegeForUser(
				ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.RESTORE, p.User(),
			); err != nil {
				return pgerror.Wrapf(
					err,
					pgcode.InsufficientPrivilege,
					"only users with the admin role or the RESTORE system privilege are allowed to perform"+
						" a cluster restore")
			}
			return checkRestoreDestinationPrivileges(ctx, p, from)
		}
	}

	// If running a database restore, check that the user has the `RESTORE` system
	// privilege.
	//
	// TODO(adityamaru): In 23.1 a missing `RESTORE` privilege should return an
	// error. In 22.2 we continue to check for old style privileges and role
	// options.
	if len(restoreStmt.Targets.Databases) > 0 {
		var hasRestoreSystemPrivilege bool
		if ok, err := p.HasPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.RESTORE, p.User()); err != nil {
			return err
		} else {
			hasRestoreSystemPrivilege = ok
		}
		if hasRestoreSystemPrivilege {
			return checkRestoreDestinationPrivileges(ctx, p, from)
		}
	}

	// The following checks are to maintain compatability with pre-22.2 privilege
	// requirements to run the backup. If we have failed to find the appropriate
	// `RESTORE` privileges, we default to our old-style privilege checks and
	// buffer a notice urging users to switch to `RESTORE` privileges.
	//
	// TODO(adityamaru): Delete deprecated privilege checks in 23.1. Users will be
	// required to have the appropriate `RESTORE` privilege instead.
	//
	// Database restores require the CREATEDB privileges.
	if len(restoreStmt.Targets.Databases) > 0 {
		notice := fmt.Sprintf("%s RESTORE DATABASE, user %s will exclusively require the "+
			"RESTORE system privilege.", deprecatedPrivilegesRestorePreamble, p.User().Normalized())
		p.BufferClientNotice(ctx, pgnotice.Newf("%s", notice))

		hasCreateDB, err := p.HasRoleOption(ctx, roleoption.CREATEDB)
		if err != nil {
			return err
		}
		if !hasCreateDB {
			return errors.WithHint(pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"only users with the CREATEDB privilege can restore databases"), notice)
		}
	}

	return checkRestoreDestinationPrivileges(ctx, p, from)
}

func checkClusterRegions(
	ctx context.Context, p sql.PlanHookState, typesByID map[descpb.ID]*typedesc.Mutable,
) error {
	regionSet := make(map[catpb.RegionName]struct{})
	for _, typ := range typesByID {
		regionTypeDesc := typedesc.NewBuilder(typ.TypeDesc()).BuildImmutableType().AsRegionEnumTypeDescriptor()
		if regionTypeDesc == nil {
			continue
		}
		_ = regionTypeDesc.ForEachPublicRegion(func(name catpb.RegionName) error {
			if _, ok := regionSet[name]; !ok {
				regionSet[name] = struct{}{}
			}
			return nil
		})
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
	exprEval *exprutil.Evaluator,
	p sql.PlanHookState,
	from [][]string,
	incFrom []string,
	passphrase string,
	kms []string,
	intoDB string,
	newDBName string,
	newTenantID *roachpb.TenantID,
	newTenantName *roachpb.TenantName,
	endTime hlc.Timestamp,
	resultsCh chan<- tree.Datums,
	subdir string,
) error {
	if len(from) == 0 || len(from[0]) == 0 {
		return errors.New("invalid base backup specified")
	}

	if subdir != "" && len(from) != 1 {
		return errors.Errorf("RESTORE FROM ... IN can only by used against a single collection path (per-locality)")
	}

	if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
		// We do this before resolving the backup manifest since resolving the
		// backup manifest can take a while.
		if err := checkForConflictingDescriptors(ctx, p.InternalSQLTxn()); err != nil {
			return err
		}
	}

	var fullyResolvedSubdir string

	if strings.EqualFold(subdir, backupbase.LatestFileName) {
		// set subdir to content of latest file
		latest, err := backupdest.ReadLatestFile(ctx, from[0][0],
			p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, p.User())
		if err != nil {
			return err
		}
		fullyResolvedSubdir = latest
	} else {
		fullyResolvedSubdir = subdir
	}

	fullyResolvedBaseDirectory, err := backuputils.AppendPaths(from[0][:], fullyResolvedSubdir)
	if err != nil {
		return err
	}

	fullyResolvedIncrementalsDirectory, err := backupdest.ResolveIncrementalsBackupLocation(
		ctx,
		p.User(),
		p.ExecCfg(),
		incFrom,
		from[0],
		fullyResolvedSubdir,
	)
	if err != nil {
		if errors.Is(err, cloud.ErrListingUnsupported) {
			log.Warningf(ctx, "storage sink %v does not support listing, only resolving the base backup", incFrom)
		} else {
			return err
		}
	}

	// fullyResolvedIncrementalsDirectory may in fact be nil, if incrementals
	// aren't supported at this location. In that case, we logged a warning,
	// further iterations over fullyResolvedIncrementalsDirectory will be
	// vacuous, and we should proceed with restoring the base backup.
	//
	// Note that incremental _backup_ requests to this location will fail loudly instead.
	mkStore := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI
	baseStores, cleanupFn, err := backupdest.MakeBackupDestinationStores(ctx, p.User(), mkStore,
		fullyResolvedBaseDirectory)
	if err != nil {
		return err
	}
	defer func() {
		if err := cleanupFn(); err != nil {
			log.Warningf(ctx, "failed to close incremental store: %+v", err)
		}
	}()

	incStores, cleanupFn, err := backupdest.MakeBackupDestinationStores(ctx, p.User(), mkStore,
		fullyResolvedIncrementalsDirectory)
	if err != nil {
		return err
	}
	defer func() {
		if err := cleanupFn(); err != nil {
			log.Warningf(ctx, "failed to close incremental store: %+v", err)
		}
	}()

	ioConf := baseStores[0].ExternalIOConf()
	kmsEnv := backupencryption.MakeBackupKMSEnv(
		p.ExecCfg().Settings, &ioConf, p.ExecCfg().InternalDB, p.User(),
	)

	var encryption *jobspb.BackupEncryptionOptions
	if restoreStmt.Options.EncryptionPassphrase != nil {
		opts, err := backupencryption.ReadEncryptionOptions(ctx, baseStores[0])
		if err != nil {
			return err
		}
		encryptionKey := storageccl.GenerateKey([]byte(passphrase), opts[0].Salt)
		encryption = &jobspb.BackupEncryptionOptions{
			Mode: jobspb.EncryptionMode_Passphrase,
			Key:  encryptionKey,
		}
	} else if restoreStmt.Options.DecryptionKMSURI != nil {
		opts, err := backupencryption.ReadEncryptionOptions(ctx, baseStores[0])
		if err != nil {
			return err
		}

		// A backup could have been encrypted with multiple KMS keys that
		// are stored across ENCRYPTION-INFO files. Iterate over all
		// ENCRYPTION-INFO files to check if the KMS passed in during
		// restore has been used to encrypt the backup at least once.
		var defaultKMSInfo *jobspb.BackupEncryptionOptions_KMSInfo
		for _, encFile := range opts {
			defaultKMSInfo, err = backupencryption.ValidateKMSURIsAgainstFullBackup(ctx, kms,
				backupencryption.NewEncryptedDataKeyMapFromProtoMap(encFile.EncryptedDataKeyByKMSMasterKeyID),
				&kmsEnv)
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

	// Given the stores for the base full backup, and the fully resolved backup
	// directories, return the URIs and manifests of all backup layers in all
	// localities.
	var defaultURIs []string
	var mainBackupManifests []backuppb.BackupManifest
	var localityInfo []jobspb.RestoreDetails_BackupLocalityInfo
	var memReserved int64
	if len(from) <= 1 {
		// Incremental layers are not specified explicitly. They will be searched for automatically.
		// This could be either INTO-syntax, OR TO-syntax.
		defaultURIs, mainBackupManifests, localityInfo, memReserved, err = backupdest.ResolveBackupManifests(
			ctx, &mem, baseStores, incStores, mkStore, fullyResolvedBaseDirectory,
			fullyResolvedIncrementalsDirectory, endTime, encryption, &kmsEnv, p.User(),
		)
	} else {
		// Incremental layers are specified explicitly.
		// This implies the old, deprecated TO-syntax.
		defaultURIs, mainBackupManifests, localityInfo, memReserved, err =
			backupdest.DeprecatedResolveBackupManifestsExplicitIncrementals(ctx, &mem, mkStore, from,
				endTime, encryption, &kmsEnv, p.User())
	}

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

	if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
		// Validate that the backup is a full cluster backup if a full cluster restore was requested.
		if mainBackupManifests[0].DescriptorCoverage == tree.RequestedDescriptors {
			return errors.Errorf("full cluster RESTORE can only be used on full cluster BACKUP files")
		}

		// Validate that we aren't in the middle of an upgrade. To avoid unforseen
		// issues, we want to avoid full cluster restores if it is possible that an
		// upgrade is in progress. We also check this during Resume.
		binaryVersion := p.ExecCfg().Settings.Version.BinaryVersion()
		clusterVersion := p.ExecCfg().Settings.Version.ActiveVersion(ctx).Version
		if clusterVersion.Less(binaryVersion) {
			return clusterRestoreDuringUpgradeErr(clusterVersion, binaryVersion)
		}
	}

	backupCodec, err := backupinfo.MakeBackupCodec(mainBackupManifests[0])
	if err != nil {
		return err
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
			table, _, _, _, _ := descpb.GetDescriptors(&m.Descriptors[i])
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
					if index.Adding() && spans.ContainsKey(backupCodec.IndexPrefix(uint32(table.ID), uint32(index.GetID()))) {
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

	sqlDescs, restoreDBs, descsByTablePattern, tenants, err := selectTargets(
		ctx, p, mainBackupManifests, restoreStmt.Targets, restoreStmt.DescriptorCoverage, endTime,
	)
	if err != nil {
		return errors.Wrap(err,
			"failed to resolve targets in the BACKUP location specified by the RESTORE statement, "+
				"use SHOW BACKUP to find correct targets")
	}

	if err := checkMissingIntroducedSpans(sqlDescs, mainBackupManifests, endTime, backupCodec); err != nil {
		return err
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

	err = ensureMultiRegionDatabaseRestoreIsAllowed(p, restoreDBs)
	if err != nil {
		return err
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
			res, err := p.InternalSQLTxn().QueryRowEx(
				ctx, "restore-lookup-tenant", p.Txn(),
				sessiondata.NodeUserSessionDataOverride,
				`SELECT active FROM system.tenants WHERE id = $1`, newTenantID.ToUint64(),
			)
			if err != nil {
				return err
			}
			if res != nil {
				return errors.Errorf("tenant %s already exists", newTenantID)
			}
			old := roachpb.MustMakeTenantID(tenants[0].ID)
			tenants[0].ID = newTenantID.ToUint64()
			if newTenantName != nil {
				tenants[0].Name = *newTenantName
			}
			oldTenantID = &old
		} else {
			for _, i := range tenants {
				res, err := p.InternalSQLTxn().QueryRowEx(
					ctx, "restore-lookup-tenant", p.Txn(),
					sessiondata.NodeUserSessionDataOverride,
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
	functionsByID := make(map[descpb.ID]*funcdesc.Mutable)

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
		case *funcdesc.Mutable:
			functionsByID[desc.ID] = desc
		}
	}

	if !restoreStmt.Options.SkipLocalitiesCheck {
		if err := checkClusterRegions(ctx, p, typesByID); err != nil {
			return err
		}
	}

	var debugPauseOn string
	if restoreStmt.Options.DebugPauseOn != nil {
		var err error
		debugPauseOn, err = exprEval.String(ctx, restoreStmt.Options.DebugPauseOn)
		if err != nil {
			return err
		}

		if _, ok := allowedDebugPauseOnValues[debugPauseOn]; len(debugPauseOn) > 0 && !ok {
			return errors.Newf("%s cannot be set with the value %s", restoreOptDebugPauseOn, debugPauseOn)
		}
	}

	var asOfInterval int64
	if !endTime.IsEmpty() {
		asOfInterval = endTime.WallTime - p.ExtendedEvalContext().StmtTimestamp.UnixNano()
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
		if err := dropDefaultUserDBs(ctx, p.InternalSQLTxn()); err != nil {
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
		functionsByID,
		restoreDBs,
		restoreStmt.DescriptorCoverage,
		restoreStmt.Options,
		intoDB,
		newDBName)
	if err != nil {
		return err
	}
	var fromDescription [][]string
	if len(from) == 1 {
		fromDescription = [][]string{fullyResolvedBaseDirectory}
	} else {
		fromDescription = from
	}
	description, err := restoreJobDescription(
		ctx,
		p,
		restoreStmt,
		fromDescription,
		fullyResolvedIncrementalsDirectory,
		restoreStmt.Options,
		intoDB,
		newDBName,
		kms)
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
	var functions []*funcdesc.Mutable
	for _, desc := range functionsByID {
		functions = append(functions, desc)
	}

	// We attempt to rewrite ID's in the collected type and table descriptors
	// to catch errors during this process here, rather than in the job itself.
	overrideDBName := intoDB
	if newDBName != "" {
		overrideDBName = newDBName
	}
	if err := rewrite.TableDescs(tables, descriptorRewrites, overrideDBName); err != nil {
		return err
	}
	if err := rewrite.DatabaseDescs(databases, descriptorRewrites, map[descpb.ID]struct{}{}); err != nil {
		return err
	}
	if err := rewrite.SchemaDescs(schemas, descriptorRewrites); err != nil {
		return err
	}
	if err := rewrite.TypeDescs(types, descriptorRewrites); err != nil {
		return err
	}
	if err := rewrite.FunctionDescs(functions, descriptorRewrites, overrideDBName); err != nil {
		return err
	}
	for i := range revalidateIndexes {
		revalidateIndexes[i].TableID = descriptorRewrites[revalidateIndexes[i].TableID].ID
	}

	encodedTables := make([]*descpb.TableDescriptor, len(tables))
	for i, table := range tables {
		encodedTables[i] = table.TableDesc()
	}

	restoreDetails := jobspb.RestoreDetails{
		EndTime:            endTime,
		DescriptorRewrites: descriptorRewrites,
		URIs:               defaultURIs,
		BackupLocalityInfo: localityInfo,
		TableDescs:         encodedTables,
		Tenants:            tenants,
		OverrideDB:         overrideDBName,
		DescriptorCoverage: restoreStmt.DescriptorCoverage,
		Encryption:         encryption,
		RevalidateIndexes:  revalidateIndexes,
		DatabaseModifiers:  databaseModifiers,
		DebugPauseOn:       debugPauseOn,

		// A RESTORE SYSTEM USERS planned on a 22.1 node will use the
		// RestoreSystemUsers field in the job details to identify this flavour of
		// RESTORE. We must continue to check this field for mixed-version
		// compatability.
		//
		// TODO(msbutler): Delete in 23.1
		RestoreSystemUsers: restoreStmt.DescriptorCoverage == tree.SystemUsers,
		PreRewriteTenantId: oldTenantID,
		SchemaOnly:         restoreStmt.Options.SchemaOnly,
		VerifyData:         restoreStmt.Options.VerifyData,
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
		Details:  restoreDetails,
		Progress: jobspb.RestoreProgress{},
	}

	if restoreStmt.Options.Detached {
		// When running in detached mode, we simply create the job record.
		// We do not wait for the job to finish.
		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		_, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
			ctx, jr, jobID, p.InternalSQLTxn(),
		)
		if err != nil {
			return err
		}
		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jobID))}
		collectRestoreTelemetry(ctx, jobID, restoreDetails, intoDB, newDBName, subdir, restoreStmt,
			descsByTablePattern, restoreDBs, asOfInterval, debugPauseOn, p.SessionData().ApplicationName)
		return nil
	}

	// We create the job record in the planner's transaction to ensure that
	// the job record creation happens transactionally.
	plannerTxn := p.Txn()

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
		if err := p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, &sj, jobID, p.InternalSQLTxn(), jr); err != nil {
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
	// Release all descriptor leases here. We need to do this because we're
	// about to kick off a job which is going to potentially rewrite every
	// descriptor. Note that we committed the underlying transaction in the
	// above closure -- so we're not using any leases anymore, but we might
	// be holding some because some sql queries might have been executed by
	// this transaction (indeed some certainly were when we created the job
	// we're going to run).
	//
	// This is all a bit of a hack to deal with the fact that we want to
	// return results as part of this statement and the usual machinery for
	// releasing leases assumes that that does not happen during statement
	// execution.
	p.InternalSQLTxn().Descriptors().ReleaseAll(ctx)
	collectRestoreTelemetry(ctx, sj.ID(), restoreDetails, intoDB, newDBName, subdir, restoreStmt,
		descsByTablePattern, restoreDBs, asOfInterval, debugPauseOn, p.SessionData().ApplicationName)
	if err := sj.Start(ctx); err != nil {
		return err
	}
	if err := sj.AwaitCompletion(ctx); err != nil {
		return err
	}
	return sj.ReportExecutionResults(ctx, resultsCh)
}

func collectRestoreTelemetry(
	ctx context.Context,
	jobID jobspb.JobID,
	details jobspb.RestoreDetails,
	intoDB string,
	newDBName string,
	subdir string,
	restoreStmt *tree.Restore,
	descsByTablePattern map[tree.TablePattern]catalog.Descriptor,
	restoreDBs []catalog.DatabaseDescriptor,
	asOfInterval int64,
	debugPauseOn string,
	applicationName string,
) {
	telemetry.Count("restore.total.started")
	if restoreStmt.DescriptorCoverage == tree.AllDescriptors {
		telemetry.Count("restore.full-cluster")
	}
	if restoreStmt.Subdir == nil {
		telemetry.Count("restore.deprecated-subdir-syntax")
	} else {
		telemetry.Count("restore.collection")
	}

	logRestoreTelemetry(ctx, jobID, details, intoDB, newDBName, subdir, asOfInterval, restoreStmt.Options,
		descsByTablePattern, restoreDBs, debugPauseOn, applicationName)
}

// checkForConflictingDescriptors checks for user-created descriptors that would
// create a conflict when doing a full cluster restore.
//
// Because we remap all descriptors, we only care about namespace conflicts.
func checkForConflictingDescriptors(ctx context.Context, txn descs.Txn) error {
	var allDescs []catalog.Descriptor
	all, err := txn.Descriptors().GetAllDescriptors(ctx, txn.KV())
	if err != nil {
		return errors.Wrap(err, "looking up user descriptors during restore")
	}
	allDescs = all.OrderedDescriptors()
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
	return nil
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
		if desc.Dropped() {
			// Exclude dropped descriptors since they should no longer have namespace
			// entries.
			continue
		}
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
		return errors.AssertionFailedf(
			"expected restoreDBs to have 1 entry but found %d entries when renaming the target database",
			len(restoreDBs))
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
		return errors.AssertionFailedf("expected *dbdesc.mutable but found %T", db)
	}
	db.SetName(newDBName)
	return nil
}

// ensureMultiRegionDatabaseRestoreIsAllowed returns an error if restoring a
// multi-region database is not allowed.
func ensureMultiRegionDatabaseRestoreIsAllowed(
	p sql.PlanHookState, restoreDBs []catalog.DatabaseDescriptor,
) error {
	if p.ExecCfg().Codec.ForSystemTenant() ||
		sql.SecondaryTenantsMultiRegionAbstractionsEnabled.Get(&p.ExecCfg().Settings.SV) {
		// The system tenant is always allowed to restore multi-region databases;
		// secondary tenants are only allowed to restore multi-region databases
		// if the cluster setting above allows such.
		return nil
	}
	for _, dbDesc := range restoreDBs {
		// If a database descriptor being restored is a multi-region database,
		// return an error.
		if dbDesc.IsMultiRegion() {
			return pgerror.Newf(
				pgcode.InvalidDatabaseDefinition,
				"setting %s disallows secondary tenant to restore a multi-region database",
				sql.SecondaryTenantsMultiRegionAbstractionsEnabledSettingName,
			)
		}
	}
	// We're good.
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
	if !p.ExecCfg().Codec.ForSystemTenant() &&
		!sql.SecondaryTenantsMultiRegionAbstractionsEnabled.Get(&p.ExecCfg().Settings.SV) {
		// We don't want to restore "regular" databases as multi-region databases
		// for secondary tenants.
		return nil, nil, nil
	}
	if defaultPrimaryRegion == "" {
		return nil, nil, nil
	}
	if err := multiregionccl.CheckClusterSupportsMultiRegion(
		p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(),
	); err != nil {
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
			nil,
			descpb.ZoneConfigExtensions{},
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
	regionArrayEnum, err := sql.CreateUserDefinedArrayTypeDesc(
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
	sql.AddPlanHook("restore", restorePlanHook, restoreTypeCheck)
}
