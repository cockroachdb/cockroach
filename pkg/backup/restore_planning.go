// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backupdest"
	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"golang.org/x/exp/maps"
)

const (
	restoreOptIntoDB                    = "into_db"
	restoreOptSkipMissingFKs            = "skip_missing_foreign_keys"
	restoreOptSkipMissingSequences      = "skip_missing_sequences"
	restoreOptSkipMissingUDFs           = "skip_missing_udfs"
	restoreOptSkipMissingSequenceOwners = "skip_missing_sequence_owners"
	restoreOptSkipMissingViews          = "skip_missing_views"
	restoreOptSkipLocalitiesCheck       = "skip_localities_check"
	restoreOptAsTenant                  = "virtual_cluster_name"
	restoreOptForceTenantID             = "virtual_cluster"

	// The temporary database system tables will be restored into for full
	// cluster backups.
	restoreTempSystemDB = "crdb_temp_system"
)

// featureRestoreEnabled is used to enable and disable the RESTORE feature.
var featureRestoreEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"feature.restore.enabled",
	"set to true to enable restore, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
	settings.WithPublic)

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

func validateTableDependenciesForOptions(
	tablesByID map[descpb.ID]*tabledesc.Mutable,
	typesByID map[descpb.ID]*typedesc.Mutable,
	functionsByID map[descpb.ID]*funcdesc.Mutable,
	opts *tree.RestoreOptions,
) error {
	for _, table := range tablesByID {
		// Check that foreign key targets exist.
		for i := range table.OutboundFKs {
			fk := &table.OutboundFKs[i]
			if _, ok := tablesByID[fk.ReferencedTableID]; !ok {
				if !opts.SkipMissingFKs {
					return errors.Errorf(
						"cannot restore table %q without referenced table %d (or %q option)",
						table.Name, fk.ReferencedTableID, restoreOptSkipMissingFKs,
					)
				}
			}
		}

		// Check that functions referenced in check constraints, column expressions,
		// and triggers exist.
		fnIDs, err := table.GetAllReferencedFunctionIDs()
		if err != nil {
			return err
		}
		for _, fnID := range fnIDs.Ordered() {
			if _, ok := functionsByID[fnID]; !ok {
				if !opts.SkipMissingUDFs {
					return errors.Errorf(
						"cannot restore table %q without referenced function %d (or %q option)",
						table.Name, fnID, restoreOptSkipMissingUDFs,
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
					return errors.Errorf(
						"cannot restore table %q without referenced type %d",
						table.Name,
						id,
					)
				}
			}
			for _, seqID := range col.UsesSequenceIds {
				if _, ok := tablesByID[seqID]; !ok {
					if !opts.SkipMissingSequences {
						return errors.Errorf(
							"cannot restore table %q without referenced sequence %d (or %q option)",
							table.Name, seqID, restoreOptSkipMissingSequences,
						)
					}
				}
			}
			for _, seqID := range col.OwnsSequenceIds {
				if _, ok := tablesByID[seqID]; !ok {
					if !opts.SkipMissingSequenceOwners {
						return errors.Errorf(
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
					return errors.Errorf(
						"cannot restore sequence %q without referenced owner table %d (or %q option)",
						table.Name,
						table.SequenceOpts.SequenceOwner.OwnerTableID,
						restoreOptSkipMissingSequenceOwners,
					)
				}
			}
		}
	}
	return nil
}

// remapSystemDBDescsToTempDB remaps all the descriptors belonging to system
// tables to the temp system DB.
func remapSystemDBDescsToTempDB(
	schemasByID map[descpb.ID]*schemadesc.Mutable,
	tablesByID map[descpb.ID]*tabledesc.Mutable,
	typesByID map[descpb.ID]*typedesc.Mutable,
	tempSysDBID catid.DescID,
	descriptorRewrites jobspb.DescRewriteMap,
) {
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

// Construct rewrites for any user defined schemas.
func remapSchemas(
	ctx context.Context,
	p sql.PlanHookState,
	databasesByID map[descpb.ID]*dbdesc.Mutable,
	schemasByID map[descpb.ID]*schemadesc.Mutable,
	descriptorCoverage tree.DescriptorCoverage,
	intoDB string,
	restoreDBNames map[string]catalog.DatabaseDescriptor,
	// Outputs
	databasesWithDeprecatedPrivileges map[string]struct{},
	descriptorRewrites jobspb.DescRewriteMap,
) (bool, error) {

	txn := p.InternalSQLTxn()
	col := txn.Descriptors()
	shouldBufferDeprecatedPrivilegeNotice := false

	for _, sc := range schemasByID {
		if _, ok := descriptorRewrites[sc.ID]; ok {
			continue
		}

		targetDB, err := resolveTargetDB(databasesByID, intoDB, descriptorCoverage, sc)
		if err != nil {
			return false, err
		}

		if _, ok := restoreDBNames[targetDB]; ok {
			descriptorRewrites[sc.ID] = &jobspb.DescriptorRewrite{}
			continue
		}
		// Look up the parent database's ID.
		parentID, parentDB, err := getDatabaseIDAndDesc(ctx, txn.KV(), col, targetDB)
		if err != nil {
			return false, err
		}
		if usesDeprecatedPrivileges, err := checkRestorePrivilegesOnDatabase(ctx, p, parentDB); err != nil {
			return false, err
		} else if usesDeprecatedPrivileges {
			shouldBufferDeprecatedPrivilegeNotice = true
			databasesWithDeprecatedPrivileges[parentDB.GetName()] = struct{}{}
		}

		// See if there is an existing schema with the same name.
		id, err := col.LookupSchemaID(ctx, txn.KV(), parentID, sc.Name)
		if err != nil {
			return false, err
		}
		if id == descpb.InvalidID {
			// If we didn't find a matching schema, then we'll restore this schema.
			descriptorRewrites[sc.ID] = &jobspb.DescriptorRewrite{ParentID: parentID}
		} else {
			// If we found an existing schema, then we need to remap all references
			// to this schema to the existing one.
			desc, err := col.ByIDWithoutLeased(txn.KV()).Get().Schema(ctx, id)
			if err != nil {
				return false, err
			}
			descriptorRewrites[sc.ID] = &jobspb.DescriptorRewrite{
				ParentID:   desc.GetParentID(),
				ID:         desc.GetID(),
				ToExisting: true,
			}
		}
	}

	return shouldBufferDeprecatedPrivilegeNotice, nil
}

func remapTables(
	ctx context.Context,
	p sql.PlanHookState,
	databasesByID map[descpb.ID]*dbdesc.Mutable,
	tablesByID map[descpb.ID]*tabledesc.Mutable,
	descriptorCoverage tree.DescriptorCoverage,
	intoDB string,
	restoreDBNames map[string]catalog.DatabaseDescriptor,
	// Outputs
	databasesWithDeprecatedPrivileges map[string]struct{},
	descriptorRewrites jobspb.DescRewriteMap,
) (bool, error) {

	txn := p.InternalSQLTxn()
	col := txn.Descriptors()
	shouldBufferDeprecatedPrivilegeNotice := false

	for _, table := range tablesByID {
		// If a descriptor has already been assigned a rewrite, then move on.
		if _, ok := descriptorRewrites[table.ID]; ok {
			continue
		}

		targetDB, err := resolveTargetDB(databasesByID, intoDB, descriptorCoverage, table)
		if err != nil {
			return false, err
		}

		if _, ok := restoreDBNames[targetDB]; ok {
			descriptorRewrites[table.ID] = &jobspb.DescriptorRewrite{}
			continue
		}
		var parentID descpb.ID
		{
			newParentID, err := col.LookupDatabaseID(ctx, txn.KV(), targetDB)
			if err != nil {
				return false, err
			}
			if newParentID == descpb.InvalidID {
				return false, errors.Errorf("a database named %q needs to exist to restore table %q",
					targetDB, table.Name)
			}
			parentID = newParentID
		}

		// If we are restoring the table into an existing schema in the target
		// database, we must ensure that the table name is _not_ in use.
		// This would fail the CPut later anyway, but this yields a prettier error.
		//
		// If we are restoring the table into a schema that is also being
		// restored then we do not need to check for collisions as the backing
		// up cluster would enforce uniqueness of table names in a schema.
		rw, ok := descriptorRewrites[table.GetParentSchemaID()]
		// We may not find an entry for the parent schema in our
		// descriptorRewrites if it is a table being restored into the public
		// schema of the system database. This public schema is a pseudo-schema
		// i.e. it is not backed by a descriptor, hence we check for that case
		// separately below.
		restoringIntoExistingSchema := ok && rw.ToExisting
		isSystemTable := table.GetParentID() == keys.SystemDatabaseID &&
			table.GetParentSchemaID() == keys.SystemPublicSchemaID
		if restoringIntoExistingSchema || isSystemTable {
			schemaID := table.GetParentSchemaID()
			if ok {
				schemaID = rw.ID
			}
			tableName := tree.NewUnqualifiedTableName(tree.Name(table.GetName()))
			err := descs.CheckObjectNameCollision(ctx, col, txn.KV(), parentID, schemaID, tableName)
			if err != nil {
				return false, err
			}
		}

		// Check privileges.
		parentDB, err := col.ByIDWithoutLeased(txn.KV()).Get().Database(ctx, parentID)
		if err != nil {
			return false, errors.Wrapf(err,
				"failed to lookup parent DB %d", errors.Safe(parentID))
		}
		if usesDeprecatedPrivileges, err := checkRestorePrivilegesOnDatabase(ctx, p, parentDB); err != nil {
			return false, err
		} else if usesDeprecatedPrivileges {
			shouldBufferDeprecatedPrivilegeNotice = true
			databasesWithDeprecatedPrivileges[parentDB.GetName()] = struct{}{}
		}

		// We're restoring a table and not its parent database. We may block
		// restoring multi-region tables to multi-region databases since
		// regions may mismatch.
		if err := checkMultiRegionCompatible(ctx, txn.KV(), col, table, parentDB); err != nil {
			return false, pgerror.WithCandidateCode(err, pgcode.FeatureNotSupported)
		}

		// Create the table rewrite with the new parent ID. We've done all the
		// up-front validation that we can.
		descriptorRewrites[table.ID] = &jobspb.DescriptorRewrite{ParentID: parentID}

		// If we're restoring to a public schema of database that already exists
		// we can populate the rewrite ParentSchemaID field here since we
		// already have the database descriptor.
		if table.GetParentSchemaID() == keys.PublicSchemaIDForBackup ||
			table.GetParentSchemaID() == descpb.InvalidID {
			publicSchemaID := parentDB.GetSchemaID(catconstants.PublicSchemaName)
			descriptorRewrites[table.ID].ParentSchemaID = publicSchemaID
		}
	}
	return shouldBufferDeprecatedPrivilegeNotice, nil
}

func remapTypes(
	ctx context.Context,
	p sql.PlanHookState,
	databasesByID map[descpb.ID]*dbdesc.Mutable,
	typesByID map[descpb.ID]*typedesc.Mutable,
	descriptorCoverage tree.DescriptorCoverage,
	intoDB string,
	restoreDBNames map[string]catalog.DatabaseDescriptor,
	// Outputs
	databasesWithDeprecatedPrivileges map[string]struct{},
	descriptorRewrites jobspb.DescRewriteMap,
) (bool, error) {

	txn := p.InternalSQLTxn()
	col := txn.Descriptors()
	shouldBufferDeprecatedPrivilegeNotice := false

	// Iterate through typesByID to construct a remapping entry for each type.
	for _, typ := range typesByID {
		// If a descriptor has already been assigned a rewrite, then move on.
		if _, ok := descriptorRewrites[typ.ID]; ok {
			continue
		}

		targetDB, err := resolveTargetDB(databasesByID, intoDB, descriptorCoverage, typ)
		if err != nil {
			return false, err
		}

		if _, ok := restoreDBNames[targetDB]; ok {
			descriptorRewrites[typ.ID] = &jobspb.DescriptorRewrite{}
			continue
		}
		// The remapping logic for a type will perform the remapping for a type's
		// array type, so don't perform this logic for the array type itself.
		if typ.AsAliasTypeDescriptor() != nil {
			continue
		}

		// Look up the parent database's ID.
		parentID, err := col.LookupDatabaseID(ctx, txn.KV(), targetDB)
		if err != nil {
			return false, err
		}
		if parentID == descpb.InvalidID {
			return false, errors.Errorf("a database named %q needs to exist to restore type %q",
				targetDB, typ.Name)
		}
		// Check privileges on the parent DB.
		parentDB, err := col.ByIDWithoutLeased(txn.KV()).Get().Database(ctx, parentID)
		if err != nil {
			return false, errors.Wrapf(err,
				"failed to lookup parent DB %d", errors.Safe(parentID))
		}

		// If we are restoring the type into an existing schema in the target
		// database, we can find the type with the same name and don't need to
		// create it as part of the restore.
		//
		// If we are restoring the type into a schema that is also being
		// restored then we need to create the type and the array type as part
		// of the restore.
		var desc catalog.Descriptor
		if rewrite, ok := descriptorRewrites[typ.GetParentSchemaID()]; ok && rewrite.ToExisting {
			var err error
			desc, err = descs.GetDescriptorCollidingWithObjectName(
				ctx,
				col,
				txn.KV(),
				parentID,
				rewrite.ID,
				typ.Name,
			)
			if err != nil {
				return false, err
			}

			if desc == nil {
				// If we did not find a type descriptor, ensure that the
				// corresponding array type descriptor does not exist. This is
				// because we will create both the type and array type below.
				arrTyp := typesByID[typ.ArrayTypeID]
				typeName := tree.NewUnqualifiedTypeName(arrTyp.GetName())
				err = descs.CheckObjectNameCollision(ctx, col, txn.KV(), parentID, rewrite.ID, typeName)
				if err != nil {
					return false, errors.Wrapf(err, "name collision for %q's array type", typ.Name)
				}
			}
		}

		if desc == nil {
			// If we didn't find a type with the same name, then mark that we
			// need to create the type.

			// Ensure that the user has the correct privilege to create types.
			if usesDeprecatedPrivileges, err := checkRestorePrivilegesOnDatabase(ctx, p, parentDB); err != nil {
				return false, err
			} else if usesDeprecatedPrivileges {
				shouldBufferDeprecatedPrivilegeNotice = true
				databasesWithDeprecatedPrivileges[parentDB.GetName()] = struct{}{}
			}

			// Create a rewrite entry for the type.
			descriptorRewrites[typ.ID] = &jobspb.DescriptorRewrite{ParentID: parentID}

			// Create the rewrite entry for the array type as well.
			arrTyp := typesByID[typ.ArrayTypeID]
			descriptorRewrites[arrTyp.ID] = &jobspb.DescriptorRewrite{ParentID: parentID}
		} else {
			// If there was a name collision, we'll try to see if we can remap
			// this type to the type existing in the cluster.

			// If the collided object isn't a type, then error out.
			existingType, isType := desc.(catalog.TypeDescriptor)
			if !isType {
				return false, sqlerrors.MakeObjectAlreadyExistsError(desc.DescriptorProto(), typ.Name)
			}

			// Check if the collided type is compatible to be remapped to.
			if err := typ.IsCompatibleWith(existingType); err != nil {
				return false, errors.Wrapf(
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
			publicSchemaID := parentDB.GetSchemaID(catconstants.PublicSchemaName)
			descriptorRewrites[typ.ID].ParentSchemaID = publicSchemaID
			descriptorRewrites[typ.ArrayTypeID].ParentSchemaID = publicSchemaID
		}
	}
	return shouldBufferDeprecatedPrivilegeNotice, nil
}

func remapFunctions(
	databasesByID map[descpb.ID]*dbdesc.Mutable,
	functionsByID map[descpb.ID]*funcdesc.Mutable,
	descriptorCoverage tree.DescriptorCoverage,
	intoDB string,
	restoreDBNames map[string]catalog.DatabaseDescriptor,
	// Outputs
	descriptorRewrites jobspb.DescRewriteMap,
) error {
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

		targetDB, err := resolveTargetDB(databasesByID, intoDB, descriptorCoverage, function)
		if err != nil {
			return err
		}

		if _, ok := restoreDBNames[targetDB]; !ok {
			return errors.AssertionFailedf("function descriptor seen when restoring tables")
		}

		descriptorRewrites[function.ID] = &jobspb.DescriptorRewrite{}
	}
	return nil
}

func remapDatabases(
	restoreDBs []catalog.DatabaseDescriptor,
	newDBName string,
	// Outputs
	descriptorRewrites jobspb.DescRewriteMap,
) {
	for _, db := range restoreDBs {
		rewrite := &jobspb.DescriptorRewrite{}
		if newDBName != "" {
			rewrite.NewDBName = newDBName
		}
		descriptorRewrites[db.GetID()] = rewrite
	}
}

func getDescriptorByID(
	id descpb.ID,
	databasesByID map[descpb.ID]*dbdesc.Mutable,
	schemasByID map[descpb.ID]*schemadesc.Mutable,
	tablesByID map[descpb.ID]*tabledesc.Mutable,
	typesByID map[descpb.ID]*typedesc.Mutable,
	functionsByID map[descpb.ID]*funcdesc.Mutable,
) catalog.Descriptor {
	if desc, ok := databasesByID[id]; ok {
		return desc
	}
	if desc, ok := schemasByID[id]; ok {
		return desc
	}
	if desc, ok := tablesByID[id]; ok {
		return desc
	}
	if desc, ok := typesByID[id]; ok {
		return desc
	}
	if desc, ok := functionsByID[id]; ok {
		return desc
	}
	return nil
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
func allocateIDs(
	ctx context.Context,
	p sql.PlanHookState,
	databasesByID map[descpb.ID]*dbdesc.Mutable,
	schemasByID map[descpb.ID]*schemadesc.Mutable,
	tablesByID map[descpb.ID]*tabledesc.Mutable,
	typesByID map[descpb.ID]*typedesc.Mutable,
	functionsByID map[descpb.ID]*funcdesc.Mutable,
	// Outputs
	descriptorRewrites jobspb.DescRewriteMap,
) error {
	oldIDs := maps.Keys(descriptorRewrites)
	sort.Sort(descpb.IDs(oldIDs))

	// First, assign new IDs to objects.
	// Do this in order to maintain sorting of keys on disk.
	for _, oldID := range oldIDs {
		rewrite := descriptorRewrites[oldID]
		if rewrite.ToExisting {
			continue
		}
		newID, err := p.ExecCfg().DescIDGenerator.GenerateUniqueDescID(ctx)
		if err != nil {
			return err
		}
		rewrite.ID = newID
	}

	// Second, iterate through all rewrite objects and update parent IDs
	// to new IDs assigned above.
	//
	// In some cases, these IDs will already be populated in the rewrite objects
	// by the mapping functions. (E.g. the remapping of system tables, or when
	// restoring objects into an existing DB or schema.) In those cases, respect
	// the choice of ID.
	//
	// When not already populated, determine the proper parent object from the
	// existing descriptor, then point the rewrite object to the already-updated
	// parent object ID.
	for _, oldID := range oldIDs {
		desc := getDescriptorByID(
			oldID,
			databasesByID,
			schemasByID,
			tablesByID,
			typesByID,
			functionsByID)
		if desc == nil {
			// Objects in the rewrite map are not guaranteed to exist in the
			// supplied descriptors. E.g. the array-type of a supplied type might
			// be implied rather than explicit. In those cases, assume the rewrite
			// object was populated correctly by the relevant remap function.
			continue
		}
		rewrite := descriptorRewrites[oldID]
		if rewrite.ParentID == descpb.InvalidID {
			if parentRewrite, ok := descriptorRewrites[desc.GetParentID()]; ok {
				rewrite.ParentID = parentRewrite.ID
			}
		}
		if rewrite.ParentSchemaID == descpb.InvalidID {
			if parentSchemaRewrite, ok := descriptorRewrites[desc.GetParentSchemaID()]; ok {
				rewrite.ParentSchemaID = parentSchemaRewrite.ID
			}
		}
	}
	return nil
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
	if err := validateTableDependenciesForOptions(tablesByID, typesByID, functionsByID, &opts); err != nil {
		return nil, err
	}

	txn := p.InternalSQLTxn()
	col := txn.Descriptors()
	// Check that any DBs being restored do _not_ exist.
	// Fail fast if the necessary databases don't exist or are otherwise
	// incompatible with this restore.
	if newDBName != "" {
		dbID, err := col.LookupDatabaseID(ctx, txn.KV(), newDBName)
		if err != nil {
			return nil, err
		}
		if dbID != descpb.InvalidID {
			return nil, errors.Errorf("database %q already exists", newDBName)
		}
	} else {
		for name := range restoreDBNames {
			dbID, err := col.LookupDatabaseID(ctx, txn.KV(), name)
			if err != nil {
				return nil, err
			}
			if dbID != descpb.InvalidID {
				return nil, errors.Errorf("database %q already exists", name)
			}
		}
	}

	if descriptorCoverage == tree.AllDescriptors || descriptorCoverage == tree.SystemUsers {
		// Increment the DescIDSequenceKey so that it is higher than both the max desc ID
		// in the backup and current max desc ID in the restoring cluster. This generator
		// keeps produced the next descriptor ID.
		tempSysDBID, err := p.ExecCfg().DescIDGenerator.GenerateUniqueDescID(ctx)
		if err != nil {
			return nil, err
		}
		remapSystemDBDescsToTempDB(schemasByID, tablesByID, typesByID, tempSysDBID, descriptorRewrites)
	}

	var shouldBufferDeprecatedPrivilegeNotice bool
	databasesWithDeprecatedPrivileges := make(map[string]struct{})

	if b, err := remapSchemas(
		ctx, p, databasesByID, schemasByID, descriptorCoverage, intoDB, restoreDBNames,
		databasesWithDeprecatedPrivileges, descriptorRewrites,
	); err != nil {
		return nil, err
	} else {
		shouldBufferDeprecatedPrivilegeNotice = b || shouldBufferDeprecatedPrivilegeNotice
	}

	if b, err := remapTables(
		ctx, p, databasesByID, tablesByID, descriptorCoverage, intoDB, restoreDBNames,
		databasesWithDeprecatedPrivileges, descriptorRewrites,
	); err != nil {
		return nil, err
	} else {
		shouldBufferDeprecatedPrivilegeNotice = b || shouldBufferDeprecatedPrivilegeNotice
	}

	if b, err := remapTypes(
		ctx, p, databasesByID, typesByID, descriptorCoverage, intoDB, restoreDBNames,
		databasesWithDeprecatedPrivileges, descriptorRewrites,
	); err != nil {
		return nil, err
	} else {
		shouldBufferDeprecatedPrivilegeNotice = b || shouldBufferDeprecatedPrivilegeNotice
	}

	if err := remapFunctions(
		databasesByID, functionsByID, descriptorCoverage, intoDB, restoreDBNames, descriptorRewrites,
	); err != nil {
		return nil, err
	}

	remapDatabases(restoreDBs, newDBName, descriptorRewrites)

	if shouldBufferDeprecatedPrivilegeNotice {
		dbNames := make([]string, 0, len(databasesWithDeprecatedPrivileges))
		for dbName := range databasesWithDeprecatedPrivileges {
			dbNames = append(dbNames, dbName)
		}
		p.BufferClientNotice(ctx, pgnotice.Newf("%s RESTORE TABLE, user %s will exclusively require the RESTORE privilege on databases %s",
			deprecatedPrivilegesRestorePreamble, p.User(), strings.Join(dbNames, ", ")))
	}

	if err := allocateIDs(
		ctx,
		p,
		databasesByID,
		schemasByID,
		tablesByID,
		typesByID,
		functionsByID,
		descriptorRewrites); err != nil {
		return nil, err
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
	dbDesc, err = col.ByIDWithoutLeased(txn).Get().Database(ctx, dbID)
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
func maybeUpgradeDescriptors(
	version clusterversion.ClusterVersion,
	descs []catalog.Descriptor,
	skipFKsWithNoMatchingTable bool,
) error {
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
		err := b.RunRestoreChanges(version, descCatalog.LookupDescriptor)
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
	ctx context.Context,
	version clusterversion.ClusterVersion,
	backupManifests []backuppb.BackupManifest,
	layerToIterFactory backupinfo.LayerToBackupManifestFileIterFactory,
	skipFKsWithNoMatchingTable bool,
) error {
	if len(backupManifests) == 0 {
		return nil
	}

	// TODO(rui): We do not need to upgrade descriptors that exist in the external
	// SST to the 19.2 style because they would've been generated by at least
	// version 22.2. Delete this function once backups must use manifests with
	// external SSTs.
	newDescriptorStyleVersion := roachpb.Version{
		Major: 19,
		Minor: 2,
	}
	if !backupManifests[0].ClusterVersion.Less(newDescriptorStyleVersion) {
		return nil
	}

	descriptors := make([]catalog.Descriptor, 0, len(backupManifests[0].Descriptors))
	for i := range backupManifests {
		descs, err := backupinfo.BackupManifestDescriptors(ctx, layerToIterFactory[i], backupManifests[i].EndTime)
		if err != nil {
			return err
		}
		descriptors = append(descriptors, descs...)
	}

	err := maybeUpgradeDescriptors(version, descriptors, skipFKsWithNoMatchingTable)
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
		SkipMissingFKs:                   opts.SkipMissingFKs,
		SkipMissingSequences:             opts.SkipMissingSequences,
		SkipMissingSequenceOwners:        opts.SkipMissingSequenceOwners,
		SkipMissingViews:                 opts.SkipMissingViews,
		SkipMissingUDFs:                  opts.SkipMissingUDFs,
		Detached:                         opts.Detached,
		SkipLocalitiesCheck:              opts.SkipLocalitiesCheck,
		AsTenant:                         opts.AsTenant,
		ForceTenantID:                    opts.ForceTenantID,
		SchemaOnly:                       opts.SchemaOnly,
		VerifyData:                       opts.VerifyData,
		UnsafeRestoreIncompatibleVersion: opts.UnsafeRestoreIncompatibleVersion,
		ExecutionLocality:                opts.ExecutionLocality,
		ExperimentalOnline:               opts.ExperimentalOnline,
		RemoveRegions:                    opts.RemoveRegions,
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
	from []string,
	incFrom []string,
	opts tree.RestoreOptions,
	intoDB string,
	newDBName string,
	kmsURIs []string,
	resolvedSubdir string,
) (string, error) {
	r := &tree.Restore{
		DescriptorCoverage: restore.DescriptorCoverage,
		AsOf:               restore.AsOf,
		Targets:            restore.Targets,
		Subdir:             tree.NewDString("/" + strings.TrimPrefix(resolvedSubdir, "/")),
	}

	var options tree.RestoreOptions
	var err error
	if options, err = resolveOptionsForRestoreJobDescription(ctx, opts, intoDB, newDBName,
		kmsURIs, incFrom); err != nil {
		return "", err
	}
	r.Options = options

	r.From, err = sanitizeURIList(from)
	if err != nil {
		return "", err
	}
	for _, uri := range r.From {
		logSanitizedRestoreDestination(ctx, uri.String())
	}

	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFlags(
		r, tree.FmtAlwaysQualifyNames|tree.FmtShowFullURIs, tree.FmtAnnotations(ann),
	), nil
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
		exprutil.StringArrays{
			tree.Exprs(restoreStmt.From),
			tree.Exprs(restoreStmt.Options.DecryptionKMSURI),
			tree.Exprs(restoreStmt.Options.IncrementalStorage),
		},
		exprutil.Strings{
			restoreStmt.Subdir,
			restoreStmt.Options.EncryptionPassphrase,
			restoreStmt.Options.IntoDB,
			restoreStmt.Options.NewDBName,
			restoreStmt.Options.ForceTenantID,
			restoreStmt.Options.AsTenant,
			restoreStmt.Options.ExecutionLocality,
		},
	); err != nil {
		return false, nil, err
	}
	if restoreStmt.Options.Detached {
		header = jobs.DetachedJobExecutionResultHeader
	} else if restoreStmt.Options.ExperimentalOnline {
		header = jobs.OnlineRestoreJobExecutionResultHeader
	} else {
		header = jobs.BackupRestoreJobResultHeader
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
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	restoreStmt, ok := stmt.(*tree.Restore)
	if !ok {
		return nil, nil, false, nil
	}

	if err := featureflag.CheckEnabled(
		ctx,
		p.ExecCfg(),
		featureRestoreEnabled,
		"RESTORE",
	); err != nil {
		return nil, nil, false, err
	}

	if !restoreStmt.Options.SchemaOnly && restoreStmt.Options.VerifyData {
		return nil, nil, false,
			errors.New("to set the verify_backup_table_data option, the schema_only option must be set")
	}

	exprEval := p.ExprEvaluator("RESTORE")

	from, err := exprEval.StringArray(ctx, tree.Exprs(restoreStmt.From))
	if err != nil {
		return nil, nil, false, err
	}

	var pw string
	if restoreStmt.Options.EncryptionPassphrase != nil {
		var err error
		pw, err = exprEval.String(ctx, restoreStmt.Options.EncryptionPassphrase)
		if err != nil {
			return nil, nil, false, err
		}
	}

	var kms []string
	if restoreStmt.Options.DecryptionKMSURI != nil {
		if restoreStmt.Options.EncryptionPassphrase != nil {
			return nil, nil, false, errors.New("cannot have both encryption_passphrase and kms option set")
		}
		var err error
		kms, err = exprEval.StringArray(
			ctx, tree.Exprs(restoreStmt.Options.DecryptionKMSURI),
		)
		if err != nil {
			return nil, nil, false, err
		}
	}

	var intoDB string
	if restoreStmt.Options.IntoDB != nil {
		if restoreStmt.DescriptorCoverage == tree.SystemUsers {
			return nil, nil, false, errors.New("cannot set into_db option when only restoring system users")
		}
		var err error
		intoDB, err = exprEval.String(ctx, restoreStmt.Options.IntoDB)
		if err != nil {
			return nil, nil, false, err
		}
	}

	var subdir string
	if restoreStmt.Subdir != nil {
		var err error
		subdir, err = exprEval.String(ctx, restoreStmt.Subdir)
		if err != nil {
			return nil, nil, false, err
		}
	} else {
		// Deprecation notice for non-collection `RESTORE FROM` syntax. Remove this
		// once the syntax is deleted in 22.2.
		p.BufferClientNotice(ctx,
			pgnotice.Newf("The `RESTORE FROM <backup>` syntax will be removed in a future release, please"+
				" switch over to using `RESTORE FROM <backup> IN <collection>` to restore a particular backup from a collection: %s",
				"https://www.cockroachlabs.com/docs/stable/restore.html#view-the-backup-subdirectories"))
	}

	var incStorage []string
	if restoreStmt.Options.IncrementalStorage != nil {
		var err error
		incStorage, err = exprEval.StringArray(
			ctx, tree.Exprs(restoreStmt.Options.IncrementalStorage),
		)
		if err != nil {
			return nil, nil, false, err
		}
	}

	var execLocality roachpb.Locality
	if restoreStmt.Options.ExecutionLocality != nil {
		loc, err := exprEval.String(ctx, restoreStmt.Options.ExecutionLocality)
		if err != nil {
			return nil, nil, false, err
		}
		if loc != "" {
			if err := execLocality.Set(loc); err != nil {
				return nil, nil, false, err
			}
		}
	}

	var newDBName string
	if restoreStmt.Options.NewDBName != nil {
		if restoreStmt.DescriptorCoverage == tree.AllDescriptors ||
			len(restoreStmt.Targets.Databases) != 1 {
			err := errors.New("new_db_name can only be used for RESTORE DATABASE with a single target" +
				" database")
			return nil, nil, false, err
		}
		if restoreStmt.DescriptorCoverage == tree.SystemUsers {
			return nil, nil, false, errors.New("cannot set new_db_name option when only restoring system users")
		}
		var err error
		newDBName, err = exprEval.String(ctx, restoreStmt.Options.NewDBName)
		if err != nil {
			return nil, nil, false, err
		}
	}

	if restoreStmt.Options.ExperimentalOnline && restoreStmt.Options.VerifyData {
		return nil, nil, false, errors.New("cannot run online restore with verify_backup_table_data")
	}

	var newTenantID *roachpb.TenantID
	var newTenantName *roachpb.TenantName
	if restoreStmt.Options.AsTenant != nil || restoreStmt.Options.ForceTenantID != nil {
		if restoreStmt.DescriptorCoverage == tree.AllDescriptors || !restoreStmt.Targets.TenantID.IsSet() {
			err := errors.Errorf("options %q/%q can only be used when running RESTORE TENANT for a single tenant",
				restoreOptAsTenant, restoreOptForceTenantID)
			return nil, nil, false, err
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
				return nil, nil, false, err
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
				return nil, nil, false, err
			}
		}
		if newTenantID == nil && newTenantName != nil {
			id, err := p.GetAvailableTenantID(ctx, *newTenantName)
			if err != nil {
				return nil, nil, false, err
			}
			newTenantID = &id
		}
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
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
			newDBName, newTenantID, newTenantName, endTime, resultsCh, subdir, execLocality,
		)
	}

	var header colinfo.ResultColumns
	if restoreStmt.Options.Detached {
		header = jobs.DetachedJobExecutionResultHeader
	} else if restoreStmt.Options.ExperimentalOnline {
		header = jobs.OnlineRestoreJobExecutionResultHeader
	} else {
		header = jobs.BackupRestoreJobResultHeader
	}
	return fn, header, false, nil
}

// checkRestoreDestinationPrivileges iterates over the External Storage URIs and
// ensures the user has adequate privileges to use each of them.
func checkRestoreDestinationPrivileges(
	ctx context.Context, p sql.PlanHookState, from []string,
) error {
	return sql.CheckDestinationPrivileges(ctx, p, from)
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
	ctx context.Context, restoreStmt *tree.Restore, p sql.PlanHookState, from []string,
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

		hasCreateDB, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.CREATEDB)
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

// checkBackupManifestVersionCompatability performs various checks to ensure
// that the manifests we are about to restore are from backups taken on a
// version compatible with our current version.
func checkBackupManifestVersionCompatability(
	ctx context.Context,
	version clusterversion.Handle,
	mainBackupManifests []backuppb.BackupManifest,
	unsafeRestoreIncompatibleVersion bool,
) error {
	// Skip the version check if the user runs the restore with
	// `UNSAFE_RESTORE_INCOMPATIBLE_VERSION`.
	if unsafeRestoreIncompatibleVersion {
		return nil
	}

	// We support restoring a backup that was taken on a cluster with a cluster
	// version >= the earliest binary version that we can interoperate with.
	minimumRestoreableVersion := version.MinSupportedVersion()
	currentActiveVersion := version.ActiveVersion(ctx)

	for i := range mainBackupManifests {
		v := mainBackupManifests[i].ClusterVersion

		// This is the "cluster" version that does not change between patch releases
		// but rather just tracks migrations run. If the backup is more migrated
		// than this cluster, then this cluster isn't ready to restore this backup.
		if currentActiveVersion.Less(v) {
			return errors.Errorf("backup from version %s is newer than current version %s", v, currentActiveVersion)
		}

		// If the backup is from a version earlier than the minimum restorable
		// version, then we do not support restoring it.
		if v.Less(minimumRestoreableVersion) {
			if v.Major == 0 {
				// This accounts for manifests that were generated on a version before
				// the `ClusterVersion` field exists.
				return errors.WithHint(errors.Newf("the backup is from a version older than our "+
					"minimum restorable version %s", minimumRestoreableVersion),
					"refer to our documentation about restoring across versions: https://www.cockroachlabs.com/docs/stable/restoring-backups-across-versions.html")
			}
			return errors.WithHint(errors.Newf("backup from version %s is older than the "+
				"minimum restorable version %s", v, minimumRestoreableVersion),
				"refer to our documentation about restoring across versions: https://www.cockroachlabs.com/docs/stable/restoring-backups-across-versions.html")
		}
	}

	return nil
}

func doRestorePlan(
	ctx context.Context,
	restoreStmt *tree.Restore,
	exprEval *exprutil.Evaluator,
	p sql.PlanHookState,
	from []string,
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
	execLocality roachpb.Locality,
) error {
	if len(from) == 0 {
		return errors.New("invalid base backup specified")
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
		latest, err := backupdest.ReadLatestFile(ctx, from[0],
			p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, p.User())
		if err != nil {
			return err
		}
		fullyResolvedSubdir = latest
	} else {
		fullyResolvedSubdir = subdir
	}

	fullyResolvedBaseDirectory, err := backuputils.AppendPaths(from[:], fullyResolvedSubdir)
	if err != nil {
		return err
	}

	fullyResolvedIncrementalsDirectory, err := backupdest.ResolveIncrementalsBackupLocation(
		ctx,
		p.User(),
		p.ExecCfg(),
		incFrom,
		from,
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
	// localities. Incrementals will be searched for automatically.
	defaultURIs, mainBackupManifests, localityInfo, memReserved, err := backupdest.ResolveBackupManifests(
		ctx, &mem, baseStores, incStores, mkStore, fullyResolvedBaseDirectory,
		fullyResolvedIncrementalsDirectory, endTime, encryption, &kmsEnv, p.User(), false,
	)

	if err != nil {
		return err
	}
	if restoreStmt.Options.ExperimentalOnline {
		for _, uri := range defaultURIs {
			if err := cloud.SchemeSupportsEarlyBoot(uri); err != nil {
				return errors.Wrap(err, "backup URI not supported for online restore")
			}
		}
	}

	defer func() {
		mem.Shrink(ctx, memReserved)
	}()

	err = checkBackupManifestVersionCompatability(ctx, p.ExecCfg().Settings.Version,
		mainBackupManifests, restoreStmt.Options.UnsafeRestoreIncompatibleVersion)
	if err != nil {
		return err
	}

	if restoreStmt.Options.ExperimentalOnline {
		if err := checkManifestsForOnlineCompat(ctx, mainBackupManifests); err != nil {
			return err
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
		latestVersion := p.ExecCfg().Settings.Version.LatestVersion()
		clusterVersion := p.ExecCfg().Settings.Version.ActiveVersion(ctx).Version
		if clusterVersion.Less(latestVersion) {
			return clusterRestoreDuringUpgradeErr(clusterVersion, latestVersion)
		}
	}

	layerToIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx, p.ExecCfg().DistSQLSrv.ExternalStorage, mainBackupManifests, encryption, &kmsEnv)
	if err != nil {
		return err
	}

	sqlDescs, restoreDBs, descsByTablePattern, tenants, err := selectTargets(
		ctx, p, mainBackupManifests, layerToIterFactory, restoreStmt.Targets, restoreStmt.DescriptorCoverage, endTime,
	)
	if err != nil {
		return errors.Wrap(err,
			"failed to resolve targets in the BACKUP location specified by the RESTORE statement, "+
				"use SHOW BACKUP to find correct targets")
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

	activeVersion := p.ExecCfg().Settings.Version.ActiveVersion(ctx)
	if err := maybeUpgradeDescriptors(activeVersion, sqlDescs, restoreStmt.Options.SkipMissingFKs); err != nil {
		return err
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
			old, err := roachpb.MakeTenantID(tenants[0].ID)
			if err != nil {
				return err
			}
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

	if restoreStmt.Options.RemoveRegions {
		for _, t := range tablesByID {
			if t.LocalityConfig.GetRegionalByRow() != nil {
				return errors.Newf("cannot perform a remove_regions RESTORE with region by row enabled table %s in BACKUP target", t.Name)
			}
		}
	}

	if !restoreStmt.Options.SkipLocalitiesCheck {
		if err := checkClusterRegions(ctx, p, typesByID); err != nil {
			return err
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

	// If we are stripping localities, wipe tables of their LocalityConfig before we allocate
	// descriptor rewrites - as validation in remapTables compares these tables with the non-mr
	// database and fails otherwise
	if restoreStmt.Options.RemoveRegions {
		for _, t := range filteredTablesByID {
			t.TableDesc().LocalityConfig = nil
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

	if restoreStmt.Options.ExperimentalOnline {
		if err := checkBackupElidedPrefixForOnlineCompat(ctx, mainBackupManifests, descriptorRewrites); err != nil {
			return err
		}
	}

	description, err := restoreJobDescription(
		ctx,
		p,
		restoreStmt,
		from,
		incFrom,
		restoreStmt.Options,
		intoDB,
		newDBName,
		kms,
		fullyResolvedSubdir)
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
		return errors.Wrapf(err, "table descriptor rewrite failed")
	}
	if err := rewrite.DatabaseDescs(databases, descriptorRewrites, map[descpb.ID]struct{}{}); err != nil {
		return errors.Wrapf(err, "database descriptor rewrite failed")
	}
	if err := rewrite.SchemaDescs(schemas, descriptorRewrites); err != nil {
		return errors.Wrapf(err, "schema descriptor rewrite failed")
	}
	if err := rewrite.TypeDescs(types, descriptorRewrites); err != nil {
		return errors.Wrapf(err, "type descriptor rewrite failed")
	}
	if err := rewrite.FunctionDescs(functions, descriptorRewrites, overrideDBName); err != nil {
		return errors.Wrapf(err, "function descriptor rewrite failed")
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
		DatabaseModifiers:  databaseModifiers,

		// A RESTORE SYSTEM USERS planned on a 22.1 node will use the
		// RestoreSystemUsers field in the job details to identify this flavour of
		// RESTORE. We must continue to check this field for mixed-version
		// compatability.
		//
		// TODO(msbutler): Delete in 23.1
		RestoreSystemUsers:               restoreStmt.DescriptorCoverage == tree.SystemUsers,
		PreRewriteTenantId:               oldTenantID,
		SchemaOnly:                       restoreStmt.Options.SchemaOnly,
		VerifyData:                       restoreStmt.Options.VerifyData,
		SkipLocalitiesCheck:              restoreStmt.Options.SkipLocalitiesCheck,
		ExecutionLocality:                execLocality,
		ExperimentalOnline:               restoreStmt.Options.ExperimentalOnline,
		RemoveRegions:                    restoreStmt.Options.RemoveRegions,
		UnsafeRestoreIncompatibleVersion: restoreStmt.Options.UnsafeRestoreIncompatibleVersion,
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
			descsByTablePattern, restoreDBs, asOfInterval, p.SessionData().ApplicationName)
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
		descsByTablePattern, restoreDBs, asOfInterval, p.SessionData().ApplicationName)
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
		descsByTablePattern, restoreDBs, applicationName)
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
		sqlclustersettings.DefaultPrimaryRegion.Get(&p.ExecCfg().Settings.SV),
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
		p.ExecCfg().Settings,
	); err != nil {
		return nil, nil, errors.WithHintf(
			err,
			"try disabling the default PRIMARY REGION by using RESET CLUSTER SETTING %s",
			sqlclustersettings.DefaultPrimaryRegionClusterSettingName,
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
			sqlclustersettings.DefaultPrimaryRegionClusterSettingName,
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
		if sc.GetName() == catconstants.PublicSchemaName {
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
				sqlclustersettings.DefaultPrimaryRegionClusterSettingName,
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
		if err := multiregion.ValidateRegionConfig(regionConfig, db.ID == keys.SystemDatabaseID); err != nil {
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
		p.SessionData(),
		regionConfig.RegionEnumID(),
		regionLabels,
		db,
		sc,
		tree.NewQualifiedTypeName(db.GetName(), catconstants.PublicSchemaName, tree.RegionEnum),
		sql.EnumTypeMultiRegion,
	)
	if err != nil {
		return nil, nil, err
	}
	regionEnum.ArrayTypeID = regionEnumArrayID
	regionArrayEnum, err := sql.CreateUserDefinedArrayTypeDesc(
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
