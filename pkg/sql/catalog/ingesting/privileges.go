// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ingesting

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// GetIngestingDescriptorPrivileges returns the privileges to set on a
// descriptor being ingested such during a RESTORE or IMPORT.
//
// wroteDBs is a map of databases also written during the same ingestion so that
// elements in those databases can inherit privileges as expected.
//
// descCoverage indicates if only some requested decriptors are being ingested
// or if all descriptors are being ingested such as during a cluster restore; in
// the latter case assumptions can be made such as that any users mentioned in
// privileges on the input descriptor still exists.
// TODO(at) these functions are a good spot to do some refactoring for rwg
func GetIngestingDescriptorPrivileges(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	desc catalog.Descriptor,
	user username.SQLUsername,
	wroteDBs map[descpb.ID]catalog.DatabaseDescriptor,
	wroteSchemas map[descpb.ID]catalog.SchemaDescriptor,
	descCoverage tree.DescriptorCoverage,
	includePublicSchemaCreatePriv bool,
	// TODO(at): replace this with an actually good api
	keep bool,
) (updatedPrivileges *catpb.PrivilegeDescriptor, err error) {
	switch desc := desc.(type) {
	case catalog.TableDescriptor:
		return getIngestingPrivilegesForTableOrSchema(
			ctx,
			txn,
			descsCol,
			desc,
			user,
			wroteDBs,
			wroteSchemas,
			descCoverage,
			privilege.Table,
			includePublicSchemaCreatePriv,
			keep,
		)
	case catalog.SchemaDescriptor:
		return getIngestingPrivilegesForTableOrSchema(
			ctx,
			txn,
			descsCol,
			desc,
			user,
			wroteDBs,
			wroteSchemas,
			descCoverage,
			privilege.Schema,
			includePublicSchemaCreatePriv,
			keep,
		)
	case catalog.TypeDescriptor:
		// If the ingestion is not a cluster restore we cannot know that the users
		// on the ingesting cluster match the ones that were on the cluster that was
		// backed up. So we wipe the privileges on the type.
		if descCoverage == tree.RequestedDescriptors {
			updatedPrivileges = catpb.NewBasePrivilegeDescriptor(user)
		}
	case catalog.DatabaseDescriptor:
		// NOTES(at):
		// so this piece of code is blowing up our descriptors for restore with grants
		// we either want to keep them in under certain conditions (if we're doing rwg),
		// or we want to get the information we need to do rwg from somewhere else.
		// alternatively, we could not end up here from above by checking if we are running rwg,
		// but im not 100% sure thats not gonna wreck something else.
		// ^ fun fact, it did
		//
		// so it looks like we're gonna have to modify this function, we want to make sure that
		// whatever constraints this is trying to uphold dont get messed up
		// also as a side note, it looks like this function is only called from sites where the type
		// of desc is known, and yet we switch on it here. could break these out into new functions,
		// which is maybe a good spot to refine the apis for each type
		// (we need some special cases for dbs and tables, but not types, functions, etc)
		//
		// If the ingestion is not a cluster restore we cannot know that the users
		// on the ingesting cluster match the ones that were on the cluster that was
		// backed up. So we wipe the privileges on the database.
		if descCoverage == tree.RequestedDescriptors && !keep {
			updatedPrivileges = catpb.NewBaseDatabasePrivilegeDescriptor(user)
		}
	case catalog.FunctionDescriptor:
		// If the ingestion is not a cluster restore we cannot know that the
		// users on the ingesting cluster match the ones that were on the
		// cluster that was backed up. So we wipe the privileges on the
		// function.
		if descCoverage == tree.RequestedDescriptors {
			updatedPrivileges = catpb.NewBaseFunctionPrivilegeDescriptor(user)
		}
	}
	return updatedPrivileges, nil
}

func getIngestingPrivilegesForTableOrSchema(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	desc catalog.Descriptor,
	user username.SQLUsername,
	wroteDBs map[descpb.ID]catalog.DatabaseDescriptor,
	wroteSchemas map[descpb.ID]catalog.SchemaDescriptor,
	descCoverage tree.DescriptorCoverage,
	privilegeType privilege.ObjectType,
	includePublicSchemaCreatePriv bool,
	// TODO(at): make this better
	keep bool,
) (updatedPrivileges *catpb.PrivilegeDescriptor, err error) {
	if _, ok := wroteDBs[desc.GetParentID()]; ok {
		// If we're creating a new database in this ingestion, the tables and
		// schemas in the database should be assigned the default privileges that
		// are granted on object creation.
		switch privilegeType {
		case privilege.Schema:
			if desc.GetName() == catconstants.PublicSchemaName {
				updatedPrivileges = catpb.NewPublicSchemaPrivilegeDescriptor(user, includePublicSchemaCreatePriv)
			} else {
				updatedPrivileges = catpb.NewBasePrivilegeDescriptor(user)
			}
		case privilege.Table:
			if !keep ||
				desc.GetName() == systemschema.UsersTable.GetName() ||
				desc.GetName() == systemschema.RoleMembersTable.GetName() ||
				desc.GetName() == systemschema.RoleOptionsTable.GetName() {
				updatedPrivileges = catpb.NewBasePrivilegeDescriptor(user)
			}
		default:
			return nil, errors.Newf("unexpected privilege type %T", privilegeType)
		}
	} else if descCoverage == tree.RequestedDescriptors {
		// If we are not creating the database as part of this ingestion, the
		// schemas and tables in the database should be given privileges based on
		// the parent database's default privileges.
		parentDB, err := descsCol.ByIDWithoutLeased(txn).Get().Database(ctx, desc.GetParentID())
		if err != nil {
			return nil, errors.Wrapf(err, "failed to lookup parent DB %d", errors.Safe(desc.GetParentID()))
		}
		dbDefaultPrivileges := parentDB.GetDefaultPrivilegeDescriptor()

		var schemaDefaultPrivileges catalog.DefaultPrivilegeDescriptor
		targetObject := privilege.Schemas
		switch privilegeType {
		case privilege.Table:
			targetObject = privilege.Tables
			schemaID := desc.GetParentSchemaID()

			// TODO(adityamaru): Remove in 22.2 once we are sure not to see synthentic public schema descriptors
			// in a mixed version state.
			if schemaID == keys.PublicSchemaID {
				schemaDefaultPrivileges = nil
			} else if schema, ok := wroteSchemas[schemaID]; ok {
				// Check if the schema is part of the objects being restored. If it is,
				// the schema's privileges have already been processed before we would
				// process any of the table's being restored. So, it is correct to use the
				// schema's default privileges.
				schemaDefaultPrivileges = schema.GetDefaultPrivilegeDescriptor()
			} else {
				// If we are restoring into an existing schema, resolve it, and fetch
				// its default privileges.
				parentSchema, err := descsCol.ByIDWithoutLeased(txn).Get().Schema(ctx, desc.GetParentSchemaID())
				if err != nil {
					return nil,
						errors.Wrapf(err, "failed to lookup parent schema %d", errors.Safe(desc.GetParentSchemaID()))
				}
				schemaDefaultPrivileges = parentSchema.GetDefaultPrivilegeDescriptor()
			}
		case privilege.Schema:
			schemaDefaultPrivileges = nil
		default:
			return nil, errors.Newf("unexpected privilege type %T", privilegeType)
		}

		if !keep ||
			desc.GetName() == systemschema.UsersTable.GetName() ||
			desc.GetName() == systemschema.RoleMembersTable.GetName() ||
			desc.GetName() == systemschema.RoleOptionsTable.GetName() {
			updatedPrivileges, err = catprivilege.CreatePrivilegesFromDefaultPrivileges(
				dbDefaultPrivileges, schemaDefaultPrivileges, parentDB.GetID(), user, targetObject,
			)
		}
		if err != nil {
			return nil, err
		}
	}
	return updatedPrivileges, nil
}
