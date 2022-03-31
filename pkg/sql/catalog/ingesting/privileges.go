// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ingesting

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// GetIngestingDescriptorPrivileges returns the privileges to set on a
// descriptor being ingested such during a RESTORE or IMPORT.
//
// wroteDBs is a map of databases also written during the same ingestion so that
// elements in those databases can inherit privileges as expected.
//
// descCoverage indicates if only some requested descriptors are being ingested
// or if all descriptors are being ingested such as during a cluster restore; in
// the latter case assumptions can be made such as that any users mentioned in
// privileges on the input descriptor still exists.
//
// preserveGrantsForUsers is only set by non-cluster RESTOREs and indicates
// which users we should preserve the backed up grants on schema objects for.
func GetIngestingDescriptorPrivileges(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	desc catalog.Descriptor,
	user security.SQLUsername,
	wroteDBs map[descpb.ID]catalog.DatabaseDescriptor,
	wroteSchemas map[descpb.ID]catalog.SchemaDescriptor,
	descCoverage tree.DescriptorCoverage,
	preserveGrantsForUsers map[security.SQLUsername]struct{},
) (updatedPrivileges *catpb.PrivilegeDescriptor, err error) {
	var objectType privilege.ObjectType
	switch desc := desc.(type) {
	case catalog.TableDescriptor:
		objectType = privilege.Table
		updatedPrivileges, err = getIngestingPrivilegesForTableOrSchema(
			ctx,
			txn,
			descsCol,
			desc,
			user,
			wroteDBs,
			wroteSchemas,
			descCoverage,
			privilege.Table,
		)
		if err != nil {
			return nil, err
		}
	case catalog.SchemaDescriptor:
		objectType = privilege.Schema
		updatedPrivileges, err = getIngestingPrivilegesForTableOrSchema(
			ctx,
			txn,
			descsCol,
			desc,
			user,
			wroteDBs,
			wroteSchemas,
			descCoverage,
			privilege.Schema,
		)
		if err != nil {
			return nil, err
		}
	case catalog.TypeDescriptor:
		objectType = privilege.Type
		// If the ingestion is not a cluster restore we cannot know that the users
		// on the ingesting cluster match the ones that were on the cluster that was
		// backed up. So we wipe the privileges on the type.
		if descCoverage == tree.RequestedDescriptors {
			updatedPrivileges = catpb.NewBasePrivilegeDescriptor(user)
		}
	case catalog.DatabaseDescriptor:
		objectType = privilege.Database
		// If the ingestion is not a cluster restore we cannot know that the users
		// on the ingesting cluster match the ones that were on the cluster that was
		// backed up. So we wipe the privileges on the database.
		if descCoverage == tree.RequestedDescriptors {
			updatedPrivileges = catpb.NewBaseDatabasePrivilegeDescriptor(user)
		}
	}

	// If we have explicitly mentioned that we should preserve the grants for
	// certain users, then we have already checked that these users exist on the
	// ingesting cluster. At this point we have a privilege descriptor that
	// represents what will be restored if we did not preserve any backed up
	// grants on restore. We can now start editing this descriptor to reflect
	// the backed up grants for the users we wish to preserve them for.

	// First iterate over the users in the privilege descriptor, and for those
	// ysers that we wish to preserve backed up grants, map their indexes in the
	// UserPrivileges slice. We will overwrite these entries below.
	userToUserPrivilegeIdx := make(map[security.SQLUsername]int)
	for i, u := range updatedPrivileges.Users {
		if _, ok := preserveGrantsForUsers[u.User()]; ok {
			userToUserPrivilegeIdx[u.User()] = i
		}
	}

	// Construct a map of user to privileges that exist on the descriptor being
	// restored.
	userToUserPrivilegesOnDescriptor := make(map[security.SQLUsername]catpb.UserPrivileges)
	for _, p := range desc.GetPrivileges().Users {
		userToUserPrivilegesOnDescriptor[p.User()] = p
	}

	// Iterate over the users that we should preserve the backed up grants for.
	usersToRemove := make([]security.SQLUsername, 0)
	for u := range preserveGrantsForUsers {
		// If the descriptor being restored has backed up privileges for the user,
		// use those.
		if priv, ok := userToUserPrivilegesOnDescriptor[u]; ok {
			if idx, ok := userToUserPrivilegeIdx[u]; ok {
				updatedPrivileges.Users[idx].Privileges =
					privilege.ListFromBitField(priv.Privileges, objectType).ToBitField()
			} else {
				// This user does not exist in the `updatedPrivileges`, create a new
				// entry for it.
				updatedPrivileges.Grant(u, privilege.ListFromBitField(priv.Privileges, objectType),
					priv.WithGrantOption != 0 /* withGrantOption */)
			}
		} else {
			// If the user has no privileges for this descriptor we should ensure that
			// the `updatedPrivileges` also has no privileges for this user. We
			// accumulate the users to remove, and do the actual removal at the end to
			// not mess with the indexing.
			if _, ok := userToUserPrivilegeIdx[u]; ok {
				usersToRemove = append(usersToRemove, u)
			}
		}
	}

	for _, u := range usersToRemove {
		updatedPrivileges.RemoveUser(u)
	}

	return updatedPrivileges, nil
}

func getIngestingPrivilegesForTableOrSchema(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	desc catalog.Descriptor,
	user security.SQLUsername,
	wroteDBs map[descpb.ID]catalog.DatabaseDescriptor,
	wroteSchemas map[descpb.ID]catalog.SchemaDescriptor,
	descCoverage tree.DescriptorCoverage,
	privilegeType privilege.ObjectType,
) (updatedPrivileges *catpb.PrivilegeDescriptor, err error) {
	if wrote, ok := wroteDBs[desc.GetParentID()]; ok {
		// If we're creating a new database in this ingestion, the privileges of the
		// table and schema should be that of the parent DB. This includes the
		// temporary system database that is created during a cluster restore.
		updatedPrivileges = wrote.GetPrivileges()
		for i, u := range updatedPrivileges.Users {
			updatedPrivileges.Users[i].Privileges =
				privilege.ListFromBitField(u.Privileges, privilegeType).ToBitField()
		}
	} else if descCoverage == tree.RequestedDescriptors {
		parentDB, err := descsCol.Direct().MustGetDatabaseDescByID(ctx, txn, desc.GetParentID())
		if err != nil {
			return nil, errors.Wrapf(err, "failed to lookup parent DB %d", errors.Safe(desc.GetParentID()))
		}
		dbDefaultPrivileges := parentDB.GetDefaultPrivilegeDescriptor()

		var schemaDefaultPrivileges catalog.DefaultPrivilegeDescriptor
		targetObject := tree.Schemas
		if privilegeType == privilege.Table {
			targetObject = tree.Tables
			schemaID := desc.GetParentSchemaID()
			// Check if the schema is part of the objects being restored. If it is,
			// the schema's privileges have already been processed before we would
			// process any of the table's being restored. So, it is correct to use the
			// schema's default privileges.
			if schema, ok := wroteSchemas[schemaID]; ok {
				schemaDefaultPrivileges = schema.GetDefaultPrivilegeDescriptor()
			} else {
				// If we are restoring into an existing schema, resolve it, and fetch
				// its default privileges.
				parentSchema, err := descsCol.Direct().MustGetSchemaDescByID(ctx, txn,
					desc.GetParentSchemaID())
				if err != nil {
					return nil,
						errors.Wrapf(err, "failed to lookup parent schema %d", errors.Safe(desc.GetParentSchemaID()))
				}
				schemaDefaultPrivileges = parentSchema.GetDefaultPrivilegeDescriptor()
			}
		}

		updatedPrivileges = catprivilege.CreatePrivilegesFromDefaultPrivileges(
			dbDefaultPrivileges, schemaDefaultPrivileges,
			parentDB.GetID(), user, targetObject, parentDB.GetPrivileges())
	}
	return updatedPrivileges, nil
}
