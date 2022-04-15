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

	"github.com/cockroachdb/cockroach/pkg/keys"
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
// descCoverage indicates if only some requested decriptors are being ingested
// or if all descriptors are being ingested such as during a cluster restore; in
// the latter case assumptions can be made such as that any users mentioned in
// privileges on the input descriptor still exists.
func GetIngestingDescriptorPrivileges(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	desc catalog.Descriptor,
	user security.SQLUsername,
	wroteDBs map[descpb.ID]catalog.DatabaseDescriptor,
	wroteSchemas map[descpb.ID]catalog.SchemaDescriptor,
	descCoverage tree.DescriptorCoverage,
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
		)
	case catalog.TypeDescriptor:
		// If the ingestion is not a cluster restore we cannot know that the users
		// on the ingesting cluster match the ones that were on the cluster that was
		// backed up. So we wipe the privileges on the type.
		if descCoverage == tree.RequestedDescriptors {
			updatedPrivileges = catpb.NewBasePrivilegeDescriptor(user)
		}
	case catalog.DatabaseDescriptor:
		// If the ingestion is not a cluster restore we cannot know that the users
		// on the ingesting cluster match the ones that were on the cluster that was
		// backed up. So we wipe the privileges on the database.
		if descCoverage == tree.RequestedDescriptors {
			updatedPrivileges = catpb.NewBaseDatabasePrivilegeDescriptor(user)
		}
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
		// table and schema should be that of the parent DB.
		//
		// Leave the privileges of the temp system tables as the default too.
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
		switch privilegeType {
		case privilege.Table:
			targetObject = tree.Tables
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
				parentSchema, err := descsCol.Direct().MustGetSchemaDescByID(ctx, txn,
					desc.GetParentSchemaID())
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

		updatedPrivileges = catprivilege.CreatePrivilegesFromDefaultPrivileges(
			dbDefaultPrivileges, schemaDefaultPrivileges,
			parentDB.GetID(), user, targetObject, parentDB.GetPrivileges())
	}
	return updatedPrivileges, nil
}
