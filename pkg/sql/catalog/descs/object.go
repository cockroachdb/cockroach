// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// GetObjectDesc looks up an object by name and returns both its
// descriptor and that of its parent database. If the object is not
// found and flags.required is true, an error is returned, otherwise
// a nil reference is returned.
//
// TODO(ajwerner): clarify the purpose of the transaction here. It's used in
// some cases for some lookups but not in others. For example, if a mutable
// descriptor is requested, it will be utilized however if an immutable
// descriptor is requested then it will only be used for its timestamp and to
// set the deadline.
func (tc *Collection) GetObjectDesc(
	ctx context.Context, txn *kv.Txn, db, schema, object string, flags tree.ObjectLookupFlags,
) (desc catalog.Descriptor, err error) {
	if isVirtual, desc, err := tc.virtual.getObjectByName(
		schema, object, flags, db,
	); isVirtual || err != nil {
		return desc, err
	}
	// Fall back to physical descriptor access.
	switch flags.DesiredObjectKind {
	case tree.TypeObject:
		typeName := tree.MakeNewQualifiedTypeName(db, schema, object)
		_, desc, err := tc.getTypeByName(ctx, txn, &typeName, flags)
		return desc, err
	case tree.TableObject:
		tableName := tree.MakeTableNameWithSchema(tree.Name(db), tree.Name(schema), tree.Name(object))
		_, desc, err := tc.getTableByName(ctx, txn, &tableName, flags)
		return desc, err
	default:
		return nil, errors.AssertionFailedf("unknown desired object kind %d", flags.DesiredObjectKind)
	}
}

func (tc *Collection) getObjectByName(
	ctx context.Context,
	txn *kv.Txn,
	catalogName, schemaName, objectName string,
	flags tree.ObjectLookupFlags,
) (found bool, _ catalog.Descriptor, err error) {

	// If we're reading the object descriptor from the store,
	// we should read its parents from the store too to ensure
	// that subsequent name resolution finds the latest name
	// in the face of a concurrent rename.
	avoidCachedForParent := flags.AvoidCached || flags.RequireMutable
	// Resolve the database.
	db, err := tc.GetImmutableDatabaseByName(ctx, txn, catalogName,
		tree.DatabaseLookupFlags{
			Required:       flags.Required,
			AvoidCached:    avoidCachedForParent,
			IncludeDropped: flags.IncludeDropped,
			IncludeOffline: flags.IncludeOffline,
		})
	if err != nil || db == nil {
		return false, nil, err
	}

	if isVirtual, desc, err := tc.virtual.getObjectByName(
		schemaName, objectName, flags, db.GetName(),
	); isVirtual || err != nil {
		return isVirtual, desc, err
	}

	// Resolve the schema.
	dbID := db.GetID()
	scDesc, err := tc.getSchemaByName(ctx, txn, db.GetID(), schemaName,
		tree.SchemaLookupFlags{
			Required:       flags.Required,
			AvoidCached:    avoidCachedForParent,
			IncludeDropped: flags.IncludeDropped,
			IncludeOffline: flags.IncludeOffline,
		})
	if err != nil || scDesc == nil {
		return false, nil, err
	}
	schemaID := scDesc.GetID()

	if found, refuseFurtherLookup, desc, err := tc.getSyntheticOrUncommittedDescriptor(
		dbID, schemaID, objectName, flags.RequireMutable,
	); err != nil || refuseFurtherLookup {
		return false, nil, err
	} else if found {
		log.VEventf(ctx, 2, "found uncommitted descriptor %d", desc.GetID())
		return true, desc, nil
	}

	// TODO(vivek): Ideally we'd avoid caching for only the
	// system.descriptor and system.lease tables, because they are
	// used for acquiring leases, creating a chicken&egg problem.
	// But doing so turned problematic and the tests pass only by also
	// disabling caching of system.eventlog, system.rangelog, and
	// system.users. For now we're sticking to disabling caching of
	// all system descriptors except role_members, role_options, and users
	// (i.e., the ones used during authn/authz flows).
	// TODO (lucy): Reevaluate the above. We have many more system tables now and
	// should be able to lease most of them.
	isAllowedSystemTable := objectName == systemschema.RoleMembersTable.GetName() ||
		objectName == systemschema.RoleOptionsTable.GetName() ||
		objectName == systemschema.UsersTable.GetName() ||
		objectName == systemschema.JobsTable.GetName() ||
		objectName == systemschema.EventLogTable.GetName()
	avoidCache := flags.AvoidCached || flags.RequireMutable || lease.TestingTableLeasesAreDisabled() ||
		(catalogName == systemschema.SystemDatabaseName && !isAllowedSystemTable)
	if avoidCache {
		return tc.kv.getByName(
			ctx, txn, dbID, schemaID, objectName, flags.RequireMutable,
		)
	}

	desc, shouldReadFromStore, err := tc.leased.getByName(
		ctx, txn, dbID, schemaID, objectName)
	if err != nil {
		return false, nil, err
	}
	if shouldReadFromStore {
		return tc.kv.getByName(
			ctx, txn, dbID, schemaID, objectName, flags.RequireMutable,
		)
	}
	return true, desc, nil
}
