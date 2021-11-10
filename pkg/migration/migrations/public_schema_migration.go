// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func publicSchemaMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	query := `
  SELECT ns_db.id
    FROM system.namespace AS ns_db
         INNER JOIN system.namespace AS ns_sc ON (
                                                                ns_db.id
                                                                = ns_sc."parentID"
                                                             )
   WHERE ns_db.id != 1
     AND ns_db."parentSchemaID" = 0
     AND ns_db."parentID" = 0
     AND ns_sc."parentSchemaID" = 0
     AND ns_sc.name = 'public'
     AND ns_sc.id = 29
ORDER BY ns_db.id ASC;
`
	rows, err := d.InternalExecutor.QueryIterator(
		ctx, "get_databases_with_synthetic_public_schemas", nil /* txn */, query,
	)
	if err != nil {
		return err
	}
	var databaseIDs []descpb.ID
	for ok, err := rows.Next(ctx); ok; ok, err = rows.Next(ctx) {
		if err != nil {
			return err
		}
		parentID := descpb.ID(tree.MustBeDInt(rows.Cur()[0]))
		databaseIDs = append(databaseIDs, parentID)
	}

	for _, dbID := range databaseIDs {
		if err := createPublicSchemaForDatabase(ctx, dbID, d); err != nil {
			return err
		}
	}

	return nil
}

func createPublicSchemaForDatabase(
	ctx context.Context, dbID descpb.ID, d migration.TenantDeps,
) error {
	return d.CollectionFactory.Txn(ctx, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {

		found, desc, err := descriptors.GetImmutableDatabaseByID(ctx, txn, dbID, tree.DatabaseLookupFlags{})
		if err != nil {
			return err
		}
		if !found {
			return errors.Newf("expected to find database with id %d", dbID)
		}
		mutableDesc := dbdesc.NewBuilder(desc.DatabaseDesc())
		dbDesc := mutableDesc.BuildExistingMutable()

		publicSchemaID, err := catalogkv.GenerateUniqueDescID(ctx, d.DB, d.Codec)
		if err != nil {
			return err
		}
		// Every database must be initialized with the public schema.
		// Create the SchemaDescriptor.
		// In postgres, the user "postgres" is the owner of the public schema in a
		// newly created db. Postgres and Public have USAGE and CREATE privileges.
		// In CockroachDB, root is our substitute for the postgres user.
		publicSchemaPrivileges := descpb.NewBasePrivilegeDescriptor(security.AdminRoleName())
		// By default, everyone has USAGE and CREATE on the public schema.
		publicSchemaPrivileges.Grant(security.PublicRoleName(), privilege.List{privilege.CREATE, privilege.USAGE})
		publicSchemaDesc := schemadesc.NewBuilder(&descpb.SchemaDescriptor{
			ParentID:   dbID,
			Name:       "public",
			ID:         publicSchemaID,
			Privileges: publicSchemaPrivileges,
			Version:    1,
		}).BuildCreatedMutableSchema()

		descID := publicSchemaDesc.GetID()
		idKey := catalogkeys.MakeSchemaNameKey(d.Codec, dbID, publicSchemaDesc.GetName())

		key := catalogkeys.EncodeNameKey(d.Codec, catalogkeys.NewNameKeyComponents(dbID, keys.RootNamespaceID, tree.PublicSchema))

		b := txn.NewBatch()

		// Remove namespace entry for old public schema.
		b.Del(key)
		b.CPut(idKey, descID, nil)
		if err := catalogkv.WriteNewDescToBatch(
			ctx,
			false,
			d.Settings,
			b,
			d.Codec,
			descID,
			publicSchemaDesc,
		); err != nil {
			return err
		}

		dbDesc.DescriptorProto().GetDatabase().Schemas = map[string]descpb.DatabaseDescriptor_SchemaInfo{
			tree.PublicSchema: {
				ID: publicSchemaID,
			},
		}
		dbDesc.MaybeIncrementVersion()
		if err := catalogkv.WriteDescToBatch(
			ctx, false, d.Settings, b, d.Codec, dbDesc.GetID(), dbDesc,
		); err != nil {
			return err
		}

		if err := migrateObjectsInDatabase(ctx, dbID, d, txn, b, publicSchemaID, descriptors); err != nil {
			return err
		}

		return txn.Run(ctx, b)
	})
}

func migrateObjectsInDatabase(
	ctx context.Context,
	dbID descpb.ID,
	d migration.TenantDeps,
	txn *kv.Txn,
	batch *kv.Batch,
	newPublicSchemaID descpb.ID,
	descriptors *descs.Collection,
) error {
	descs, err := descriptors.GetAllDescriptorsInDatabase(ctx, txn, dbID)
	if err != nil {
		return err
	}

	var modifiedDescs []catalog.MutableDescriptor
	for _, desc := range descs {
		b := catalogkv.NewBuilder(desc.DescriptorProto())
		switch b.DescriptorType() {
		case catalog.Database, catalog.Schema:
			panic(fmt.Sprintf("unexpected descriptor type %v", b.DescriptorType()))
		case catalog.Table:
			table := b.BuildExistingMutable()
			oldKey := catalogkeys.MakeObjectNameKey(d.Codec, table.GetParentID(), table.GetParentSchemaID(), table.GetName())
			batch.Del(oldKey)
			table.DescriptorProto().GetTable().UnexposedParentSchemaID = newPublicSchemaID
			newKey := catalogkeys.MakeObjectNameKey(d.Codec, table.GetParentID(), table.GetParentSchemaID(), table.GetName())
			batch.Put(newKey, table.GetID())
			modifiedDescs = append(modifiedDescs, table)
		case catalog.Type:
			typ := b.BuildExistingMutable()
			oldKey := catalogkeys.MakeObjectNameKey(d.Codec, typ.GetParentID(), typ.GetParentSchemaID(), typ.GetName())
			batch.Del(oldKey)
			typ.DescriptorProto().GetType().ParentSchemaID = newPublicSchemaID
			newKey := catalogkeys.MakeObjectNameKey(d.Codec, typ.GetParentID(), typ.GetParentSchemaID(), typ.GetName())
			batch.Put(newKey, typ.GetID())
			modifiedDescs = append(modifiedDescs, typ)
		}
	}
	for _, modified := range modifiedDescs {
		err := catalogkv.WriteDescToBatch(
			ctx, false, d.Settings, batch, d.Codec, modified.GetID(), modified,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
