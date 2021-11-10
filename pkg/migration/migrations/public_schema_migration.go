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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func publicSchemaMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	query := `
  SELECT ns_db.id
    FROM system.namespace AS ns_db
         INNER JOIN system.namespace
                AS ns_sc ON (
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
		fmt.Println("dbID:", dbID)
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
		if desc.HasPublicSchemaWithDescriptor() {
			// If the database already has a descriptor backed public schema,
			// there is no work to be done.
			return nil
		}
		dbDescBuilder := dbdesc.NewBuilder(desc.DatabaseDesc())
		dbDesc := dbDescBuilder.BuildExistingMutableDatabase()

		b := txn.NewBatch()

		publicSchemaDesc, _, err := sql.CreateSchemaDescriptorWithPrivileges(
			ctx, d.DB, d.Codec, desc, tree.PublicSchema, security.AdminRoleName(), security.AdminRoleName(), true, /* allocateID */
		)
		if err != nil {
			return err
		}
		publicSchemaID := publicSchemaDesc.GetID()
		newKey := catalogkeys.MakeSchemaNameKey(d.Codec, dbID, publicSchemaDesc.GetName())
		oldKey := catalogkeys.EncodeNameKey(d.Codec, catalogkeys.NewNameKeyComponents(dbID, keys.RootNamespaceID, tree.PublicSchema))
		// Remove namespace entry for old public schema.
		b.Del(oldKey)
		b.CPut(newKey, publicSchemaID, nil)
		if err := catalogkv.WriteNewDescToBatch(
			ctx,
			false,
			d.Settings,
			b,
			d.Codec,
			publicSchemaID,
			publicSchemaDesc,
		); err != nil {
			return err
		}

		if desc.DatabaseDesc().Schemas == nil {
			dbDesc.Schemas = map[string]descpb.DatabaseDescriptor_SchemaInfo{
				tree.PublicSchema: {
					ID: publicSchemaID,
				},
			}
		} else {
			dbDesc.Schemas[tree.PublicSchema] = descpb.DatabaseDescriptor_SchemaInfo{
				ID: publicSchemaID,
			}
		}
		if err := descriptors.WriteDescToBatch(ctx, false, dbDesc, b); err != nil {
			return err
		}
		allDescriptors, err := descriptors.GetAllDescriptors(ctx, txn)
		if err != nil {
			return err
		}
		if err := migrateObjectsInDatabase(ctx, dbID, d, b, publicSchemaID, descriptors, allDescriptors); err != nil {
			return err
		}

		return txn.Run(ctx, b)
	})
}

func migrateObjectsInDatabase(
	ctx context.Context,
	dbID descpb.ID,
	d migration.TenantDeps,
	batch *kv.Batch,
	newPublicSchemaID descpb.ID,
	descriptors *descs.Collection,
	allDescriptors []catalog.Descriptor,
) error {
	var modifiedDescs []catalog.MutableDescriptor
	for _, desc := range allDescriptors {
		b := desc.NewBuilder()
		// Only update descriptors in the parent db and public schema.
		if desc.Dropped() || desc.GetParentID() != dbID ||
			(desc.GetParentSchemaID() != keys.PublicSchemaID && desc.GetParentSchemaID() != descpb.InvalidID) {
			continue
		}
		updateDesc := func(mut catalog.MutableDescriptor, newPublicSchemaID descpb.ID) {
			oldKey := catalogkeys.MakeObjectNameKey(d.Codec, mut.GetParentID(), mut.GetParentSchemaID(), mut.GetName())
			batch.Del(oldKey)
			newKey := catalogkeys.MakeObjectNameKey(d.Codec, mut.GetParentID(), newPublicSchemaID, mut.GetName())
			batch.Put(newKey, mut.GetID())
			modifiedDescs = append(modifiedDescs, mut)
		}
		switch mut := b.BuildExistingMutable().(type) {
		case *dbdesc.Mutable, *schemadesc.Mutable:
			// Ignore database and schema descriptors.
		case *tabledesc.Mutable:
			updateDesc(mut, newPublicSchemaID)
			mut.UnexposedParentSchemaID = newPublicSchemaID
		case *typedesc.Mutable:
			updateDesc(mut, newPublicSchemaID)
			mut.ParentSchemaID = newPublicSchemaID
		}
	}
	for _, modified := range modifiedDescs {
		err := descriptors.WriteDescToBatch(
			ctx, false, modified, batch,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
