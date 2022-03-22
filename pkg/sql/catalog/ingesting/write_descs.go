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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// WriteDescriptors writes all the new descriptors: First the ID ->
// TableDescriptor for the new table, then flip (or initialize) the name -> ID
// entry so any new queries will use the new one. The tables are assigned the
// permissions of their parent database and the user must have CREATE permission
// on that database at the time this function is called.
// Any database with a name matching inheritParentName will be included in the
// set of wroteIDs passed to GetIngestingDescriptorPrivileges even if coverage
// is AllDescriptors, e.g. to cause the tmp system db's tables to have
// inherited privileges during a cluster restore.
func WriteDescriptors(
	ctx context.Context,
	codec keys.SQLCodec,
	txn *kv.Txn,
	user security.SQLUsername,
	descsCol *descs.Collection,
	databases []catalog.DatabaseDescriptor,
	schemas []catalog.SchemaDescriptor,
	tables []catalog.TableDescriptor,
	types []catalog.TypeDescriptor,
	descCoverage tree.DescriptorCoverage,
	extra []roachpb.KeyValue,
	inheritParentName string,
) (err error) {
	ctx, span := tracing.ChildSpan(ctx, "WriteDescriptors")
	defer span.Finish()
	defer func() {
		err = errors.Wrapf(err, "restoring table desc and namespace entries")
	}()

	b := txn.NewBatch()
	wroteDBs := make(map[descpb.ID]catalog.DatabaseDescriptor)
	for i := range databases {
		desc := databases[i]
		updatedPrivileges, err := GetIngestingDescriptorPrivileges(ctx, txn, descsCol, desc, user, wroteDBs, descCoverage)
		if err != nil {
			return err
		}
		if updatedPrivileges != nil {
			if mut, ok := desc.(*dbdesc.Mutable); ok {
				mut.Privileges = updatedPrivileges
			} else {
				log.Fatalf(ctx, "wrong type for database %d, %T, expected Mutable",
					desc.GetID(), desc)
			}
		}
		privilegeDesc := desc.GetPrivileges()
		catprivilege.MaybeFixUsagePrivForTablesAndDBs(&privilegeDesc)
		if descCoverage == tree.RequestedDescriptors || desc.GetName() == inheritParentName {
			wroteDBs[desc.GetID()] = desc
		}
		if err := descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, desc.(catalog.MutableDescriptor), b,
		); err != nil {
			return err
		}
		b.CPut(catalogkeys.EncodeNameKey(codec, desc), desc.GetID(), nil)

		// We also have to put a system.namespace entry for the public schema
		// if the database does not have a public schema backed by a descriptor.
		if !desc.HasPublicSchemaWithDescriptor() {
			b.CPut(catalogkeys.MakeSchemaNameKey(codec, desc.GetID(), tree.PublicSchema), keys.PublicSchemaID, nil)
		}
	}

	// Write namespace and descriptor entries for each schema.
	for i := range schemas {
		sc := schemas[i]
		updatedPrivileges, err := GetIngestingDescriptorPrivileges(ctx, txn, descsCol, sc, user, wroteDBs, descCoverage)
		if err != nil {
			return err
		}
		if updatedPrivileges != nil {
			if mut, ok := sc.(*schemadesc.Mutable); ok {
				mut.Privileges = updatedPrivileges
			} else {
				log.Fatalf(ctx, "wrong type for schema %d, %T, expected Mutable",
					sc.GetID(), sc)
			}
		}
		if err := descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, sc.(catalog.MutableDescriptor), b,
		); err != nil {
			return err
		}
		b.CPut(catalogkeys.EncodeNameKey(codec, sc), sc.GetID(), nil)
	}

	for i := range tables {
		table := tables[i]
		updatedPrivileges, err := GetIngestingDescriptorPrivileges(ctx, txn, descsCol, table, user, wroteDBs, descCoverage)
		if err != nil {
			return err
		}
		if updatedPrivileges != nil {
			if mut, ok := table.(*tabledesc.Mutable); ok {
				mut.Privileges = updatedPrivileges
			} else {
				log.Fatalf(ctx, "wrong type for table %d, %T, expected Mutable",
					table.GetID(), table)
			}
		}
		privilegeDesc := table.GetPrivileges()
		catprivilege.MaybeFixUsagePrivForTablesAndDBs(&privilegeDesc)

		if err := processTableForMultiRegion(ctx, txn, descsCol, table); err != nil {
			return err
		}

		if err := descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, tables[i].(catalog.MutableDescriptor), b,
		); err != nil {
			return err
		}
		b.CPut(catalogkeys.EncodeNameKey(codec, table), table.GetID(), nil)
	}

	// Write all type descriptors -- create namespace entries and write to
	// the system.descriptor table.
	for i := range types {
		typ := types[i]
		updatedPrivileges, err := GetIngestingDescriptorPrivileges(ctx, txn, descsCol, typ, user, wroteDBs, descCoverage)
		if err != nil {
			return err
		}
		if updatedPrivileges != nil {
			if mut, ok := typ.(*typedesc.Mutable); ok {
				mut.Privileges = updatedPrivileges
			} else {
				log.Fatalf(ctx, "wrong type for type %d, %T, expected Mutable",
					typ.GetID(), typ)
			}
		}
		if err := descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, typ.(catalog.MutableDescriptor), b,
		); err != nil {
			return err
		}
		b.CPut(catalogkeys.EncodeNameKey(codec, typ), typ.GetID(), nil)
	}

	for _, kv := range extra {
		b.InitPut(kv.Key, &kv.Value, false)
	}
	if err := txn.Run(ctx, b); err != nil {
		if errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
			return pgerror.Newf(pgcode.DuplicateObject, "table already exists")
		}
		return err
	}
	return nil
}

func processTableForMultiRegion(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, table catalog.TableDescriptor,
) error {
	_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
		ctx, txn, table.GetParentID(), tree.DatabaseLookupFlags{
			Required:       true,
			AvoidLeased:    true,
			IncludeOffline: true,
		})
	if err != nil {
		return err
	}
	// If the table descriptor is being written to a multi-region database and
	// the table does not have a locality config setup, set one up here. The
	// table's locality config will be set to the default locality - REGIONAL
	// BY TABLE IN PRIMARY REGION.
	if dbDesc.IsMultiRegion() {
		if table.GetLocalityConfig() == nil {
			table.(*tabledesc.Mutable).SetTableLocalityRegionalByTable(tree.PrimaryRegionNotSpecifiedName)
		}
	} else {
		// If the database is not multi-region enabled, ensure that we don't
		// write any multi-region table descriptors into it.
		if table.GetLocalityConfig() != nil {
			return pgerror.Newf(pgcode.FeatureNotSupported,
				"cannot restore or create multi-region table %s into non-multi-region database %s",
				table.GetName(),
				dbDesc.GetName(),
			)
		}
	}
	return nil
}
