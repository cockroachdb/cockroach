// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ingesting

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
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
	txn *kv.Txn,
	user username.SQLUsername,
	descsCol *descs.Collection,
	databases []catalog.DatabaseDescriptor,
	schemas []catalog.SchemaDescriptor,
	tables []catalog.TableDescriptor,
	types []catalog.TypeDescriptor,
	functions []catalog.FunctionDescriptor,
	descCoverage tree.DescriptorCoverage,
	extra []roachpb.KeyValue,
	inheritParentName string,
	includePublicSchemaCreatePriv bool,
) (err error) {
	ctx, span := tracing.ChildSpan(ctx, "WriteDescriptors")
	defer span.Finish()
	defer func() {
		err = errors.Wrapf(err, "restoring table desc and namespace entries")
	}()

	const kvTrace = false
	b := txn.NewBatch()
	// wroteDBs contains the database descriptors that are being published as part
	// of this restore.
	//
	// In the case of a cluster restore, this only includes the temporary system
	// database being restored.
	wroteDBs := make(map[descpb.ID]catalog.DatabaseDescriptor)

	// wroteSchemas contains the schema descriptors that are being published as
	// part of this restore.
	wroteSchemas := make(map[descpb.ID]catalog.SchemaDescriptor)
	for i := range databases {
		desc := databases[i]
		updatedPrivileges, err := GetIngestingDescriptorPrivileges(ctx, txn, descsCol, desc, user,
			wroteDBs, wroteSchemas, descCoverage, includePublicSchemaCreatePriv)
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
		if descCoverage == tree.RequestedDescriptors || desc.GetName() == inheritParentName {
			wroteDBs[desc.GetID()] = desc
		}
		if err := descsCol.WriteDescToBatch(
			ctx, kvTrace, desc.(catalog.MutableDescriptor), b,
		); err != nil {
			return err
		}
		if err := descsCol.InsertNamespaceEntryToBatch(ctx, kvTrace, desc, b); err != nil {
			return err
		}

		// We also have to put a system.namespace entry for the public schema
		// if the database does not have a public schema backed by a descriptor.
		if !desc.HasPublicSchemaWithDescriptor() {
			if err := descsCol.InsertDescriptorlessPublicSchemaToBatch(ctx, kvTrace, desc, b); err != nil {
				return err
			}
		}
	}

	// Write namespace and descriptor entries for each schema.
	for i := range schemas {
		sc := schemas[i]
		updatedPrivileges, err := GetIngestingDescriptorPrivileges(ctx, txn, descsCol, sc, user,
			wroteDBs, wroteSchemas, descCoverage, includePublicSchemaCreatePriv)
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
		if descCoverage == tree.RequestedDescriptors {
			wroteSchemas[sc.GetID()] = sc
		}
		if err := descsCol.WriteDescToBatch(
			ctx, kvTrace, sc.(catalog.MutableDescriptor), b,
		); err != nil {
			return err
		}
		if err := descsCol.InsertNamespaceEntryToBatch(ctx, kvTrace, sc, b); err != nil {
			return err
		}
	}

	for i := range tables {
		table := tables[i]
		updatedPrivileges, err := GetIngestingDescriptorPrivileges(ctx, txn, descsCol, table, user,
			wroteDBs, wroteSchemas, descCoverage, includePublicSchemaCreatePriv)
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
		if err := processTableForMultiRegion(ctx, txn, descsCol, table); err != nil {
			return err
		}

		if err := descsCol.WriteDescToBatch(
			ctx, kvTrace, tables[i].(catalog.MutableDescriptor), b,
		); err != nil {
			return err
		}
		if err := descsCol.InsertNamespaceEntryToBatch(ctx, kvTrace, table, b); err != nil {
			return err
		}
	}

	// Write all type descriptors -- create namespace entries and write to
	// the system.descriptor table.
	for i := range types {
		typ := types[i]
		updatedPrivileges, err := GetIngestingDescriptorPrivileges(ctx, txn, descsCol, typ, user,
			wroteDBs, wroteSchemas, descCoverage, includePublicSchemaCreatePriv)
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
			ctx, kvTrace, typ.(catalog.MutableDescriptor), b,
		); err != nil {
			return err
		}
		if err := descsCol.InsertNamespaceEntryToBatch(ctx, kvTrace, typ, b); err != nil {
			return err
		}
	}

	for _, fn := range functions {
		updatedPrivileges, err := GetIngestingDescriptorPrivileges(
			ctx, txn, descsCol, fn, user, wroteDBs, wroteSchemas, descCoverage, includePublicSchemaCreatePriv,
		)
		if err != nil {
			return err
		}
		if updatedPrivileges != nil {
			if mut, ok := fn.(*funcdesc.Mutable); ok {
				mut.Privileges = updatedPrivileges
			} else {
				log.Fatalf(ctx, "wrong type for function %d, %T, expected Mutable", fn.GetID(), fn)
			}
		}
		if err := descsCol.WriteDescToBatch(
			ctx, kvTrace, fn.(catalog.MutableDescriptor), b,
		); err != nil {
			return err
		}
		// Function does not have namespace entry.
	}

	for _, kv := range extra {
		b.InitPut(kv.Key, &kv.Value, false)
	}
	if err := txn.Run(ctx, b); err != nil {
		if errors.HasType(err, (*kvpb.ConditionFailedError)(nil)) {
			return pgerror.Newf(pgcode.DuplicateObject, "table already exists")
		}
		return err
	}
	return nil
}

func processTableForMultiRegion(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, table catalog.TableDescriptor,
) error {
	dbDesc, err := descsCol.ByIDWithoutLeased(txn).WithoutDropped().Get().Database(ctx, table.GetParentID())
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
