// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalogkv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// TestingGetTableDescriptorFromSchema retrieves a table descriptor directly
// from the KV layer.
// TODO (lucy): TestingGetTableDescriptor should become this. It would be a
// trivial change that just touches lots of lines.
func TestingGetTableDescriptorFromSchema(
	kvDB *kv.DB, codec keys.SQLCodec, database string, schema string, table string,
) *tabledesc.Immutable {
	desc, ok := testingGetObjectDescriptor(
		kvDB, codec, tree.TableObject, false /* mutable */, database, schema, table,
	).(*tabledesc.Immutable)
	if !ok {
		return nil
	}
	return desc
}

// TestingGetTableDescriptor retrieves a table descriptor directly from the KV
// layer.
//
// TODO(ajwerner): Move this to catalogkv and/or question the very existence of
// this function. Consider renaming to TestingGetTableDescriptorByName or
// removing it altogether.
func TestingGetTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *tabledesc.Immutable {
	return TestingGetImmutableTableDescriptor(kvDB, codec, database, table)
}

// TestingGetImmutableTableDescriptor retrieves an immutable table descriptor
// directly from the KV layer.
func TestingGetImmutableTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *tabledesc.Immutable {
	desc, ok := testingGetObjectDescriptor(
		kvDB, codec, tree.TableObject, false /* mutable */, database, "public", table,
	).(*tabledesc.Immutable)
	if !ok {
		return nil
	}
	return desc
}

// TestingGetMutableExistingTableDescriptor retrieves a Mutable
// directly from the KV layer.
func TestingGetMutableExistingTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *tabledesc.Mutable {
	desc, ok := testingGetObjectDescriptor(
		kvDB, codec, tree.TableObject, true /* mutable */, database, "public", table,
	).(*tabledesc.Mutable)
	if !ok {
		return nil
	}
	return desc
}

// TestingGetTypeDescriptorFromSchema retrieves a type descriptor directly from
// the KV layer.
// TODO (lucy): TestingGetTypeDescriptor should become this. It would be a
// trivial change that just touches lots of lines.
func TestingGetTypeDescriptorFromSchema(
	kvDB *kv.DB, codec keys.SQLCodec, database string, schema string, object string,
) *typedesc.Immutable {
	desc, ok := testingGetObjectDescriptor(
		kvDB, codec, tree.TypeObject, false /* mutable */, database, schema, object,
	).(*typedesc.Immutable)
	if !ok {
		return nil
	}
	return desc
}

// TestingGetTypeDescriptor retrieves a type descriptor directly from the kv layer.
//
// This function should be moved wherever TestingGetTableDescriptor is moved.
func TestingGetTypeDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, object string,
) *typedesc.Immutable {
	desc, ok := testingGetObjectDescriptor(
		kvDB, codec, tree.TypeObject, false /* mutable */, database, "public", object,
	).(*typedesc.Immutable)
	if !ok {
		return nil
	}
	return desc
}

// TestingGetDatabaseDescriptor retrieves a database descriptor directly from
// the kv layer.
//
// This function should be moved wherever TestingGetTableDescriptor is moved.
func TestingGetDatabaseDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string,
) (db *dbdesc.Immutable) {
	ctx := context.TODO()
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		desc, err := UncachedPhysicalAccessor{}.GetDatabaseDesc(
			ctx, txn, codec, database, tree.DatabaseLookupFlags{
				Required:       true,
				AvoidCached:    true,
				IncludeOffline: true,
				IncludeDropped: true,
			})
		if err != nil {
			return err
		}
		db = desc.(*dbdesc.Immutable)
		return nil
	}); err != nil {
		panic(err)
	}
	return db
}

// TestingGetSchemaDescriptor retrieves a schema descriptor directly from the kv
// layer.
func TestingGetSchemaDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, dbID descpb.ID, schemaName string,
) (schema *schemadesc.Immutable) {
	ctx := context.TODO()
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		_, schemaMeta, err := UncachedPhysicalAccessor{}.GetSchema(
			ctx, txn, codec, dbID, schemaName, tree.SchemaLookupFlags{
				Required:       true,
				AvoidCached:    true,
				IncludeDropped: true,
				IncludeOffline: true,
			})
		if err != nil {
			return err
		}
		desc := schemaMeta.Desc
		if desc == nil {
			return nil
		}
		schema = desc.(*schemadesc.Immutable)
		return nil
	}); err != nil {
		panic(err)
	}
	return schema
}

func testingGetObjectDescriptor(
	kvDB *kv.DB,
	codec keys.SQLCodec,
	kind tree.DesiredObjectKind,
	mutable bool,
	database string,
	schema string,
	object string,
) (desc catalog.Descriptor) {
	ctx := context.TODO()
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		lookupFlags := tree.ObjectLookupFlagsWithRequired()
		lookupFlags.IncludeOffline = true
		lookupFlags.IncludeDropped = true
		lookupFlags.DesiredObjectKind = kind
		lookupFlags.RequireMutable = mutable
		desc, err = UncachedPhysicalAccessor{}.GetObjectDesc(ctx,
			txn, cluster.MakeTestingClusterSettings(), codec,
			database, schema, object, lookupFlags)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return desc
}
