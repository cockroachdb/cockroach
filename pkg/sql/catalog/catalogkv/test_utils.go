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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
)

// TestingGetTableDescriptorFromSchema retrieves a table descriptor directly
// from the KV layer.
// TODO (lucy): TestingGetTableDescriptor should become this. It would be a
// trivial change that just touches lots of lines.
func TestingGetTableDescriptorFromSchema(
	kvDB *kv.DB, codec keys.SQLCodec, database string, schema string, table string,
) *tabledesc.Immutable {
	return testingGetObjectDescriptor(kvDB, codec, database, schema, table).(*tabledesc.Immutable)
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
	return testingGetObjectDescriptor(kvDB, codec, database, "public", table).(*tabledesc.Immutable)
}

// TestingGetMutableExistingTableDescriptor retrieves a Mutable
// directly from the KV layer.
func TestingGetMutableExistingTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *tabledesc.Mutable {
	return tabledesc.NewExistingMutable(
		*TestingGetImmutableTableDescriptor(kvDB, codec, database, table).TableDesc())
}

// TestingGetTypeDescriptorFromSchema retrieves a type descriptor directly from
// the KV layer.
// TODO (lucy): TestingGetTypeDescriptor should become this. It would be a
// trivial change that just touches lots of lines.
func TestingGetTypeDescriptorFromSchema(
	kvDB *kv.DB, codec keys.SQLCodec, database string, schema string, object string,
) *typedesc.Immutable {
	return testingGetObjectDescriptor(kvDB, codec, database, schema, object).(*typedesc.Immutable)
}

// TestingGetTypeDescriptor retrieves a type descriptor directly from the kv
// layer.
func TestingGetTypeDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, object string,
) *typedesc.Immutable {
	return TestingGetTypeDescriptorFromSchema(kvDB, codec, database, "public", object)
}

// TestingGetDatabaseDescriptor retrieves a database descriptor directly from
// the kv layer.
func TestingGetDatabaseDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string,
) (db *dbdesc.Immutable) {
	ctx := context.Background()
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		found, id, err := LookupDatabaseID(ctx, txn, codec, database)
		if err != nil {
			panic(err)
		} else if !found {
			panic(fmt.Sprintf("database %s not found", database))
		}
		db, err = MustGetDatabaseDescByID(ctx, txn, codec, id)
		if err != nil {
			panic(err)
		}
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
	ctx := context.Background()
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		exists, schemaID, err := ResolveSchemaID(ctx, txn, codec, dbID, schemaName)
		if err != nil {
			panic(err)
		} else if !exists {
			panic(fmt.Sprintf("schema %s not found", schemaName))
		}
		schema, err = MustGetSchemaDescByID(ctx, txn, codec, schemaID)
		if err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return schema
}

func testingGetObjectDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, schema string, object string,
) (desc catalog.Descriptor) {
	ctx := context.Background()
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		found, dbID, err := LookupDatabaseID(ctx, txn, codec, database)
		if err != nil {
			panic(err)
		} else if !found {
			panic(fmt.Sprintf("database %s not found", database))
		}
		exists, schemaID, err := ResolveSchemaID(ctx, txn, codec, dbID, schema)
		if err != nil {
			panic(err)
		} else if !exists {
			panic(fmt.Sprintf("schema %s not found", schema))
		}
		found, objectID, err := LookupObjectID(ctx, txn, codec, dbID, schemaID, object)
		if err != nil {
			panic(err)
		} else if !found {
			panic(fmt.Sprintf("object %s not found", object))
		}
		desc, err = GetDescriptorByID(
			ctx, txn, codec, objectID, Immutable, AnyDescriptorKind, true /* required */)
		if err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return desc
}
