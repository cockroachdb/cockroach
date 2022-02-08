// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package desctestutils

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/errors"
)

var (
	latestBinaryVersion = clusterversion.TestingClusterVersion
)

// TestingGetDatabaseDescriptorWitVersion retrieves a database descriptor directly from
// the kv layer.
func TestingGetDatabaseDescriptorWitVersion(
	kvDB *kv.DB, codec keys.SQLCodec, version clusterversion.ClusterVersion, database string,
) catalog.DatabaseDescriptor {
	ctx := context.Background()
	var desc catalog.Descriptor
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		id, err := catkv.LookupID(ctx, txn, codec, keys.RootNamespaceID, keys.RootNamespaceID, database)
		if err != nil {
			panic(err)
		} else if id == descpb.InvalidID {
			panic(fmt.Sprintf("database %s not found", database))
		}
		desc, err = catkv.MustGetDescriptorByID(ctx, txn, codec, version, id, catalog.Database)
		if err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return desc.(catalog.DatabaseDescriptor)
}

// TestingGetDatabaseDescriptor retrieves a database descriptor directly from
// the kv layer.
func TestingGetDatabaseDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string,
) catalog.DatabaseDescriptor {
	return TestingGetDatabaseDescriptorWitVersion(kvDB, codec, latestBinaryVersion, database)
}

// TestingGetSchemaDescriptorWithVersion retrieves a schema descriptor directly from the kv
// layer.
func TestingGetSchemaDescriptorWithVersion(
	kvDB *kv.DB,
	codec keys.SQLCodec,
	version clusterversion.ClusterVersion,
	dbID descpb.ID,
	schemaName string,
) catalog.SchemaDescriptor {
	ctx := context.Background()
	var desc catalog.Descriptor
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		schemaID, err := catkv.LookupID(ctx, txn, codec, dbID, keys.RootNamespaceID, schemaName)
		if err != nil {
			panic(err)
		} else if schemaID == descpb.InvalidID {
			panic(fmt.Sprintf("schema %s not found", schemaName))
		}
		desc, err = catkv.MustGetDescriptorByID(ctx, txn, codec, version, schemaID, catalog.Schema)
		if err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return desc.(catalog.SchemaDescriptor)
}

// TestingGetSchemaDescriptor retrieves a schema descriptor directly from the kv
// layer.
func TestingGetSchemaDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, dbID descpb.ID, schemaName string,
) catalog.SchemaDescriptor {
	return TestingGetSchemaDescriptorWithVersion(
		kvDB,
		codec,
		latestBinaryVersion,
		dbID,
		schemaName,
	)
}

// TestingGetTableDescriptorWithVersion retrieves a table descriptor directly
// from the KV layer.
func TestingGetTableDescriptorWithVersion(
	kvDB *kv.DB,
	codec keys.SQLCodec,
	version clusterversion.ClusterVersion,
	database string,
	schema string,
	table string,
) catalog.TableDescriptor {
	return testingGetObjectDescriptor(kvDB, codec, version, database, schema, table).(catalog.TableDescriptor)
}

// TestingGetTableDescriptor retrieves a table descriptor directly
// from the KV layer.
func TestingGetTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, schema string, table string,
) catalog.TableDescriptor {
	return TestingGetTableDescriptorWithVersion(kvDB, codec, latestBinaryVersion, database, schema, table)
}

// TestingGetPublicTableDescriptor retrieves a table descriptor directly from
// the KV layer.
func TestingGetPublicTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) catalog.TableDescriptor {
	return testingGetObjectDescriptor(kvDB, codec, latestBinaryVersion, database, "public", table).(catalog.TableDescriptor)
}

// TestingGetMutableExistingTableDescriptor retrieves a mutable table descriptor
// directly from the KV layer.
func TestingGetMutableExistingTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *tabledesc.Mutable {
	imm := TestingGetPublicTableDescriptor(kvDB, codec, database, table)
	return tabledesc.NewBuilder(imm.TableDesc()).BuildExistingMutableTable()
}

// TestingGetTypeDescriptor retrieves a type descriptor directly from
// the KV layer.
func TestingGetTypeDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, schema string, object string,
) catalog.TypeDescriptor {
	return testingGetObjectDescriptor(kvDB, codec, latestBinaryVersion, database, schema, object).(catalog.TypeDescriptor)
}

// TestingGetPublicTypeDescriptor retrieves a type descriptor directly from the
// KV layer.
func TestingGetPublicTypeDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, object string,
) catalog.TypeDescriptor {
	return TestingGetTypeDescriptor(kvDB, codec, database, "public", object)
}

func testingGetObjectDescriptor(
	kvDB *kv.DB,
	codec keys.SQLCodec,
	version clusterversion.ClusterVersion,
	database string,
	schema string,
	object string,
) (desc catalog.Descriptor) {
	ctx := context.Background()
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		dbID, err := catkv.LookupID(ctx, txn, codec, keys.RootNamespaceID, keys.RootNamespaceID, database)
		if err != nil {
			return err
		}
		if dbID == descpb.InvalidID {
			return errors.Errorf("database %s not found", database)
		}
		schemaID, err := catkv.LookupID(ctx, txn, codec, dbID, keys.RootNamespaceID, schema)
		if err != nil {
			return err
		}
		if schemaID == descpb.InvalidID {
			return errors.Errorf("schema %s not found", schema)
		}
		objectID, err := catkv.LookupID(ctx, txn, codec, dbID, schemaID, object)
		if err != nil {
			return err
		}
		if objectID == descpb.InvalidID {
			return errors.Errorf("object %s not found", object)
		}
		desc, err = catkv.MustGetDescriptorByID(ctx, txn, codec, latestBinaryVersion, objectID, catalog.Any)
		return err
	}); err != nil {
		panic(err)
	}
	return desc
}
