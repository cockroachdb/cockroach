// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/errors"
)

var (
	latestBinaryVersion = clusterversion.TestingClusterVersion
)

// TestingGetDatabaseDescriptorWithVersion retrieves a database descriptor directly from
// the kv layer.
func TestingGetDatabaseDescriptorWithVersion(
	kvDB *kv.DB, codec keys.SQLCodec, version clusterversion.ClusterVersion, database string,
) catalog.DatabaseDescriptor {
	ctx := context.Background()
	var desc catalog.Descriptor
	cr := catkv.NewCatalogReader(
		codec, version, nil /* systemDatabaseCache */, nil, /* maybeMonitor */
	)
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		id, err := lookupDescriptorID(ctx, cr, txn, keys.RootNamespaceID, keys.RootNamespaceID, database)
		if err != nil {
			panic(err)
		} else if id == descpb.InvalidID {
			panic(fmt.Sprintf("database %s not found", database))
		}
		desc, err = mustGetDescriptorByID(ctx, version, cr, txn, id, catalog.Database)
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
	return TestingGetDatabaseDescriptorWithVersion(kvDB, codec, latestBinaryVersion, database)
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
	cr := catkv.NewCatalogReader(
		codec, version, nil /* systemDatabaseCache */, nil, /* maybeMonitor */
	)
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		schemaID, err := lookupDescriptorID(ctx, cr, txn, dbID, keys.RootNamespaceID, schemaName)
		if err != nil {
			panic(err)
		} else if schemaID == descpb.InvalidID {
			panic(fmt.Sprintf("schema %s not found", schemaName))
		}
		desc, err = mustGetDescriptorByID(ctx, version, cr, txn, schemaID, catalog.Schema)
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

func TestingGetFunctionDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, schema string, fName string,
) catalog.FunctionDescriptor {
	db := TestingGetDatabaseDescriptor(kvDB, codec, database)
	sc := TestingGetSchemaDescriptor(kvDB, codec, db.GetID(), schema)
	f, found := sc.GetFunction(fName)
	if !found {
		panic(fmt.Sprintf("function %s.%s.%s does not exist", database, schema, fName))
	}
	if len(f.Signatures) != 1 {
		panic(fmt.Sprintf("expected only 1 function %s.%s.%s, found %d", database, schema, fName, len(f.Signatures)))
	}
	fnID := f.Signatures[0].ID
	ctx := context.Background()
	var fnDesc catalog.Descriptor
	cr := catkv.NewCatalogReader(
		codec, latestBinaryVersion, nil /* systemDatabaseCache */, nil, /* maybeMonitor */
	)
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		fnDesc, err = mustGetDescriptorByID(ctx, latestBinaryVersion, cr, txn, fnID, catalog.Function)
		if err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return fnDesc.(catalog.FunctionDescriptor)
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
	cr := catkv.NewCatalogReader(
		codec, version, nil /* systemDatabaseCache */, nil, /* maybeMonitor */
	)
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		dbID, err := lookupDescriptorID(ctx, cr, txn, keys.RootNamespaceID, keys.RootNamespaceID, database)
		if err != nil {
			return err
		}
		if dbID == descpb.InvalidID {
			return errors.Errorf("database %s not found", database)
		}
		schemaID, err := lookupDescriptorID(ctx, cr, txn, dbID, keys.RootNamespaceID, schema)
		if err != nil {
			return err
		}
		if schemaID == descpb.InvalidID {
			return errors.Errorf("schema %s not found", schema)
		}
		objectID, err := lookupDescriptorID(ctx, cr, txn, dbID, schemaID, object)
		if err != nil {
			return err
		}
		if objectID == descpb.InvalidID {
			return errors.Errorf("object %s not found", object)
		}
		desc, err = mustGetDescriptorByID(ctx, version, cr, txn, objectID, catalog.Any)
		return err
	}); err != nil {
		panic(err)
	}
	return desc
}

func lookupDescriptorID(
	ctx context.Context,
	cr catkv.CatalogReader,
	txn *kv.Txn,
	dbID descpb.ID,
	schemaID descpb.ID,
	objectName string,
) (descpb.ID, error) {
	key := descpb.NameInfo{ParentID: dbID, ParentSchemaID: schemaID, Name: objectName}
	c, err := cr.GetByNames(ctx, txn, []descpb.NameInfo{key})
	if err != nil {
		return descpb.InvalidID, err
	}
	if e := c.LookupNamespaceEntry(key); e != nil {
		return e.GetID(), nil
	}
	return descpb.InvalidID, nil
}

func mustGetDescriptorByID(
	ctx context.Context,
	version clusterversion.ClusterVersion,
	cr catkv.CatalogReader,
	txn *kv.Txn,
	id descpb.ID,
	expectedType catalog.DescriptorType,
) (catalog.Descriptor, error) {
	const isDescriptorRequired = true
	c, err := cr.GetByIDs(ctx, txn, []descpb.ID{id}, isDescriptorRequired, expectedType)
	if err != nil {
		return nil, err
	}
	desc := c.LookupDescriptor(id)
	vd := catkv.NewCatalogReaderBackedValidationDereferencer(cr, txn, nil /* dvmpMaybe */)
	ve := validate.Validate(
		ctx, version, vd, catalog.ValidationReadTelemetry, validate.ImmutableRead, desc,
	)
	if err := ve.CombinedError(); err != nil {
		return nil, err
	}
	return desc, nil
}

// TestingValidateSelf is a convenience function for internal descriptor
// validation.
func TestingValidateSelf(desc catalog.Descriptor) error {
	return validate.Self(clusterversion.TestingClusterVersion, desc)
}
