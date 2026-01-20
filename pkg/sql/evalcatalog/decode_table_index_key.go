// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package evalcatalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// DecodeTableIndexKey is part of eval.CatalogBuiltins.
// It decodes an encoded key and resolves it to table, index, and column
// information, returning the result as JSON.
func (ec *Builtins) DecodeTableIndexKey(ctx context.Context, key []byte) (json.JSON, error) {
	// Decode the index prefix to extract table ID and index ID.
	remaining, tableID, indexID, err := ec.codec.DecodeIndexPrefix(key)
	if err != nil {
		// Invalid key format, return NULL.
		//nolint:returnerrcheck
		return json.NullJSONValue, nil
	}

	builder := json.NewObjectBuilder(8)
	builder.Add("table_id", json.FromInt(int(tableID)))
	builder.Add("index_id", json.FromInt(int(indexID)))

	// Try to look up the table descriptor.
	tableDesc, err := ec.dc.ByIDWithLeased(ec.txn).WithoutNonPublic().Get().Table(ctx, descpb.ID(tableID))
	if err != nil {
		// Table not found (possibly dropped). Return partial info with error.
		builder.Add("error", json.FromString("table not found"))
		//nolint:returnerrcheck
		return builder.Build(), nil
	}

	// Check that the user has any privilege on the table.
	if ec.authzAccessor != nil {
		if err := ec.authzAccessor.CheckAnyPrivilege(ctx, tableDesc); err != nil {
			// User doesn't have access to this table. Return partial info with error.
			builder.Add("error", json.FromString("permission denied"))
			//nolint:returnerrcheck
			return builder.Build(), nil
		}
	}

	builder.Add("table_name", json.FromString(tableDesc.GetName()))

	// Look up parent database and schema names.
	if dbName, schemaName, err := ec.lookupParentNames(ctx, tableDesc); err == nil {
		builder.Add("database_name", json.FromString(dbName))
		builder.Add("schema_name", json.FromString(schemaName))
	}

	// Look up the index.
	index, err := catalog.MustFindIndexByID(tableDesc, descpb.IndexID(indexID))
	if err != nil {
		// Index not found. Return partial info with error.
		builder.Add("error", json.FromString("index not found"))
		//nolint:returnerrcheck
		return builder.Build(), nil
	}

	builder.Add("index_name", json.FromString(index.GetName()))

	// Decode key column values.
	keyColumns, err := ec.decodeKeyColumns(tableDesc, index, remaining)
	if err != nil {
		// Column decode failed. Return result without key_columns, add error.
		builder.Add("error", json.FromString("failed to decode key columns: "+err.Error()))
		//nolint:returnerrcheck
		return builder.Build(), nil
	}

	builder.Add("key_columns", keyColumns)
	return builder.Build(), nil
}

// lookupParentNames looks up the database and schema names for a table descriptor.
func (ec *Builtins) lookupParentNames(
	ctx context.Context, tableDesc catalog.TableDescriptor,
) (dbName string, schemaName string, err error) {
	parentID := tableDesc.GetParentID()
	if parentID != descpb.InvalidID {
		dbDesc, err := ec.dc.ByIDWithLeased(ec.txn).WithoutNonPublic().Get().Database(ctx, parentID)
		if err == nil {
			dbName = dbDesc.GetName()
		}
	}

	parentSchemaID := tableDesc.GetParentSchemaID()
	if parentSchemaID != descpb.InvalidID {
		schemaDesc, err := ec.dc.ByIDWithLeased(ec.txn).WithoutNonPublic().Get().Schema(ctx, parentSchemaID)
		if err == nil {
			schemaName = schemaDesc.GetName()
		}
	}

	return dbName, schemaName, nil
}

// decodeKeyColumns decodes the column values from the key bytes.
func (ec *Builtins) decodeKeyColumns(
	tableDesc catalog.TableDescriptor, index catalog.Index, keyBytes []byte,
) (json.JSON, error) {
	var da tree.DatumAlloc
	arrBuilder := json.NewArrayBuilder(index.NumKeyColumns())

	for i := 0; i < index.NumKeyColumns(); i++ {
		colID := index.GetKeyColumnID(i)
		col, err := catalog.MustFindColumnByID(tableDesc, colID)
		if err != nil {
			return nil, err
		}

		dir, err := catalogkeys.IndexColumnEncodingDirection(index.GetKeyColumnDirection(i))
		if err != nil {
			return nil, err
		}

		// If we've run out of key bytes, stop decoding.
		if len(keyBytes) == 0 {
			break
		}

		datum, remaining, err := keyside.Decode(&da, col.GetType(), keyBytes, dir)
		if err != nil {
			return nil, err
		}
		keyBytes = remaining

		// Build the column info JSON object.
		colBuilder := json.NewObjectBuilder(3)
		colBuilder.Add("name", json.FromString(col.GetName()))
		colBuilder.Add("type", json.FromString(col.GetType().SQLString()))

		// Convert the datum to a string representation.
		if datum == tree.DNull {
			colBuilder.Add("value", json.NullJSONValue)
		} else {
			colBuilder.Add("value", json.FromString(datum.String()))
		}

		arrBuilder.Add(colBuilder.Build())
	}

	return arrBuilder.Build(), nil
}
