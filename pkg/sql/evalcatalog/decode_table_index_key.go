// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package evalcatalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/keydecoder"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// DecodeTableIndexKey is part of eval.CatalogBuiltins.
// It decodes an encoded key and resolves it to table, index, and column
// information, returning the result as JSON.
func (ec *Builtins) DecodeTableIndexKey(ctx context.Context, key []byte) (json.JSON, error) {
	info, err := keydecoder.DecodeKey(ctx, ec.codec, ec.dc, ec.txn, ec.authzAccessor, key)
	if err != nil {
		return nil, err
	}
	return decodedKeyInfoToJSON(info), nil
}

// decodedKeyInfoToJSON converts DecodedKeyInfo to a JSON object.
func decodedKeyInfoToJSON(d *keydecoder.DecodedKeyInfo) json.JSON {
	builder := json.NewObjectBuilder(7)
	builder.Add("table_id", json.FromInt(int(d.TableID)))
	builder.Add("index_id", json.FromInt(int(d.IndexID)))
	builder.Add("database_name", json.FromString(d.DatabaseName))
	builder.Add("schema_name", json.FromString(d.SchemaName))
	builder.Add("table_name", json.FromString(d.TableName))
	builder.Add("index_name", json.FromString(d.IndexName))

	// Build key_columns array.
	arrBuilder := json.NewArrayBuilder(len(d.KeyColumns))
	for _, col := range d.KeyColumns {
		colBuilder := json.NewObjectBuilder(3)
		colBuilder.Add("name", json.FromString(col.Name))
		colBuilder.Add("type", json.FromString(col.Type))
		if col.Value == tree.DNull {
			colBuilder.Add("value", json.NullJSONValue)
		} else {
			colBuilder.Add("value", json.FromString(col.Value.String()))
		}
		arrBuilder.Add(colBuilder.Build())
	}
	builder.Add("key_columns", arrBuilder.Build())

	return builder.Build()
}
