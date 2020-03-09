// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package schema is to avoid a circular dependency on pkg/sql/row and pkg/sql.
// It can be removed once schemas can be resolved to a protobuf.
package schema

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ResolveNameByID resolves a schema's name based on db and schema id.
// TODO(sqlexec): this should return the descriptor instead if given an ID.
// Instead, we have to rely on a scan of the kv table.
// TODO(sqlexec): this should probably be cached.
func ResolveNameByID(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, schemaID sqlbase.ID,
) (string, error) {
	// Fast-path for public schema, to avoid hot lookups.
	if schemaID == keys.PublicSchemaID {
		return string(tree.PublicSchemaName), nil
	}
	schemas, err := GetForDatabase(ctx, txn, dbID)
	if err != nil {
		return "", err
	}
	if schema, ok := schemas[schemaID]; ok {
		return schema, nil
	}
	return "", errors.Newf("unable to resolve schema id %d for db %d", schemaID, dbID)
}

// GetForDatabase looks up and returns all available
// schema ids to names for a given database.
func GetForDatabase(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID,
) (map[sqlbase.ID]string, error) {
	log.Eventf(ctx, "fetching all schema descriptor IDs for %d", dbID)

	nameKey := sqlbase.NewSchemaKey(dbID, "" /* name */).Key()
	kvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /* maxRows */)
	if err != nil {
		return nil, err
	}

	// Always add public schema ID.
	// TODO(solon): This can be removed in 20.2, when this is always written.
	// In 20.1, in a migrating state, it may be not included yet.
	ret := make(map[sqlbase.ID]string, len(kvs)+1)
	ret[sqlbase.ID(keys.PublicSchemaID)] = tree.PublicSchema

	for _, kv := range kvs {
		id := sqlbase.ID(kv.ValueInt())
		if _, ok := ret[id]; ok {
			continue
		}
		_, _, name, err := sqlbase.DecodeNameMetadataKey(kv.Key)
		if err != nil {
			return nil, err
		}
		ret[id] = name
	}
	return ret, nil
}
