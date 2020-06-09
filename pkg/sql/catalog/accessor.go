// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Accessor provides access to sql object descriptors.
type Accessor interface {

	// GetDatabaseDesc looks up a database by name and returns its
	// descriptor. If the database is not found and required is true,
	// an error is returned; otherwise a nil reference is returned.
	GetDatabaseDesc(ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, dbName string, flags tree.DatabaseLookupFlags) (sqlbase.DatabaseDescriptorInterface, error)

	// IsValidSchema returns true and the SchemaID if the given schema name is valid for the given database.
	IsValidSchema(ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, dbID sqlbase.ID, scName string) (bool, sqlbase.ID, error)

	// GetObjectNames returns the list of all objects in the given
	// database and schema.
	// TODO(solon): when separate schemas are supported, this
	// API should be extended to use schema descriptors.
	//
	// TODO(ajwerner,rohany): This API is utilized to support glob patterns that
	// are fundamentally sometimes ambiguous (see GRANT and the ambiguity between
	// tables and types). Furthermore, the fact that this buffers everything
	// in ram in unfortunate.
	GetObjectNames(ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, db sqlbase.DatabaseDescriptorInterface, scName string, flags tree.DatabaseListFlags) (tree.TableNames, error)

	// GetObjectDesc looks up an object by name and returns both its
	// descriptor and that of its parent database. If the object is not
	// found and flags.required is true, an error is returned, otherwise
	// a nil reference is returned.
	GetObjectDesc(ctx context.Context, txn *kv.Txn, settings *cluster.Settings, codec keys.SQLCodec, db, schema, object string, flags tree.ObjectLookupFlags) (Descriptor, error)
}
