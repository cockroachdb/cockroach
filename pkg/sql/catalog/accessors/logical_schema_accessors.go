// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package accessors

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// This file provides reference implementations of the schema accessor
// interfaces defined in schema_accessors.go.
//

// NewLogicalAccessor constructs a new accessor given an underlying physical
// accessor and VirtualSchemas.
func NewLogicalAccessor(
	descsCol *descs.Collection, vs catalog.VirtualSchemas,
) *LogicalSchemaAccessor {
	return &LogicalSchemaAccessor{
		tc: descsCol,
		vs: vs,
	}
}

// LogicalSchemaAccessor extends an existing DatabaseLister with the
// ability to list tables in a virtual schema.
type LogicalSchemaAccessor struct {
	tc *descs.Collection
	vs catalog.VirtualSchemas
}

// GetDatabaseDesc implements the Accessor interface.
//
// Warning: This method uses no virtual schema information and only exists to
// accommodate the existing resolver.SchemaResolver interface (see #58228).
// Use GetMutableDatabaseByName() and GetImmutableDatabaseByName() on
// descs.Collection instead when possible.
func (l *LogicalSchemaAccessor) GetDatabaseDesc(
	ctx context.Context, txn *kv.Txn, dbName string, flags tree.DatabaseLookupFlags,
) (desc catalog.DatabaseDescriptor, err error) {
	return l.tc.GetDatabaseDesc(ctx, txn, dbName, flags)
}

// GetSchema implements the Accessor interface.
func (l *LogicalSchemaAccessor) GetSchemaByName(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, scName string, flags tree.SchemaLookupFlags,
) (bool, catalog.ResolvedSchema, error) {
	return l.tc.GetSchemaByName(ctx, txn, dbID, scName, flags)
}

// GetObjectNamesAndIDs implements the DatabaseLister interface.
func (l *LogicalSchemaAccessor) GetObjectNamesAndIDs(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	scName string,
	flags tree.DatabaseListFlags,
) (tree.TableNames, descpb.IDs, error) {
	return l.tc.GetObjectNamesAndIDs(ctx, txn, db, scName, flags)
}

// GetObjectDesc implements the ObjectAccessor interface.
func (l *LogicalSchemaAccessor) GetObjectDesc(
	ctx context.Context, txn *kv.Txn, db, schema, object string, flags tree.ObjectLookupFlags,
) (desc catalog.Descriptor, err error) {
	return l.tc.GetObjectDesc(ctx, txn, db, schema, object, flags)
}
