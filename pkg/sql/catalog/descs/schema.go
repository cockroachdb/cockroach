// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// GetMutableSchemaByName resolves the schema and returns a mutable descriptor
// usable by the transaction. RequireMutable is ignored.
//
// TODO(ajwerner): Change this to take database by name to avoid any weirdness
// due to the descriptor being passed in having been cached and causing
// problems.
func (tc *Collection) GetMutableSchemaByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	schemaName string,
	flags tree.SchemaLookupFlags,
) (*schemadesc.Mutable, error) {
	return tc.ByName(txn).WithFlags(flags).Mutable().Schema(ctx, db, schemaName)
}

// GetImmutableSchemaByName returns a catalog.SchemaDescriptor object if the
// target schema exists under the target database. RequireMutable is ignored.
//
// TODO(ajwerner): Change this to take database by name to avoid any weirdness
// due to the descriptor being passed in having been cached and causing
// problems.
func (tc *Collection) GetImmutableSchemaByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	scName string,
	flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	return tc.ByName(txn).WithFlags(flags).Immutable().Schema(ctx, db, scName)
}

// GetImmutableSchemaByID returns a ResolvedSchema wrapping an immutable
// descriptor, if applicable. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetImmutableSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	return tc.ByID(txn).WithFlags(flags).Immutable().Schema(ctx, schemaID)
}

// GetMutableSchemaByID returns a mutable schema descriptor with the given
// schema ID. An error is always returned if the descriptor is not physical.
func (tc *Collection) GetMutableSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, flags tree.SchemaLookupFlags,
) (*schemadesc.Mutable, error) {
	return tc.ByID(txn).WithFlags(flags).Mutable().Schema(ctx, schemaID)
}

// InsertDescriptorlessPublicSchemaToBatch adds the creation of a new descriptorless public
// schema to the batch.
func (tc *Collection) InsertDescriptorlessPublicSchemaToBatch(
	ctx context.Context, kvTrace bool, db catalog.DatabaseDescriptor, b *kv.Batch,
) error {
	return tc.InsertTempSchemaToBatch(ctx, kvTrace, db, tree.PublicSchema, keys.PublicSchemaID, b)
}

// DeleteDescriptorlessPublicSchemaToBatch adds the deletion of a new descriptorless public
// schema to the batch.
func (tc *Collection) DeleteDescriptorlessPublicSchemaToBatch(
	ctx context.Context, kvTrace bool, db catalog.DatabaseDescriptor, b *kv.Batch,
) error {
	return tc.DeleteTempSchemaToBatch(ctx, kvTrace, db, tree.PublicSchema, b)
}

// InsertTempSchemaToBatch adds the creation of a new temporary schema to
// the batch.
func (tc *Collection) InsertTempSchemaToBatch(
	ctx context.Context,
	kvTrace bool,
	db catalog.DatabaseDescriptor,
	tempSchemaName string,
	tempSchemaID descpb.ID,
	b *kv.Batch,
) error {
	entry := tempSchemaNameEntry{
		NameInfo: descpb.NameInfo{
			ParentID: db.GetID(),
			Name:     tempSchemaName,
		},
		id: tempSchemaID,
	}
	return tc.InsertNamespaceEntryToBatch(ctx, kvTrace, &entry, b)
}

// DeleteTempSchemaToBatch adds the deletion of a temporary schema to the
// batch.
func (tc *Collection) DeleteTempSchemaToBatch(
	ctx context.Context,
	kvTrace bool,
	db catalog.DatabaseDescriptor,
	tempSchemaName string,
	b *kv.Batch,
) error {
	nameInfo := descpb.NameInfo{
		ParentID: db.GetID(),
		Name:     tempSchemaName,
	}
	return tc.DeleteNamespaceEntryToBatch(ctx, kvTrace, &nameInfo, b)
}

type tempSchemaNameEntry struct {
	descpb.NameInfo
	id descpb.ID
}

var _ catalog.NameEntry = &tempSchemaNameEntry{}

// GetID is part of the catalog.NameEntry interface.
func (t tempSchemaNameEntry) GetID() descpb.ID {
	return t.id
}
