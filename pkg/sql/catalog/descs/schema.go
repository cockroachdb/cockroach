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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// MustGetMutableSchemaByName resolves the schema and returns a mutable descriptor
// usable by the transaction.
//
// TODO(ajwerner): Change this to take database by name to avoid any weirdness
// due to the descriptor being passed in having been cached and causing
// problems.
func (tc *Collection) MustGetMutableSchemaByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	schemaName string,
	options ...LookupOption,
) (*schemadesc.Mutable, error) {
	return tc.MayGetMutableSchemaByName(ctx, txn, db, schemaName, prependWithRequired(options)...)
}

// MayGetMutableSchemaByName resolves the schema and returns a mutable descriptor
// usable by the transaction.
//
// TODO(ajwerner): Change this to take database by name to avoid any weirdness
// due to the descriptor being passed in having been cached and causing
// problems.
func (tc *Collection) MayGetMutableSchemaByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	schemaName string,
	options ...LookupOption,
) (*schemadesc.Mutable, error) {
	flags := catalog.SchemaLookupFlags{RequireMutable: true}
	for _, opt := range options {
		opt.apply(&flags)
	}
	sc, err := tc.getSchemaByName(ctx, txn, db, schemaName, flags)
	if err != nil || sc == nil {
		return nil, err
	}
	return sc.(*schemadesc.Mutable), nil
}

// MustGetImmutableSchemaByName returns a catalog.SchemaDescriptor object if the
// target schema exists under the target database.
//
// TODO(ajwerner): Change this to take database by name to avoid any weirdness
// due to the descriptor being passed in having been cached and causing
// problems.
func (tc *Collection) MustGetImmutableSchemaByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	scName string,
	options ...LookupOption,
) (catalog.SchemaDescriptor, error) {
	return tc.MayGetImmutableSchemaByName(ctx, txn, db, scName, prependWithRequired(options)...)
}

// MayGetImmutableSchemaByName returns a catalog.SchemaDescriptor object if the
// target schema exists under the target database.
//
// TODO(ajwerner): Change this to take database by name to avoid any weirdness
// due to the descriptor being passed in having been cached and causing
// problems.
func (tc *Collection) MayGetImmutableSchemaByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	scName string,
	options ...LookupOption,
) (catalog.SchemaDescriptor, error) {
	flags := catalog.SchemaLookupFlags{}
	for _, opt := range options {
		opt.apply(&flags)
	}
	return tc.getSchemaByName(ctx, txn, db, scName, flags)
}

// getSchemaByName resolves the schema and, if applicable, returns a descriptor
// usable by the transaction.
func (tc *Collection) getSchemaByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	schemaName string,
	flags catalog.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	desc, err := tc.getDescriptorByName(ctx, txn, db, nil /* sc */, schemaName, flags, catalog.Schema)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedSchemaError(schemaName)
		}
		return nil, nil
	}
	schema, ok := desc.(catalog.SchemaDescriptor)
	if !ok {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedSchemaError(schemaName)
		}
		return nil, nil
	}
	return schema, nil
}

// MustGetImmutableSchemaByID looks up an immutable schema descriptor by its
// ID, returning an error if none is found.
func (tc *Collection) MustGetImmutableSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, options ...LookupOption,
) (catalog.SchemaDescriptor, error) {
	return tc.MayGetImmutableSchemaByID(ctx, txn, schemaID, prependWithRequired(options)...)
}

// MayGetImmutableSchemaByID looks up an immutable schema descriptor by its
// ID, returning nil if none is found.
func (tc *Collection) MayGetImmutableSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, options ...LookupOption,
) (catalog.SchemaDescriptor, error) {
	flags := catalog.SchemaLookupFlags{}
	for _, opt := range options {
		opt.apply(&flags)
	}
	return tc.getSchemaByID(ctx, txn, schemaID, flags)
}

// MustGetMutableSchemaByID returns a mutable schema descriptor with the given
// schema ID. An error is always returned if the descriptor is not physical.
func (tc *Collection) MustGetMutableSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, options ...LookupOption,
) (*schemadesc.Mutable, error) {
	return tc.MayGetMutableSchemaByID(ctx, txn, schemaID, prependWithRequired(options)...)
}

// MayGetMutableSchemaByID returns a mutable schema descriptor with the given
// schema ID. An error is always returned if the descriptor is not physical.
func (tc *Collection) MayGetMutableSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, options ...LookupOption,
) (*schemadesc.Mutable, error) {
	flags := catalog.SchemaLookupFlags{RequireMutable: true}
	for _, opt := range options {
		opt.apply(&flags)
	}
	desc, err := tc.getSchemaByID(ctx, txn, schemaID, flags)
	if err != nil {
		return nil, err
	}
	return desc.(*schemadesc.Mutable), nil
}

func (tc *Collection) getSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, flags catalog.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	descs, err := tc.getDescriptorsByID(ctx, txn, flags, schemaID)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			if flags.Required {
				return nil, sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", schemaID))
			}
			return nil, nil
		}
		return nil, err
	}
	schemaDesc, ok := descs[0].(catalog.SchemaDescriptor)
	if !ok {
		return nil, sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", schemaID))
	}
	return schemaDesc, nil
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
