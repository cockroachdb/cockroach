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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
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
	flags.RequireMutable = true
	sc, err := tc.getSchemaByName(ctx, txn, db, schemaName, flags)
	if err != nil || sc == nil {
		return nil, err
	}
	return sc.(*schemadesc.Mutable), nil
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
	flags.RequireMutable = false
	return tc.getSchemaByName(ctx, txn, db, scName, flags)
}

// getSchemaByName resolves the schema and, if applicable, returns a descriptor
// usable by the transaction.
func (tc *Collection) getSchemaByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	schemaName string,
	flags tree.SchemaLookupFlags,
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

// GetImmutableSchemaByID returns a ResolvedSchema wrapping an immutable
// descriptor, if applicable. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetImmutableSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	flags.RequireMutable = false
	return tc.getSchemaByID(ctx, txn, schemaID, flags)
}

// GetMutableSchemaByID returns a mutable schema descriptor with the given
// schema ID. An error is always returned if the descriptor is not physical.
func (tc *Collection) GetMutableSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, flags tree.SchemaLookupFlags,
) (*schemadesc.Mutable, error) {
	flags.RequireMutable = true
	desc, err := tc.getSchemaByID(ctx, txn, schemaID, flags)
	if err != nil {
		return nil, err
	}
	return desc.(*schemadesc.Mutable), nil
}

func (tc *Collection) getSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, flags tree.SchemaLookupFlags,
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
