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

// GetMutableSchemaByName resolves the schema and, if applicable, returns a
// mutable descriptor usable by the transaction. RequireMutable is ignored.
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
) (catalog.SchemaDescriptor, error) {
	flags.RequireMutable = true
	return tc.getSchemaByName(ctx, txn, db, schemaName, flags)
}

// GetSchemaByName returns true and a ResolvedSchema object if the target schema
// exists under the target database.
//
// TODO(ajwerner): Change this to take database by name to avoid any weirdness
// due to the descriptor being passed in having been cached and causing
// problems.
func (tc *Collection) GetSchemaByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	scName string,
	flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
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
	const alwaysLookupLeasedPublicSchema = false
	return tc.getSchemaByNameMaybeLookingUpPublicSchema(
		ctx, txn, db, schemaName, flags, alwaysLookupLeasedPublicSchema,
	)
}

// Like getSchemaByName but with the optional flag to avoid trusting a
// cache miss in the database descriptor for the ID of the public schema.
//
// TODO(ajwerner): Remove this split in 22.2.
func (tc *Collection) getSchemaByNameMaybeLookingUpPublicSchema(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	schemaName string,
	flags tree.SchemaLookupFlags,
	alwaysLookupLeasedPublicSchema bool,
) (catalog.SchemaDescriptor, error) {
	found, desc, err := tc.getByName(
		ctx, txn, db, nil, schemaName, flags.AvoidLeased, flags.RequireMutable,
		flags.AvoidSynthetic, alwaysLookupLeasedPublicSchema,
	)
	if err != nil {
		return nil, err
	} else if !found {
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
	if dropped, err := filterDescriptorState(schema, flags.Required, flags); dropped || err != nil {
		return nil, err
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
// schema ID.
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

// GetImmutableSchemaByName returns a ResolvedSchema wrapping an immutable
// descriptor, if applicable. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetImmutableSchemaByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	schemaName string,
	flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	flags.RequireMutable = false
	return tc.getSchemaByName(ctx, txn, db, schemaName, flags)
}

func (tc *Collection) getSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	// TODO(richardjcai): Remove this in 22.2, new schemas created in 22.1
	// are regular UDS and do not use keys.PublicSchemaID.
	// We can remove this after 22.1 when we no longer have to consider
	// mixed version clusters between 21.2 and 22.1.
	if schemaID == keys.PublicSchemaID {
		return schemadesc.GetPublicSchema(), nil
	}
	if sc, err := tc.virtual.getSchemaByID(
		ctx, schemaID, flags.RequireMutable,
	); err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			if flags.Required {
				return nil, sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", schemaID))
			}
			return nil, nil
		}
		return nil, err
	} else if sc != nil {
		return sc, err
	}

	// If this collection is attached to a session and the session has created
	// a temporary schema, then check if the schema ID matches.
	if sc := tc.temporary.getSchemaByID(ctx, schemaID); sc != nil {
		return sc, nil
	}

	// Otherwise, fall back to looking up the descriptor with the desired ID.
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

	hydrated, err := tc.hydrateTypesInDescWithOptions(ctx, txn, schemaDesc, flags.IncludeOffline, flags.AvoidLeased)
	if err != nil {
		return nil, err
	}

	return hydrated.(catalog.SchemaDescriptor), nil
}
