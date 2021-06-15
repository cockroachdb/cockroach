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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// GetMutableSchemaByName resolves the schema and, if applicable, returns a
// mutable descriptor usable by the transaction. RequireMutable is ignored.
func (tc *Collection) GetMutableSchemaByName(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string, flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	flags.RequireMutable = true
	return tc.getSchemaByName(ctx, txn, dbID, schemaName, flags)
}

// GetSchemaByName returns true and a ResolvedSchema object if the target schema
// exists under the target database.
func (tc *Collection) GetSchemaByName(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, scName string, flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	return tc.getSchemaByName(ctx, txn, dbID, scName, flags)
}

// getSchemaByName resolves the schema and, if applicable, returns a descriptor
// usable by the transaction.
func (tc *Collection) getSchemaByName(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string, flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	// Fast path public schema, as it is always found.
	if schemaName == tree.PublicSchema {
		return schemadesc.GetPublicSchema(), nil
	}

	if sc := tc.virtual.getSchemaByName(schemaName); sc != nil {
		return sc, nil
	}
	if strings.HasPrefix(schemaName, catconstants.PgTempSchemaName) {
		return tc.temporary.getSchemaByName(ctx, txn, dbID, schemaName, flags)
	}

	// Otherwise, the schema is user-defined. Get the descriptor.
	desc, err := tc.getUserDefinedSchemaByName(ctx, txn, dbID, schemaName, flags)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc, nil
}

// TODO (lucy): Should this just take a database name? We're separately
// resolving the database name in lots of places where we (indirectly) call
// this.
func (tc *Collection) getUserDefinedSchemaByName(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, schemaName string, flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	getSchemaByName := func() (found bool, _ catalog.Descriptor, err error) {
		if descFound, refuseFurtherLookup, desc, err := tc.getSyntheticOrUncommittedDescriptor(
			dbID, keys.RootNamespaceID, schemaName, flags.RequireMutable,
		); err != nil || refuseFurtherLookup {
			return false, nil, err
		} else if descFound {
			log.VEventf(ctx, 2, "found uncommitted descriptor %d", desc.GetID())
			return true, desc, nil
		}

		if flags.AvoidCached || flags.RequireMutable || lease.TestingTableLeasesAreDisabled() {
			return tc.kv.getByName(
				ctx, txn, dbID, keys.RootNamespaceID, schemaName, flags.RequireMutable,
			)
		}

		// Look up whether the schema is on the database descriptor and return early
		// if it's not.
		_, dbDesc, err := tc.GetImmutableDatabaseByID(
			ctx, txn, dbID, tree.DatabaseLookupFlags{Required: true},
		)
		if err != nil {
			return false, nil, err
		}
		schemaID := dbDesc.GetSchemaID(schemaName)
		if schemaID == descpb.InvalidID {
			return false, nil, nil
		}
		foundSchemaName := dbDesc.GetNonDroppedSchemaName(schemaID)
		if foundSchemaName != schemaName {
			// If there's another schema name entry with the same ID as this one, then
			// the schema has been renamed, so don't return anything.
			if foundSchemaName != "" {
				return false, nil, nil
			}
			// Otherwise, the schema has been dropped. Return early, except in the
			// specific case where flags.Required and flags.IncludeDropped are both
			// true, which forces us to look up the dropped descriptor and return it.
			if !flags.Required {
				return false, nil, nil
			}
			if !flags.IncludeDropped {
				return false, nil, catalog.NewInactiveDescriptorError(catalog.ErrDescriptorDropped)
			}
		}

		// If we have a schema ID from the database, get the schema descriptor. Since
		// the schema and database descriptors are updated in the same transaction,
		// their leased "versions" (not the descriptor version, but the state in the
		// abstract sequence of states in adding, renaming, or dropping a schema) can
		// differ by at most 1 while waiting for old leases to drain. So false
		// negatives can occur from the database lookup, in some sense, if we have a
		// lease on the latest version of the schema and on the previous version of
		// the database which doesn't reflect the changes to the schema. But this
		// isn't a problem for correctness; it can only happen on other sessions
		// before the schema change has returned results.
		desc, err := tc.getDescriptorByID(ctx, txn, schemaID, flags)
		if err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) ||
				errors.Is(err, catalog.ErrDescriptorDropped) {
				return false, nil, nil
			}
			return false, nil, err
		}
		return true, desc, nil
	}

	found, desc, err := getSchemaByName()
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

func (tc *Collection) getSchemaByID(
	ctx context.Context, txn *kv.Txn, schemaID descpb.ID, flags tree.SchemaLookupFlags,
) (catalog.SchemaDescriptor, error) {
	if schemaID == keys.PublicSchemaID {
		return schemadesc.GetPublicSchema(), nil
	}

	// We have already considered if the schemaID is PublicSchemaID,
	// if the id appears in staticSchemaIDMap, it must map to a virtual schema.
	if sc, err := tc.virtual.getSchemaByID(
		ctx, schemaID, flags.RequireMutable,
	); sc != nil || err != nil {
		return sc, err
	}

	// If this collection is attached to a session and the session has created
	// a temporary schema, then check if the schema ID matches.
	if sc := tc.temporary.getSchemaByID(ctx, schemaID); sc != nil {
		return sc, nil
	}

	// Otherwise, fall back to looking up the descriptor with the desired ID.
	desc, err := tc.getDescriptorByID(ctx, txn, schemaID, flags)
	if err != nil {
		return nil, err
	}

	schemaDesc, ok := desc.(catalog.SchemaDescriptor)
	if !ok {
		return nil, pgerror.Newf(pgcode.WrongObjectType,
			"descriptor %d was not a schema", schemaID)
	}

	return schemaDesc, nil
}
