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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// GetMutableDatabaseByName returns a mutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetMutableDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (*dbdesc.Mutable, error) {
	flags.RequireMutable = true
	desc, err := tc.getDatabaseByName(ctx, txn, name, flags)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*dbdesc.Mutable), nil
}

// GetImmutableDatabaseByName returns an immutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (catalog.DatabaseDescriptor, error) {
	flags.RequireMutable = false
	return tc.getDatabaseByName(ctx, txn, name, flags)
}

// GetDatabaseDesc implements the Accessor interface.
//
// TODO(ajwerner): This exists to support the SchemaResolver interface and
// should be removed or adjusted.
func (tc *Collection) GetDatabaseDesc(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (desc catalog.DatabaseDescriptor, err error) {
	return tc.getDatabaseByName(ctx, txn, name, flags)
}

// getDatabaseByName returns a database descriptor with properties according to
// the provided lookup flags.
func (tc *Collection) getDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (catalog.DatabaseDescriptor, error) {
	if name == systemschema.SystemDatabaseName {
		// The system database descriptor should never actually be mutated, which is
		// why we return the same hard-coded descriptor every time. It's assumed
		// that callers of this method will check the privileges on the descriptor
		// (like any other database) and return an error.
		if flags.RequireMutable {
			proto := systemschema.MakeSystemDatabaseDesc().DatabaseDesc()
			return dbdesc.NewBuilder(proto).BuildExistingMutableDatabase(), nil
		}
		return systemschema.MakeSystemDatabaseDesc(), nil
	}

	getDatabaseByName := func() (found bool, _ catalog.Descriptor, err error) {
		if found, refuseFurtherLookup, desc, err := tc.getSyntheticOrUncommittedDescriptor(
			keys.RootNamespaceID, keys.RootNamespaceID, name, flags.RequireMutable,
		); err != nil || refuseFurtherLookup {
			return false, nil, err
		} else if found {
			log.VEventf(ctx, 2, "found uncommitted descriptor %d", desc.GetID())
			return true, desc, nil
		}

		if flags.AvoidCached || flags.RequireMutable || lease.TestingTableLeasesAreDisabled() {
			return tc.kv.getByName(
				ctx, txn, keys.RootNamespaceID, keys.RootNamespaceID, name, flags.RequireMutable,
			)
		}

		desc, shouldReadFromStore, err := tc.leased.getByName(
			ctx, txn, keys.RootNamespaceID, keys.RootNamespaceID, name)
		if err != nil {
			return false, nil, err
		}
		if shouldReadFromStore {
			return tc.kv.getByName(
				ctx, txn, keys.RootNamespaceID, keys.RootNamespaceID, name, flags.RequireMutable,
			)
		}
		return true, desc, nil
	}

	found, desc, err := getDatabaseByName()
	if err != nil {
		return nil, err
	} else if !found {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedDatabaseError(name)
		}
		return nil, nil
	}
	db, ok := desc.(catalog.DatabaseDescriptor)
	if !ok {
		if flags.Required {
			return nil, sqlerrors.NewUndefinedDatabaseError(name)
		}
		return nil, nil
	}
	if dropped, err := filterDescriptorState(db, flags.Required, flags); err != nil || dropped {
		return nil, err
	}
	return db, nil
}

// GetMutableDatabaseByID returns a mutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetMutableDatabaseByID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, flags tree.DatabaseLookupFlags,
) (*dbdesc.Mutable, error) {
	flags.RequireMutable = true
	_, desc, err := tc.getDatabaseByID(ctx, txn, dbID, flags)
	if err != nil {
		return nil, err
	}
	return desc.(*dbdesc.Mutable), nil
}

// GetImmutableDatabaseByID returns an immutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableDatabaseByID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, flags tree.DatabaseLookupFlags,
) (bool, catalog.DatabaseDescriptor, error) {
	flags.RequireMutable = false
	return tc.getDatabaseByID(ctx, txn, dbID, flags)
}

func (tc *Collection) getDatabaseByID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, flags tree.DatabaseLookupFlags,
) (bool, catalog.DatabaseDescriptor, error) {
	desc, err := tc.getDescriptorByID(ctx, txn, dbID, flags)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			if flags.Required {
				return false, nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", dbID))
			}
			return false, nil, nil
		}
		return false, nil, err
	}
	db, ok := desc.(catalog.DatabaseDescriptor)
	if !ok {
		return false, nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", dbID))
	}
	return true, db, nil
}
