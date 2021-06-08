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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// GetMutableTableByName returns a mutable table descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetMutableTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ *tabledesc.Mutable, _ error) {
	flags.RequireMutable = true
	found, desc, err := tc.getTableByName(ctx, txn, name, flags)
	if err != nil || !found {
		return false, nil, err
	}
	return true, desc.(*tabledesc.Mutable), nil
}

// GetImmutableTableByName returns a mutable table descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ catalog.TableDescriptor, _ error) {
	flags.RequireMutable = false
	found, desc, err := tc.getTableByName(ctx, txn, name, flags)
	if err != nil || !found {
		return false, nil, err
	}
	return true, desc, nil
}

// getTableByName returns a table descriptor with properties according to the
// provided lookup flags.
func (tc *Collection) getTableByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, flags tree.ObjectLookupFlags,
) (found bool, _ catalog.TableDescriptor, err error) {
	found, desc, err := tc.getObjectByName(
		ctx, txn, name.Catalog(), name.Schema(), name.Object(), flags)
	if err != nil {
		return false, nil, err
	} else if !found {
		if flags.Required {
			return false, nil, sqlerrors.NewUndefinedRelationError(name)
		}
		return false, nil, nil
	}
	table, ok := desc.(catalog.TableDescriptor)
	if !ok {
		if flags.Required {
			return false, nil, sqlerrors.NewUndefinedRelationError(name)
		}
		return false, nil, nil
	}
	if table.Adding() && table.IsUncommittedVersion() &&
		(flags.RequireMutable || flags.CommonLookupFlags.AvoidCached) {
		// Special case: We always return tables in the adding state if they were
		// created in the same transaction and a descriptor (effectively) read in
		// the same transaction is requested. What this basically amounts to is
		// resolving adding descriptors only for DDLs (etc.).
		// TODO (lucy): I'm not sure where this logic should live. We could add an
		// IncludeAdding flag and pull the special case handling up into the
		// callers. Figure that out after we clean up the name resolution layers
		// and it becomes more Clear what the callers should be.
		return true, table, nil
	}
	if dropped, err := filterDescriptorState(
		table, flags.Required, flags.CommonLookupFlags,
	); err != nil || dropped {
		return false, nil, err
	}
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, table)
	if err != nil {
		return false, nil, err
	}

	return true, hydrated, nil
}

// GetUncommittedTableByID returns an uncommitted table by its ID.
func (tc *Collection) GetUncommittedTableByID(id descpb.ID) *tabledesc.Mutable {
	if ud := tc.kv.getUncommittedByID(id); ud != nil {
		if table, ok := ud.mutable.(*tabledesc.Mutable); ok {
			return table
		}
	}
	return nil
}

// GetMutableTableByID returns a mutable table descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetMutableTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, flags tree.ObjectLookupFlags,
) (*tabledesc.Mutable, error) {
	flags.RequireMutable = true
	desc, err := tc.getTableByID(ctx, txn, tableID, flags)
	if err != nil {
		return nil, err
	}
	return desc.(*tabledesc.Mutable), nil
}

// GetMutableTableVersionByID is a variant of sqlbase.getTableDescFromID which returns a mutable
// table descriptor of the table modified in the same transaction.
// Deprecated in favor of GetMutableTableByID.
// TODO (lucy): Usages should be replaced with GetMutableTableByID, but this
// needs a careful look at what flags should be passed in at each call site.
func (tc *Collection) GetMutableTableVersionByID(
	ctx context.Context, tableID descpb.ID, txn *kv.Txn,
) (*tabledesc.Mutable, error) {
	return tc.GetMutableTableByID(ctx, txn, tableID, tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			IncludeOffline: true,
			IncludeDropped: true,
		},
	})
}

// GetImmutableTableByID returns an immutable table descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
// Required is ignored, and an error is always returned if no descriptor with
// the ID exists.
func (tc *Collection) GetImmutableTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, flags tree.ObjectLookupFlags,
) (catalog.TableDescriptor, error) {
	flags.RequireMutable = false
	desc, err := tc.getTableByID(ctx, txn, tableID, flags)
	if err != nil {
		return nil, err
	}
	return desc, nil
}

func (tc *Collection) getTableByID(
	ctx context.Context, txn *kv.Txn, tableID descpb.ID, flags tree.ObjectLookupFlags,
) (catalog.TableDescriptor, error) {
	desc, err := tc.getDescriptorByID(ctx, txn, tableID, flags.CommonLookupFlags)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, sqlerrors.NewUndefinedRelationError(
				&tree.TableRef{TableID: int64(tableID)})
		}
		return nil, err
	}
	table, ok := desc.(catalog.TableDescriptor)
	if !ok {
		return nil, sqlerrors.NewUndefinedRelationError(
			&tree.TableRef{TableID: int64(tableID)})
	}
	hydrated, err := tc.hydrateTypesInTableDesc(ctx, txn, table)
	if err != nil {
		return nil, err
	}
	return hydrated, nil
}
