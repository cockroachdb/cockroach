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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// GetMutableDatabaseByName returns a mutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetMutableDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (*dbdesc.Mutable, error) {
	return tc.ByName(txn).WithFlags(flags).Mutable().Database(ctx, name)
}

// GetImmutableDatabaseByName returns an immutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableDatabaseByName(
	ctx context.Context, txn *kv.Txn, name string, flags tree.DatabaseLookupFlags,
) (catalog.DatabaseDescriptor, error) {
	return tc.ByName(txn).WithFlags(flags).Immutable().Database(ctx, name)
}

// GetImmutableDatabaseByID returns an immutable database descriptor with
// properties according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableDatabaseByID(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, flags tree.DatabaseLookupFlags,
) (bool, catalog.DatabaseDescriptor, error) {
	db, err := tc.ByID(txn).WithFlags(flags).Immutable().Database(ctx, dbID)
	return db != nil, db, err
}
