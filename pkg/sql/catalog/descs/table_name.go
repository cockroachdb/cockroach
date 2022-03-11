// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// GetTableNameByID fetches the full tree table name by the given ID.
func GetTableNameByID(
	ctx context.Context, txn *kv.Txn, tc *Collection, tableID descpb.ID,
) (*tree.TableName, error) {
	tbl, err := tc.GetImmutableTableByID(ctx, txn, tableID, tree.ObjectLookupFlagsWithRequired())
	if err != nil {
		return nil, err
	}
	return GetTableNameByDesc(ctx, txn, tc, tbl)
}

// GetTableNameByDesc fetches the full tree table name by the given table descriptor.
func GetTableNameByDesc(
	ctx context.Context, txn *kv.Txn, tc *Collection, tbl catalog.TableDescriptor,
) (*tree.TableName, error) {
	sc, err := tc.GetImmutableSchemaByID(ctx, txn, tbl.GetParentSchemaID(), tree.SchemaLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}
	found, db, err := tc.GetImmutableDatabaseByID(ctx, txn, tbl.GetParentID(), tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.AssertionFailedf("expected database %d to exist", tbl.GetParentID())
	}
	return tree.NewTableNameWithSchema(tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(tbl.GetName())), nil
}
