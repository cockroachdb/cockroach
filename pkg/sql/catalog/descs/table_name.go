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
)

// GetTableNameByID fetches the full tree table name by the given ID.
func GetTableNameByID(
	ctx context.Context, txn *kv.Txn, tc *Collection, tableID descpb.ID,
) (*tree.TableName, error) {
	tbl, err := tc.MustGetImmutableTableByID(ctx, txn, tableID)
	if err != nil {
		return nil, err
	}
	return GetTableNameByDesc(ctx, txn, tc, tbl)
}

// GetTableNameByDesc fetches the full tree table name by the given table descriptor.
func GetTableNameByDesc(
	ctx context.Context, txn *kv.Txn, tc *Collection, tbl catalog.TableDescriptor,
) (*tree.TableName, error) {
	sc, err := tc.MustGetImmutableSchemaByID(ctx, txn, tbl.GetParentSchemaID())
	if err != nil {
		return nil, err
	}
	db, err := tc.MustGetImmutableDatabaseByID(ctx, txn, tbl.GetParentID())
	if err != nil {
		return nil, err
	}
	return tree.NewTableNameWithSchema(tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(tbl.GetName())), nil
}
