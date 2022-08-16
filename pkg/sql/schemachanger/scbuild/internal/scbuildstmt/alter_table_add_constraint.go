// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableAddConstraint(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAddConstraint,
) {
	d, ok := t.ConstraintDef.(*tree.UniqueConstraintTableDef)
	if !ok || !d.PrimaryKey || t.ValidationBehavior != tree.ValidationDefault {
		panic(scerrors.NotImplementedError(t))
	}
	// Ensure that there is a default rowid column.
	oldPrimaryIndex := mustRetrievePrimaryIndexElement(b, tbl.TableID)
	if getPrimaryIndexDefaultRowIDColumn(
		b, tbl.TableID, oldPrimaryIndex.IndexID,
	) == nil {
		panic(scerrors.NotImplementedError(t))
	}
	alterPrimaryKey(b, tn, tbl, alterPrimaryKeySpec{
		n:             t,
		Columns:       d.Columns,
		Sharded:       d.Sharded,
		Name:          d.Name,
		StorageParams: d.StorageParams,
	})
}
