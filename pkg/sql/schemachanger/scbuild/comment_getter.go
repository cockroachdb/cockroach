// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

type commentGetter struct {
	tc *descs.Collection
}

// GetDatabaseComment implements the scdecomp.CommentGetter interface.
func (mf *commentGetter) GetDatabaseComment(
	ctx context.Context, dbID catid.DescID,
) (comment string, ok bool, err error) {
	return mf.tc.GetComment(dbID, 0, keys.DatabaseCommentType)
}

// GetSchemaComment implements the scdecomp.CommentGetter interface.
func (mf *commentGetter) GetSchemaComment(
	ctx context.Context, schemaID catid.DescID,
) (comment string, ok bool, err error) {
	return mf.tc.GetComment(schemaID, 0, keys.SchemaCommentType)
}

// GetTableComment implements the scdecomp.CommentGetter interface.
func (mf *commentGetter) GetTableComment(
	ctx context.Context, tableID catid.DescID,
) (comment string, ok bool, err error) {
	return mf.tc.GetComment(tableID, 0, keys.TableCommentType)
}

// GetColumnComment implements the scdecomp.CommentGetter interface.
func (mf *commentGetter) GetColumnComment(
	ctx context.Context, tableID catid.DescID, pgAttrNum catid.PGAttributeNum,
) (comment string, ok bool, err error) {
	return mf.tc.GetComment(tableID, uint32(pgAttrNum), keys.ColumnCommentType)
}

// GetIndexComment implements the scdecomp.CommentGetter interface.
func (mf *commentGetter) GetIndexComment(
	ctx context.Context, tableID catid.DescID, indexID catid.IndexID,
) (comment string, ok bool, err error) {
	return mf.tc.GetComment(tableID, uint32(indexID), keys.IndexCommentType)
}

// GetConstraintComment implements the scdecomp.CommentGetter interface.
func (mf *commentGetter) GetConstraintComment(
	ctx context.Context, tableID catid.DescID, constraintID catid.ConstraintID,
) (comment string, ok bool, err error) {
	return mf.tc.GetComment(tableID, uint32(constraintID), keys.ConstraintCommentType)
}

// NewCommentGetter returns a new scbuild.CommentCache.
func NewCommentGetter(tc *descs.Collection) scdecomp.CommentGetter {
	return &commentGetter{
		tc: tc,
	}
}
