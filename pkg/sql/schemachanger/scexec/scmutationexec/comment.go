// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
)

func (i *immediateVisitor) UpsertDatabaseComment(
	_ context.Context, op scop.UpsertDatabaseComment,
) error {
	i.AddComment(op.DatabaseID, 0, catalogkeys.DatabaseCommentType, op.Comment)
	return nil
}

func (i *immediateVisitor) UpsertSchemaComment(
	_ context.Context, op scop.UpsertSchemaComment,
) error {
	i.AddComment(op.SchemaID, 0, catalogkeys.SchemaCommentType, op.Comment)
	return nil
}

func (i *immediateVisitor) UpsertTableComment(_ context.Context, op scop.UpsertTableComment) error {
	i.AddComment(op.TableID, 0, catalogkeys.TableCommentType, op.Comment)
	return nil
}

func (i *immediateVisitor) UpsertTypeComment(_ context.Context, op scop.UpsertTypeComment) error {
	i.AddComment(op.TypeID, 0, catalogkeys.TypeCommentType, op.Comment)
	return nil
}

func (i *immediateVisitor) UpsertColumnComment(
	_ context.Context, op scop.UpsertColumnComment,
) error {
	subID := int(op.ColumnID)
	if op.PGAttributeNum != 0 {
		// PGAttributeNum is only set if it differs from the ColumnID.
		subID = int(op.PGAttributeNum)
	}
	i.AddComment(op.TableID, subID, catalogkeys.ColumnCommentType, op.Comment)
	return nil
}

func (i *immediateVisitor) UpsertIndexComment(_ context.Context, op scop.UpsertIndexComment) error {
	i.AddComment(op.TableID, int(op.IndexID), catalogkeys.IndexCommentType, op.Comment)
	return nil
}

func (i *immediateVisitor) UpsertConstraintComment(
	_ context.Context, op scop.UpsertConstraintComment,
) error {
	i.AddComment(op.TableID, int(op.ConstraintID), catalogkeys.ConstraintCommentType, op.Comment)
	return nil
}
