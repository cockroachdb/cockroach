// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

func (i *immediateVisitor) UpsertColumnComment(
	_ context.Context, op scop.UpsertColumnComment,
) error {
	i.AddComment(op.TableID, int(op.PGAttributeNum), catalogkeys.ColumnCommentType, op.Comment)
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
