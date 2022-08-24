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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
)

func (m *visitor) UpsertDatabaseComment(_ context.Context, op scop.UpsertDatabaseComment) error {
	m.s.AddComment(op.DatabaseID, 0, keys.DatabaseCommentType, op.Comment)
	return nil
}

func (m *visitor) UpsertSchemaComment(_ context.Context, op scop.UpsertSchemaComment) error {
	m.s.AddComment(op.SchemaID, 0, keys.SchemaCommentType, op.Comment)
	return nil
}

func (m *visitor) UpsertTableComment(_ context.Context, op scop.UpsertTableComment) error {
	m.s.AddComment(op.TableID, 0, keys.TableCommentType, op.Comment)
	return nil
}

func (m *visitor) UpsertColumnComment(_ context.Context, op scop.UpsertColumnComment) error {
	m.s.AddComment(op.TableID, int(op.PGAttributeNum), keys.ColumnCommentType, op.Comment)
	return nil
}

func (m *visitor) UpsertIndexComment(_ context.Context, op scop.UpsertIndexComment) error {
	m.s.AddComment(op.TableID, int(op.IndexID), keys.IndexCommentType, op.Comment)
	return nil
}

func (m *visitor) UpsertConstraintComment(
	_ context.Context, op scop.UpsertConstraintComment,
) error {
	m.s.AddComment(op.TableID, int(op.ConstraintID), keys.ConstraintCommentType, op.Comment)
	return nil
}
