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

func (m *visitor) AddDatabaseComment(_ context.Context, op scop.AddDatabaseComment) error {
	m.s.AddComment(op.DatabaseID, 0, keys.DatabaseCommentType, op.Comment)
	return nil
}

func (m *visitor) AddSchemaComment(_ context.Context, op scop.AddSchemaComment) error {
	m.s.AddComment(op.SchemaID, 0, keys.SchemaCommentType, op.Comment)
	return nil
}

func (m *visitor) AddTableComment(_ context.Context, op scop.AddTableComment) error {
	m.s.AddComment(op.TableID, 0, keys.TableCommentType, op.Comment)
	return nil
}

func (m *visitor) AddColumnComment(_ context.Context, op scop.AddColumnComment) error {
	m.s.AddComment(op.TableID, int(op.ColumnID), keys.ColumnCommentType, op.Comment)
	return nil
}

func (m *visitor) AddIndexComment(_ context.Context, op scop.AddIndexComment) error {
	m.s.AddComment(op.TableID, int(op.IndexID), keys.IndexCommentType, op.Comment)
	return nil
}

func (m *visitor) AddConstraintComment(_ context.Context, op scop.AddConstraintComment) error {
	m.s.AddComment(op.TableID, int(op.ConstraintID), keys.ConstraintCommentType, op.Comment)
	return nil
}
