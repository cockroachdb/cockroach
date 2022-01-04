// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import "github.com/cockroachdb/cockroach/pkg/keys"

// CommentType the type of the schema object on which a comment has been
// applied.
type CommentType int

//go:generate stringer --type CommentType --output=comment_type_string.go

const (
	// DatabaseCommentType comment on a database.
	DatabaseCommentType CommentType = keys.DatabaseCommentType
	// TableCommentType comment on a table/view/sequence.
	TableCommentType CommentType = keys.TableCommentType
	// ColumnCommentType comment on a column.
	ColumnCommentType CommentType = keys.ColumnCommentType
	// IndexCommentType comment on an index.
	IndexCommentType CommentType = keys.IndexCommentType
	// SchemaCommentType comment on a schema.
	SchemaCommentType CommentType = keys.SchemaCommentType
	// ConstraintCommentType comment on a constraint.
	ConstraintCommentType CommentType = keys.ConstraintCommentType
)
