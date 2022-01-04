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
	DatabaseCommentType   CommentType = keys.DatabaseCommentType
	TableCommentType      CommentType = keys.TableCommentType
	ColumnCommentType     CommentType = keys.ColumnCommentType
	IndexCommentType      CommentType = keys.IndexCommentType
	SchemaCommentType     CommentType = keys.SchemaCommentType
	ConstraintCommentType CommentType = keys.ConstraintCommentType
)
