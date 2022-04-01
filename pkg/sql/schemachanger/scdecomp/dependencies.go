// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdecomp

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

// CommentKey is used to uniquely identify an comment item from system.comments
// table.
type CommentKey struct {
	ObjectID    catid.DescID
	SubID       descpb.ID
	CommentType keys.CommentType
}

// DescriptorCommentCache represent an interface to fetch metadata like
// comment for descriptors.
type DescriptorCommentCache interface {
	// Get returns comment for a (objID, subID, commentType) tuple if exists. "ok"
	// returned indicates if the comment actually exists or not.
	Get(ctx context.Context, objID catid.DescID, subID descpb.ID, commentType keys.CommentType) (comment string, ok bool, err error)

	// LoadCommentsForObjects explicitly loads commentCache into the cache give a list
	// of object id of a descriptor type.
	LoadCommentsForObjects(ctx context.Context, descType catalog.DescriptorType, objIDs []descpb.ID) error
}
