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
)

type commentKey struct {
	objectID    int64
	subID       int64
	commentType keys.CommentType
}

func newCommentKey(objectID int64, subID int64, commentType keys.CommentType) commentKey {
	return commentKey{
		objectID:    objectID,
		subID:       subID,
		commentType: commentType,
	}
}

// CommentCache maintains a mapping from (objectID, subID, commentType) tuple to
// comment string.
type CommentCache struct {
	comments map[commentKey]string
}

// NewCommentCache returns a new empty CommentCache.
func NewCommentCache() *CommentCache {
	return &CommentCache{comments: make(map[commentKey]string)}
}

// Add adds a comment to the cache.
func (c *CommentCache) Add(
	objectID int64, subID int64, commentType keys.CommentType, comment string,
) {
	key := newCommentKey(objectID, subID, commentType)
	c.comments[key] = comment
}

// Contains checks if the cache contains a comment.
func (c *CommentCache) Contains(objectID int64, subID int64, commentType keys.CommentType) bool {
	key := newCommentKey(objectID, subID, commentType)
	_, ok := c.comments[key]
	return ok
}

// Get retrieves comment for a target if it exists, empty string is returned
// otherwise.
func (c *CommentCache) Get(objectID int64, subID int64, commentType keys.CommentType) string {
	key := newCommentKey(objectID, subID, commentType)
	return c.comments[key]
}

// CommentsForObject returns a subset of comments which all belong to an object.
func (c *CommentCache) CommentsForObject(objectID int64) *CommentCache {
	comments := NewCommentCache()
	for key, comment := range c.comments {
		if key.objectID == objectID {
			comments.Add(key.objectID, key.subID, key.commentType, comment)
		}
	}
	return comments
}

// DescriptorMetadataFetcher represent an interface to fetch metadata like
// comment for descriptors.
type DescriptorMetadataFetcher interface {
	// GetAllCommentsOnObject fetches all comments related to an object such as a
	// database, a schema or a table.
	GetAllCommentsOnObject(ctx context.Context, objectID int64) (*CommentCache, error)
}
