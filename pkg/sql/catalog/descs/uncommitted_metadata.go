// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

type uncommittedComments struct {
	uncommitted map[keys.CommentKey]string
	cachedKeys  map[keys.CommentKey]struct{}
}

func newUncommittedComments() *uncommittedComments {
	return &uncommittedComments{
		uncommitted: make(map[keys.CommentKey]string),
		cachedKeys:  make(map[keys.CommentKey]struct{}),
	}
}

func (uc *uncommittedComments) reset() {
	uc.uncommitted = make(map[keys.CommentKey]string)
	uc.cachedKeys = make(map[keys.CommentKey]struct{})
}

func (uc *uncommittedComments) getUncommitted(
	objID descpb.ID, subID uint32, cmtType keys.CommentType,
) (cmt string, hasCmt bool, cached bool) {
	key := keys.CommentKey{
		ObjectID:    uint32(objID),
		SubID:       subID,
		CommentType: cmtType,
	}
	if _, ok := uc.cachedKeys[key]; !ok {
		return "", false, false
	}

	cmt, hasCmt = uc.uncommitted[key]
	return cmt, hasCmt, true
}

// markNoComment lets the cache know that the comment for this key is dropped.
func (uc *uncommittedComments) markNoComment(
	objID descpb.ID, subID uint32, cmtType keys.CommentType,
) {
	key := keys.CommentKey{
		ObjectID:    uint32(objID),
		SubID:       subID,
		CommentType: cmtType,
	}
	delete(uc.uncommitted, key)
	uc.cachedKeys[key] = struct{}{}
}

func (uc *uncommittedComments) markTableDeleted(tblID descpb.ID) {
	var keysToDel []keys.CommentKey
	for k := range uc.uncommitted {
		if k.ObjectID == uint32(tblID) {
			keysToDel = append(keysToDel, k)
		}
	}
	for _, k := range keysToDel {
		delete(uc.uncommitted, k)
	}
}

func (uc *uncommittedComments) upsert(
	objID descpb.ID, subID uint32, cmtType keys.CommentType, cmt string,
) {
	key := keys.CommentKey{
		ObjectID:    uint32(objID),
		SubID:       subID,
		CommentType: cmtType,
	}
	uc.cachedKeys[key] = struct{}{}
	uc.uncommitted[key] = cmt
}
