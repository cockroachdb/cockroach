// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descmetadata

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

type metadataCache struct {
	txn           *kv.Txn
	ie            sqlutil.InternalExecutor
	comments      map[scdecomp.CommentKey]string
	objIDsChecked map[catid.DescID]struct{}
}

func (mf metadataCache) Get(
	ctx context.Context, objID catid.DescID, subID descpb.ID, commentType keys.CommentType,
) (comment string, ok bool, err error) {
	if _, ok := mf.objIDsChecked[objID]; !ok {
		var descType catalog.DescriptorType
		switch commentType {
		case keys.DatabaseCommentType:
			descType = catalog.Database
		case keys.SchemaCommentType:
			descType = catalog.Schema
		case keys.TableCommentType, keys.ColumnCommentType, keys.IndexCommentType, keys.ConstraintCommentType:
			descType = catalog.Table
		}
		if err := mf.LoadCommentsForObjects(ctx, descType, []catid.DescID{objID}); err != nil {
			return "", false, err
		}
	}

	key := scdecomp.CommentKey{
		ObjectID:    objID,
		SubID:       subID,
		CommentType: commentType,
	}

	comment, ok = mf.comments[key]
	return comment, ok, nil
}

// LoadCommentsForObjects implements the scdecomp.DescriptorCommentCache interface.
func (mf metadataCache) LoadCommentsForObjects(
	ctx context.Context, descType catalog.DescriptorType, objIDs []descpb.ID,
) error {
	var uncheckedObjIDs []catid.DescID
	for _, id := range objIDs {
		if _, ok := mf.objIDsChecked[id]; !ok {
			uncheckedObjIDs = append(uncheckedObjIDs, id)
		}
	}
	if len(uncheckedObjIDs) == 0 {
		return nil
	}

	var commentTypes []keys.CommentType
	switch descType {
	case catalog.Database:
		commentTypes = []keys.CommentType{keys.DatabaseCommentType}
	case catalog.Schema:
		commentTypes = []keys.CommentType{keys.SchemaCommentType}
	case catalog.Table:
		commentTypes = []keys.CommentType{
			keys.TableCommentType, keys.ColumnCommentType, keys.IndexCommentType, keys.ConstraintCommentType,
		}
	default:
		return errors.Errorf("unexpected descriptor type %q for fetching comment", descType)
	}

	rows, err := mf.ie.QueryBufferedEx(
		ctx,
		"mf-get-table-comments",
		mf.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"SELECT type, obj_id, sub_id, comment FROM system.comments WHERE type IN ($1) AND object_id IN ($2)",
		commentTypes,
		objIDs,
	)

	if err != nil {
		return err
	}
	for _, objID := range objIDs {
		mf.objIDsChecked[objID] = struct{}{}
	}

	for _, row := range rows {
		key := scdecomp.CommentKey{
			ObjectID:    catid.DescID(tree.MustBeDInt(row[1])),
			SubID:       descpb.ID(tree.MustBeDInt(row[2])),
			CommentType: keys.CommentType(tree.MustBeDInt(row[0])),
		}
		mf.comments[key] = string(tree.MustBeDString(row[3]))
	}

	return nil
}

// NewCommentCache returns a new DescriptorCommentCache.
func NewCommentCache(txn *kv.Txn, ie sqlutil.InternalExecutor) scdecomp.DescriptorCommentCache {
	return metadataCache{
		txn:           txn,
		ie:            ie,
		comments:      make(map[scdecomp.CommentKey]string),
		objIDsChecked: make(map[catid.DescID]struct{}),
	}
}
