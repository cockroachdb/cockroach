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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// CommentKey is used to uniquely identify an comment item from system.comments
// table.
type CommentKey struct {
	ObjectID    catid.DescID
	SubID       uint32
	CommentType keys.CommentType
}

type metadataCache struct {
	txn           *kv.Txn
	ie            sqlutil.InternalExecutor
	comments      map[CommentKey]string
	objIDsChecked catalog.DescriptorIDSet
}

// Get implements the scdecomp.CommentGetter interface.
func (mf *metadataCache) get(
	ctx context.Context, objID catid.DescID, subID uint32, commentType keys.CommentType,
) (comment string, ok bool, err error) {
	if !mf.objIDsChecked.Contains(objID) {
		if err := mf.LoadCommentsForObjects(ctx, []catid.DescID{objID}); err != nil {
			return "", false, err
		}
	}

	key := CommentKey{
		ObjectID:    objID,
		SubID:       subID,
		CommentType: commentType,
	}

	comment, ok = mf.comments[key]
	return comment, ok, nil
}

// GetDatabaseComment implements the scdecomp.CommentGetter interface.
func (mf *metadataCache) GetDatabaseComment(
	ctx context.Context, dbID catid.DescID,
) (comment string, ok bool, err error) {
	return mf.get(ctx, dbID, 0, keys.DatabaseCommentType)
}

// GetSchemaComment implements the scdecomp.CommentGetter interface.
func (mf *metadataCache) GetSchemaComment(
	ctx context.Context, schemaID catid.DescID,
) (comment string, ok bool, err error) {
	return mf.get(ctx, schemaID, 0, keys.SchemaCommentType)
}

// GetTableComment implements the scdecomp.CommentGetter interface.
func (mf *metadataCache) GetTableComment(
	ctx context.Context, tableID catid.DescID,
) (comment string, ok bool, err error) {
	return mf.get(ctx, tableID, 0, keys.TableCommentType)
}

// GetColumnComment implements the scdecomp.CommentGetter interface.
func (mf *metadataCache) GetColumnComment(
	ctx context.Context, tableID catid.DescID, pgAttrNum catid.PGAttributeNum,
) (comment string, ok bool, err error) {
	return mf.get(ctx, tableID, uint32(pgAttrNum), keys.ColumnCommentType)
}

// GetIndexComment implements the scdecomp.CommentGetter interface.
func (mf *metadataCache) GetIndexComment(
	ctx context.Context, tableID catid.DescID, indexID catid.IndexID,
) (comment string, ok bool, err error) {
	return mf.get(ctx, tableID, uint32(indexID), keys.IndexCommentType)
}

// GetConstraintComment implements the scdecomp.CommentGetter interface.
func (mf *metadataCache) GetConstraintComment(
	ctx context.Context, tableID catid.DescID, constraintID catid.ConstraintID,
) (comment string, ok bool, err error) {
	return mf.get(ctx, tableID, uint32(constraintID), keys.ConstraintCommentType)
}

// LoadCommentsForObjects implements the scdecomp.CommentGetter interface.
func (mf *metadataCache) LoadCommentsForObjects(ctx context.Context, objIDs []descpb.ID) error {
	var uncheckedObjIDs []catid.DescID
	for _, id := range objIDs {
		if !mf.objIDsChecked.Contains(id) {
			uncheckedObjIDs = append(uncheckedObjIDs, id)
		}
	}
	if len(uncheckedObjIDs) == 0 {
		return nil
	}

	var buf strings.Builder
	_, _ = fmt.Fprintf(&buf, `SELECT type, object_id, sub_id, comment FROM system.comments WHERE object_id IN (%d`, uncheckedObjIDs[0])
	for _, id := range uncheckedObjIDs[1:] {
		_, _ = fmt.Fprintf(&buf, ", %d", id)
	}
	buf.WriteString(")")

	rows, err := mf.ie.QueryBufferedEx(
		ctx,
		"mf-get-table-comments",
		mf.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		buf.String(),
	)

	if err != nil {
		return err
	}
	for _, objID := range uncheckedObjIDs {
		mf.objIDsChecked.Add(objID)
	}

	for _, row := range rows {
		key := CommentKey{
			ObjectID:    catid.DescID(tree.MustBeDInt(row[1])),
			SubID:       uint32(tree.MustBeDInt(row[2])),
			CommentType: keys.CommentType(tree.MustBeDInt(row[0])),
		}
		mf.comments[key] = string(tree.MustBeDString(row[3]))
	}

	return nil
}

// warmCache warms the cache with first 1000 comments. error is swallowed since
// we still kinda eagerly load comments to cache with LoadCommentsForObjects.
func (mf *metadataCache) warmCache() {
	rows, err := mf.ie.QueryBufferedEx(
		context.Background(),
		"mf-get-table-comments",
		mf.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		`SELECT type, object_id, sub_id, comment FROM system.comments LIMIT 1000`,
	)

	if err != nil {
		return
	}

	for _, row := range rows {
		key := CommentKey{
			ObjectID:    catid.DescID(tree.MustBeDInt(row[1])),
			SubID:       uint32(tree.MustBeDInt(row[2])),
			CommentType: keys.CommentType(tree.MustBeDInt(row[0])),
		}
		mf.comments[key] = string(tree.MustBeDString(row[3]))
		mf.objIDsChecked.Add(key.ObjectID)
	}
}

// NewCommentCache returns a new CommentGetter.
func NewCommentCache(txn *kv.Txn, ie sqlutil.InternalExecutor) scbuild.CommentCache {
	cache := &metadataCache{
		txn:           txn,
		ie:            ie,
		comments:      make(map[CommentKey]string),
		objIDsChecked: catalog.MakeDescriptorIDSet(),
	}
	cache.warmCache()
	return cache
}
