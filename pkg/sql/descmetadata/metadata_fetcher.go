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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

type metadataFetcher struct {
	txn *kv.Txn
	ie  sqlutil.InternalExecutor
}

// GetAllCommentsOnObject implements the scdecomp.DescriptorMetadataFetcher interface.
func (mf metadataFetcher) GetAllCommentsOnObject(
	ctx context.Context, objectID int64,
) (*scdecomp.CommentCache, error) {
	rows, err := mf.ie.QueryBufferedEx(
		ctx,
		"mf-get-table-comments",
		mf.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"SELECT type, sub_id, comment FROM system.comments WHERE object_id=$1",
		objectID,
	)

	if err != nil {
		return nil, err
	}

	cache := scdecomp.NewCommentCache()
	for _, row := range rows {
		cache.Add(
			objectID,
			int64(tree.MustBeDInt(row[1])),
			keys.CommentType(tree.MustBeDInt(row[0])),
			string(tree.MustBeDString(row[2])),
		)
	}

	return cache, nil
}

// NewMetadataFetcher returns a new DescriptorMetadataFetcher.
func NewMetadataFetcher(
	txn *kv.Txn, ie sqlutil.InternalExecutor,
) scdecomp.DescriptorMetadataFetcher {
	return metadataFetcher{
		txn: txn,
		ie:  ie,
	}
}
