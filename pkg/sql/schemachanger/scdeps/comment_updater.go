// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdeps

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

type commentUpdater struct {
	txn *kv.Txn
	ie  sqlutil.InternalExecutor
}

// NewCommentUpdater creates a new comment updater
func NewCommentUpdater(
	ctx context.Context,
	ieFactory sqlutil.SessionBoundInternalExecutorFactory,
	sessionData *sessiondata.SessionData,
	txn *kv.Txn,
) scexec.CommentUpdater {
	return &commentUpdater{
		txn: txn,
		ie:  ieFactory(ctx, sessionData),
	}
}

// UpdateComment implements scexec.CommentUpdater
func (cu commentUpdater) UpdateComment(id descpb.ID, comment string) error {
	// If an empty comment is being added, we will delete the entry
	// here.
	if comment == "" {
		_, err := cu.ie.ExecEx(context.Background(),
			"delete-table-comment",
			cu.txn,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			"DELETE FROM system.comments WHERE object_id=$1",
			id,
		)
		return err
	}
	_, err := cu.ie.ExecEx(context.Background(),
		"insert-table-comment",
		cu.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"UPSERT INTO system.comments VALUES ($1, $2)",
		id,
		comment,
	)
	return err

}
