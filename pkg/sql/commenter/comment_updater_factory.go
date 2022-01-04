// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package commenter

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// CommentUpdaterFactory used to construct a commenter.CommentUpdater, which
// can be used to update comments on schema objects.
type CommentUpdaterFactory struct {
	ieFactory         sqlutil.SessionBoundInternalExecutorFactory
	oidFromConstraint OidFromConstraintCallback
}

// NewCommentUpdaterFactory creates a new comment updater factory.
func NewCommentUpdaterFactory(
	ieFactory sqlutil.SessionBoundInternalExecutorFactory,
	oidFromConstraint OidFromConstraintCallback,
) CommentUpdaterFactory {
	return CommentUpdaterFactory{
		ieFactory:         ieFactory,
		oidFromConstraint: oidFromConstraint,
	}
}

// NewCommentUpdater creates a new comment updater, which can be used to
// create / destroy comments associated with different schema objects.
func (cf CommentUpdaterFactory) NewCommentUpdater(
	ctx context.Context, txn *kv.Txn, sessionData *sessiondata.SessionData,
) commentUpdater {
	return commentUpdater{
		txn:               txn,
		ie:                cf.ieFactory(ctx, sessionData),
		oidFromConstraint: cf.oidFromConstraint,
	}
}
