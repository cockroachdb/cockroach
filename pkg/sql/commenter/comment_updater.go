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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// commentUpdater which implements scexec.CommentUpdater that is used to update
// comments on different schema objects.
type commentUpdater struct {
	txn *kv.Txn
	ie  sqlutil.InternalExecutor
}

// UpsertDescriptorComment implements scexec.CommentUpdater.
func (cu commentUpdater) UpsertDescriptorComment(
	id int64, subID int64, commentType keys.CommentType, comment string,
) error {
	_, err := cu.ie.ExecEx(context.Background(),
		fmt.Sprintf("upsert-%s-comment", commentType),
		cu.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"UPSERT INTO system.comments VALUES ($1, $2, $3, $4)",
		commentType,
		id,
		subID,
		comment,
	)
	return err
}

// DeleteDescriptorComment implements scexec.CommentUpdater.
func (cu commentUpdater) DeleteDescriptorComment(
	id int64, subID int64, commentType keys.CommentType,
) error {
	_, err := cu.ie.ExecEx(context.Background(),
		fmt.Sprintf("delete-%s-comment", commentType),
		cu.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"DELETE FROM system.comments WHERE object_id = $1 AND sub_id = $2 AND "+
			"type = $3;",
		id,
		subID,
		commentType,
	)
	return err
}

// UpsertConstraintComment implements scexec.CommentUpdater.
func (cu commentUpdater) UpsertConstraintComment(
	desc catalog.TableDescriptor, constraintID descpb.ConstraintID, comment string,
) error {
	return cu.UpsertDescriptorComment(int64(desc.GetID()), int64(constraintID), keys.ConstraintCommentType, comment)
}

// DeleteConstraintComment implements scexec.CommentUpdater.
func (cu commentUpdater) DeleteConstraintComment(
	desc catalog.TableDescriptor, constraintID descpb.ConstraintID,
) error {
	return cu.DeleteDescriptorComment(int64(desc.GetID()), int64(constraintID), keys.ConstraintCommentType)
}
