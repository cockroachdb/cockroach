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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

//OidFromConstraintCallback call back to get an OID from constraint information.
type OidFromConstraintCallback func(
	desc catalog.TableDescriptor,
	schemaName string,
	constraintOrdinal int,
	constraintType scpb.ConstraintType,
) *tree.DOid

// commentUpdater which implements scexec.CommentUpdater that is used to update
// comments on different schema objects.
type commentUpdater struct {
	txn               *kv.Txn
	ie                sqlutil.InternalExecutor
	oidFromConstraint OidFromConstraintCallback
}

// UpsertDescriptorComment implements scexec.CommentUpdater.
func (cu commentUpdater) UpsertDescriptorComment(
	id int64, subID int64, commentType scpb.CommentType, comment string,
) error {
	_, err := cu.ie.ExecEx(context.Background(),
		"upsert-schema-comment",
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
	id int64, subID int64, commentType scpb.CommentType,
) error {
	_, err := cu.ie.ExecEx(context.Background(),
		"delete-schema-comment",
		cu.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"DELETE FROM system.comments WHERE object_id=$1 AND sub_id=$2 and "+
			"type = $3",
		id,
		subID,
		commentType,
	)
	return err
}

// UpsertConstraintComment implements scexec.CommentUpdater.
func (cu commentUpdater) UpsertConstraintComment(
	desc catalog.TableDescriptor,
	schemaName string,
	constraintOrdinal int,
	constraintType scpb.ConstraintType,
	comment string,
) error {
	oid := cu.oidFromConstraint(desc, schemaName, constraintOrdinal, constraintType)
	return cu.UpsertDescriptorComment(int64(oid.DInt), 0, keys.ConstraintCommentType, comment)
}

// DeleteConstraintComment implements scexec.CommentUpdater.
func (cu commentUpdater) DeleteConstraintComment(
	desc catalog.TableDescriptor,
	schemaName string,
	constraintOrdinal int,
	constraintType scpb.ConstraintType,
) error {
	oid := cu.oidFromConstraint(desc, schemaName, constraintOrdinal, constraintType)
	return cu.DeleteDescriptorComment(int64(oid.DInt), 0, keys.ConstraintCommentType)
}
