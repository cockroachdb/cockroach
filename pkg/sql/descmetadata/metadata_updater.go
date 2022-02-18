// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// metadataUpdater which implements scexec.MetaDataUpdater that is used to update
// comments on different schema objects.
type metadataUpdater struct {
	txn               *kv.Txn
	ie                sqlutil.InternalExecutor
	collectionFactory *descs.CollectionFactory
	cacheEnabled      bool
}

// UpsertDescriptorComment implements scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) UpsertDescriptorComment(
	id int64, subID int64, commentType keys.CommentType, comment string,
) error {
	_, err := mu.ie.ExecEx(context.Background(),
		fmt.Sprintf("upsert-%s-comment", commentType),
		mu.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"UPSERT INTO system.comments VALUES ($1, $2, $3, $4)",
		commentType,
		id,
		subID,
		comment,
	)
	return err
}

// DeleteDescriptorComment implements scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) DeleteDescriptorComment(
	id int64, subID int64, commentType keys.CommentType,
) error {
	_, err := mu.ie.ExecEx(context.Background(),
		fmt.Sprintf("delete-%s-comment", commentType),
		mu.txn,
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
func (mu metadataUpdater) UpsertConstraintComment(
	tableID descpb.ID, constraintID descpb.ConstraintID, comment string,
) error {
	return mu.UpsertDescriptorComment(int64(tableID), int64(constraintID), keys.ConstraintCommentType, comment)
}

// DeleteConstraintComment implements scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) DeleteConstraintComment(
	tableID descpb.ID, constraintID descpb.ConstraintID,
) error {
	return mu.DeleteDescriptorComment(int64(tableID), int64(constraintID), keys.ConstraintCommentType)
}

// DeleteDatabaseRoleSettings implement scexec.DescriptorMetaDataUpdater.
func (mu metadataUpdater) DeleteDatabaseRoleSettings(ctx context.Context, dbID descpb.ID) error {
	rowsDeleted, err := mu.ie.ExecEx(ctx,
		"delete-db-role-setting",
		mu.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		fmt.Sprintf(
			`DELETE FROM %s WHERE database_id = $1`,
			sessioninit.DatabaseRoleSettingsTableName,
		),
		dbID,
	)
	if err != nil {
		return err
	}
	// If system table updates should be minimized, avoid bumping up the version
	// number of the table below.
	if mu.cacheEnabled || rowsDeleted == 0 {
		return nil
	}
	// Bump the table version for the role settings table when we modify it.
	return mu.collectionFactory.Txn(ctx,
		mu.ie,
		mu.txn.DB(),
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			desc, err := descriptors.GetMutableTableByID(
				ctx,
				txn,
				keys.DatabaseRoleSettingsTableID,
				tree.ObjectLookupFlags{
					CommonLookupFlags: tree.CommonLookupFlags{
						Required:       true,
						RequireMutable: true,
					},
				})
			if err != nil {
				return err
			}
			desc.MaybeIncrementVersion()
			return descriptors.WriteDesc(ctx, false /*kvTrace*/, desc, txn)
		})
}

// SwapDescriptorSubComment implements scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) SwapDescriptorSubComment(
	id int64, oldSubID int64, newSubID int64, commentType keys.CommentType,
) error {
	_, err := mu.ie.ExecEx(context.Background(),
		fmt.Sprintf("upsert-%s-comment", commentType),
		mu.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"UPDATE system.comments  SET sub_id= $1 WHERE "+
			"object_id = $2 AND sub_id = $3 AND type = $4",
		newSubID,
		id,
		oldSubID,
		commentType,
	)
	return err
}

// DeleteSchedule implement scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) DeleteSchedule(ctx context.Context, scheduleID int64) error {
	_, err := mu.ie.ExecEx(
		ctx,
		"delete-schedule",
		mu.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"DELETE FROM system.scheduled_jobs WHERE schedule_id = $1",
		scheduleID,
	)
	return err
}
