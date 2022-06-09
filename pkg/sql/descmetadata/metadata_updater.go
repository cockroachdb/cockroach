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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
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
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		"DELETE FROM system.comments WHERE object_id = $1 AND sub_id = $2 AND "+
			"type = $3;",
		id,
		subID,
		commentType,
	)
	return err
}

// DeleteAllCommentsForTables implements scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) DeleteAllCommentsForTables(idSet catalog.DescriptorIDSet) error {
	if idSet.Empty() {
		return nil
	}
	var buf strings.Builder
	ids := idSet.Ordered()
	_, _ = fmt.Fprintf(&buf, `
DELETE FROM system.comments
      WHERE type IN (%d, %d, %d, %d)
        AND object_id IN (%d`,
		keys.TableCommentType, keys.ColumnCommentType, keys.ConstraintCommentType,
		keys.IndexCommentType, ids[0],
	)
	for _, id := range ids[1:] {
		_, _ = fmt.Fprintf(&buf, ", %d", id)
	}
	buf.WriteString(")")
	_, err := mu.ie.ExecEx(context.Background(),
		"delete-all-comments-for-tables",
		mu.txn,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		buf.String(),
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
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		fmt.Sprintf(
			`DELETE FROM %s WHERE database_id = $1`,
			sessioninit.DatabaseRoleSettingsTableName,
		),
		dbID,
	)
	if err != nil {
		return err
	}
	// If the cache is off or if no rows changed, there's no need to bump the
	// table version.
	if !mu.cacheEnabled || rowsDeleted == 0 {
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
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
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
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		"DELETE FROM system.scheduled_jobs WHERE schedule_id = $1",
		scheduleID,
	)
	return err
}

func (mu metadataUpdater) SetZoneConfig(
	ctx context.Context, id descpb.ID, zone *zonepb.ZoneConfig,
) error {
	if zone == nil {
		_, err := mu.ie.Exec(ctx, "delete-zone", mu.txn,
			"DELETE FROM system.zones WHERE id = $1", id)
		return err
	}
	bytes, err := protoutil.Marshal(zone)
	if err != nil {
		return err
	}
	_, err = mu.ie.Exec(ctx, "update-zone", mu.txn,
		"UPSERT INTO system.zones (id, config) VALUES ($1, $2)", id, bytes)
	return err
}
