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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// ConstraintOidBuilder constructs an OID based on constraint information.
type ConstraintOidBuilder interface {
	// ForeignKeyConstraintOid generates a foreign key OID.
	ForeignKeyConstraintOid(
		dbID descpb.ID, scName string, tableID descpb.ID, fk *descpb.ForeignKeyConstraint,
	) *tree.DOid
	// UniqueWithoutIndexConstraintOid generates a unique without index constraint OID.
	UniqueWithoutIndexConstraintOid(
		dbID descpb.ID, scName string, tableID descpb.ID, uc *descpb.UniqueWithoutIndexConstraint,
	) *tree.DOid
	// UniqueConstraintOid generates a unique with index constraint OID.
	UniqueConstraintOid(
		dbID descpb.ID, scName string, tableID descpb.ID, indexID descpb.IndexID,
	) *tree.DOid
	// PrimaryKeyConstraintOid generates a primary key constraint OID.
	PrimaryKeyConstraintOid(
		dbID descpb.ID, scName string, tableID descpb.ID, pkey *descpb.IndexDescriptor,
	) *tree.DOid
	// CheckConstraintOid generates check constraint OID.
	CheckConstraintOid(
		dbID descpb.ID, scName string, tableID descpb.ID, check *descpb.TableDescriptor_CheckConstraint,
	) *tree.DOid
}

// metadataUpdater which implements scexec.DescriptorMetadataUpdater that is used to update
// metaadata such as comments on different schema objects.
type metadataUpdater struct {
	txn               *kv.Txn
	ie                sqlutil.InternalExecutor
	oidBuilder        ConstraintOidBuilder
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

func (mu metadataUpdater) oidFromConstraint(
	desc catalog.TableDescriptor,
	schemaName string,
	constraintName string,
	constraintType scpb.ConstraintType,
) *tree.DOid {
	switch constraintType {
	case scpb.ConstraintType_FK:
		for _, fk := range desc.AllActiveAndInactiveForeignKeys() {
			if fk.Name == constraintName {
				return mu.oidBuilder.ForeignKeyConstraintOid(
					desc.GetParentID(),
					schemaName,
					desc.GetID(),
					fk,
				)
			}
		}
	case scpb.ConstraintType_PrimaryKey:
		for _, idx := range desc.AllIndexes() {
			if idx.GetName() == constraintName {
				mu.oidBuilder.UniqueConstraintOid(
					desc.GetParentID(),
					schemaName,
					desc.GetID(),
					idx.GetID(),
				)
			}
		}
	case scpb.ConstraintType_UniqueWithoutIndex:
		for _, unique := range desc.GetUniqueWithoutIndexConstraints() {
			if unique.GetName() == constraintName {
				return mu.oidBuilder.UniqueWithoutIndexConstraintOid(
					desc.GetParentID(),
					schemaName,
					desc.GetID(),
					&unique,
				)
			}
		}
	case scpb.ConstraintType_Check:
		for _, check := range desc.GetChecks() {
			if check.Name == constraintName {
				return mu.oidBuilder.CheckConstraintOid(
					desc.GetParentID(),
					schemaName,
					desc.GetID(),
					check,
				)
			}
		}
	}
	return nil
}

// UpsertConstraintComment implements scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) UpsertConstraintComment(
	desc catalog.TableDescriptor,
	schemaName string,
	constraintName string,
	constraintType scpb.ConstraintType,
	comment string,
) error {
	oid := mu.oidFromConstraint(desc, schemaName, constraintName, constraintType)
	// Constraint was not found.
	if oid == nil {
		return nil
	}
	return mu.UpsertDescriptorComment(int64(oid.DInt), 0, keys.ConstraintCommentType, comment)
}

// DeleteConstraintComment implements scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) DeleteConstraintComment(
	desc catalog.TableDescriptor,
	schemaName string,
	constraintName string,
	constraintType scpb.ConstraintType,
) error {
	oid := mu.oidFromConstraint(desc, schemaName, constraintName, constraintType)
	// Constraint was not found.
	if oid == nil {
		return nil
	}
	return mu.DeleteDescriptorComment(int64(oid.DInt), 0, keys.ConstraintCommentType)
}

// DeleteDatabaseRoleSettings implement scexec.DescriptorMetaDataUpdater.
func (mu metadataUpdater) DeleteDatabaseRoleSettings(
	ctx context.Context, database catalog.DatabaseDescriptor,
) error {
	rowsDeleted, err := mu.ie.ExecEx(ctx,
		"delete-db-role-setting",
		mu.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		fmt.Sprintf(
			`DELETE FROM %s WHERE database_id = $1`,
			sessioninit.DatabaseRoleSettingsTableName,
		),
		database.GetID(),
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
			return descriptors.WriteDesc(ctx, false, desc, txn)
		})
}
