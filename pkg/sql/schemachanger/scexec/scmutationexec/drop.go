// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func (d *deferredVisitor) CreateGCJobForTable(
	_ context.Context, op scop.CreateGCJobForTable,
) error {
	d.AddNewGCJobForTable(op.StatementForDropJob, op.DatabaseID, op.TableID)
	return nil
}

func (d *deferredVisitor) CreateGCJobForDatabase(
	_ context.Context, op scop.CreateGCJobForDatabase,
) error {
	d.AddNewGCJobForDatabase(op.StatementForDropJob, op.DatabaseID)
	return nil
}

func (d *deferredVisitor) CreateGCJobForIndex(
	_ context.Context, op scop.CreateGCJobForIndex,
) error {
	d.AddNewGCJobForIndex(op.StatementForDropJob, op.TableID, op.IndexID)
	return nil
}

func (i *immediateVisitor) MarkDescriptorAsDropped(
	ctx context.Context, op scop.MarkDescriptorAsDropped,
) error {
	desc, err := i.checkOutDescriptor(ctx, op.DescriptorID)
	if err != nil {
		return err
	}
	desc.SetDropped()
	// After marking a table as dropped we will populate the drop time.
	if tableDesc, ok := desc.(*tabledesc.Mutable); ok && tableDesc.IsTable() {
		tableDesc.DropTime = timeutil.Now().UnixNano()
	}
	return nil
}

func (i *immediateVisitor) DrainDescriptorName(
	_ context.Context, op scop.DrainDescriptorName,
) error {
	nameDetails := descpb.NameInfo{
		ParentID:       op.Namespace.DatabaseID,
		ParentSchemaID: op.Namespace.SchemaID,
		Name:           op.Namespace.Name,
	}
	i.DeleteName(op.Namespace.DescriptorID, nameDetails)
	return nil
}

func (i *immediateVisitor) DeleteDescriptor(_ context.Context, op scop.DeleteDescriptor) error {
	i.ImmediateMutationStateUpdater.DeleteDescriptor(op.DescriptorID)
	return nil
}

func (i *immediateVisitor) RemoveTableComment(_ context.Context, op scop.RemoveTableComment) error {
	i.DeleteComment(op.TableID, 0, catalogkeys.TableCommentType)
	return nil
}

func (i *immediateVisitor) RemoveTypeComment(_ context.Context, op scop.RemoveTypeComment) error {
	i.DeleteComment(op.TypeID, 0, catalogkeys.TypeCommentType)
	return nil
}

func (i *immediateVisitor) RemoveDatabaseComment(
	_ context.Context, op scop.RemoveDatabaseComment,
) error {
	i.DeleteComment(op.DatabaseID, 0, catalogkeys.DatabaseCommentType)
	return nil
}

func (i *immediateVisitor) RemoveSchemaComment(
	_ context.Context, op scop.RemoveSchemaComment,
) error {
	i.DeleteComment(op.SchemaID, 0, catalogkeys.SchemaCommentType)
	return nil
}

func (i *immediateVisitor) RemoveIndexComment(_ context.Context, op scop.RemoveIndexComment) error {
	i.DeleteComment(op.TableID, int(op.IndexID), catalogkeys.IndexCommentType)
	return nil
}

func (i *immediateVisitor) RemoveColumnComment(
	_ context.Context, op scop.RemoveColumnComment,
) error {
	i.DeleteComment(op.TableID, int(op.PgAttributeNum), catalogkeys.ColumnCommentType)
	return nil
}

func (i *immediateVisitor) RemoveConstraintComment(
	_ context.Context, op scop.RemoveConstraintComment,
) error {
	i.DeleteComment(op.TableID, int(op.ConstraintID), catalogkeys.ConstraintCommentType)
	return nil
}

func (d *deferredVisitor) RemoveDatabaseRoleSettings(
	ctx context.Context, op scop.RemoveDatabaseRoleSettings,
) error {
	return d.DeleteDatabaseRoleSettings(ctx, op.DatabaseID)
}

func (i *immediateVisitor) RemoveUserPrivileges(
	ctx context.Context, op scop.RemoveUserPrivileges,
) error {
	desc, err := i.checkOutDescriptor(ctx, op.DescriptorID)
	if err != nil || desc.Dropped() {
		return err
	}
	user, err := username.MakeSQLUsernameFromUserInput(op.User, username.PurposeValidation)
	if err != nil {
		return err
	}
	desc.GetPrivileges().RemoveUser(user)
	return nil
}

func (d *deferredVisitor) DeleteSchedule(_ context.Context, op scop.DeleteSchedule) error {
	if op.ScheduleID != 0 {
		d.DeferredMutationStateUpdater.DeleteSchedule(op.ScheduleID)
	}
	return nil
}
