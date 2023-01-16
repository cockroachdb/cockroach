// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

func (m *visitor) CreateGCJobForTable(_ context.Context, op scop.CreateGCJobForTable) error {
	m.s.AddNewGCJobForTable(op.StatementForDropJob, op.DatabaseID, op.TableID)
	return nil
}

func (m *visitor) CreateGCJobForDatabase(_ context.Context, op scop.CreateGCJobForDatabase) error {
	m.s.AddNewGCJobForDatabase(op.StatementForDropJob, op.DatabaseID)
	return nil
}

func (m *visitor) CreateGCJobForIndex(_ context.Context, op scop.CreateGCJobForIndex) error {
	m.s.AddNewGCJobForIndex(op.StatementForDropJob, op.TableID, op.IndexID)
	return nil
}

func (m *visitor) MarkDescriptorAsPublic(
	ctx context.Context, op scop.MarkDescriptorAsPublic,
) error {
	desc, err := m.s.CheckOutDescriptor(ctx, op.DescriptorID)
	if err != nil {
		return err
	}
	desc.SetPublic()
	return nil
}

func (m *visitor) MarkDescriptorAsSyntheticallyDropped(
	ctx context.Context, op scop.MarkDescriptorAsSyntheticallyDropped,
) error {
	desc, err := m.s.GetDescriptor(ctx, op.DescriptorID)
	if err != nil {
		return err
	}
	synth := desc.NewBuilder().BuildExistingMutable()
	synth.SetDropped()
	m.sd.AddSyntheticDescriptor(synth)
	return nil
}

func (m *visitor) MarkDescriptorAsDropped(
	ctx context.Context, op scop.MarkDescriptorAsDropped,
) error {
	desc, err := m.s.CheckOutDescriptor(ctx, op.DescriptorID)
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

func (m *visitor) DrainDescriptorName(_ context.Context, op scop.DrainDescriptorName) error {
	nameDetails := descpb.NameInfo{
		ParentID:       op.Namespace.DatabaseID,
		ParentSchemaID: op.Namespace.SchemaID,
		Name:           op.Namespace.Name,
	}
	m.s.AddDrainedName(op.Namespace.DescriptorID, nameDetails)
	return nil
}

func (m *visitor) DeleteDescriptor(_ context.Context, op scop.DeleteDescriptor) error {
	m.s.DeleteDescriptor(op.DescriptorID)
	return nil
}

func (m *visitor) RemoveTableComment(_ context.Context, op scop.RemoveTableComment) error {
	m.s.DeleteComment(op.TableID, 0, catalogkeys.TableCommentType)
	return nil
}

func (m *visitor) RemoveDatabaseComment(_ context.Context, op scop.RemoveDatabaseComment) error {
	m.s.DeleteComment(op.DatabaseID, 0, catalogkeys.DatabaseCommentType)
	return nil
}

func (m *visitor) RemoveSchemaComment(_ context.Context, op scop.RemoveSchemaComment) error {
	m.s.DeleteComment(op.SchemaID, 0, catalogkeys.SchemaCommentType)
	return nil
}

func (m *visitor) RemoveIndexComment(_ context.Context, op scop.RemoveIndexComment) error {
	m.s.DeleteComment(op.TableID, int(op.IndexID), catalogkeys.IndexCommentType)
	return nil
}

func (m *visitor) RemoveColumnComment(_ context.Context, op scop.RemoveColumnComment) error {
	m.s.DeleteComment(op.TableID, int(op.PgAttributeNum), catalogkeys.ColumnCommentType)
	return nil
}

func (m *visitor) RemoveConstraintComment(
	_ context.Context, op scop.RemoveConstraintComment,
) error {
	m.s.DeleteComment(op.TableID, int(op.ConstraintID), catalogkeys.ConstraintCommentType)
	return nil
}

func (m *visitor) RemoveDatabaseRoleSettings(
	ctx context.Context, op scop.RemoveDatabaseRoleSettings,
) error {
	return m.s.DeleteDatabaseRoleSettings(ctx, op.DatabaseID)
}

func (m *visitor) RemoveUserPrivileges(ctx context.Context, op scop.RemoveUserPrivileges) error {
	desc, err := m.s.CheckOutDescriptor(ctx, op.DescriptorID)
	if err != nil {
		return err
	}
	user, err := username.MakeSQLUsernameFromUserInput(op.User, username.PurposeValidation)
	if err != nil {
		return err
	}
	desc.GetPrivileges().RemoveUser(user)
	return nil
}

func (m *visitor) DeleteSchedule(_ context.Context, op scop.DeleteSchedule) error {
	if op.ScheduleID != 0 {
		m.s.DeleteSchedule(op.ScheduleID)
	}
	return nil
}
