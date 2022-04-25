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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func (m *visitor) LogEvent(ctx context.Context, op scop.LogEvent) error {
	descID := screl.GetDescID(op.Element.Element())
	fullName, err := m.nr.GetFullyQualifiedName(ctx, descID)
	if err != nil {
		return err
	}
	event, err := asEventPayload(ctx, fullName, op.Element.Element(), op.TargetStatus, m)
	if err != nil {
		return err
	}
	details := eventpb.CommonSQLEventDetails{
		ApplicationName: op.Authorization.AppName,
		User:            op.Authorization.UserName,
		Statement:       redact.RedactableString(op.Statement),
		Tag:             op.StatementTag,
	}
	return m.s.EnqueueEvent(descID, op.TargetMetadata, details, event)
}

func asEventPayload(
	ctx context.Context, fullName string, e scpb.Element, targetStatus scpb.Status, m *visitor,
) (eventpb.EventPayload, error) {
	if targetStatus == scpb.Status_ABSENT {
		switch e.(type) {
		case *scpb.Table:
			return &eventpb.DropTable{TableName: fullName}, nil
		case *scpb.View:
			return &eventpb.DropView{ViewName: fullName}, nil
		case *scpb.Sequence:
			return &eventpb.DropSequence{SequenceName: fullName}, nil
		case *scpb.Database:
			return &eventpb.DropDatabase{DatabaseName: fullName}, nil
		case *scpb.Schema:
			return &eventpb.DropSchema{SchemaName: fullName}, nil
		case *scpb.AliasType, *scpb.EnumType:
			return &eventpb.DropType{TypeName: fullName}, nil
		case *scpb.TableComment, *scpb.ColumnComment, *scpb.IndexComment, *scpb.ConstraintComment, *scpb.DatabaseComment:
			return asCommentEventPayload(ctx, fullName, e, targetStatus, m, true /* isNullComment */)
		}
	}
	switch e := e.(type) {
	case *scpb.Column:
		tbl, err := m.checkOutTable(ctx, e.TableID)
		if err != nil {
			return nil, err
		}
		mutation, err := FindMutation(tbl, MakeColumnIDMutationSelector(e.ColumnID))
		if err != nil {
			return nil, err
		}
		return &eventpb.AlterTable{
			TableName:  fullName,
			MutationID: uint32(mutation.MutationID()),
		}, nil
	case *scpb.SecondaryIndex:
		tbl, err := m.checkOutTable(ctx, e.TableID)
		if err != nil {
			return nil, err
		}
		mutation, err := FindMutation(tbl, MakeIndexIDMutationSelector(e.IndexID))
		if err != nil {
			return nil, err
		}
		switch targetStatus {
		case scpb.Status_PUBLIC:
			return &eventpb.AlterTable{
				TableName:  fullName,
				MutationID: uint32(mutation.MutationID()),
			}, nil
		case scpb.Status_ABSENT:
			return &eventpb.DropIndex{
				TableName:  fullName,
				IndexName:  mutation.AsIndex().GetName(),
				MutationID: uint32(mutation.MutationID()),
			}, nil
		default:
			return nil, errors.AssertionFailedf("unknown target status %s", targetStatus)
		}
	case *scpb.TableComment, *scpb.ColumnComment, *scpb.IndexComment, *scpb.ConstraintComment, *scpb.DatabaseComment:
		return asCommentEventPayload(ctx, fullName, e, targetStatus, m, false /* isNullComment */)
	}
	return nil, errors.AssertionFailedf("unknown %s element type %T", targetStatus.String(), e)
}

// TODO (Chengxiong): add event log support for schema comment
func asCommentEventPayload(
	ctx context.Context,
	fullName string,
	e scpb.Element,
	targetStatus scpb.Status,
	m *visitor,
	isNullComment bool,
) (eventpb.EventPayload, error) {
	switch e := e.(type) {
	case *scpb.TableComment:
		return &eventpb.CommentOnTable{
			TableName:   fullName,
			Comment:     e.Comment,
			NullComment: isNullComment,
		}, nil
	case *scpb.ColumnComment:
		tbl, err := m.checkOutTable(ctx, e.TableID)
		if err != nil {
			return nil, err
		}
		col, err := tbl.FindColumnWithID(e.ColumnID)
		if err != nil {
			return nil, err
		}
		return &eventpb.CommentOnColumn{
			TableName:   fullName,
			ColumnName:  col.GetName(),
			Comment:     e.Comment,
			NullComment: isNullComment,
		}, nil
	case *scpb.IndexComment:
		tbl, err := m.checkOutTable(ctx, e.TableID)
		if err != nil {
			return nil, err
		}
		idx, err := tbl.FindIndexWithID(e.IndexID)
		if err != nil {
			return nil, err
		}
		return &eventpb.CommentOnIndex{
			TableName:   fullName,
			IndexName:   idx.GetName(),
			Comment:     e.Comment,
			NullComment: isNullComment,
		}, nil
	case *scpb.ConstraintComment:
		tbl, err := m.checkOutTable(ctx, e.TableID)
		if err != nil {
			return nil, err
		}
		constraint, err := tbl.FindConstraintWithID(e.ConstraintID)
		if err != nil {
			return nil, err
		}
		return &eventpb.CommentOnConstraint{
			TableName:      fullName,
			ConstraintName: constraint.GetConstraintName(),
			Comment:        e.Comment,
			NullComment:    isNullComment,
		}, nil
	case *scpb.DatabaseComment:
		return &eventpb.CommentOnDatabase{
			DatabaseName: fullName,
			Comment:      e.Comment,
			NullComment:  isNullComment,
		}, nil
	}
	return nil, errors.AssertionFailedf("unknown %s element type %T", targetStatus.String(), e)
}
