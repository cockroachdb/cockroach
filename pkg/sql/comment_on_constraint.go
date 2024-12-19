// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type commentOnConstraintNode struct {
	zeroInputPlanNode
	n         *tree.CommentOnConstraint
	tableDesc catalog.TableDescriptor
}

// CommentOnConstraint add comment on a constraint
// Privileges: CREATE on table
func (p *planner) CommentOnConstraint(
	ctx context.Context, n *tree.CommentOnConstraint,
) (planNode, error) {
	// Block comments on constraint until cluster is updated.
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"COMMENT ON CONSTRAINT",
	); err != nil {
		return nil, err
	}

	tableDesc, err := p.ResolveUncachedTableDescriptorEx(ctx, n.Table, true, tree.ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnConstraintNode{
		n:         n,
		tableDesc: tableDesc,
	}, nil

}

func (n *commentOnConstraintNode) startExec(params runParams) error {
	constraintName := string(n.n.Constraint)
	constraint := catalog.FindConstraintByName(n.tableDesc, constraintName)
	if constraint == nil {
		return sqlerrors.NewUndefinedConstraintError(constraintName, n.tableDesc.GetName())
	}

	var err error
	if n.n.Comment == nil {
		err = params.p.deleteComment(
			params.ctx, n.tableDesc.GetID(), uint32(constraint.GetConstraintID()), catalogkeys.ConstraintCommentType,
		)
	} else {
		err = params.p.updateComment(
			params.ctx, n.tableDesc.GetID(), uint32(constraint.GetConstraintID()), catalogkeys.ConstraintCommentType, *n.n.Comment,
		)
	}
	if err != nil {
		return err
	}

	comment := ""
	if n.n.Comment != nil {
		comment = *n.n.Comment
	}

	return params.p.logEvent(params.ctx,
		n.tableDesc.GetID(),
		&eventpb.CommentOnConstraint{
			TableName:      params.p.ResolvedName(n.n.Table).FQString(),
			ConstraintName: n.n.Constraint.String(),
			Comment:        comment,
			NullComment:    n.n.Comment == nil,
		})
}

func (n *commentOnConstraintNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnConstraintNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnConstraintNode) Close(context.Context)        {}
