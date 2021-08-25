// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

type commentOnConstraintNode struct {
	n         *tree.CommentOnConstraint
	constraintInfo *ConstraintAndTable
}

// CommentOnConstraint add comment on a constraint.
func (p *planner) CommentOnConstraint(ctx context.Context, n *tree.CommentOnConstraint) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"COMMENT ON CONSTRAINT",
	); err != nil {
		return nil, err
	}

	dbName := p.CurrentDatabase()
	if dbName == "" {
		return nil, pgerror.New(pgcode.UndefinedDatabase,
			"cannot comment schema without being connected to a database")
	}

	desc, err := p.Descriptors().GetImmutableDatabaseByName(ctx, p.txn,
		dbName, tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}


	constraint,err := p.searchConstraintFromAllMutableTables(ctx,desc,n.Name)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, desc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnConstraintNode{n: n, constraintInfo: constraint}, nil

}

func (n *commentOnConstraintNode) startExec(params runParams) error {

	if n.n.Comment != nil {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"set-constraint-comment",
			params.p.Txn(),
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			"UPSERT INTO system.comments VALUES ($1, $2, 0, $3)",
			keys.ConstraintCommentType,
			n.constraintInfo.tableDesc.GetID(),
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		err := params.p.removeConstraintComment(params.ctx, n.constraintInfo.tableDesc.GetID())
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *planner) removeConstraintComment(
	ctx context.Context, tableID descpb.ID,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-constraint-comment",
		p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
		keys.ConstraintCommentType,
		tableID)

	return err
}

func (n *commentOnConstraintNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnConstraintNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnConstraintNode) Close(context.Context)        {}
