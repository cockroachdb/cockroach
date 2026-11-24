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
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type commentOnDatabaseNode struct {
	zeroInputPlanNode
	n      *tree.CommentOnDatabase
	dbDesc catalog.DatabaseDescriptor
}

// CommentOnDatabase add comment on a database.
// Privileges: CREATE on database.
//
//	notes: postgres requires CREATE on the database.
func (p *planner) CommentOnDatabase(
	ctx context.Context, n *tree.CommentOnDatabase,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"COMMENT ON DATABASE",
	); err != nil {
		return nil, err
	}

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.Name))
	if err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnDatabaseNode{n: n, dbDesc: dbDesc}, nil
}

func (n *commentOnDatabaseNode) startExec(params runParams) error {
	var err error
	if n.n.Comment == nil {
		err = params.p.deleteComment(
			params.ctx, n.dbDesc.GetID(), 0 /* subID */, catalogkeys.DatabaseCommentType,
		)
	} else {
		err = params.p.updateComment(
			params.ctx, n.dbDesc.GetID(), 0 /* subID */, catalogkeys.DatabaseCommentType, *n.n.Comment,
		)
	}
	if err != nil {
		return err
	}

	dbComment := ""
	if n.n.Comment != nil {
		dbComment = *n.n.Comment
	}
	return params.p.logEvent(params.ctx,
		n.dbDesc.GetID(),
		&eventpb.CommentOnDatabase{
			DatabaseName: n.n.Name.String(),
			Comment:      dbComment,
			NullComment:  n.n.Comment == nil,
		})
}

func (n *commentOnDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnDatabaseNode) Close(context.Context)        {}
