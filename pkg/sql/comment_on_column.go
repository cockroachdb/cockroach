// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type commentOnColumnNode struct {
	zeroInputPlanNode
	n         *tree.CommentOnColumn
	tableDesc catalog.TableDescriptor
}

// CommentOnColumn add comment on a column.
// Privileges: CREATE on table.
func (p *planner) CommentOnColumn(ctx context.Context, n *tree.CommentOnColumn) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"COMMENT ON COLUMN",
	); err != nil {
		return nil, err
	}

	if n.ColumnItem.TableName == nil {
		return nil, pgerror.New(pgcode.Syntax, "column name must be qualified")
	}
	tableDesc, err := p.ResolveUncachedTableDescriptorEx(ctx, n.ColumnItem.TableName, true, tree.ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnColumnNode{n: n, tableDesc: tableDesc}, nil
}

func (n *commentOnColumnNode) startExec(params runParams) error {
	col, err := catalog.MustFindColumnByTreeName(n.tableDesc, n.n.ColumnItem.ColumnName)
	if err != nil {
		return err
	}

	if n.n.Comment == nil {
		err = params.p.deleteComment(
			params.ctx, n.tableDesc.GetID(), uint32(col.GetPGAttributeNum()), catalogkeys.ColumnCommentType,
		)
	} else {
		err = params.p.updateComment(
			params.ctx, n.tableDesc.GetID(), uint32(col.GetPGAttributeNum()), catalogkeys.ColumnCommentType, *n.n.Comment,
		)
	}
	if err != nil {
		return err
	}

	comment := ""
	if n.n.Comment != nil {
		comment = *n.n.Comment
	}

	tn, err := params.p.getQualifiedTableName(params.ctx, n.tableDesc)
	if err != nil {
		return err
	}

	return params.p.logEvent(params.ctx,
		n.tableDesc.GetID(),
		&eventpb.CommentOnColumn{
			TableName:   tn.FQString(),
			ColumnName:  string(n.n.ColumnItem.ColumnName),
			Comment:     comment,
			NullComment: n.n.Comment == nil,
		})
}

func (n *commentOnColumnNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnColumnNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnColumnNode) Close(context.Context)        {}
