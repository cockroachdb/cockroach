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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type commentOnColumnNode struct {
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

	var tableName tree.TableName
	if n.ColumnItem.TableName != nil {
		tableName = n.ColumnItem.TableName.ToTableName()
	}
	tableDesc, err := p.resolveUncachedTableDescriptor(ctx, &tableName, true, tree.ResolveRequireTableDesc)
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
