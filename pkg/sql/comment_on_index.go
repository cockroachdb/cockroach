// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type commentOnIndexNode struct {
	n         *tree.CommentOnIndex
	tableDesc *tabledesc.Mutable
	index     catalog.Index
}

// CommentOnIndex adds a comment on an index.
// Privileges: CREATE on table.
func (p *planner) CommentOnIndex(ctx context.Context, n *tree.CommentOnIndex) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"COMMENT ON INDEX",
	); err != nil {
		return nil, err
	}

	tableDesc, index, err := p.getTableAndIndex(ctx, &n.Index, privilege.CREATE)
	if err != nil {
		return nil, err
	}

	return &commentOnIndexNode{n: n, tableDesc: tableDesc, index: index}, nil
}

func (n *commentOnIndexNode) startExec(params runParams) error {
	if n.n.Comment != nil {
		err := params.p.upsertIndexComment(
			params.ctx,
			n.tableDesc.ID,
			n.index.GetID(),
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		err := params.p.removeIndexComment(params.ctx, n.tableDesc.ID, n.index.GetID())
		if err != nil {
			return err
		}
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
		n.tableDesc.ID,
		&eventpb.CommentOnIndex{
			TableName:   tn.FQString(),
			IndexName:   string(n.n.Index.Index),
			Comment:     comment,
			NullComment: n.n.Comment == nil,
		})
}

func (p *planner) upsertIndexComment(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID, comment string,
) error {
	_, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"set-index-comment",
		p.Txn(),
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"UPSERT INTO system.comments VALUES ($1, $2, $3, $4)",
		keys.IndexCommentType,
		tableID,
		indexID,
		comment)

	return err
}

func (p *planner) removeIndexComment(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-index-comment",
		p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
		keys.IndexCommentType,
		tableID,
		indexID)

	return err
}

func (n *commentOnIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnIndexNode) Close(context.Context)        {}
