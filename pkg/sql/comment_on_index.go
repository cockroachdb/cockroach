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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type commentOnIndexNode struct {
	n         *tree.CommentOnIndex
	tableDesc *sqlbase.TableDescriptor
	indexDesc *sqlbase.IndexDescriptor
}

// CommentOnIndex adds a comment on an index.
// Privileges: CREATE on table.
func (p *planner) CommentOnIndex(ctx context.Context, n *tree.CommentOnIndex) (planNode, error) {
	tableDesc, indexDesc, err := p.getTableAndIndex(ctx, &n.Index, privilege.CREATE)
	if err != nil {
		return nil, err
	}

	return &commentOnIndexNode{n: n, tableDesc: tableDesc.TableDesc(), indexDesc: indexDesc}, nil
}

func (n *commentOnIndexNode) startExec(params runParams) error {
	if n.n.Comment != nil {
		err := params.p.upsertIndexComment(
			params.ctx,
			n.tableDesc.ID,
			n.indexDesc.ID,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		err := params.p.removeIndexComment(params.ctx, n.tableDesc.ID, n.indexDesc.ID)
		if err != nil {
			return err
		}
	}

	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCommentOnIndex,
		int32(n.tableDesc.ID),
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
		struct {
			TableName string
			IndexName string
			Statement string
			User      string
			Comment   *string
		}{
			n.tableDesc.Name,
			string(n.n.Index.Index),
			n.n.String(),
			params.SessionData().User,
			n.n.Comment},
	)
}

func (p *planner) upsertIndexComment(
	ctx context.Context, tableID sqlbase.ID, indexID sqlbase.IndexID, comment string,
) error {
	_, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"set-index-comment",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"UPSERT INTO system.comments VALUES ($1, $2, $3, $4)",
		keys.IndexCommentType,
		tableID,
		indexID,
		comment)

	return err
}

func (p *planner) removeIndexComment(
	ctx context.Context, tableID sqlbase.ID, indexID sqlbase.IndexID,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-index-comment",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
		keys.IndexCommentType,
		tableID,
		indexID)

	return err
}

func (n *commentOnIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnIndexNode) Close(context.Context)        {}
