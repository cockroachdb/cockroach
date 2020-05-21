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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type commentOnTableNode struct {
	n         *tree.CommentOnTable
	tableDesc *ImmutableTableDescriptor
}

// CommentOnTable add comment on a table.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) CommentOnTable(ctx context.Context, n *tree.CommentOnTable) (planNode, error) {
	tableDesc, err := p.ResolveUncachedTableDescriptorEx(ctx, n.Table, true, resolver.ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnTableNode{n: n, tableDesc: tableDesc}, nil
}

func (n *commentOnTableNode) startExec(params runParams) error {
	if n.n.Comment != nil {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"set-table-comment",
			params.p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"UPSERT INTO system.comments VALUES ($1, $2, 0, $3)",
			keys.TableCommentType,
			n.tableDesc.ID,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"delete-table-comment",
			params.p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
			keys.TableCommentType,
			n.tableDesc.ID)
		if err != nil {
			return err
		}
	}

	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCommentOnTable,
		int32(n.tableDesc.ID),
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
		struct {
			TableName string
			Statement string
			User      string
			Comment   *string
		}{
			params.p.ResolvedName(n.n.Table).FQString(),
			n.n.String(),
			params.SessionData().User,
			n.n.Comment,
		},
	)
}

func (n *commentOnTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnTableNode) Close(context.Context)        {}
