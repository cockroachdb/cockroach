// Copyright 2020 The Cockroach Authors.
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

type commentOnViewNode struct {
	n        *tree.CommentOnView
	viewDesc *ImmutableTableDescriptor
}

// CommentOnView adds comment on a view.
// Privileges: CREATE on view.
func (p *planner) CommentOnView(ctx context.Context, n *tree.CommentOnView) (planNode, error) {
	viewDesc, err := p.ResolveUncachedTableDescriptorEx(ctx, n.View, true, tree.ResolveRequireViewDesc)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, viewDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnViewNode{
		n:        n,
		viewDesc: viewDesc,
	}, nil
}

func (n *commentOnViewNode) startExec(params runParams) error {
	if n.n.Comment != nil {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"set-view-comment",
			params.p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"UPSERT INTO system.comments VALUES ($1, $2, 0, $3)",
			keys.ViewCommentType,
			n.viewDesc.ID,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"delete-view-comment",
			params.p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
			keys.ViewCommentType,
			n.viewDesc.ID)
		if err != nil {
			return err
		}
	}

	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCommentOnView,
		int32(n.viewDesc.ID),
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
		struct {
			ViewName  string
			Statement string
			User      string
			Comment   *string
		}{
			params.p.ResolvedName(n.n.View).FQString(),
			n.n.String(),
			params.SessionData().User,
			n.n.Comment,
		},
	)
}

func (n *commentOnViewNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnViewNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnViewNode) Close(context.Context)        {}
