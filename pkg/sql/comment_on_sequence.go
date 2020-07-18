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

type commentOnSequenceNode struct {
	n            *tree.CommentOnSequence
	sequenceDesc *ImmutableTableDescriptor
}

// CommentOnSequence adds comment on a sequence.
// Privileges: CREATE on sequence.
func (p *planner) CommentOnSequence(
	ctx context.Context, n *tree.CommentOnSequence,
) (planNode, error) {
	sequenceDesc, err := p.ResolveUncachedTableDescriptorEx(ctx, n.Sequence, true, tree.ResolveRequireSequenceDesc)
	if err != nil {
		return nil, err
	}

	// Require that the current user can create a sequence to begin with.
	if err := p.CheckPrivilege(ctx, sequenceDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnSequenceNode{
		n:            n,
		sequenceDesc: sequenceDesc,
	}, nil
}

func (n *commentOnSequenceNode) startExec(params runParams) error {
	if n.n.Comment != nil {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"set-sequence-comment",
			params.p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"UPSERT INTO system.comments VALUES ($1, $2, 0, $3)",
			keys.SequenceCommentType,
			n.sequenceDesc.ID,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"delete-sequence-comment",
			params.p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
			keys.SequenceCommentType,
			n.sequenceDesc.ID)
		if err != nil {
			return err
		}
	}

	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCommentOnSequence,
		int32(n.sequenceDesc.ID),
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
		struct {
			SequenceName string
			Statement    string
			User         string
			Comment      *string
		}{
			params.p.ResolvedName(n.n.Sequence).FQString(),
			n.n.String(),
			params.SessionData().User,
			n.n.Comment,
		},
	)
}

func (n *commentOnSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnSequenceNode) Close(context.Context)        {}
