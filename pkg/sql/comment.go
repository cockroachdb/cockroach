// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

func (p *planner) updateComment(
	ctx context.Context, objID descpb.ID, subID uint32, cmtType catalogkeys.CommentType, cmt string,
) error {
	b := p.Txn().NewBatch()
	if err := p.descCollection.WriteCommentToBatch(
		ctx,
		p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		b,
		catalogkeys.MakeCommentKey(uint32(objID), subID, cmtType),
		cmt,
	); err != nil {
		return err
	}
	return p.Txn().Run(ctx, b)
}

func (p *planner) deleteComment(
	ctx context.Context, objID descpb.ID, subID uint32, cmtType catalogkeys.CommentType,
) error {
	b := p.Txn().NewBatch()
	if err := p.descCollection.DeleteCommentInBatch(
		ctx,
		p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		b,
		catalogkeys.MakeCommentKey(uint32(objID), subID, cmtType),
	); err != nil {
		return err
	}
	return p.Txn().Run(ctx, b)
}
