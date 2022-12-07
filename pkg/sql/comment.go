// Copyright 2022 The Cockroach Authors.
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
