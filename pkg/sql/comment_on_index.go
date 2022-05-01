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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type commentOnIndexNode struct {
	n               *tree.CommentOnIndex
	tableDesc       *tabledesc.Mutable
	index           catalog.Index
	metadataUpdater scexec.DescriptorMetadataUpdater
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

	_, tableDesc, index, err := p.getTableAndIndex(ctx, &n.Index, privilege.CREATE, true /* skipCache */)
	if err != nil {
		return nil, err
	}

	return &commentOnIndexNode{
		n:         n,
		tableDesc: tableDesc,
		index:     index,
		metadataUpdater: p.execCfg.DescMetadaUpdaterFactory.NewMetadataUpdater(
			ctx,
			p.txn,
			p.SessionData(),
		)}, nil
}

func (n *commentOnIndexNode) startExec(params runParams) error {
	if n.n.Comment != nil {
		err := n.metadataUpdater.UpsertDescriptorComment(
			int64(n.tableDesc.ID),
			int64(n.index.GetID()),
			keys.IndexCommentType,
			*n.n.Comment,
		)
		if err != nil {
			return err
		}
	} else {
		err := n.metadataUpdater.DeleteDescriptorComment(
			int64(n.tableDesc.ID), int64(n.index.GetID()), keys.IndexCommentType)
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

func (n *commentOnIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnIndexNode) Close(context.Context)        {}
