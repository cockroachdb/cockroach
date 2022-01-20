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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type commentOnConstraintNode struct {
	n               *tree.CommentOnConstraint
	tableDesc       catalog.TableDescriptor
	oid             *tree.DOid
	metadataUpdater scexec.DescriptorMetadataUpdater
}

//CommentOnConstraint add comment on a constraint
//Privileges: CREATE on table
func (p *planner) CommentOnConstraint(
	ctx context.Context, n *tree.CommentOnConstraint,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"COMMENT ON CONSTRAINT",
	); err != nil {
		return nil, err
	}

	tableDesc, err := p.ResolveUncachedTableDescriptorEx(ctx, n.Table, true, tree.ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnConstraintNode{
		n:         n,
		tableDesc: tableDesc,
		metadataUpdater: p.execCfg.DescMetadaUpdaterFactory.NewMetadataUpdater(
			ctx,
			p.txn,
			p.SessionData(),
		),
	}, nil

}

func (n *commentOnConstraintNode) startExec(params runParams) error {
	info, err := n.tableDesc.GetConstraintInfo()
	if err != nil {
		return err
	}
	schema, err := params.p.Descriptors().GetImmutableSchemaByID(
		params.ctx, params.extendedEvalCtx.Txn, n.tableDesc.GetParentSchemaID(), tree.SchemaLookupFlags{},
	)
	if err != nil {
		return err
	}

	constraintName := string(n.n.Constraint)
	constraint, ok := info[constraintName]
	if !ok {
		return pgerror.Newf(pgcode.UndefinedObject,
			"constraint %q of relation %q does not exist", constraintName, n.tableDesc.GetName())
	}

	hasher := makeOidHasher()
	switch kind := constraint.Kind; kind {
	case descpb.ConstraintTypePK:
		constraintDesc := constraint.Index
		n.oid = hasher.PrimaryKeyConstraintOid(n.tableDesc.GetParentID(), schema.GetName(), n.tableDesc.GetID(), constraintDesc)
	case descpb.ConstraintTypeFK:
		constraintDesc := constraint.FK
		n.oid = hasher.ForeignKeyConstraintOid(n.tableDesc.GetParentID(), schema.GetName(), n.tableDesc.GetID(), constraintDesc)
	case descpb.ConstraintTypeUnique:
		constraintDesc := constraint.Index.ID
		n.oid = hasher.UniqueConstraintOid(n.tableDesc.GetParentID(), schema.GetName(), n.tableDesc.GetID(), constraintDesc)
	case descpb.ConstraintTypeCheck:
		constraintDesc := constraint.CheckConstraint
		n.oid = hasher.CheckConstraintOid(n.tableDesc.GetParentID(), schema.GetName(), n.tableDesc.GetID(), constraintDesc)

	}
	// Setting the comment to NULL is the
	// equivalent of deleting the comment.
	if n.n.Comment != nil {
		err := n.metadataUpdater.UpsertDescriptorComment(
			int64(n.oid.DInt),
			0,
			keys.ConstraintCommentType,
			*n.n.Comment,
		)
		if err != nil {
			return err
		}
	} else {
		err := n.metadataUpdater.DeleteDescriptorComment(
			int64(n.oid.DInt),
			0,
			keys.ConstraintCommentType,
		)
		if err != nil {
			return err
		}
	}

	comment := ""
	if n.n.Comment != nil {
		comment = *n.n.Comment
	}

	return params.p.logEvent(params.ctx,
		n.tableDesc.GetID(),
		&eventpb.CommentOnConstraint{
			TableName:      params.p.ResolvedName(n.n.Table).FQString(),
			ConstraintName: n.n.Constraint.String(),
			Comment:        comment,
			NullComment:    n.n.Comment == nil,
		})
}

func (n *commentOnConstraintNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnConstraintNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnConstraintNode) Close(context.Context)        {}
