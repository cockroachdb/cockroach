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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type commentOnConstraintNode struct {
	n         *tree.CommentOnConstraint
	tableDesc catalog.TableDescriptor
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

	return &commentOnConstraintNode{n: n, tableDesc: tableDesc}, nil

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

	constraintOid, err := makeConstraintOid(
		constraint, n.tableDesc.GetParentID(), schema.GetName(), n.tableDesc.GetID(),
	)
	if err != nil {
		return err
	}
	if n.n.Comment != nil {
		if _, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"set-constraint-comment",
			params.p.Txn(),
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			"UPSERT INTO system.comments VALUES ($1, $2, 0, $3)",
			keys.ConstraintCommentType,
			constraintOid.DInt,
			*n.n.Comment,
		); err != nil {
			return err
		}
	} else {
		// Setting the comment to NULL is the equivalent of deleting the comment.
		if _, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"delete-constraint-comment",
			params.p.Txn(),
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
			keys.ConstraintCommentType,
			constraintOid.DInt,
		); err != nil {
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

func (p *planner) removeConstraintComment(
	ctx context.Context, constraintDetail descpb.ConstraintDetail, tableDesc catalog.TableDescriptor,
) error {
	schemaDesc, err := p.Descriptors().GetImmutableSchemaByID(
		ctx, p.Txn(), tableDesc.GetParentSchemaID(), tree.SchemaLookupFlags{},
	)
	if err != nil {
		return err
	}
	constraintOid, err := makeConstraintOid(
		constraintDetail, tableDesc.GetParentID(), schemaDesc.GetName(), tableDesc.GetID(),
	)
	if err != nil {
		return err
	}
	if _, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-constraint-comment",
		p.Txn(),
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
		keys.ConstraintCommentType,
		constraintOid.DInt,
	); err != nil {
		return err
	}
	return nil
}

func makeConstraintOid(
	constraint descpb.ConstraintDetail, dbID descpb.ID, schemaName string, tableID descpb.ID,
) (*tree.DOid, error) {
	var constraintOid *tree.DOid
	hasher := makeOidHasher()
	switch kind := constraint.Kind; kind {
	case descpb.ConstraintTypePK:
		constraintDesc := constraint.Index
		constraintOid = hasher.PrimaryKeyConstraintOid(dbID, schemaName, tableID, constraintDesc)
	case descpb.ConstraintTypeFK:
		constraintDesc := constraint.FK
		constraintOid = hasher.ForeignKeyConstraintOid(dbID, schemaName, tableID, constraintDesc)
	case descpb.ConstraintTypeUnique:
		constraintDesc := constraint.Index.ID
		constraintOid = hasher.UniqueConstraintOid(dbID, schemaName, tableID, constraintDesc)
	case descpb.ConstraintTypeCheck:
		constraintDesc := constraint.CheckConstraint
		constraintOid = hasher.CheckConstraintOid(dbID, schemaName, tableID, constraintDesc)
	default:
		return nil, errors.AssertionFailedf("unknown constraint type %q", kind)
	}
	return constraintOid, nil
}

func (n *commentOnConstraintNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnConstraintNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnConstraintNode) Close(context.Context)        {}
