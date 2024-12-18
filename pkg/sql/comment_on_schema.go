// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type commentOnSchemaNode struct {
	zeroInputPlanNode
	n          *tree.CommentOnSchema
	schemaDesc catalog.SchemaDescriptor
}

// CommentOnSchema add comment on a schema.
// Privileges: CREATE on scheme.
//
//	notes: postgres requires CREATE on the scheme.
func (p *planner) CommentOnSchema(ctx context.Context, n *tree.CommentOnSchema) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"COMMENT ON SCHEMA",
	); err != nil {
		return nil, err
	}

	var dbName string
	if n.Name.ExplicitCatalog {
		dbName = n.Name.Catalog()
	} else {
		dbName = p.CurrentDatabase()
	}
	if dbName == "" {
		return nil, pgerror.New(pgcode.UndefinedDatabase,
			"cannot comment schema without being connected to a database")
	}

	db, err := p.Descriptors().ByNameWithLeased(p.txn).Get().Database(ctx, dbName)
	if err != nil {
		return nil, err
	}

	schemaDesc, err := p.Descriptors().MutableByID(p.txn).Schema(ctx, db.GetSchemaID(n.Name.Schema()))
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, db, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnSchemaNode{
		n:          n,
		schemaDesc: schemaDesc,
	}, nil
}

func (n *commentOnSchemaNode) startExec(params runParams) error {
	var err error
	if n.n.Comment == nil {
		err = params.p.deleteComment(
			params.ctx, n.schemaDesc.GetID(), 0 /* subID */, catalogkeys.SchemaCommentType,
		)
	} else {
		err = params.p.updateComment(
			params.ctx, n.schemaDesc.GetID(), 0 /* subID */, catalogkeys.SchemaCommentType, *n.n.Comment,
		)
	}
	if err != nil {
		return err
	}

	scComment := ""
	if n.n.Comment != nil {
		scComment = *n.n.Comment
	}

	return params.p.logEvent(
		params.ctx,
		n.schemaDesc.GetID(),
		&eventpb.CommentOnSchema{
			SchemaName:  n.n.Name.String(),
			Comment:     scComment,
			NullComment: n.n.Comment == nil,
		})
}

func (n *commentOnSchemaNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnSchemaNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnSchemaNode) Close(context.Context)        {}
