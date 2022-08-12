// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/descmetadata"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type commentOnSchemaNode struct {
	n               *tree.CommentOnSchema
	schemaDesc      catalog.SchemaDescriptor
	metadataUpdater scexec.DescriptorMetadataUpdater
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

	db, err := p.Descriptors().GetImmutableDatabaseByName(ctx, p.txn,
		dbName, tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	schemaDesc, err := p.Descriptors().GetImmutableSchemaByID(ctx, p.txn,
		db.GetSchemaID(n.Name.Schema()), tree.SchemaLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, db, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnSchemaNode{
		n:          n,
		schemaDesc: schemaDesc,
		metadataUpdater: descmetadata.NewMetadataUpdater(
			ctx,
			p.ExecCfg().InternalExecutorFactory,
			p.Descriptors(),
			&p.ExecCfg().Settings.SV,
			p.txn,
			p.SessionData(),
		),
	}, nil
}

func (n *commentOnSchemaNode) startExec(params runParams) error {
	if n.n.Comment != nil {
		err := n.metadataUpdater.UpsertDescriptorComment(
			int64(n.schemaDesc.GetID()), 0, keys.SchemaCommentType, *n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		err := n.metadataUpdater.DeleteDescriptorComment(
			int64(n.schemaDesc.GetID()), 0, keys.SchemaCommentType)
		if err != nil {
			return err
		}
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
