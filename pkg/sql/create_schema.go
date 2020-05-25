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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type createSchemaNode struct {
	n *tree.CreateSchema
}

func (n *createSchemaNode) startExec(params runParams) error {
	// Pre-20.2 implementation without user-defined schemas.
	if !params.p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.VersionUserDefinedSchemas) {
		if n.n.Schema.NumParts > 1 {
			return unimplemented.NewWithIssuef(26443, "specifying a database for a schema is unsupported")
		}
		name := n.n.Schema.Schema()
		var schemaExists bool
		if name == tree.PublicSchema {
			schemaExists = true
		} else {
			for _, virtualSchema := range virtualSchemas {
				if name == virtualSchema.name {
					schemaExists = true
					break
				}
			}
		}
		if !schemaExists {
			return unimplemented.NewWithIssuef(26443,
				"new schemas are unsupported")
		}
		if n.n.IfNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateSchema, "schema %q already exists", n.n.Schema.String())
	}
	return params.p.createUserDefinedSchema(params, n.n)
}

func (*createSchemaNode) Next(runParams) (bool, error) { return false, nil }
func (*createSchemaNode) Values() tree.Datums          { return tree.Datums{} }
func (n *createSchemaNode) Close(ctx context.Context)  {}

// CreateSchema creates a schema.
func (p *planner) CreateSchema(ctx context.Context, n *tree.CreateSchema) (planNode, error) {
	return &createSchemaNode{
		n: n,
	}, nil
}

func (p *planner) createUserDefinedSchema(params runParams, n *tree.CreateSchema) error {
	dbName := p.CurrentDatabase()
	// This is actually the database.
	if n.Schema.HasExplicitCatalog() {
		dbName = n.Schema.Catalog()
	}

	db, err := p.ResolveUncachedDatabaseByName(params.ctx, dbName, true /* required */)
	if err != nil {
		return err
	}

	// Check to see if this schema already exists.
	exists, _, err := sqlbase.LookupObjectID(params.ctx, p.txn, p.ExecCfg().Codec, db.ID, keys.RootNamespaceID, n.Schema.Schema())
	if err != nil {
		return err
	}
	if exists {
		if n.IfNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateSchema, "schema %q already exists", n.Schema.String())
	}

	// Generate a schema ID.
	id, err := catalogkv.GenerateUniqueDescID(params.ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
	if err != nil {
		return err
	}
	// Generate a namespace key.
	schemaKey := sqlbase.NewSchemaKey(db.ID, n.Schema.Schema())

	schemaDesc := &sqlbase.SchemaDescriptor{
		ParentID:   db.ID,
		Name:       n.Schema.Schema(),
		ID:         id,
		Privileges: sqlbase.NewDefaultPrivilegeDescriptor(),
	}
	if err := p.createDescriptorWithID(
		params.ctx, schemaKey.Key(p.ExecCfg().Codec), id, schemaDesc, nil, tree.AsStringWithFQNames(n, params.Ann()),
	); err != nil {
		return err
	}
	return nil
}
