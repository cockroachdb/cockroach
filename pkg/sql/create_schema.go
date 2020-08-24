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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type createSchemaNode struct {
	n *tree.CreateSchema
}

func (n *createSchemaNode) startExec(params runParams) error {
	return params.p.createUserDefinedSchema(params, n.n)
}

func (p *planner) createUserDefinedSchema(params runParams, n *tree.CreateSchema) error {
	// Users can't create a schema without being connected to a DB.
	if p.CurrentDatabase() == "" {
		return pgerror.New(pgcode.UndefinedDatabase,
			"cannot create schema without being connected to a database")
	}

	db, err := p.ResolveMutableDatabaseDescriptor(params.ctx, p.CurrentDatabase(), true /* required */)
	if err != nil {
		return err
	}

	// Users cannot create schemas within the system database.
	if db.ID == keys.SystemDatabaseID {
		return pgerror.New(pgcode.InvalidObjectDefinition, "cannot create schemas in the system database")
	}

	// Ensure there aren't any name collisions.
	exists, err := p.schemaExists(params.ctx, db.ID, n.Schema)
	if err != nil {
		return err
	}

	if exists {
		if n.IfNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateSchema, "schema %q already exists", n.Schema)
	}

	// Check validity of the schema name.
	if err := schemadesc.IsSchemaNameValid(n.Schema); err != nil {
		return err
	}

	// Ensure that the cluster version is high enough to create the schema.
	if !params.p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.VersionUserDefinedSchemas) {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			`creating schemas requires all nodes to be upgraded to %s`,
			clusterversion.VersionByKey(clusterversion.VersionUserDefinedSchemas))
	}

	// Check that creation of schemas is enabled.
	if !p.EvalContext().SessionData.UserDefinedSchemasEnabled {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"session variable experimental_enable_user_defined_schemas is set to false, cannot create a schema")
	}

	// Create the ID.
	id, err := catalogkv.GenerateUniqueDescID(params.ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
	if err != nil {
		return err
	}

	// Inherit the parent privileges.
	privs := db.GetPrivileges()
	privs.SetOwner(params.SessionData().User)

	// Create the SchemaDescriptor.
	desc := schemadesc.NewCreatedMutable(descpb.SchemaDescriptor{
		ParentID:   db.ID,
		Name:       n.Schema,
		ID:         id,
		Privileges: privs,
		Version:    1,
	})

	// Update the parent database with this schema information.
	if db.Schemas == nil {
		db.Schemas = make(map[string]descpb.DatabaseDescriptor_SchemaInfo)
	}
	db.Schemas[desc.Name] = descpb.DatabaseDescriptor_SchemaInfo{
		ID:      desc.ID,
		Dropped: false,
	}

	if err := p.writeNonDropDatabaseChange(
		params.ctx, db,
		fmt.Sprintf("updating parent database %s for %s", db.GetName(), tree.AsStringWithFQNames(n, params.Ann())),
	); err != nil {
		return err
	}

	// Finally create the schema on disk.
	return p.createDescriptorWithID(
		params.ctx,
		catalogkeys.NewSchemaKey(db.ID, n.Schema).Key(p.ExecCfg().Codec),
		id,
		desc,
		params.ExecCfg().Settings,
		tree.AsStringWithFQNames(n, params.Ann()),
	)
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
