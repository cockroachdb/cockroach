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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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

	sqltelemetry.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaCreate)
	dbName := p.CurrentDatabase()
	if n.Schema.ExplicitCatalog {
		dbName = n.Schema.Catalog()
	}

	db, err := p.ResolveMutableDatabaseDescriptor(params.ctx, dbName, true /* required */)
	if err != nil {
		return err
	}

	// Users cannot create schemas within the system database.
	if db.ID == keys.SystemDatabaseID {
		return pgerror.New(pgcode.InvalidObjectDefinition, "cannot create schemas in the system database")
	}

	if err := p.CheckPrivilege(params.ctx, db, privilege.CREATE); err != nil {
		return err
	}

	var schemaName string
	if !n.Schema.ExplicitSchema {
		schemaName = n.AuthRole.Normalized()
	} else {
		schemaName = n.Schema.Schema()
	}

	// Ensure there aren't any name collisions.
	exists, err := p.schemaExists(params.ctx, db.ID, schemaName)
	if err != nil {
		return err
	}

	if exists {
		if n.IfNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateSchema, "schema %q already exists", schemaName)
	}

	// Check validity of the schema name.
	if err := schemadesc.IsSchemaNameValid(schemaName); err != nil {
		return err
	}

	// Ensure that the cluster version is high enough to create the schema.
	if !params.p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.VersionUserDefinedSchemas) {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			`creating schemas requires all nodes to be upgraded to %s`,
			clusterversion.VersionByKey(clusterversion.VersionUserDefinedSchemas))
	}

	// Create the ID.
	id, err := catalogkv.GenerateUniqueDescID(params.ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
	if err != nil {
		return err
	}

	// Inherit the parent privileges and filter out those which are not valid for
	// schemas.
	privs := protoutil.Clone(db.GetPrivileges()).(*descpb.PrivilegeDescriptor)
	for i := range privs.Users {
		privs.Users[i].Privileges &= privilege.SchemaPrivileges.ToBitField()
	}

	if !n.AuthRole.Undefined() {
		exists, err := p.RoleExists(params.ctx, n.AuthRole)
		if err != nil {
			return err
		}
		if !exists {
			return pgerror.Newf(pgcode.UndefinedObject, "role/user %q does not exist", n.AuthRole)
		}
		privs.SetOwner(n.AuthRole)
	} else {
		privs.SetOwner(params.SessionData().User())
	}

	// Create the SchemaDescriptor.
	desc := schemadesc.NewCreatedMutable(descpb.SchemaDescriptor{
		ParentID:   db.ID,
		Name:       schemaName,
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
	if err := p.createDescriptorWithID(
		params.ctx,
		catalogkeys.NewSchemaKey(db.ID, schemaName).Key(p.ExecCfg().Codec),
		id,
		desc,
		params.ExecCfg().Settings,
		tree.AsStringWithFQNames(n, params.Ann()),
	); err != nil {
		return err
	}
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCreateSchema,
		int32(desc.GetID()),
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
		struct {
			SchemaName string
			Owner      string
			User       string
		}{schemaName, privs.Owner().Normalized(), params.p.User().Normalized()},
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
