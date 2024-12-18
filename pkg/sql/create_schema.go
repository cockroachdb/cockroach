// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type createSchemaNode struct {
	zeroInputPlanNode
	n *tree.CreateSchema
}

func (n *createSchemaNode) startExec(params runParams) error {
	return params.p.createUserDefinedSchema(params, n.n)
}

// CreateUserDefinedSchemaDescriptor constructs a mutable schema descriptor.
func CreateUserDefinedSchemaDescriptor(
	ctx context.Context,
	sessionData *sessiondata.SessionData,
	n *tree.CreateSchema,
	txn descs.Txn,
	descIDGenerator eval.DescIDGenerator,
	db catalog.DatabaseDescriptor,
	allocateID bool,
) (*schemadesc.Mutable, *catpb.PrivilegeDescriptor, error) {
	authRole, err := decodeusername.FromRoleSpec(
		sessionData, username.PurposeValidation, n.AuthRole,
	)
	if err != nil {
		return nil, nil, err
	}
	user := sessionData.User()
	var schemaName string
	if !n.Schema.ExplicitSchema {
		schemaName = authRole.Normalized()
	} else {
		schemaName = n.Schema.Schema()
	}

	// Ensure there aren't any name collisions.
	exists, schemaID, err := schemaExists(ctx, txn.KV(), txn.Descriptors(), db.GetID(), schemaName)
	if err != nil {
		return nil, nil, err
	}

	if exists {
		if n.IfNotExists {
			// Virtual schemas will return an InvalidID
			// and can't be in a dropping state.
			if schemaID != descpb.InvalidID {
				// Check if the object already exists in a dropped state
				sc, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).Get().Schema(ctx, schemaID)
				if err != nil || sc.SchemaKind() != catalog.SchemaUserDefined {
					return nil, nil, err
				}
				if sc.Dropped() {
					return nil, nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"schema %q is being dropped, try again later",
						schemaName)
				}
			}
			return nil, nil, nil
		}
		return nil, nil, sqlerrors.NewSchemaAlreadyExistsError(schemaName)
	}

	// Check validity of the schema name.
	if err := schemadesc.IsSchemaNameValid(schemaName); err != nil {
		return nil, nil, err
	}

	owner := user
	if !n.AuthRole.Undefined() {
		exists, err := RoleExists(ctx, txn, authRole)
		if err != nil {
			return nil, nil, err
		}
		if !exists {
			return nil, nil, sqlerrors.NewUndefinedUserError(authRole)
		}
		owner = authRole
	}

	desc, privs, err := CreateSchemaDescriptorWithPrivileges(
		ctx, descIDGenerator, db, schemaName, user, owner, allocateID,
	)
	if err != nil {
		return nil, nil, err
	}

	return desc, privs, nil
}

// CreateSchemaDescriptorWithPrivileges creates a new schema descriptor with
// the provided name and privileges.
func CreateSchemaDescriptorWithPrivileges(
	ctx context.Context,
	descIDGenerator eval.DescIDGenerator,
	db catalog.DatabaseDescriptor,
	schemaName string,
	user, owner username.SQLUsername,
	allocateID bool,
) (*schemadesc.Mutable, *catpb.PrivilegeDescriptor, error) {
	// Create the ID.
	var id descpb.ID
	var err error
	if allocateID {
		id, err = descIDGenerator.GenerateUniqueDescID(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	privs, err := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		db.GetDefaultPrivilegeDescriptor(),
		nil, /* schemaDefaultPrivilegeDescriptor */
		db.GetID(),
		user,
		privilege.Schemas,
	)
	if err != nil {
		return nil, nil, err
	}

	privs.SetOwner(owner)

	// Create the SchemaDescriptor.
	desc := schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		ParentID:   db.GetID(),
		Name:       schemaName,
		ID:         id,
		Privileges: privs,
		Version:    1,
	}).BuildCreatedMutableSchema()

	return desc, privs, nil
}

func (p *planner) createUserDefinedSchema(params runParams, n *tree.CreateSchema) error {
	if err := checkSchemaChangeEnabled(
		params.ctx,
		p.ExecCfg(),
		"CREATE SCHEMA",
	); err != nil {
		return err
	}

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

	db, err := p.Descriptors().MutableByName(p.txn).Database(params.ctx, dbName)
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

	desc, privs, err := CreateUserDefinedSchemaDescriptor(
		params.ctx, params.SessionData(), n, p.InternalSQLTxn(),
		p.extendedEvalCtx.DescIDGenerator, db, true, /* allocateID */
	)
	if err != nil {
		return err
	}

	// This is true when the schema exists and we are processing a
	// CREATE SCHEMA IF NOT EXISTS statement.
	if desc == nil {
		return nil
	}

	// Update the parent database with this schema information.
	db.AddSchemaToDatabase(desc.Name, descpb.DatabaseDescriptor_SchemaInfo{ID: desc.ID})

	if err := p.writeNonDropDatabaseChange(
		params.ctx, db,
		fmt.Sprintf("updating parent database %s for %s", db.GetName(), tree.AsStringWithFQNames(n, params.Ann())),
	); err != nil {
		return err
	}

	// Finally create the schema on disk.
	if err := p.createDescriptor(
		params.ctx,
		desc,
		tree.AsStringWithFQNames(n, params.Ann()),
	); err != nil {
		return err
	}

	qualifiedSchemaName, err := p.getQualifiedSchemaName(params.ctx, desc)
	if err != nil {
		return err
	}

	return params.p.logEvent(params.ctx,
		desc.GetID(),
		&eventpb.CreateSchema{
			SchemaName: qualifiedSchemaName.String(),
			Owner:      privs.Owner().Normalized(),
		})
}

func (*createSchemaNode) Next(runParams) (bool, error) { return false, nil }
func (*createSchemaNode) Values() tree.Datums          { return tree.Datums{} }
func (n *createSchemaNode) Close(ctx context.Context)  {}

// CreateSchema creates a schema.
func (p *planner) CreateSchema(ctx context.Context, n *tree.CreateSchema) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"CREATE SCHEMA",
	); err != nil {
		return nil, err
	}

	return &createSchemaNode{
		n: n,
	}, nil
}
