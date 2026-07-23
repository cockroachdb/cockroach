// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterSchemaNode struct {
	zeroInputPlanNode
	n    *tree.AlterSchema
	db   *dbdesc.Mutable
	desc *schemadesc.Mutable
}

// Use to satisfy the linter.
var _ planNode = &alterSchemaNode{n: nil}

func (p *planner) AlterSchema(ctx context.Context, n *tree.AlterSchema) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER SCHEMA",
	); err != nil {
		return nil, err
	}

	dbName := p.CurrentDatabase()
	if n.Schema.ExplicitCatalog {
		dbName = n.Schema.Catalog()
	}
	db, err := p.Descriptors().MutableByName(p.txn).Database(ctx, dbName)
	if err != nil {
		return nil, err
	}
	schema, err := p.Descriptors().ByName(p.txn).Get().Schema(ctx, db, string(n.Schema.SchemaName))
	if err != nil {
		return nil, err
	}
	// Explicitly disallow renaming public schema. In the future, we may want
	// to support this. In order to support it, we have to remove the logic that
	// automatically re-creates the public schema if it doesn't exist.
	_, isRename := n.Cmd.(*tree.AlterSchemaRename)
	if schema.GetName() == catconstants.PublicSchemaName && isRename {
		return nil, pgerror.Newf(pgcode.InvalidSchemaName, "cannot rename schema %q", n.Schema.String())
	}
	switch schema.SchemaKind() {
	case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
		return nil, pgerror.Newf(pgcode.InvalidSchemaName, "cannot modify schema %q", n.Schema.String())
	case catalog.SchemaUserDefined:
		desc, err := p.Descriptors().MutableByID(p.txn).Schema(ctx, schema.GetID())
		if err != nil {
			return nil, err
		}
		// The user must be the owner of the schema to modify it.
		hasOwnership, err := p.HasOwnership(ctx, desc)
		if err != nil {
			return nil, err
		}
		if !hasOwnership {
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege, "must be owner of schema %q", desc.Name)
		}
		sqltelemetry.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaAlter)
		return &alterSchemaNode{n: n, db: db, desc: desc}, nil
	default:
		return nil, errors.AssertionFailedf("unknown schema kind")
	}
}

func (n *alterSchemaNode) startExec(params runParams) error {
	// Exit early with an error if the schema is undergoing a declarative schema
	// change.
	if catalog.HasConcurrentDeclarativeSchemaChange(n.desc) {
		return scerrors.ConcurrentSchemaChangeError(n.desc)
	}

	switch t := n.n.Cmd.(type) {
	case *tree.AlterSchemaRename:
		newName := string(t.NewName)

		oldQualifiedSchemaName, err := params.p.getQualifiedSchemaName(params.ctx, n.desc)
		if err != nil {
			return err
		}

		// Ensure that the new name is a valid schema name.
		if err := schemadesc.IsSchemaNameValid(newName); err != nil {
			return err
		}

		// Check that there isn't a name collision with the new name.
		found, _, err := schemaExists(params.ctx, params.p.txn, params.p.Descriptors(), n.db.ID, newName)
		if err != nil {
			return err
		}
		if found {
			return sqlerrors.NewSchemaAlreadyExistsError(newName)
		}

		if err := maybeFailOnDependentDescInRename(
			params.ctx, params.p, n.db, n.desc, false /* withLeased */, catalog.Schema,
		); err != nil {
			return err
		}

		if err := params.p.renameSchema(
			params.ctx, n.db, n.desc, newName, tree.AsStringWithFQNames(n.n, params.Ann()),
		); err != nil {
			return err
		}

		newQualifiedSchemaName, err := params.p.getQualifiedSchemaName(params.ctx, n.desc)
		if err != nil {
			return err
		}

		return params.p.logEvent(params.ctx, n.desc.ID, &eventpb.RenameSchema{
			SchemaName:    oldQualifiedSchemaName.String(),
			NewSchemaName: newQualifiedSchemaName.String(),
		})
	case *tree.AlterSchemaOwner:
		newOwner, err := decodeusername.FromRoleSpec(
			params.p.SessionData(), username.PurposeValidation, t.Owner,
		)
		if err != nil {
			return err
		}
		return params.p.alterSchemaOwner(
			params.ctx, n.desc, newOwner, tree.AsStringWithFQNames(n.n, params.Ann()),
		)
	default:
		return errors.AssertionFailedf("unknown schema cmd %T", t)
	}
}

func (p *planner) alterSchemaOwner(
	ctx context.Context,
	scDesc *schemadesc.Mutable,
	newOwner username.SQLUsername,
	jobDescription string,
) error {
	oldOwner := scDesc.GetPrivileges().Owner()

	if err := p.checkCanAlterToNewOwner(ctx, scDesc, newOwner); err != nil {
		return err
	}

	// The user must also have CREATE privilege on the schema's database.
	parentDBDesc, err := p.Descriptors().MutableByID(p.txn).Desc(ctx, scDesc.GetParentID())
	if err != nil {
		return err
	}
	if err := p.CheckPrivilege(ctx, parentDBDesc, privilege.CREATE); err != nil {
		return err
	}

	if err := p.setNewSchemaOwner(ctx, parentDBDesc.(*dbdesc.Mutable), scDesc, newOwner); err != nil {
		return err
	}

	// If the owner we want to set to is the current owner, do a no-op.
	if newOwner == oldOwner {
		return nil
	}

	return p.writeSchemaDescChange(ctx, scDesc, jobDescription)
}

// setNewSchemaOwner handles setting a new schema owner.
// Called in ALTER SCHEMA and REASSIGN OWNED BY.
func (p *planner) setNewSchemaOwner(
	ctx context.Context,
	dbDesc *dbdesc.Mutable,
	scDesc *schemadesc.Mutable,
	newOwner username.SQLUsername,
) error {
	// Update the owner of the schema.
	privs := scDesc.GetPrivileges()
	privs.SetOwner(newOwner)

	qualifiedSchemaName := &tree.ObjectNamePrefix{
		CatalogName:     tree.Name(dbDesc.GetName()),
		SchemaName:      tree.Name(scDesc.GetName()),
		ExplicitCatalog: true,
		ExplicitSchema:  true,
	}

	return p.logEvent(ctx,
		scDesc.GetID(),
		&eventpb.AlterSchemaOwner{
			SchemaName: qualifiedSchemaName.String(),
			Owner:      newOwner.Normalized(),
		})
}

func (p *planner) renameSchema(
	ctx context.Context, db *dbdesc.Mutable, desc *schemadesc.Mutable, newName string, jobDesc string,
) error {
	oldNameKey := descpb.NameInfo{
		ParentID:       desc.GetParentID(),
		ParentSchemaID: desc.GetParentSchemaID(),
		Name:           desc.GetName(),
	}

	// Set the new name for the descriptor.
	oldName := oldNameKey.GetName()
	desc.SetName(newName)

	// Write the new name and remove the old name.
	b := p.txn.NewBatch()
	if err := p.renameNamespaceEntry(ctx, b, oldNameKey, desc); err != nil {
		return err
	}
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// Update the schema mapping in the parent database.

	// First, ensure that the new name isn't present, and that we have an entry
	// for the old name.
	_, oldPresent := db.Schemas[oldName]
	_, newPresent := db.Schemas[newName]
	if !oldPresent {
		return errors.AssertionFailedf(
			"old name %q not present in database schema mapping",
			oldName,
		)
	}
	if newPresent {
		return errors.AssertionFailedf(
			"new name %q already present in database schema mapping",
			newName,
		)
	}

	// Remove the old schema name.
	delete(db.Schemas, oldName)

	// Create an entry for the new schema name.
	db.Schemas[newName] = descpb.DatabaseDescriptor_SchemaInfo{ID: desc.ID}
	if err := p.writeNonDropDatabaseChange(
		ctx, db,
		fmt.Sprintf("updating parent database %s for %s", db.GetName(), jobDesc),
	); err != nil {
		return err
	}

	// Write the change to the schema itself.
	return p.writeSchemaDescChange(ctx, desc, jobDesc)
}

func (n *alterSchemaNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterSchemaNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterSchemaNode) Close(ctx context.Context)           {}
func (n *alterSchemaNode) ReadingOwnWrites()                   {}
