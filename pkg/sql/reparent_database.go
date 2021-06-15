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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type reparentDatabaseNode struct {
	n         *tree.ReparentDatabase
	db        *dbdesc.Mutable
	newParent *dbdesc.Mutable
}

func (p *planner) ReparentDatabase(
	ctx context.Context, n *tree.ReparentDatabase,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"REPARENT DATABASE",
	); err != nil {
		return nil, err
	}

	// We'll only allow the admin to perform this reparenting action.
	if err := p.RequireAdminRole(ctx, "ALTER DATABASE ... CONVERT TO SCHEMA"); err != nil {
		return nil, err
	}

	// Ensure that the cluster version is high enough to create the schema.
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.UserDefinedSchemas) {
		return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			`creating schemas requires all nodes to be upgraded to %s`,
			clusterversion.ByKey(clusterversion.UserDefinedSchemas))
	}

	if string(n.Name) == p.CurrentDatabase() {
		return nil, pgerror.DangerousStatementf("CONVERT TO SCHEMA on current database")
	}

	sqltelemetry.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaReparentDatabase)

	db, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, string(n.Name),
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	parent, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, string(n.Parent),
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	// Ensure that this database wouldn't collide with a name under the new database.
	exists, _, err := schemaExists(ctx, p.txn, p.ExecCfg().Codec, parent.ID, db.Name)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, sqlerrors.NewSchemaAlreadyExistsError(db.Name)
	}

	// Ensure the database has a valid schema name.
	if err := schemadesc.IsSchemaNameValid(db.Name); err != nil {
		return nil, err
	}

	// We can't reparent a database that has any child schemas other than public.
	if len(db.Schemas) > 0 {
		return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "cannot convert database with schemas into schema")
	}

	return &reparentDatabaseNode{
		n:         n,
		db:        db,
		newParent: parent,
	}, nil
}

func (n *reparentDatabaseNode) startExec(params runParams) error {
	ctx, p, codec := params.ctx, params.p, params.ExecCfg().Codec

	// Make a new schema corresponding to the target db.
	id, err := catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, codec)
	if err != nil {
		return err
	}

	// Not all Privileges on databases are valid on schemas.
	// Remove any privileges that are not valid for schemas.
	schemaPrivs := privilege.GetValidPrivilegesForObject(privilege.Schema).ToBitField()
	privs := n.db.GetPrivileges()
	for i, u := range privs.Users {
		// Remove privileges that are valid for databases but not for schemas.
		privs.Users[i].Privileges = u.Privileges & schemaPrivs
	}

	schema := schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		ParentID:   n.newParent.ID,
		Name:       n.db.Name,
		ID:         id,
		Privileges: protoutil.Clone(n.db.Privileges).(*descpb.PrivilegeDescriptor),
		Version:    1,
	}).BuildCreatedMutable()
	// Add the new schema to the parent database's name map.
	if n.newParent.Schemas == nil {
		n.newParent.Schemas = make(map[string]descpb.DatabaseDescriptor_SchemaInfo)
	}
	n.newParent.Schemas[n.db.Name] = descpb.DatabaseDescriptor_SchemaInfo{
		ID:      schema.GetID(),
		Dropped: false,
	}

	if err := p.createDescriptorWithID(
		ctx,
		catalogkeys.MakeSchemaNameKey(p.ExecCfg().Codec, n.newParent.ID, schema.GetName()),
		id,
		schema,
		params.ExecCfg().Settings,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	b := p.txn.NewBatch()

	// Get all objects under the target database.
	objNames, _, err := resolver.GetObjectNamesAndIDs(ctx, p.txn, p, codec, n.db, tree.PublicSchema, true /* explicitPrefix */)
	if err != nil {
		return err
	}

	// For each object, adjust the ParentID and ParentSchemaID fields to point
	// to the new parent DB and schema.
	for _, objName := range objNames {
		// First try looking up objName as a table.
		found, _, desc, err := p.LookupObject(
			ctx,
			tree.ObjectLookupFlags{
				// Note we set required to be false here in order to not error out
				// if we don't find the object.
				CommonLookupFlags: tree.CommonLookupFlags{
					Required:       false,
					RequireMutable: true,
					IncludeOffline: true,
				},
				DesiredObjectKind: tree.TableObject,
			},
			objName.Catalog(),
			objName.Schema(),
			objName.Object(),
		)
		if err != nil {
			return err
		}
		if found {
			// Remap the ID's on the table.
			tbl, ok := desc.(*tabledesc.Mutable)
			if !ok {
				return errors.AssertionFailedf("%q was not a Mutable", objName.Object())
			}

			// If this table has any dependents, then we can't proceed (similar to the
			// restrictions around renaming tables). See #10083.
			if len(tbl.GetDependedOnBy()) > 0 {
				var names []string
				const errStr = "cannot convert database %q into schema because %q has dependent objects"
				tblName, err := p.getQualifiedTableName(ctx, tbl)
				if err != nil {
					return errors.Wrapf(err, errStr, n.db.Name, tbl.Name)
				}
				for _, ref := range tbl.GetDependedOnBy() {
					dep, err := p.Descriptors().GetMutableTableVersionByID(ctx, ref.ID, p.txn)
					if err != nil {
						return errors.Wrapf(err, errStr, n.db.Name, tblName.FQString())
					}
					fqName, err := p.getQualifiedTableName(ctx, dep)
					if err != nil {
						return errors.Wrapf(err, errStr, n.db.Name, dep.Name)
					}
					names = append(names, fqName.FQString())
				}
				return sqlerrors.NewDependentObjectErrorf(
					"could not convert database %q into schema because %q has dependent objects %v",
					n.db.Name,
					tblName.FQString(),
					names,
				)
			}

			tbl.AddDrainingName(descpb.NameInfo{
				ParentID:       tbl.ParentID,
				ParentSchemaID: tbl.GetParentSchemaID(),
				Name:           tbl.Name,
			})
			tbl.ParentID = n.newParent.ID
			tbl.UnexposedParentSchemaID = schema.GetID()
			objKey := catalogkeys.EncodeNameKey(codec, tbl)
			b.CPut(objKey, tbl.GetID(), nil /* expected */)
			if err := p.writeSchemaChange(ctx, tbl, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann())); err != nil {
				return err
			}
		} else {
			// If we couldn't resolve objName as a table, try a type.
			found, _, desc, err := p.LookupObject(
				ctx,
				tree.ObjectLookupFlags{
					CommonLookupFlags: tree.CommonLookupFlags{
						Required:       true,
						RequireMutable: true,
						IncludeOffline: true,
					},
					DesiredObjectKind: tree.TypeObject,
				},
				objName.Catalog(),
				objName.Schema(),
				objName.Object(),
			)
			if err != nil {
				return err
			}
			// If we couldn't find the object at all, then continue.
			if !found {
				continue
			}
			// Remap the ID's on the type.
			typ, ok := desc.(*typedesc.Mutable)
			if !ok {
				return errors.AssertionFailedf("%q was not a Mutable", objName.Object())
			}
			typ.AddDrainingName(descpb.NameInfo{
				ParentID:       typ.ParentID,
				ParentSchemaID: typ.ParentSchemaID,
				Name:           typ.Name,
			})
			typ.ParentID = n.newParent.ID
			typ.ParentSchemaID = schema.GetID()
			objKey := catalogkeys.EncodeNameKey(codec, typ)
			b.CPut(objKey, typ.ID, nil /* expected */)
			if err := p.writeTypeSchemaChange(ctx, typ, tree.AsStringWithFQNames(n.n, params.Ann())); err != nil {
				return err
			}
		}
	}

	// Delete the public schema namespace entry for this database. Per our check
	// during initialization, this is the only schema present under n.db.
	b.Del(catalogkeys.MakePublicSchemaNameKey(codec, n.db.ID))

	// This command can only be run when database leasing is supported, so we don't
	// have to handle the case where it isn't.
	n.db.AddDrainingName(descpb.NameInfo{
		ParentID:       keys.RootNamespaceID,
		ParentSchemaID: keys.RootNamespaceID,
		Name:           n.db.Name,
	})
	n.db.State = descpb.DescriptorState_DROP
	if err := p.writeDatabaseChangeToBatch(ctx, n.db, b); err != nil {
		return err
	}

	// Update the new parent database with the new schema map.
	if err := p.writeDatabaseChangeToBatch(ctx, n.newParent, b); err != nil {
		return err
	}

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	if err := p.createDropDatabaseJob(
		ctx,
		n.db.ID,
		nil, /* schemasToDrop */
		nil, /* tableDropDetails */
		nil, /* typesToDrop */
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Log Rename Database event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	return p.logEvent(ctx,
		n.db.ID,
		&eventpb.ConvertToSchema{
			DatabaseName:      n.db.Name,
			NewDatabaseParent: n.newParent.Name,
		})
}

func (n *reparentDatabaseNode) Next(params runParams) (bool, error) { return false, nil }
func (n *reparentDatabaseNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *reparentDatabaseNode) Close(ctx context.Context)           {}
func (n *reparentDatabaseNode) ReadingOwnWrites()                   {}
