// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type renameDatabaseNode struct {
	n       *tree.RenameDatabase
	dbDesc  *dbdesc.Mutable
	newName string
}

// RenameDatabase renames the database.
// Privileges: superuser + DROP or ownership + CREATEDB privileges
//
//	Notes: mysql >= 5.1.23 does not allow database renames.
func (p *planner) RenameDatabase(ctx context.Context, n *tree.RenameDatabase) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	if n.Name == "" || n.NewName == "" {
		return nil, errEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf("RENAME DATABASE on current database")
	}

	dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, string(n.Name),
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}

	if hasAdmin {
		// The user must have DROP privileges on the database. This prevents a
		// superuser from renaming, e.g., the system database.
		if err := p.CheckPrivilege(ctx, dbDesc, privilege.DROP); err != nil {
			return nil, err
		}
	} else {
		// Non-superusers must be the owner and have the CREATEDB privilege.
		hasOwnership, err := p.HasOwnership(ctx, dbDesc)
		if err != nil {
			return nil, err
		}
		if !hasOwnership {
			return nil, pgerror.Newf(
				pgcode.InsufficientPrivilege, "must be owner of database %s", n.Name)
		}
		hasCreateDB, err := p.HasRoleOption(ctx, roleoption.CREATEDB)
		if err != nil {
			return nil, err
		}
		if !hasCreateDB {
			return nil, pgerror.New(
				pgcode.InsufficientPrivilege, "permission denied to rename database")
		}
	}

	if n.Name == n.NewName {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	return &renameDatabaseNode{
		n:       n,
		dbDesc:  dbDesc,
		newName: string(n.NewName),
	}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because RENAME DATABASE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *renameDatabaseNode) ReadingOwnWrites() {}

func (n *renameDatabaseNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	dbDesc := n.dbDesc

	// Check if any other tables depend on tables in the database.
	// Because our views and sequence defaults are currently just stored as
	// strings, they (may) explicitly specify the database name.
	// Rather than trying to rewrite them with the changed DB name, we
	// simply disallow such renames for now.
	// See #34416.
	lookupFlags := p.CommonLookupFlagsRequired()
	// DDL statements bypass the cache.
	lookupFlags.AvoidLeased = true
	schemas, err := p.Descriptors().GetSchemasForDatabase(ctx, p.txn, dbDesc)
	if err != nil {
		return err
	}
	for _, schema := range schemas {
		if err := maybeFailOnDependentDescInRename(ctx, p, dbDesc, schema, lookupFlags, catalog.Database); err != nil {
			return err
		}
	}

	if err := p.renameDatabase(ctx, dbDesc, n.newName, tree.AsStringWithFQNames(n.n, params.Ann())); err != nil {
		return err
	}

	// Log Rename Database event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	return p.logEvent(ctx,
		n.dbDesc.GetID(),
		&eventpb.RenameDatabase{
			DatabaseName:    n.n.Name.String(),
			NewDatabaseName: n.newName,
		})
}

func getQualifiedDependentObjectName(
	ctx context.Context,
	p *planner,
	dbName string,
	scName string,
	desc catalog.TableDescriptor,
	depDesc catalog.Descriptor,
) (string, error) {
	tbTableName := tree.MakeTableNameWithSchema(
		tree.Name(dbName),
		tree.Name(scName),
		tree.Name(desc.GetName()),
	)

	if desc.GetParentID() != depDesc.GetParentID() || desc.GetParentSchemaID() != depDesc.GetParentSchemaID() {
		var descFQName tree.ObjectName
		var err error
		switch t := depDesc.(type) {
		case catalog.TableDescriptor:
			descFQName, err = p.getQualifiedTableName(ctx, t)
		case catalog.FunctionDescriptor:
			descFQName, err = p.getQualifiedFunctionName(ctx, t)
		default:
			return "", errors.AssertionFailedf("expected only function or table descriptor, but got %s", t.DescriptorType())
		}
		if err != nil {
			log.Warningf(ctx, "unable to retrieve fully-qualified name of %s (id: %d): %v", tbTableName.String(), depDesc.GetID(), err)
			return "", sqlerrors.NewDependentObjectErrorf(
				"cannot rename database because a relation depends on relation %q",
				tbTableName.String(),
			)
		}

		return descFQName.FQString(), nil
	}

	switch t := depDesc.(type) {
	case catalog.TableDescriptor:
		depTblName := tree.MakeTableNameWithSchema(tree.Name(dbName), tree.Name(scName), tree.Name(t.GetName()))
		return depTblName.String(), nil
	case catalog.FunctionDescriptor:
		depFnName := tree.MakeQualifiedFunctionName(dbName, scName, t.GetName())
		return depFnName.String(), nil
	default:
		return "", errors.AssertionFailedf("expected only function or table descriptor, but got %s", t.DescriptorType())
	}
}

func maybeFailOnDependentDescInRename(
	ctx context.Context,
	p *planner,
	dbDesc catalog.DatabaseDescriptor,
	schema string,
	lookupFlags tree.CommonLookupFlags,
	renameDescType catalog.DescriptorType,
) error {
	tbNames, _, err := p.Descriptors().GetObjectNamesAndIDs(
		ctx,
		p.txn,
		dbDesc,
		schema,
		tree.DatabaseListFlags{
			CommonLookupFlags: lookupFlags,
			ExplicitPrefix:    true,
		},
	)
	if err != nil {
		return err
	}
	lookupFlags.Required = false
	// TODO(ajwerner): Make this do something better than one-at-a-time lookups
	// followed by catalogkv reads on each dependency.
	for i := range tbNames {
		found, tbDesc, err := p.Descriptors().GetImmutableTableByName(
			ctx, p.txn, &tbNames[i], tree.ObjectLookupFlags{CommonLookupFlags: lookupFlags},
		)
		if err != nil {
			return err
		}
		if !found {
			continue
		}

		// Since we now only reference sequences with IDs, it's always safe to
		// rename a db or schema containing the sequence. There is one exception
		// being tracked with issue #87509.
		if tbDesc.IsSequence() {
			continue
		}

		if err := tbDesc.ForeachDependedOnBy(func(dependedOn *descpb.TableDescriptor_Reference) error {
			dependentDesc, err := p.Descriptors().GetMutableDescriptorByID(ctx, p.txn, dependedOn.ID)
			if err != nil {
				return err
			}

			tbTableName := tree.MakeTableNameWithSchema(
				tree.Name(dbDesc.GetName()),
				tree.Name(schema),
				tree.Name(tbDesc.GetName()),
			)
			dependentDescQualifiedString, err := getQualifiedDependentObjectName(
				ctx, p, dbDesc.GetName(), schema, tbDesc, dependentDesc,
			)
			if err != nil {
				return err
			}
			depErr := sqlerrors.NewDependentObjectErrorf(
				"cannot rename %s because relation %q depends on relation %q",
				renameDescType,
				dependentDescQualifiedString,
				tbTableName.String(),
			)

			// Otherwise, we default to the view error message.
			return errors.WithHintf(depErr,
				"you can drop %q instead", dependentDescQualifiedString)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (n *renameDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameDatabaseNode) Close(context.Context)        {}
