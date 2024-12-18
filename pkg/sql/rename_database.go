// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type renameDatabaseNode struct {
	zeroInputPlanNode
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
		return nil, sqlerrors.ErrEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf("RENAME DATABASE on current database")
	}

	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, string(n.Name))
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
		hasCreateDB, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.CREATEDB)
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
	schemas, err := p.Descriptors().GetSchemasForDatabase(ctx, p.txn, dbDesc)
	if err != nil {
		return err
	}
	for _, schema := range schemas {
		sc, err := p.Descriptors().ByName(p.txn).Get().Schema(ctx, dbDesc, schema)
		if err != nil {
			return err
		}
		if err := maybeFailOnDependentDescInRename(ctx, p, dbDesc, sc, !p.skipDescriptorCache, catalog.Database); err != nil {
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
		depFnName := tree.MakeQualifiedRoutineName(dbName, scName, t.GetName())
		return depFnName.String(), nil
	default:
		return "", errors.AssertionFailedf("expected only function or table descriptor, but got %s", t.DescriptorType())
	}
}

func maybeFailOnDependentDescInRename(
	ctx context.Context,
	p *planner,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	withLeased bool,
	renameDescType catalog.DescriptorType,
) error {
	_, ids, err := p.GetObjectNamesAndIDs(ctx, db, sc)
	if err != nil {
		return err
	}
	var b descs.ByIDGetterBuilder
	if withLeased {
		b = p.Descriptors().ByIDWithLeased(p.txn)
	} else {
		b = p.Descriptors().ByIDWithoutLeased(p.txn)
	}
	descs, err := b.WithoutNonPublic().Get().Descs(ctx, ids)
	if err != nil {
		return err
	}
	for _, desc := range descs {
		tbDesc, ok := desc.(catalog.TableDescriptor)
		if !ok {
			continue
		}

		// Since we now only reference sequences with IDs, it's always safe to
		// rename a db or schema containing the sequence. There is one exception
		// being tracked with issue #87509.
		if tbDesc.IsSequence() {
			continue
		}

		if err := tbDesc.ForeachDependedOnBy(func(dependedOn *descpb.TableDescriptor_Reference) error {
			dependentDesc, err := p.Descriptors().MutableByID(p.txn).Desc(ctx, dependedOn.ID)
			if err != nil {
				return err
			}

			tbTableName := tree.MakeTableNameWithSchema(
				tree.Name(db.GetName()),
				tree.Name(sc.GetName()),
				tree.Name(tbDesc.GetName()),
			)
			dependentDescQualifiedString, err := getQualifiedDependentObjectName(
				ctx, p, db.GetName(), sc.GetName(), tbDesc, dependentDesc,
			)
			if err != nil {
				return err
			}
			// Otherwise, we default to the view error message.
			return errors.WithHintf(sqlerrors.NewDependentObjectErrorf(
				"cannot rename %s because relation %q depends on relation %q",
				renameDescType,
				dependentDescQualifiedString,
				tbTableName.String(),
			), "consider dropping %q first", dependentDescQualifiedString)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (n *renameDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameDatabaseNode) Close(context.Context)        {}
