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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type renameDatabaseNode struct {
	dbDesc  *sqlbase.DatabaseDescriptor
	newName string
}

// RenameDatabase renames the database.
// Privileges: superuser, DROP on source database.
//   Notes: postgres requires superuser, db owner, or "CREATEDB".
//          mysql >= 5.1.23 does not allow database renames.
func (p *planner) RenameDatabase(ctx context.Context, n *tree.RenameDatabase) (planNode, error) {
	if n.Name == "" || n.NewName == "" {
		return nil, errEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf("RENAME DATABASE on current database")
	}

	if err := p.RequireAdminRole(ctx, "ALTER DATABASE ... RENAME"); err != nil {
		return nil, err
	}

	dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(n.Name), true /*required*/)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	if n.Name == n.NewName {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	return &renameDatabaseNode{
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

	// Check if any views depend on tables in the database. Because our views
	// are currently just stored as strings, they explicitly specify the database
	// name. Rather than trying to rewrite them with the changed DB name, we
	// simply disallow such renames for now.
	phyAccessor := p.PhysicalSchemaAccessor()
	lookupFlags := p.CommonLookupFlags(true /*required*/)
	// DDL statements bypass the cache.
	lookupFlags.AvoidCached = true
	schemas, err := p.Tables().getSchemasForDatabase(ctx, p.txn, dbDesc.ID)
	if err != nil {
		return err
	}
	for _, schema := range schemas {
		tbNames, err := phyAccessor.GetObjectNames(
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
		for i := range tbNames {
			objDesc, err := phyAccessor.GetObjectDesc(ctx, p.txn, p.ExecCfg().Settings,
				&tbNames[i], tree.ObjectLookupFlags{CommonLookupFlags: lookupFlags})
			if err != nil {
				return err
			}
			if objDesc == nil {
				continue
			}
			tbDesc := objDesc.TableDesc()
			if len(tbDesc.DependedOnBy) > 0 {
				viewDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, tbDesc.DependedOnBy[0].ID)
				if err != nil {
					return err
				}
				viewName := viewDesc.Name
				if dbDesc.ID != viewDesc.ParentID {
					var err error
					viewName, err = p.getQualifiedTableName(ctx, viewDesc)
					if err != nil {
						log.Warningf(ctx, "unable to retrieve fully-qualified name of view %d: %v",
							viewDesc.ID, err)
						msg := fmt.Sprintf("cannot rename database because a view depends on table %q", tbDesc.Name)
						return sqlbase.NewDependentObjectError(msg)
					}
				}
				msg := fmt.Sprintf("cannot rename database because view %q depends on table %q", viewName, tbDesc.Name)
				hint := fmt.Sprintf("you can drop %s instead.", viewName)
				return sqlbase.NewDependentObjectErrorWithHint(msg, hint)
			}
		}
	}

	return p.renameDatabase(ctx, dbDesc, n.newName)
}

func (n *renameDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameDatabaseNode) Close(context.Context)        {}
