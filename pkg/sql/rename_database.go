// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
		return nil, pgerror.NewDangerousStatementErrorf("RENAME DATABASE on current database")
	}

	if err := p.RequireSuperUser(ctx, "ALTER DATABASE ... RENAME"); err != nil {
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
	lookupFlags.avoidCached = true
	tbNames, err := phyAccessor.GetObjectNames(
		ctx, p.txn, dbDesc, tree.PublicSchema, DatabaseListFlags{
			CommonLookupFlags: lookupFlags,
			explicitPrefix:    true,
		})
	if err != nil {
		return err
	}
	lookupFlags.required = false
	for i := range tbNames {
		objDesc, _, err := phyAccessor.GetObjectDesc(ctx, p.txn, &tbNames[i],
			ObjectLookupFlags{CommonLookupFlags: lookupFlags})
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

	return p.renameDatabase(ctx, dbDesc, n.newName)
}

func (n *renameDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameDatabaseNode) Close(context.Context)        {}
