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
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// RenameDatabase renames the database.
// Privileges: security.RootUser user, DROP on source database.
//   Notes: postgres requires superuser, db owner, or "CREATEDB".
//          mysql >= 5.1.23 does not allow database renames.
func (p *planner) RenameDatabase(ctx context.Context, n *tree.RenameDatabase) (planNode, error) {
	if n.Name == "" || n.NewName == "" {
		return nil, errEmptyDatabaseName
	}

	if err := p.RequireSuperUser("ALTER DATABASE ... RENAME"); err != nil {
		return nil, err
	}

	dbDesc, err := MustGetDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), string(n.Name))
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	if n.Name == n.NewName {
		// Noop.
		return &zeroNode{}, nil
	}

	// Check if any views depend on tables in the database. Because our views
	// are currently just stored as strings, they explicitly specify the database
	// name. Rather than trying to rewrite them with the changed DB name, we
	// simply disallow such renames for now.
	tbNames, err := getTableNames(ctx, p.txn, p.getVirtualTabler(), dbDesc, false)
	if err != nil {
		return nil, err
	}
	for i := range tbNames {
		tbDesc, err := getTableOrViewDesc(ctx, p.txn, p.getVirtualTabler(), &tbNames[i])
		if err != nil {
			return nil, err
		}
		if tbDesc == nil {
			continue
		}
		if len(tbDesc.DependedOnBy) > 0 {
			viewDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, tbDesc.DependedOnBy[0].ID)
			if err != nil {
				return nil, err
			}
			viewName := viewDesc.Name
			if dbDesc.ID != viewDesc.ParentID {
				var err error
				viewName, err = p.getQualifiedTableName(ctx, viewDesc)
				if err != nil {
					log.Warningf(ctx, "Unable to retrieve fully-qualified name of view %d: %v",
						viewDesc.ID, err)
					msg := fmt.Sprintf("cannot rename database because a view depends on table %q", tbDesc.Name)
					return nil, sqlbase.NewDependentObjectError(msg)
				}
			}
			msg := fmt.Sprintf("cannot rename database because view %q depends on table %q", viewName, tbDesc.Name)
			hint := fmt.Sprintf("you can drop %s instead.", viewName)
			return nil, sqlbase.NewDependentObjectErrorWithHint(msg, hint)
		}
	}

	if err := p.renameDatabase(ctx, dbDesc, string(n.NewName)); err != nil {
		return nil, err
	}
	return &zeroNode{}, nil
}
