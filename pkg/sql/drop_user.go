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
	"bytes"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

type dropUserNode struct {
	ifExists bool
	names    func() ([]string, error)

	run dropUserRun
}

// DropUser drops a list of users.
// Privileges: DELETE on system.users.
func (p *planner) DropUser(ctx context.Context, n *tree.DropUser) (planNode, error) {
	tDesc, err := getTableDesc(ctx, p.txn, p.getVirtualTabler(), &tree.TableName{DatabaseName: "system", TableName: "users"})
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(tDesc, privilege.DELETE); err != nil {
		return nil, err
	}

	names, err := p.TypeAsStringArray(n.Names, "DROP USER")
	if err != nil {
		return nil, err
	}

	return &dropUserNode{
		ifExists: n.IfExists,
		names:    names,
	}, nil
}

// dropUserRun contains the run-time state of dropUserNode during local execution.
type dropUserRun struct {
	// The number of users deleted.
	numDeleted int
}

func (n *dropUserNode) Start(params runParams) error {
	names, err := n.names()
	if err != nil {
		return err
	}

	userNames := make(map[string]struct{})
	for _, name := range names {
		normalizedUsername, err := NormalizeAndValidateUsername(name)
		if err != nil {
			return err
		}
		userNames[normalizedUsername] = struct{}{}
	}

	var usedBy bytes.Buffer
	if err := forEachDatabaseDesc(params.ctx, params.p,
		func(db *sqlbase.DatabaseDescriptor) error {
			for _, u := range db.GetPrivileges().Users {
				if _, ok := userNames[u.User]; ok {
					if usedBy.Len() > 0 {
						usedBy.WriteString(", ")
					}
					tree.Name(db.Name).Format(&usedBy, tree.FmtSimple)
				}
			}
			return nil
		}); err != nil {
		return err
	}
	if err := forEachTableDescAll(params.ctx, params.p, "",
		func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			for _, u := range table.GetPrivileges().Users {
				if _, ok := userNames[u.User]; ok {
					tn := tree.TableName{
						DatabaseName: tree.Name(db.Name),
						TableName:    tree.Name(table.Name),
					}
					if usedBy.Len() > 0 {
						usedBy.WriteString(", ")
					}
					tn.Format(&usedBy, tree.FmtSimple)
				}
			}
			return nil
		}); err != nil {
		return err
	}
	if usedBy.Len() > 0 {
		var nameList bytes.Buffer
		for i, name := range names {
			if i > 0 {
				nameList.WriteString(", ")
			}
			tree.Name(name).Format(&nameList, tree.FmtSimple)
		}
		return pgerror.NewErrorf(pgerror.CodeGroupingError,
			"cannot drop user%s %s: grants still exist on %s",
			util.Pluralize(int64(nameList.Len())), nameList.String(), usedBy.String(),
		)
	}

	numDeleted := 0
	for normalizedUsername := range userNames {
		if normalizedUsername == security.RootUser {
			return errors.Errorf("user %s cannot be dropped", security.RootUser)
		}

		// TODO: Remove the privileges granted to the user.
		// Note: The current remove user from CLI just deletes the entry from system.users,
		// keeping the functionality same for now.
		internalExecutor := InternalExecutor{LeaseManager: params.p.LeaseMgr()}
		rowsAffected, err := internalExecutor.ExecuteStatementInTransaction(
			params.ctx,
			"drop-user",
			params.p.txn,
			"DELETE FROM system.users WHERE username=$1",
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		if rowsAffected == 0 && !n.ifExists {
			return errors.Errorf("user %s does not exist", normalizedUsername)
		}

		numDeleted += rowsAffected
	}

	n.run.numDeleted = numDeleted

	return nil
}

func (*dropUserNode) Next(runParams) (bool, error) { return false, nil }
func (*dropUserNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropUserNode) Close(context.Context)        {}

func (n *dropUserNode) FastPathResults() (int, bool) { return n.run.numDeleted, true }
