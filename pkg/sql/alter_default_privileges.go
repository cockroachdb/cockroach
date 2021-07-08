// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

var targetObjectToPrivilegeObject = map[tree.AlterDefaultPrivilegesTargetObject]privilege.ObjectType{
	tree.Tables:    privilege.Table,
	tree.Sequences: privilege.Table,
	tree.Types:     privilege.Type,
	tree.Schemas:   privilege.Schema,
}

type alterDefaultPrivilegesNode struct {
	n *tree.AlterDefaultPrivileges

	// Only one of dbDesc or schemaDesc should be populated.
	dbDesc     *dbdesc.Mutable
	schemaDesc *schemadesc.Mutable
}

func (n *alterDefaultPrivilegesNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDefaultPrivilegesNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDefaultPrivilegesNode) Close(context.Context)        {}

func (p *planner) alterDefaultPrivileges(
	ctx context.Context, n *tree.AlterDefaultPrivileges,
) (planNode, error) {
	// ALTER DEFAULT PRIVILEGES without specifying a schema alters the privileges
	// for the current database.
	database := p.CurrentDatabase()
	dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, database,
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	if len(n.Schemas) > 0 {
		return nil, unimplemented.NewWithIssue(
			67376, "ALTER DEFAULT PRIVILEGES IN SCHEMA not implemented",
		)
	}

	return &alterDefaultPrivilegesNode{
		n:      n,
		dbDesc: dbDesc,
	}, err
}

func (n *alterDefaultPrivilegesNode) startExec(params runParams) error {
	var targetRoles []security.SQLUsername
	if len(n.n.Roles) == 0 {
		targetRoles = append(targetRoles, params.p.User())
	} else {
		targetRoles = n.n.Roles
	}

	users, err := params.p.GetAllRoles(params.ctx)
	if err != nil {
		return err
	}

	for _, targetRole := range targetRoles {
		if _, found := users[targetRole]; !found {
			return pgerror.Newf(pgcode.UndefinedObject,
				"role %s does not exist", targetRole.Normalized())
		}
	}

	privileges := n.n.Grant.Privileges
	grantees := n.n.Grant.Grantees
	objectType := n.n.Grant.Target
	if !n.n.IsGrant {
		privileges = n.n.Revoke.Privileges
		grantees = n.n.Revoke.Grantees
		objectType = n.n.Revoke.Target
	}

	granteesSQLUsername := make([]security.SQLUsername, len(grantees))
	for i, grantee := range grantees {
		granteesSQLUsername[i] = security.MakeSQLUsernameFromPreNormalizedString(string(grantee))
	}

	for _, grantee := range granteesSQLUsername {
		if _, found := users[grantee]; !found {
			return pgerror.Newf(pgcode.UndefinedObject,
				"role %s does not exist", grantee.Normalized())
		}
	}

	// You can change default privileges only for objects that will be created
	// by yourself or by roles that you are a member of.
	for _, targetRole := range targetRoles {
		if targetRole != params.p.User() {
			memberOf, err := params.p.MemberOfWithAdminOption(params.ctx, params.p.User())
			if err != nil {
				return err
			}

			if _, found := memberOf[targetRole]; !found {
				return pgerror.Newf(pgcode.InsufficientPrivilege,
					"must be a member of %s", targetRole.Normalized())
			}
		}
	}

	if err := privilege.ValidatePrivileges(
		privileges,
		targetObjectToPrivilegeObject[objectType],
	); err != nil {
		return err
	}

	defaultPrivs := n.dbDesc.GetDefaultPrivilegeDescriptor()

	for _, targetRole := range targetRoles {
		if n.n.IsGrant {
			defaultPrivs.GrantDefaultPrivileges(
				targetRole, privileges, grantees, objectType,
			)
		} else {
			defaultPrivs.RevokeDefaultPrivileges(
				targetRole, privileges, grantees, objectType,
			)
		}
	}

	err = params.p.writeNonDropDatabaseChange(
		params.ctx, n.dbDesc, tree.AsStringWithFQNames(n.n, params.Ann()),
	)
	if err != nil {
		return err
	}

	if err := params.p.createNonDropDatabaseChangeJob(params.ctx, n.dbDesc.ID,
		fmt.Sprintf("updating privileges for database %d", n.dbDesc.ID)); err != nil {
		return err
	}
	return nil
}
