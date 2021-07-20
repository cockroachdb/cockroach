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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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

	dbDesc *dbdesc.Mutable
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
		for _, role := range n.n.Roles {
			user, err := security.MakeSQLUsernameFromUserInput(string(role), security.UsernameValidation)
			if err != nil {
				return err
			}
			targetRoles = append(targetRoles, user)
		}
	}

	if err := params.p.validateRoles(params.ctx, targetRoles, false /* isPublicValid */); err != nil {
		return err
	}

	privileges := n.n.Grant.Privileges
	grantees := n.n.Grant.Grantees
	objectType := n.n.Grant.Target
	if !n.n.IsGrant {
		privileges = n.n.Revoke.Privileges
		grantees = n.n.Revoke.Grantees
		objectType = n.n.Revoke.Target
	}

	granteeSQLUsernames := make([]security.SQLUsername, len(grantees))
	for i, grantee := range grantees {
		user, err := security.MakeSQLUsernameFromUserInput(string(grantee), security.UsernameValidation)
		if err != nil {
			return err
		}
		granteeSQLUsernames[i] = user
	}

	if err := params.p.validateRoles(params.ctx, granteeSQLUsernames, true /* isPublicValid */); err != nil {
		return err
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

	if n.dbDesc.GetDefaultPrivileges() == nil {
		n.dbDesc.SetDefaultPrivilegeDescriptor(descpb.InitDefaultPrivilegeDescriptor())
	}

	defaultPrivs := n.dbDesc.GetDefaultPrivileges()

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

	return params.p.writeNonDropDatabaseChange(
		params.ctx, n.dbDesc, tree.AsStringWithFQNames(n.n, params.Ann()),
	)
}
