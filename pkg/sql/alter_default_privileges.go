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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
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
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.DefaultPrivileges) {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"version %v must be finalized to use default privileges",
			clusterversion.DefaultPrivileges)
	}

	// ALTER DEFAULT PRIVILEGES without specifying a schema alters the privileges
	// for the current database.
	database := p.CurrentDatabase()
	if n.Database != nil {
		database = n.Database.Normalize()
	}
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
	targetRoles, err := n.n.Roles.ToSQLUsernames()
	if err != nil {
		return err
	}

	if len(targetRoles) == 0 {
		targetRoles = append(targetRoles, params.p.User())
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

	if n.n.ForAllRoles {
		if err := params.p.RequireAdminRole(params.ctx, "ALTER DEFAULT PRIVILEGES"); err != nil {
			return err
		}
	} else {
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
	}

	if err := privilege.ValidatePrivileges(
		privileges,
		targetObjectToPrivilegeObject[objectType],
	); err != nil {
		return err
	}

	if n.dbDesc.GetDefaultPrivileges() == nil {
		n.dbDesc.SetDefaultPrivilegeDescriptor(catprivilege.MakeNewDefaultPrivilegeDescriptor())
	}

	defaultPrivs := n.dbDesc.GetMutableDefaultPrivilegeDescriptor()

	var roles []descpb.DefaultPrivilegesRole
	if n.n.ForAllRoles {
		roles = append(roles, descpb.DefaultPrivilegesRole{
			ForAllRoles: true,
		})
	} else {
		roles = make([]descpb.DefaultPrivilegesRole, len(targetRoles))
		for i, role := range targetRoles {
			roles[i] = descpb.DefaultPrivilegesRole{
				Role: role,
			}
		}
	}

	var events []eventLogEntry
	granteeSQLUsernames, err = grantees.ToSQLUsernames()
	if err != nil {
		return err
	}
	for _, role := range roles {
		if n.n.IsGrant {
			defaultPrivs.GrantDefaultPrivileges(
				role, privileges, granteeSQLUsernames, objectType,
			)
		} else {
			defaultPrivs.RevokeDefaultPrivileges(
				role, privileges, granteeSQLUsernames, objectType,
			)
		}

		eventDetails := eventpb.CommonSQLPrivilegeEventDetails{}
		if n.n.IsGrant {
			eventDetails.GrantedPrivileges = privileges.SortedNames()
		} else {
			eventDetails.RevokedPrivileges = privileges.SortedNames()
		}
		event := eventpb.AlterDefaultPrivileges{
			CommonSQLPrivilegeEventDetails: eventDetails,
			DatabaseName:                   n.dbDesc.GetName(),
		}
		if n.n.ForAllRoles {
			event.ForAllRoles = true
		} else {
			event.RoleName = role.Role.Normalized()
		}

		events = append(events, eventLogEntry{
			targetID: int32(n.dbDesc.GetID()),
			event:    &event,
		})
	}

	if err := params.p.writeNonDropDatabaseChange(
		params.ctx, n.dbDesc, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	return params.p.logEvents(params.ctx, events...)
}
