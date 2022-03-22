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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

var targetObjectToPrivilegeObject = map[tree.AlterDefaultPrivilegesTargetObject]privilege.ObjectType{
	tree.Tables:    privilege.Table,
	tree.Sequences: privilege.Table,
	tree.Types:     privilege.Type,
	tree.Schemas:   privilege.Schema,
}

type alterDefaultPrivilegesNode struct {
	n *tree.AlterDefaultPrivileges

	dbDesc      *dbdesc.Mutable
	schemaDescs []*schemadesc.Mutable
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
	if n.Database != nil {
		database = n.Database.Normalize()
	}
	dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, database,
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	objectType := n.Grant.Target
	if !n.IsGrant {
		objectType = n.Revoke.Target
	}

	if len(n.Schemas) > 0 && objectType == tree.Schemas {
		return nil, pgerror.WithCandidateCode(errors.New(
			"cannot use IN SCHEMA clause when using GRANT/REVOKE ON SCHEMAS"),
			pgcode.InvalidGrantOperation,
		)
	}

	var schemaDescs []*schemadesc.Mutable
	for _, sc := range n.Schemas {
		schemaDesc, err := p.Descriptors().GetMutableSchemaByName(ctx, p.txn, dbDesc, sc.Schema(), tree.SchemaLookupFlags{Required: true})
		if err != nil {
			return nil, err
		}
		schemaDescs = append(schemaDescs, schemaDesc.(*schemadesc.Mutable))
	}

	return &alterDefaultPrivilegesNode{
		n:           n,
		dbDesc:      dbDesc,
		schemaDescs: schemaDescs,
	}, err
}

func (n *alterDefaultPrivilegesNode) startExec(params runParams) error {
	if (n.n.Grant.WithGrantOption || n.n.Revoke.GrantOptionFor) &&
		!params.p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.ValidateGrantOption) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"version %v must be finalized to use grant options",
			clusterversion.ByKey(clusterversion.ValidateGrantOption))
	}
	targetRoles, err := n.n.Roles.ToSQLUsernames(params.SessionData(), security.UsernameValidation)
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
	grantOption := n.n.Grant.WithGrantOption
	if !n.n.IsGrant {
		privileges = n.n.Revoke.Privileges
		grantees = n.n.Revoke.Grantees
		objectType = n.n.Revoke.Target
		grantOption = n.n.Revoke.GrantOptionFor
	}

	granteeSQLUsernames, err := grantees.ToSQLUsernames(params.p.SessionData(), security.UsernameValidation)
	if err != nil {
		return err
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

	if len(n.schemaDescs) == 0 {
		return n.alterDefaultPrivilegesForDatabase(params, targetRoles, objectType, grantees, privileges, grantOption)
	}
	return n.alterDefaultPrivilegesForSchemas(params, targetRoles, objectType, grantees, privileges, grantOption)
}

func (n *alterDefaultPrivilegesNode) alterDefaultPrivilegesForSchemas(
	params runParams,
	targetRoles []security.SQLUsername,
	objectType tree.AlterDefaultPrivilegesTargetObject,
	grantees tree.RoleSpecList,
	privileges privilege.List,
	grantOption bool,
) error {
	var events []eventLogEntry
	for _, schemaDesc := range n.schemaDescs {
		if schemaDesc.GetDefaultPrivileges() == nil {
			schemaDesc.SetDefaultPrivilegeDescriptor(catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_SCHEMA))
		}

		defaultPrivs := schemaDesc.GetMutableDefaultPrivilegeDescriptor()

		var roles []catpb.DefaultPrivilegesRole
		if n.n.ForAllRoles {
			roles = append(roles, catpb.DefaultPrivilegesRole{
				ForAllRoles: true,
			})
		} else {
			roles = make([]catpb.DefaultPrivilegesRole, len(targetRoles))
			for i, role := range targetRoles {
				roles[i] = catpb.DefaultPrivilegesRole{
					Role: role,
				}
			}
		}

		granteeSQLUsernames, err := grantees.ToSQLUsernames(params.SessionData(), security.UsernameValidation)
		if err != nil {
			return err
		}

		grantPresent, allPresent := false, false
		for _, priv := range privileges {
			grantPresent = grantPresent || priv == privilege.GRANT
			allPresent = allPresent || priv == privilege.ALL
		}
		if params.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.ValidateGrantOption) {
			noticeMessage := ""
			// we only output the message for ALL privilege if it is being granted without the WITH GRANT OPTION flag
			// if GRANT privilege is involved, we must always output the message
			if allPresent && n.n.IsGrant && !grantOption {
				noticeMessage = "grant options were automatically applied but this behavior is deprecated"
			} else if grantPresent {
				noticeMessage = "the GRANT privilege is deprecated"
			}
			if len(noticeMessage) > 0 {
				params.p.BufferClientNotice(
					params.ctx,
					errors.WithHint(
						pgnotice.Newf("%s", noticeMessage),
						"please use WITH GRANT OPTION",
					),
				)
			}
		}

		for _, role := range roles {
			if n.n.IsGrant {
				defaultPrivs.GrantDefaultPrivileges(
					role, privileges, granteeSQLUsernames, objectType, grantOption, grantPresent || allPresent,
				)
			} else {
				defaultPrivs.RevokeDefaultPrivileges(
					role, privileges, granteeSQLUsernames, objectType, grantOption, grantPresent || allPresent,
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
				SchemaName:                     schemaDesc.GetName(),
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

			if err := params.p.writeSchemaDescChange(
				params.ctx, schemaDesc, tree.AsStringWithFQNames(n.n, params.Ann()),
			); err != nil {
				return err
			}
		}
	}

	return params.p.logEvents(params.ctx, events...)
}

func (n *alterDefaultPrivilegesNode) alterDefaultPrivilegesForDatabase(
	params runParams,
	targetRoles []security.SQLUsername,
	objectType tree.AlterDefaultPrivilegesTargetObject,
	grantees tree.RoleSpecList,
	privileges privilege.List,
	grantOption bool,
) error {
	if n.dbDesc.GetDefaultPrivileges() == nil {
		n.dbDesc.SetDefaultPrivilegeDescriptor(catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE))
	}

	defaultPrivs := n.dbDesc.GetMutableDefaultPrivilegeDescriptor()

	var roles []catpb.DefaultPrivilegesRole
	if n.n.ForAllRoles {
		roles = append(roles, catpb.DefaultPrivilegesRole{
			ForAllRoles: true,
		})
	} else {
		roles = make([]catpb.DefaultPrivilegesRole, len(targetRoles))
		for i, role := range targetRoles {
			roles[i] = catpb.DefaultPrivilegesRole{
				Role: role,
			}
		}
	}

	var events []eventLogEntry
	granteeSQLUsernames, err := grantees.ToSQLUsernames(params.SessionData(), security.UsernameValidation)
	if err != nil {
		return err
	}

	grantPresent, allPresent := false, false
	for _, priv := range privileges {
		grantPresent = grantPresent || priv == privilege.GRANT
		allPresent = allPresent || priv == privilege.ALL
	}
	if params.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.ValidateGrantOption) {
		noticeMessage := ""
		// we only output the message for ALL privilege if it is being granted without the WITH GRANT OPTION flag
		// if GRANT privilege is involved, we must always output the message
		if allPresent && n.n.IsGrant && !grantOption {
			noticeMessage = "grant options were automatically applied but this behavior is deprecated"
		} else if grantPresent {
			noticeMessage = "the GRANT privilege is deprecated"
		}

		if len(noticeMessage) > 0 {
			params.p.BufferClientNotice(
				params.ctx,
				errors.WithHint(
					pgnotice.Newf("%s", noticeMessage),
					"please use WITH GRANT OPTION",
				),
			)
		}
	}

	for _, role := range roles {
		if n.n.IsGrant {
			defaultPrivs.GrantDefaultPrivileges(
				role, privileges, granteeSQLUsernames, objectType, grantOption, grantPresent || allPresent,
			)
		} else {
			defaultPrivs.RevokeDefaultPrivileges(
				role, privileges, granteeSQLUsernames, objectType, grantOption, grantPresent || allPresent,
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
