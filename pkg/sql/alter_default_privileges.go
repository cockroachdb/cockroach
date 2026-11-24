// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
)

var targetObjectToPrivilegeObject = map[privilege.TargetObjectType]privilege.ObjectType{
	privilege.Tables:    privilege.Table,
	privilege.Sequences: privilege.Sequence,
	privilege.Types:     privilege.Type,
	privilege.Schemas:   privilege.Schema,
	privilege.Routines:  privilege.Routine,
}

type alterDefaultPrivilegesNode struct {
	zeroInputPlanNode
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
	dbDesc, err := p.Descriptors().MutableByName(p.txn).Database(ctx, database)
	if err != nil {
		return nil, err
	}
	if dbDesc.GetID() == keys.SystemDatabaseID {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "cannot alter system database")
	}

	objectType := n.Grant.Target
	if !n.IsGrant {
		objectType = n.Revoke.Target
	}

	if len(n.Schemas) > 0 && objectType == privilege.Schemas {
		return nil, pgerror.WithCandidateCode(errors.New(
			"cannot use IN SCHEMA clause when using GRANT/REVOKE ON SCHEMAS"),
			pgcode.InvalidGrantOperation,
		)
	}

	var schemaDescs []*schemadesc.Mutable
	for _, sc := range n.Schemas {
		immSchema, err := p.Descriptors().ByName(p.txn).Get().Schema(ctx, dbDesc, sc.Schema())
		if err != nil {
			return nil, err
		}
		if immSchema.SchemaKind() != catalog.SchemaUserDefined {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "%s is not a physical schema", immSchema.GetName())
		}
		mutableSchemaDesc, err := p.Descriptors().MutableByID(p.txn).Schema(ctx, immSchema.GetID())
		if err != nil {
			return nil, err
		}
		schemaDescs = append(schemaDescs, mutableSchemaDesc)
	}

	return &alterDefaultPrivilegesNode{
		n:           n,
		dbDesc:      dbDesc,
		schemaDescs: schemaDescs,
	}, err
}

func (n *alterDefaultPrivilegesNode) startExec(params runParams) error {
	targetRoles, err := decodeusername.FromRoleSpecList(
		params.SessionData(), username.PurposeValidation, n.n.Roles,
	)
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

	granteeSQLUsernames, err := decodeusername.FromRoleSpecList(
		params.p.SessionData(), username.PurposeValidation, grantees,
	)
	if err != nil {
		return err
	}

	if err := params.p.preChangePrivilegesValidation(params.ctx, granteeSQLUsernames, grantOption, n.n.IsGrant); err != nil {
		return err
	}

	var hasAdmin bool
	if hasAdmin, err = params.p.HasAdminRole(params.ctx); err != nil {
		return err
	}
	if n.n.ForAllRoles {
		if !hasAdmin {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"only users with the admin role are allowed to ALTER DEFAULT PRIVILEGES FOR ALL ROLES")
		}
	} else {
		// You can change default privileges only for objects that will be created
		// by yourself or by roles that you are a member of.
		for _, targetRole := range targetRoles {
			if targetRole != params.p.User() && !hasAdmin {
				memberOf, err := params.p.MemberOfWithAdminOption(params.ctx, params.p.User())
				if err != nil {
					return err
				}

				if _, found := memberOf[targetRole]; !found {
					return pgerror.Newf(pgcode.InsufficientPrivilege,
						"must be an admin or member of %s", targetRole.Normalized())
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
	targetRoles []username.SQLUsername,
	objectType privilege.TargetObjectType,
	grantees tree.RoleSpecList,
	privileges privilege.List,
	grantOption bool,
) error {
	var events []logpb.EventPayload
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

		granteeSQLUsernames, err := decodeusername.FromRoleSpecList(
			params.SessionData(), username.PurposeValidation, grantees,
		)
		if err != nil {
			return err
		}

		for _, role := range roles {
			if n.n.IsGrant {
				if err := defaultPrivs.GrantDefaultPrivileges(
					role, privileges, granteeSQLUsernames, objectType, grantOption,
				); err != nil {
					return err
				}
			} else {
				if err := defaultPrivs.RevokeDefaultPrivileges(
					role, privileges, granteeSQLUsernames, objectType, grantOption,
				); err != nil {
					return err
				}
			}

			eventDetails := eventpb.CommonSQLPrivilegeEventDetails{}
			if n.n.IsGrant {
				eventDetails.GrantedPrivileges = privileges.SortedDisplayNames()
			} else {
				eventDetails.RevokedPrivileges = privileges.SortedDisplayNames()
			}
			event := eventpb.AlterDefaultPrivileges{
				CommonSQLEventDetails: eventpb.CommonSQLEventDetails{
					DescriptorID: uint32(n.dbDesc.GetID()),
				},
				CommonSQLPrivilegeEventDetails: eventDetails,
				SchemaName:                     schemaDesc.GetName(),
			}
			if n.n.ForAllRoles {
				event.ForAllRoles = true
			} else {
				event.RoleName = role.Role.Normalized()
			}

			events = append(events, &event)

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
	targetRoles []username.SQLUsername,
	objectType privilege.TargetObjectType,
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

	var events []logpb.EventPayload
	granteeSQLUsernames, err := decodeusername.FromRoleSpecList(
		params.SessionData(), username.PurposeValidation, grantees,
	)
	if err != nil {
		return err
	}

	for _, role := range roles {
		if n.n.IsGrant {
			if err := defaultPrivs.GrantDefaultPrivileges(
				role, privileges, granteeSQLUsernames, objectType, grantOption,
			); err != nil {
				return err
			}
		} else {
			if err := defaultPrivs.RevokeDefaultPrivileges(
				role, privileges, granteeSQLUsernames, objectType, grantOption,
			); err != nil {
				return err
			}
		}

		eventDetails := eventpb.CommonSQLPrivilegeEventDetails{}
		if n.n.IsGrant {
			eventDetails.GrantedPrivileges = privileges.SortedDisplayNames()
		} else {
			eventDetails.RevokedPrivileges = privileges.SortedDisplayNames()
		}
		event := eventpb.AlterDefaultPrivileges{
			CommonSQLEventDetails: eventpb.CommonSQLEventDetails{
				DescriptorID: uint32(n.dbDesc.GetID()),
			},
			CommonSQLPrivilegeEventDetails: eventDetails,
			DatabaseName:                   n.dbDesc.GetName(),
		}
		if n.n.ForAllRoles {
			event.ForAllRoles = true
		} else {
			event.RoleName = role.Role.Normalized()
		}

		events = append(events, &event)
	}

	if err := params.p.writeNonDropDatabaseChange(
		params.ctx, n.dbDesc, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	return params.p.logEvents(params.ctx, events...)
}
