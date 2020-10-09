// Copyright 2015 The Cockroach Authors.
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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// Grant adds privileges to users.
// Current status:
// - Target: single database, table, or view.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// - should we make users case-insensitive?
// Privileges: GRANT on database/table/view.
//   Notes: postgres requires the object owner.
//          mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Grant(ctx context.Context, n *tree.Grant) (planNode, error) {
	var grantOn privilege.ObjectType
	switch {
	case n.Targets.Databases != nil:
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnDatabase)
		grantOn = privilege.Database
	case n.Targets.Schemas != nil:
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnSchema)
		grantOn = privilege.Schema
	case n.Targets.Types != nil:
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnType)
		grantOn = privilege.Type
	default:
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnTable)
		grantOn = privilege.Table
	}

	if err := privilege.ValidatePrivileges(n.Privileges, grantOn); err != nil {
		return nil, err
	}

	// TODO(solon): there are SQL identifiers (tree.Name) in n.Grantees,
	// but we want SQL usernames. Do we normalize or not? For reference,
	// REASSIGN / OWNER TO do normalize.
	// Related: https://github.com/cockroachdb/cockroach/issues/54696
	grantees := make([]security.SQLUsername, len(n.Grantees))
	for i, grantee := range n.Grantees {
		grantees[i] = security.MakeSQLUsernameFromPreNormalizedString(string(grantee))
	}

	return &changePrivilegesNode{
		targets:      n.Targets,
		grantees:     grantees,
		desiredprivs: n.Privileges,
		changePrivilege: func(privDesc *descpb.PrivilegeDescriptor, grantee security.SQLUsername) {
			privDesc.Grant(grantee, n.Privileges)
		},
		grantOn:      grantOn,
		eventLogType: EventLogGrantPrivilege,
	}, nil
}

// Revoke removes privileges from users.
// Current status:
// - Target: single database, table, or view.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// - should we make users case-insensitive?
// Privileges: GRANT on database/table/view.
//   Notes: postgres requires the object owner.
//          mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Revoke(ctx context.Context, n *tree.Revoke) (planNode, error) {
	var grantOn privilege.ObjectType
	switch {
	case n.Targets.Databases != nil:
		sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnDatabase)
		grantOn = privilege.Database
	case n.Targets.Schemas != nil:
		sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnSchema)
		grantOn = privilege.Schema
	case n.Targets.Types != nil:
		sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnType)
		grantOn = privilege.Type
	default:
		sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnTable)
		grantOn = privilege.Table
	}

	if err := privilege.ValidatePrivileges(n.Privileges, grantOn); err != nil {
		return nil, err
	}

	// TODO(solon): there are SQL identifiers (tree.Name) in n.Grantees,
	// but we want SQL usernames. Do we normalize or not? For reference,
	// REASSIGN / OWNER TO do normalize.
	// Related: https://github.com/cockroachdb/cockroach/issues/54696
	grantees := make([]security.SQLUsername, len(n.Grantees))
	for i, grantee := range n.Grantees {
		grantees[i] = security.MakeSQLUsernameFromPreNormalizedString(string(grantee))
	}

	return &changePrivilegesNode{
		targets:      n.Targets,
		grantees:     grantees,
		desiredprivs: n.Privileges,
		changePrivilege: func(privDesc *descpb.PrivilegeDescriptor, grantee security.SQLUsername) {
			privDesc.Revoke(grantee, n.Privileges, grantOn)
		},
		grantOn:      grantOn,
		eventLogType: EventLogRevokePrivilege,
	}, nil
}

type changePrivilegesNode struct {
	targets         tree.TargetList
	grantees        []security.SQLUsername
	desiredprivs    privilege.List
	changePrivilege func(*descpb.PrivilegeDescriptor, security.SQLUsername)
	grantOn         privilege.ObjectType
	eventLogType    EventLogType
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because GRANT/REVOKE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *changePrivilegesNode) ReadingOwnWrites() {}

func (n *changePrivilegesNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p
	// Check whether grantees exists
	users, err := p.GetAllRoles(ctx)
	if err != nil {
		return err
	}

	// We're allowed to grant/revoke privileges to/from the "public" role even though
	// it does not exist: add it to the list of all users and roles.
	users[security.PublicRoleName()] = true // isRole

	for i, grantee := range n.grantees {
		if _, ok := users[grantee]; !ok {
			sqlName := tree.Name(n.grantees[i].Normalized())
			return errors.Errorf("user or role %s does not exist", &sqlName)
		}
	}

	var descriptors []catalog.Descriptor
	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		descriptors, err = getDescriptorsFromTargetListForPrivilegeChange(ctx, p, n.targets)
	})
	if err != nil {
		return err
	}

	// First, update the descriptors. We want to catch all errors before
	// we update them in KV below.
	b := p.txn.NewBatch()
	for _, descriptor := range descriptors {
		if err := p.CheckPrivilege(ctx, descriptor, privilege.GRANT); err != nil {
			return err
		}

		// Only allow granting/revoking privileges that the requesting
		// user themselves have on the descriptor.
		for _, priv := range n.desiredprivs {
			if err := p.CheckPrivilege(ctx, descriptor, priv); err != nil {
				return err
			}
		}

		privileges := descriptor.GetPrivileges()
		for _, grantee := range n.grantees {
			n.changePrivilege(privileges, grantee)
		}

		// Validate privilege descriptors directly as the db/table level Validate
		// may fix up the descriptor.
		if err := privileges.Validate(descriptor.GetID(), n.grantOn); err != nil {
			return err
		}

		switch d := descriptor.(type) {
		case *dbdesc.Mutable:
			if err := p.writeDatabaseChangeToBatch(ctx, d, b); err != nil {
				return err
			}
			if err := p.createNonDropDatabaseChangeJob(ctx, d.ID,
				fmt.Sprintf("updating privileges for database %d", d.ID),
			); err != nil {
				return err
			}

		case *tabledesc.Mutable:
			// TODO (lucy): This should probably have a single consolidated job like
			// DROP DATABASE.
			if err := p.createOrUpdateSchemaChangeJob(
				ctx, d,
				fmt.Sprintf("updating privileges for table %d", d.ID),
				descpb.InvalidMutationID,
			); err != nil {
				return err
			}
			if !d.Dropped() {
				if err := p.writeSchemaChangeToBatch(ctx, d, b); err != nil {
					return err
				}
			}
		case *typedesc.Mutable:
			err := p.writeTypeSchemaChange(ctx, d, fmt.Sprintf("updating privileges for type %d", d.ID))
			if err != nil {
				return err
			}
		case *schemadesc.Mutable:
			if err := p.writeSchemaDescChange(
				ctx,
				d,
				fmt.Sprintf("updating privileges for schema %d", d.ID),
			); err != nil {
				return err
			}
		}
	}

	// Now update the descriptors transactionally.
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// Record this index alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	n.targets.Format(fmtCtx)
	targets := fmtCtx.CloseAndGetString()

	var grantees strings.Builder
	comma := ""
	for _, g := range n.grantees {
		grantees.WriteString(comma)
		grantees.WriteString(g.Normalized())
		comma = ","
	}

	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		n.eventLogType,
		0, /* no target */
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
		struct {
			Target     string
			User       string
			Grantees   string
			Privileges string
		}{targets, p.User().Normalized(), grantees.String(), n.desiredprivs.String()},
	)
}

func (*changePrivilegesNode) Next(runParams) (bool, error) { return false, nil }
func (*changePrivilegesNode) Values() tree.Datums          { return tree.Datums{} }
func (*changePrivilegesNode) Close(context.Context)        {}
