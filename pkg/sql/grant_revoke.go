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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	if n.Targets.Databases != nil {
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnDatabase)
		grantOn = privilege.Database
	} else if n.Targets.Types != nil {
		grantOn = privilege.Type
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnType)
	} else {
		sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnTable)
		grantOn = privilege.Table
	}

	if err := privilege.ValidatePrivileges(n.Privileges, grantOn); err != nil {
		return nil, err
	}

	return &changePrivilegesNode{
		targets:      n.Targets,
		grantees:     n.Grantees,
		desiredprivs: n.Privileges,
		changePrivilege: func(privDesc *descpb.PrivilegeDescriptor, grantee string) {
			privDesc.Grant(grantee, n.Privileges)
		},
		grantOn: grantOn,
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
	if n.Targets.Databases != nil {
		sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnDatabase)
		grantOn = privilege.Database
	} else if n.Targets.Types != nil {
		grantOn = privilege.Type
		sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnType)
	} else {
		sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnTable)
		grantOn = privilege.Table
	}

	if err := privilege.ValidatePrivileges(n.Privileges, grantOn); err != nil {
		return nil, err
	}

	return &changePrivilegesNode{
		targets:      n.Targets,
		grantees:     n.Grantees,
		desiredprivs: n.Privileges,
		changePrivilege: func(privDesc *descpb.PrivilegeDescriptor, grantee string) {
			privDesc.Revoke(grantee, n.Privileges, grantOn)
		},
		grantOn: grantOn,
	}, nil
}

type changePrivilegesNode struct {
	targets         tree.TargetList
	grantees        tree.NameList
	desiredprivs    privilege.List
	changePrivilege func(*descpb.PrivilegeDescriptor, string)
	grantOn         privilege.ObjectType
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
	users[security.PublicRole] = true // isRole

	for _, grantee := range n.grantees {
		if _, ok := users[string(grantee)]; !ok {
			return errors.Errorf("user or role %s does not exist", &grantee)
		}
	}

	var descriptors []catalog.Descriptor
	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		descriptors, err = getDescriptorsFromTargetList(ctx, p, n.targets)
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
			n.changePrivilege(privileges, string(grantee))
		}

		// Validate privilege descriptors directly as the db/table level Validate
		// may fix up the descriptor.
		if err := privileges.Validate(descriptor.GetID(), n.grantOn); err != nil {
			return err
		}

		switch d := descriptor.(type) {
		case *dbdesc.MutableDatabaseDescriptor:
			if p.Descriptors().DatabaseLeasingUnsupported() {
				if err := d.Validate(); err != nil {
					return err
				}
				if err := catalogkv.WriteDescToBatch(
					ctx,
					p.extendedEvalCtx.Tracing.KVTracingEnabled(),
					p.ExecCfg().Settings,
					b,
					p.ExecCfg().Codec,
					descriptor.GetID(),
					descriptor,
				); err != nil {
					return err
				}
			} else {
				if err := p.writeDatabaseChangeToBatch(ctx, d, b); err != nil {
					return err
				}
				if err := p.createNonDropDatabaseChangeJob(ctx, d.ID,
					fmt.Sprintf("updating privileges for database %d", d.ID),
				); err != nil {
					return err
				}
			}

		case *sqlbase.MutableTableDescriptor:
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
		case *typedesc.MutableTypeDescriptor:
			err := p.writeTypeSchemaChange(ctx, d, fmt.Sprintf("updating privileges for type %d", d.ID))
			if err != nil {
				return err
			}
		}
	}

	// Now update the descriptors transactionally.
	return p.txn.Run(ctx, b)
}

func (*changePrivilegesNode) Next(runParams) (bool, error) { return false, nil }
func (*changePrivilegesNode) Values() tree.Datums          { return tree.Datums{} }
func (*changePrivilegesNode) Close(context.Context)        {}
