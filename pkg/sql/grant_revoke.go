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

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
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
	return &changePrivilegesNode{
		targets:  n.Targets,
		grantees: n.Grantees,
		changePrivilege: func(privDesc *sqlbase.PrivilegeDescriptor, grantee string) {
			privDesc.Grant(grantee, n.Privileges)
		},
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
	return &changePrivilegesNode{
		targets:  n.Targets,
		grantees: n.Grantees,
		changePrivilege: func(privDesc *sqlbase.PrivilegeDescriptor, grantee string) {
			privDesc.Revoke(grantee, n.Privileges)
		},
	}, nil
}

type changePrivilegesNode struct {
	targets         tree.TargetList
	grantees        tree.NameList
	changePrivilege func(*sqlbase.PrivilegeDescriptor, string)
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because GRANT/REVOKE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *changePrivilegesNode) ReadingOwnWrites() {}

func (n *changePrivilegesNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p
	// Check whether grantees exists
	users, err := p.GetAllUsersAndRoles(ctx)
	if err != nil {
		return err
	}

	// We're allowed to grant/revoke privileges to/from the "public" role even though
	// it does not exist: add it to the list of all users and roles.
	users[sqlbase.PublicRole] = true // isRole

	for _, grantee := range n.grantees {
		if _, ok := users[string(grantee)]; !ok {
			return errors.Errorf("user or role %s does not exist", &grantee)
		}
	}

	var descriptors []sqlbase.DescriptorProto
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
		privileges := descriptor.GetPrivileges()
		for _, grantee := range n.grantees {
			n.changePrivilege(privileges, string(grantee))
		}

		// Validate privilege descriptors directly as the db/table level Validate
		// may fix up the descriptor.
		if err := privileges.Validate(descriptor.GetID()); err != nil {
			return err
		}

		switch d := descriptor.(type) {
		case *sqlbase.DatabaseDescriptor:
			if err := d.Validate(); err != nil {
				return err
			}
			if err := writeDescToBatch(ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), p.execCfg.Settings, b, descriptor.GetID(), descriptor); err != nil {
				return err
			}

		case *sqlbase.MutableTableDescriptor:
			if !d.Dropped() {
				if err := p.writeSchemaChangeToBatch(
					ctx, d, sqlbase.InvalidMutationID, b); err != nil {
					return err
				}
			}
		}
	}

	// Now update the descriptors transactionally.
	return p.txn.Run(ctx, b)
}

func (*changePrivilegesNode) Next(runParams) (bool, error) { return false, nil }
func (*changePrivilegesNode) Values() tree.Datums          { return tree.Datums{} }
func (*changePrivilegesNode) Close(context.Context)        {}
