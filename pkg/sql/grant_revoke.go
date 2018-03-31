// Copyright 2015 The Cockroach Authors.
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
	return p.changePrivileges(ctx, n.Targets, n.Grantees, func(privDesc *sqlbase.PrivilegeDescriptor, grantee string) {
		privDesc.Grant(grantee, n.Privileges)
	})
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
	return p.changePrivileges(ctx, n.Targets, n.Grantees, func(privDesc *sqlbase.PrivilegeDescriptor, grantee string) {
		privDesc.Revoke(grantee, n.Privileges)
	})
}

func (p *planner) changePrivileges(
	ctx context.Context,
	targets tree.TargetList,
	grantees tree.NameList,
	changePrivilege func(*sqlbase.PrivilegeDescriptor, string),
) (planNode, error) {
	// Check whether grantees exists
	users, err := p.GetAllUsersAndRoles(ctx)
	if err != nil {
		return nil, err
	}
	for _, grantee := range grantees {
		if _, ok := users[string(grantee)]; !ok {
			return nil, errors.Errorf("user or role %s does not exist", &grantee)
		}
	}

	var descriptors []sqlbase.DescriptorProto
	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{skipCache: true, allowAdding: true}, func() {
		descriptors, err = getDescriptorsFromTargetList(ctx, p, targets)
	})
	if err != nil {
		return nil, err
	}

	// First, update the descriptors. We want to catch all errors before
	// we update them in KV below.
	for _, descriptor := range descriptors {
		if err := p.CheckPrivilege(ctx, descriptor, privilege.GRANT); err != nil {
			return nil, err
		}
		privileges := descriptor.GetPrivileges()
		for _, grantee := range grantees {
			changePrivilege(privileges, string(grantee))
		}

		// Validate privilege descriptors directly as the db/table level Validate
		// may fix up the descriptor.
		if err := privileges.Validate(descriptor.GetID()); err != nil {
			return nil, err
		}

		switch d := descriptor.(type) {
		case *sqlbase.DatabaseDescriptor:
			if err := d.Validate(); err != nil {
				return nil, err
			}

		case *sqlbase.TableDescriptor:
			if err := d.Validate(ctx, p.txn, p.EvalContext().Settings); err != nil {
				return nil, err
			}
			if err := d.SetUpVersion(); err != nil {
				return nil, err
			}
			p.notifySchemaChange(d, sqlbase.InvalidMutationID)
		}
	}

	// Now update the descriptors transactionally.
	b := p.txn.NewBatch()
	for _, descriptor := range descriptors {
		descKey := sqlbase.MakeDescMetadataKey(descriptor.GetID())
		b.Put(descKey, sqlbase.WrapDescriptor(descriptor))
	}
	if err := p.txn.Run(ctx, b); err != nil {
		return nil, err
	}
	return newZeroNode(nil /* columns */), nil
}
