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
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

func (p *planner) changePrivileges(
	targets parser.TargetList,
	grantees parser.NameList,
	changePrivilege func(*sqlbase.PrivilegeDescriptor, string),
) (planNode, error) {
	descriptors, err := p.getDescriptorsFromTargetList(targets)
	if err != nil {
		return nil, err
	}

	for _, descriptor := range descriptors {
		if err := p.checkPrivilege(descriptor, privilege.GRANT); err != nil {
			return nil, err
		}
		privileges := descriptor.GetPrivileges()
		for _, grantee := range grantees {
			changePrivilege(privileges, grantee)
		}

		if err := descriptor.Validate(); err != nil {
			return nil, err
		}

		tableDesc, updatingTable := descriptor.(*sqlbase.TableDescriptor)
		if updatingTable {
			if err := tableDesc.SetUpVersion(); err != nil {
				return nil, err
			}
			p.notifySchemaChange(tableDesc.ID, sqlbase.InvalidMutationID)
		}
	}

	// Now update the descriptors transactionally.
	b := p.txn.NewBatch()
	for _, descriptor := range descriptors {
		descKey := sqlbase.MakeDescMetadataKey(descriptor.GetID())
		b.Put(descKey, sqlbase.WrapDescriptor(descriptor))
	}
	if pErr := p.txn.Run(b); pErr != nil {
		return nil, pErr
	}
	return &emptyNode{}, nil
}

// Grant adds privileges to users.
// Current status:
// - Target: single database or table.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// - should we make users case-insensitive?
// Privileges: GRANT on database/table.
//   Notes: postgres requires the object owner.
//          mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Grant(n *parser.Grant) (planNode, error) {
	return p.changePrivileges(n.Targets, n.Grantees, func(privDesc *sqlbase.PrivilegeDescriptor, grantee string) {
		privDesc.Grant(grantee, n.Privileges)
	})
}

// Revoke removes privileges from users.
// Current status:
// - Target: single database or table.
// TODO(marc): open questions:
// - should we have root always allowed and not present in the permissions list?
// - should we make users case-insensitive?
// Privileges: GRANT on database/table.
//   Notes: postgres requires the object owner.
//          mysql requires the "grant option" and the same privileges, and sometimes superuser.
func (p *planner) Revoke(n *parser.Revoke) (planNode, error) {
	return p.changePrivileges(n.Targets, n.Grantees, func(privDesc *sqlbase.PrivilegeDescriptor, grantee string) {
		privDesc.Revoke(grantee, n.Privileges)
	})
}
