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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

func (p *planner) changePrivileges(targets parser.TargetList, grantees parser.NameList, changePrivilege func(*PrivilegeDescriptor, string)) (planNode, error) {
	descriptor, err := p.getDescriptorFromTargetList(targets)
	if err != nil {
		return nil, err
	}

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

	tableDesc, ok := descriptor.(*TableDescriptor)
	if ok {
		tableDesc.UpVersion = true
		p.applyUpVersion(tableDesc)
	}

	// Now update the descriptor.
	// TODO(marc): do this inside a transaction. This will be needed
	// when modifying multiple descriptors in the same op.
	descKey := MakeDescMetadataKey(descriptor.GetID())
	if err := p.txn.Put(descKey, wrapDescriptor(descriptor)); err != nil {
		return nil, err
	}
	if ok {
		p.notifyCompletedSchemaChange(tableDesc.ID)
	}
	return &valuesNode{}, nil
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
	return p.changePrivileges(n.Targets, n.Grantees, func(privDesc *PrivilegeDescriptor, grantee string) {
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
	return p.changePrivileges(n.Targets, n.Grantees, func(privDesc *PrivilegeDescriptor, grantee string) {
		privDesc.Revoke(grantee, n.Privileges)
	})
}
