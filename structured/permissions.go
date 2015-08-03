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

package structured

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// permissions describes the full set of permissions for a descriptor.
// This is used for easier manipulation of permisssions.
// TODO(marc): we should eventually store ALL as a specific permission,
// so that newly-added permissions will automatically be picked up.
type permissions struct {
	read  map[string]struct{}
	write map[string]struct{}
}

// permissionsFromDescriptor builds a permissions object from a descriptor.
func permissionsFromDescriptor(descriptor *DatabaseDescriptor) (*permissions, error) {
	p := &permissions{
		read:  map[string]struct{}{},
		write: map[string]struct{}{},
	}

	for _, readUser := range descriptor.Read {
		p.read[readUser] = struct{}{}
	}
	for _, writeUser := range descriptor.Write {
		p.write[writeUser] = struct{}{}
	}

	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

// Validate verifies the validity of the permissions object.
// For now, this only involves making sure that the root user
// still has all permissions.
func (p *permissions) Validate() error {
	if _, ok := p.read[security.RootUser]; !ok {
		return fmt.Errorf("%s user does not have read privileges", security.RootUser)
	}
	if _, ok := p.write[security.RootUser]; !ok {
		return fmt.Errorf("%s user does not have write privileges", security.RootUser)
	}
	return nil
}

// FillDescriptor writes the permissions from this object into a descriptor.
func (p *permissions) FillDescriptor(descriptor *DatabaseDescriptor) error {
	if err := p.Validate(); err != nil {
		return err
	}

	descriptor.Read, descriptor.Write = []string{}, []string{}
	for u := range p.read {
		descriptor.Read = append(descriptor.Read, u)
	}

	for u := range p.write {
		descriptor.Write = append(descriptor.Write, u)
	}

	sort.Strings(descriptor.Read)
	sort.Strings(descriptor.Write)
	return nil
}

// addToMap adds all entries in `newUsers` to the `users` map.
func addToMap(users map[string]struct{}, newUsers []string) {
	for _, u := range newUsers {
		users[u] = struct{}{}
	}
}

// removeFromMap removes all entries in `newUsers` from the `users` map.
func removeFromMap(users map[string]struct{}, newUsers []string) {
	for _, u := range newUsers {
		delete(users, u)
	}
}

// Grant takes a parser.Grant node and grants the specified privileges to users.
func (p *permissions) Grant(n *parser.Grant) error {
	for _, priv := range n.Privileges {
		switch priv {
		case parser.PrivilegeAll:
			addToMap(p.read, n.Grantees)
			addToMap(p.write, n.Grantees)
		case parser.PrivilegeRead:
			addToMap(p.read, n.Grantees)
		case parser.PrivilegeWrite:
			addToMap(p.write, n.Grantees)
		default:
			return fmt.Errorf("grant: unknown privilege %q", priv)
		}
	}
	return nil
}

// Revoke takes a parser.Revoke node and removes the specified privileges from users.
func (p *permissions) Revoke(n *parser.Revoke) error {
	for _, priv := range n.Privileges {
		switch priv {
		case parser.PrivilegeAll:
			removeFromMap(p.read, n.Grantees)
			removeFromMap(p.write, n.Grantees)
		case parser.PrivilegeRead:
			removeFromMap(p.read, n.Grantees)
		case parser.PrivilegeWrite:
			removeFromMap(p.write, n.Grantees)
		default:
			return fmt.Errorf("revoke: unknown privilege %q", priv)
		}
	}
	return nil
}

// UserPrivileges contains a user and list of associated privileges.
type UserPrivileges struct {
	User       string
	Privileges parser.PrivilegeList
}

// UserPrivilegeList is a list of UserPrivileges.
type UserPrivilegeList []*UserPrivileges

func (upl UserPrivilegeList) Len() int {
	return len(upl)
}

func (upl UserPrivilegeList) Swap(i, j int) {
	upl[i], upl[j] = upl[j], upl[i]
}

func (upl UserPrivilegeList) Less(i, j int) bool {
	return upl[i].User < upl[j].User
}

// Show returns a slice of { user, list of privileges } sorted by user.
func (p *permissions) Show() UserPrivilegeList {
	// Combine all privileges by user.
	tmp := map[string]*UserPrivileges{}
	for u := range p.read {
		tmp[u] = &UserPrivileges{User: u, Privileges: []parser.PrivilegeType{parser.PrivilegeRead}}
	}
	for u := range p.write {
		if _, ok := tmp[u]; !ok {
			tmp[u] = &UserPrivileges{User: u, Privileges: []parser.PrivilegeType{parser.PrivilegeWrite}}
		} else {
			tmp[u].Privileges = append(tmp[u].Privileges, parser.PrivilegeWrite)
		}
	}

	// Put them all into a slice and sort.
	ret := make(UserPrivilegeList, len(tmp), len(tmp))
	i := 0
	for _, up := range tmp {
		ret[i] = up
		i++
	}
	sort.Sort(ret)
	return ret
}
