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
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
)

// Permissions describes the full set of permissions for a descriptor.
// This allows quick lookup for a given privilege and user.
type Permissions struct {
	read  map[string]struct{}
	write map[string]struct{}
}

// PermissionsFromDescriptor builds a Permissions object from a descriptor.
func PermissionsFromDescriptor(descriptor *structured.DatabaseDescriptor) (*Permissions, error) {
	p := &Permissions{
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

// Validate verifies the validity of the Permissions object.
// For now, this only involves making sure that the root user
// still has all permissions.
func (p *Permissions) Validate() error {
	if _, ok := p.read[security.RootUser]; !ok {
		return fmt.Errorf("%s user does not have read privileges", security.RootUser)
	}
	if _, ok := p.write[security.RootUser]; !ok {
		return fmt.Errorf("%s user does not have write privileges", security.RootUser)
	}
	return nil
}

// FillDescriptor writes the permissions from this object into a descriptor.
func (p *Permissions) FillDescriptor(descriptor *structured.DatabaseDescriptor) error {
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
func (p *Permissions) Grant(n *parser.Grant) error {
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
func (p *Permissions) Revoke(n *parser.Revoke) error {
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
