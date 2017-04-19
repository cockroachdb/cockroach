// Copyright 2017 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// AuthorizationAccessor for checking authorization (e.g. desc privileges).
type AuthorizationAccessor interface {
	// CheckPrivilege verifies that the user has `privilege` on `descriptor`.
	CheckPrivilege(
		descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
	) error

	// anyPrivilege verifies that the user has any privilege on `descriptor`.
	anyPrivilege(descriptor sqlbase.DescriptorProto) error

	// SuperUser errors if the session user is the super-user (i.e. root).
	// Includes the named action in thhe error message.
	RequireSuperUser(action string) error
}

var _ AuthorizationAccessor = &planner{}

// CheckPrivilege implements the AuthorizationAccessor interface.
func (p *planner) CheckPrivilege(
	descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
) error {
	if descriptor.GetPrivileges().CheckPrivilege(p.session.User, privilege) {
		return nil
	}
	return fmt.Errorf("user %s does not have %s privilege on %s %s",
		p.session.User, privilege, descriptor.TypeName(), descriptor.GetName())
}

// anyPrivilege implements the AuthorizationAccessor interface.
func (p *planner) anyPrivilege(descriptor sqlbase.DescriptorProto) error {
	if userCanSeeDescriptor(descriptor, p.session.User) {
		return nil
	}
	return fmt.Errorf("user %s has no privileges on %s %s",
		p.session.User, descriptor.TypeName(), descriptor.GetName())
}

// RequireSuperUser implements the AuthorizationAccessor interface.
func (p *planner) RequireSuperUser(action string) error {
	if p.session.User != security.RootUser && p.session.User != security.NodeUser {
		return fmt.Errorf("only %s is allowed to %s", security.RootUser, action)
	}
	return nil
}

func userCanSeeDescriptor(descriptor sqlbase.DescriptorProto, user string) bool {
	return descriptor.GetPrivileges().AnyPrivilege(user) || isVirtualDescriptor(descriptor)
}
