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

	// CheckAnyPrivilege returns nil if user has any privileges at all.
	CheckAnyPrivilege(descriptor sqlbase.DescriptorProto) error

	// RequiresSuperUser errors if the session user isn't a super-user (i.e. root
	// or node). Includes the named action in the error message.
	RequireSuperUser(action string) error
}

var _ AuthorizationAccessor = &planner{}

// CheckPrivilegeForUser verifies that `user`` has `privilege` on `descriptor`.
func CheckPrivilegeForUser(
	user string, descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
) error {
	if descriptor.GetPrivileges().CheckPrivilege(user, privilege) {
		return nil
	}
	return fmt.Errorf("user %s does not have %s privilege on %s %s",
		user, privilege, descriptor.TypeName(), descriptor.GetName())
}

// CheckPrivilege implements the AuthorizationAccessor interface.
func (p *planner) CheckPrivilege(
	descriptor sqlbase.DescriptorProto, privilege privilege.Kind,
) error {
	return CheckPrivilegeForUser(p.SessionData().User, descriptor, privilege)
}

// CheckAnyPrivilege implements the AuthorizationAccessor interface.
func (p *planner) CheckAnyPrivilege(descriptor sqlbase.DescriptorProto) error {
	if isVirtualDescriptor(descriptor) {
		return nil
	}
	if descriptor.GetPrivileges().AnyPrivilege(p.SessionData().User) {
		return nil
	}

	return fmt.Errorf("user %s has no privileges on %s %s",
		p.SessionData().User, descriptor.TypeName(), descriptor.GetName())
}

// RequireSuperUser implements the AuthorizationAccessor interface.
func (p *planner) RequireSuperUser(action string) error {
	user := p.SessionData().User
	if user != security.RootUser && user != security.NodeUser {
		return fmt.Errorf("only %s is allowed to %s", security.RootUser, action)
	}
	return nil
}
