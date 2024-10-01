// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
)

// Authorizer is a component of an AuthMethod that adds additional system
// privilege information for the client session, specifically when we want to
// synchronize this information from some external authorization system (e.g.:
// LDAP groups, JWT claims or X.509 SAN or other fields, etc). It returns list
// of system identities which map to roles created specifically to assign the
// privileges to the session and must be valid SQL users/roles. Authorizer is
// intended to be used with GrantRolesFn which assigns it to intended
// groups(roles).
type Authorizer = func(
	ctx context.Context,
	systemIdentity string,
	clientConnection bool,
) ([]username.SQLUsername, error)

// RoleGranter defines a mechanism by which an AuthMethod associated with an
// incoming connection may grant additional roles obtained from an external
// authorization systems (e.g.: LDAP groups, JWT claims or X.509 SAN or other
// fields, etc.) to a sql session. It is expected that at this point we have
// created these groups(roles) with requisite privileges which are retrieved
// during the granting process and assigned to the sql session identified by the
// systemIdentity. Both Authorizer and RoleGranter are executed as part of
// (*AuthBehaviors).MaybeAuthorize().
type RoleGranter = func(
	ctx context.Context,
	systemIdentity string,
	sqlGroups []username.SQLUsername,
) error
