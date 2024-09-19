// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import "context"

// Authorizer is a component of an AuthMethod that adds additional system
// privilege information for the client session, specifically when we want to
// synchronize this information from some external authorization system (e.g.:
// LDAP groups, JWT claims or X.509 SAN or other fields, etc). The system
// identity is used to find a list of valid SQL roles. These roles are then
// granted to the SQL user who is logging in.
type Authorizer = func(
	ctx context.Context,
	systemIdentity string,
	clientConnection bool,
) error
