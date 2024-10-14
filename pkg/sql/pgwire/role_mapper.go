// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/errors"
)

// RoleMapper defines a mechanism by which an AuthMethod associated
// with an incoming connection may replace the caller-provided system
// identity (e.g.: GSSAPI or X.509 principal, LDAP DN, etc.) with zero
// or more SQLUsernames that will be subsequently validated against the
// SQL roles defined within the database. The mapping from system
// identity to database roles may be derived from the host-based
// authentication mechanism built into CockroachDB, or it could
// conceivably be implemented by an external directory service which
// maps groups of users onto database roles.
type RoleMapper = func(
	ctx context.Context,
	systemIdentity string,
) ([]username.SQLUsername, error)

// UseProvidedIdentity is a trivial implementation of RoleMapper which always
// returns its input.
func UseProvidedIdentity(_ context.Context, id string) ([]username.SQLUsername, error) {
	u, err := username.MakeSQLUsernameFromUserInput(id, username.PurposeValidation)
	if err != nil {
		return nil, err
	}
	return []username.SQLUsername{u}, nil
}

var _ RoleMapper = UseProvidedIdentity

// UseSpecifiedIdentity is a RoleMapper that always returns a fixed user.
func UseSpecifiedIdentity(user username.SQLUsername) RoleMapper {
	return func(_ context.Context, _ string) ([]username.SQLUsername, error) {
		return []username.SQLUsername{user}, nil
	}
}

// HbaMapper implements the "map" option that may be defined in a
// host-based authentication rule. If the HBA entry does not define a
// "map" option, this function will return UseProvidedIdentity.
//
// This mapper will return an error if an applied mapping rule results
// in the root user or a reserved user, which includes the node,
// "public", and various other magic prefixes.
func HbaMapper(hbaEntry *hba.Entry, identMap *identmap.Conf) RoleMapper {
	mapName := hbaEntry.GetOption("map")
	if mapName == "" {
		return UseProvidedIdentity
	}
	return func(_ context.Context, id string) ([]username.SQLUsername, error) {
		users, _, err := identMap.Map(mapName, id)
		if err != nil {
			return nil, err
		}
		for _, user := range users {
			if user.IsRootUser() || user.IsReserved() {
				return nil, errors.Newf("system identity %q mapped to reserved database role %q",
					id, user.Normalized())
			}
		}
		return users, nil
	}
}
