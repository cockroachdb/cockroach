// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// We use a non-standard build tag here because we want to only build on
// linux-gnu targets (i.e., not musl). Since go doesn't have a builtin way
// to do that, we have to set this in the top-level Makefile.

//go:build gss
// +build gss

package gssapiccl

import (
	"context"
	"crypto/tls"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/errors"
)

const (
	authTypeGSS         int32 = 7
	authTypeGSSContinue int32 = 8
)

// authGSS performs GSS authentication. See:
// https://github.com/postgres/postgres/blob/0f9cdd7dca694d487ab663d463b308919f591c02/src/backend/libpq/auth.c#L1090
func authGSS(
	_ context.Context,
	c pgwire.AuthConn,
	_ tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
	identMap *identmap.Conf,
) (*pgwire.AuthBehaviors, error) {
	behaviors := &pgwire.AuthBehaviors{}

	connClose, gssUser, err := getGssUser(c)
	behaviors.SetConnClose(connClose)
	if err != nil {
		return behaviors, err
	}

	// Update the incoming connection with the GSS username. We'll expect
	// to see this value come back to the mapper function below.
	if u, err := username.MakeSQLUsernameFromUserInput(gssUser, username.PurposeValidation); err != nil {
		return nil, err
	} else {
		behaviors.SetReplacementIdentity(u)
	}

	// We enforce that the "map" and/or "include_realm=0" options are set
	// in the HBA validation function below.
	include0 := entry.GetOption("include_realm") == "0"
	if entry.GetOption("map") != "" {
		mapper := pgwire.HbaMapper(entry, identMap)
		// Per behavior in PostgreSQL, combining both map and
		// include_realm=0 means that the incoming principal is stripped,
		// then the map is applied. See also:
		// https://github.com/postgres/postgres/blob/4ac0f450b698442c3273ddfe8eed0e1a7e56645f/src/backend/libpq/auth.c#L1474
		if include0 {
			mapper = stripAndDelegateMapper(mapper)
		}
		behaviors.SetRoleMapper(mapper)
	} else if include0 {
		// Strip the trailing realm information, if any, from the gssapi username.
		behaviors.SetRoleMapper(stripRealmMapper)
	} else {
		return nil, errors.New("unsupported HBA entry configuration")
	}

	behaviors.SetAuthenticator(func(
		_ context.Context, _ username.SQLUsername, _ bool, _ pgwire.PasswordRetrievalFn,
	) error {
		// Enforce krb_realm option, if any.
		if realms := entry.GetOptions("krb_realm"); len(realms) > 0 {
			if idx := strings.IndexByte(gssUser, '@'); idx >= 0 {
				realm := gssUser[idx+1:]
				matched := false
				for _, krbRealm := range realms {
					if realm == krbRealm {
						matched = true
						break
					}
				}
				if !matched {
					return errors.Errorf("GSSAPI realm (%s) didn't match any configured realm", realm)
				}
			} else {
				return errors.New("GSSAPI did not return realm but realm matching was requested")
			}
		}

		// Do the license check last so that administrators are able to test whether
		// their GSS configuration is correct. That is, the presence of this error
		// message means they have a correctly functioning GSS/Kerberos setup,
		// but now need to enable enterprise features.
		return utilccl.CheckEnterpriseEnabled(execCfg.Settings, execCfg.NodeInfo.LogicalClusterID(), "GSS authentication")
	})
	return behaviors, nil
}

// checkEntry validates that the HBA entry contains exactly one of the
// include_realm=0 directive or an identity-mapping configuration.
func checkEntry(_ *settings.Values, entry hba.Entry) error {
	hasInclude0 := false
	hasMap := false
	for _, op := range entry.Options {
		switch op[0] {
		case "include_realm":
			if op[1] == "0" {
				hasInclude0 = true
			} else {
				return errors.Errorf("include_realm must be set to 0: %s", op[1])
			}
		case "krb_realm":
		// OK.
		case "map":
			hasMap = true
		default:
			return errors.Errorf("unsupported option %s", op[0])
		}
	}
	if !hasMap && !hasInclude0 {
		return errors.New(`at least one of "include_realm=0" or "map" options required`)
	}
	return nil
}

// stripRealm removes the realm data, if any, from the provided username.
func stripRealm(u username.SQLUsername) (username.SQLUsername, error) {
	norm := u.Normalized()
	if idx := strings.Index(norm, "@"); idx != -1 {
		norm = norm[:idx]
	}
	return username.MakeSQLUsernameFromUserInput(norm, username.PurposeValidation)
}

// stripRealmMapper is a pgwire.RoleMapper that just strips the trailing
// realm information, if any, from the gssapi username.
func stripRealmMapper(
	_ context.Context, systemIdentity username.SQLUsername,
) ([]username.SQLUsername, error) {
	ret, err := stripRealm(systemIdentity)
	return []username.SQLUsername{ret}, err
}

// stripAndDelegateMapper wraps a delegate pgwire.RoleMapper such that
// the incoming identity has its realm information stripped before the
// next mapping is applied.
func stripAndDelegateMapper(delegate pgwire.RoleMapper) pgwire.RoleMapper {
	return func(ctx context.Context, systemIdentity username.SQLUsername) ([]username.SQLUsername, error) {
		next, err := stripRealm(systemIdentity)
		if err != nil {
			return nil, err
		}
		return delegate(ctx, next)
	}
}

func init() {
	pgwire.RegisterAuthMethod("gss", authGSS, hba.ConnHostSSL, checkEntry)
}
