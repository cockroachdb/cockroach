// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// This file contains the methods that are accepted to perform
// authentication of users during the pgwire connection handshake.
//
// Which method are accepted for which user is selected using
// the HBA config loaded into the cluster setting
// server.host_based_authentication.configuration.
//
// Other methods can be added using RegisterAuthMethod(). This is done
// e.g. in the CCL modules to add support for GSS authentication using
// Kerberos.

func loadDefaultMethods() {
	// The "password" method requires a clear text password.
	//
	// Care should be taken by administrators to only accept this auth
	// method over secure connections, e.g. those encrypted using SSL.
	RegisterAuthMethod("password", authPassword, hba.ConnAny, NoOptionsAllowed)

	// The "cert" method requires a valid client certificate for the
	// user attempting to connect.
	//
	// This method is only usable over SSL connections.
	RegisterAuthMethod("cert", authCert, hba.ConnHostSSL, nil)

	// The "cert-password" method requires either a valid client
	// certificate for the connecting user, or, if no cert is provided,
	// a cleartext password.
	RegisterAuthMethod("cert-password", authCertPassword, hba.ConnAny, nil)

	// The "reject" method rejects any connection attempt that matches
	// the current rule.
	RegisterAuthMethod("reject", authReject, hba.ConnAny, NoOptionsAllowed)

	// The "trust" method accepts any connection attempt that matches
	// the current rule.
	RegisterAuthMethod("trust", authTrust, hba.ConnAny, NoOptionsAllowed)
}

// AuthMethod is a top-level factory for composing the various
// functionality needed to authenticate an incoming connection.
// Implementations of AuthMethod should do as little work as possible,
// preferring to move all complex or fallible behaviors into
// RoleMapper and Authorizer.
type AuthMethod = func(
	ctx context.Context,
	c AuthConn,
	tlsState tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
	identMap *identmap.Conf,
) (RoleMapper, Authorizer)

// Authorizer is a component of an AuthMethod that determines if the
// given system identity (e.g.: Kerberos or X.509 principal, plain-old
// username, etc) is who it claims to be.
type Authorizer = func(
	ctx context.Context,
	systemIdentity security.SQLUsername,
	clientConnection bool,
	pwRetrieveFn PasswordRetrievalFn,
) (connClose func(), _ error)

// PasswordRetrievalFn defines a method to retrieve a hashed password
// and expiration time for a user logging in with password-based
// authentication.
type PasswordRetrievalFn = func(context.Context) (pwHash []byte, pwExpiration *tree.DTimestamp, _ error)

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
	systemIdentity security.SQLUsername,
) ([]security.SQLUsername, error)

// HbaMapper implements the "map" option that may be defined in a
// host-based authentication rule. If the HBA entry does not define a
// "map" option, this function will return UseProvidedIdentity.
func HbaMapper(hbaEntry *hba.Entry, identMap *identmap.Conf) RoleMapper {
	mapName := hbaEntry.GetOption("map")
	if mapName == "" {
		return UseProvidedIdentity
	}
	return func(_ context.Context, id security.SQLUsername) ([]security.SQLUsername, error) {
		return identMap.Map(mapName, id.Normalized())
	}
}

// UseProvidedIdentity is a trivial implementation of RoleMapper which always
// returns its input.
var UseProvidedIdentity = func(
	_ context.Context, id security.SQLUsername,
) ([]security.SQLUsername, error) {
	return []security.SQLUsername{id}, nil
}

func authPassword(
	_ context.Context,
	c AuthConn,
	_ tls.ConnectionState,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
	_ *identmap.Conf,
) (RoleMapper, Authorizer) {
	return UseProvidedIdentity,
		func(ctx context.Context, systemIdentity security.SQLUsername, clientConnection bool, pwRetrieveFn PasswordRetrievalFn) (connClose func(), _ error) {
			if err := c.SendAuthRequest(authCleartextPassword, nil /* data */); err != nil {
				return nil, err
			}
			pwdData, err := c.GetPwdData()
			if err != nil {
				return nil, err
			}
			password, err := passwordString(pwdData)
			if err != nil {
				return nil, err
			}
			hashedPassword, pwValidUntil, err := pwRetrieveFn(ctx)
			if err != nil {
				return nil, err
			}
			if len(hashedPassword) == 0 {
				c.LogAuthInfof(ctx, "user has no password defined")
			}

			if pwValidUntil != nil {
				if pwValidUntil.Sub(timeutil.Now()) < 0 {
					c.LogAuthFailed(ctx, eventpb.AuthFailReason_CREDENTIALS_EXPIRED, nil)
					return nil, errors.New("password is expired")
				}
			}
			return security.UserAuthPasswordHook(
				false /*insecure*/, password, hashedPassword,
			)(ctx, systemIdentity, clientConnection)
		}
}

func passwordString(pwdData []byte) (string, error) {
	// Make a string out of the byte array.
	if bytes.IndexByte(pwdData, 0) != len(pwdData)-1 {
		return "", fmt.Errorf("expected 0-terminated byte array")
	}
	return string(pwdData[:len(pwdData)-1]), nil
}

func authCert(
	_ context.Context,
	_ AuthConn,
	tlsState tls.ConnectionState,
	_ *sql.ExecutorConfig,
	hbaEntry *hba.Entry,
	identMap *identmap.Conf,
) (RoleMapper, Authorizer) {
	return HbaMapper(hbaEntry, identMap),
		func(ctx context.Context, systemIdentity security.SQLUsername, clientConnection bool, pwRetrieveFn PasswordRetrievalFn) (connClose func(), _ error) {
			if len(tlsState.PeerCertificates) == 0 {
				return nil, errors.New("no TLS peer certificates, but required for auth")
			}
			// Normalize the username contained in the certificate.
			tlsState.PeerCertificates[0].Subject.CommonName = tree.Name(
				tlsState.PeerCertificates[0].Subject.CommonName,
			).Normalize()
			hook, err := security.UserAuthCertHook(false /*insecure*/, &tlsState)
			if err != nil {
				return nil, err
			}
			return hook(ctx, systemIdentity, clientConnection)
		}
}

func authCertPassword(
	ctx context.Context,
	c AuthConn,
	tlsState tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
	identMap *identmap.Conf,
) (RoleMapper, Authorizer) {
	var fn AuthMethod
	if len(tlsState.PeerCertificates) == 0 {
		c.LogAuthInfof(ctx, "no client certificate, proceeding with password authentication")
		fn = authPassword
	} else {
		c.LogAuthInfof(ctx, "client presented certificate, proceeding with certificate validation")
		fn = authCert
	}
	return fn(ctx, c, tlsState, execCfg, entry, identMap)
}

func authTrust(
	_ context.Context,
	_ AuthConn,
	_ tls.ConnectionState,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
	_ *identmap.Conf,
) (RoleMapper, Authorizer) {
	return UseProvidedIdentity,
		func(_ context.Context, _ security.SQLUsername, _ bool, _ PasswordRetrievalFn) (func(), error) {
			return nil, nil
		}
}

func authReject(
	_ context.Context,
	_ AuthConn,
	_ tls.ConnectionState,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
	_ *identmap.Conf,
) (RoleMapper, Authorizer) {
	return UseProvidedIdentity,
		func(_ context.Context, _ security.SQLUsername, _ bool, _ PasswordRetrievalFn) (func(), error) {
			return nil, errors.New("authentication rejected by configuration")
		}
}
