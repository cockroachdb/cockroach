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
type AuthMethod = func(
	ctx context.Context,
	c AuthConn,
	tlsState tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
	identMap *identmap.Conf,
) (*AuthBehaviors, error)

// AuthBehaviors encapsulates the per-connection behaviors that may be
// configured. This type is returned by AuthMethod implementations.
//
// Callers should call the AuthBehaviors.ConnClose method once the
// associated network connection has been terminated to allow external
// resources to be released.
type AuthBehaviors struct {
	authenticator       Authenticator
	connClose           func()
	replacementIdentity security.SQLUsername
	replacedIdentity    bool
	roleMapper          RoleMapper
}

// Ensure that an AuthBehaviors is easily composable with itself.
var _ Authenticator = (*AuthBehaviors)(nil).Authenticate
var _ func() = (*AuthBehaviors)(nil).ConnClose
var _ RoleMapper = (*AuthBehaviors)(nil).MapRole

// Authenticate delegates to the Authenticator passed to
// SetAuthenticator or returns an error if SetAuthenticator has not been
// called.
func (b *AuthBehaviors) Authenticate(
	ctx context.Context,
	systemIdentity security.SQLUsername,
	clientConnection bool,
	pwRetrieveFn PasswordRetrievalFn,
) error {
	if found := b.authenticator; found != nil {
		return found(ctx, systemIdentity, clientConnection, pwRetrieveFn)
	}
	return errors.New("no Authenticator provided to AuthBehaviors")
}

// SetAuthenticator updates the Authenticator to be used.
func (b *AuthBehaviors) SetAuthenticator(a Authenticator) {
	b.authenticator = a
}

// ConnClose delegates to the function passed to SetConnClose to release
// any resources associated with the connection. This method is a no-op
// if SetConnClose has not been called or was called with nil.
func (b *AuthBehaviors) ConnClose() {
	if fn := b.connClose; fn != nil {
		fn()
	}
}

// SetConnClose updates the connection-close callback.
func (b *AuthBehaviors) SetConnClose(fn func()) {
	b.connClose = fn
}

// ReplacementIdentity returns an optional replacement for the
// client-provided identity when validating the incoming connection.
// This allows "ambient" authentication mechanisms, such as GSSAPI, to
// provide replacement values. This method will return ok==false if
// SetReplacementIdentity has not been called.
func (b *AuthBehaviors) ReplacementIdentity() (_ security.SQLUsername, ok bool) {
	return b.replacementIdentity, b.replacedIdentity
}

// SetReplacementIdentity allows the AuthMethod to override the
// client-reported system identity.
func (b *AuthBehaviors) SetReplacementIdentity(id security.SQLUsername) {
	b.replacementIdentity = id
	b.replacedIdentity = true
}

// MapRole delegates to the RoleMapper passed to SetRoleMapper or
// returns an error if SetRoleMapper has not been called.
func (b *AuthBehaviors) MapRole(
	ctx context.Context, systemIdentity security.SQLUsername,
) ([]security.SQLUsername, error) {
	if found := b.roleMapper; found != nil {
		return found(ctx, systemIdentity)
	}
	return nil, errors.New("no RoleMapper provided to AuthBehaviors")
}

// SetRoleMapper updates the RoleMapper to be used.
func (b *AuthBehaviors) SetRoleMapper(m RoleMapper) {
	b.roleMapper = m
}

// Authenticator is a component of an AuthMethod that determines if the
// given system identity (e.g.: Kerberos or X.509 principal, plain-old
// username, etc) is who it claims to be.
type Authenticator = func(
	ctx context.Context,
	systemIdentity security.SQLUsername,
	clientConnection bool,
	pwRetrieveFn PasswordRetrievalFn,
) error

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
) (*AuthBehaviors, error) {
	b := &AuthBehaviors{}
	b.SetRoleMapper(UseProvidedIdentity)
	b.SetAuthenticator(func(
		ctx context.Context,
		systemIdentity security.SQLUsername,
		clientConnection bool,
		pwRetrieveFn PasswordRetrievalFn,
	) error {
		if err := c.SendAuthRequest(authCleartextPassword, nil /* data */); err != nil {
			return err
		}
		pwdData, err := c.GetPwdData()
		if err != nil {
			return err
		}
		password, err := passwordString(pwdData)
		if err != nil {
			return err
		}
		hashedPassword, pwValidUntil, err := pwRetrieveFn(ctx)
		if err != nil {
			return err
		}
		if len(hashedPassword) == 0 {
			c.LogAuthInfof(ctx, "user has no password defined")
		}

		if pwValidUntil != nil {
			if pwValidUntil.Sub(timeutil.Now()) < 0 {
				c.LogAuthFailed(ctx, eventpb.AuthFailReason_CREDENTIALS_EXPIRED, nil)
				return errors.New("password is expired")
			}
		}
		return security.UserAuthPasswordHook(
			false /*insecure*/, password, hashedPassword,
		)(ctx, systemIdentity, clientConnection)
	})
	return b, nil
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
) (*AuthBehaviors, error) {
	b := &AuthBehaviors{}
	b.SetRoleMapper(HbaMapper(hbaEntry, identMap))
	b.SetAuthenticator(func(
		ctx context.Context,
		systemIdentity security.SQLUsername,
		clientConnection bool,
		pwRetrieveFn PasswordRetrievalFn,
	) error {
		if len(tlsState.PeerCertificates) == 0 {
			return errors.New("no TLS peer certificates, but required for auth")
		}
		// Normalize the username contained in the certificate.
		tlsState.PeerCertificates[0].Subject.CommonName = tree.Name(
			tlsState.PeerCertificates[0].Subject.CommonName,
		).Normalize()
		hook, err := security.UserAuthCertHook(false /*insecure*/, &tlsState)
		if err != nil {
			return err
		}
		return hook(ctx, systemIdentity, clientConnection)
	})
	return b, nil
}

func authCertPassword(
	ctx context.Context,
	c AuthConn,
	tlsState tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
	identMap *identmap.Conf,
) (*AuthBehaviors, error) {
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
) (*AuthBehaviors, error) {
	b := &AuthBehaviors{}
	b.SetRoleMapper(UseProvidedIdentity)
	b.SetAuthenticator(func(_ context.Context, _ security.SQLUsername, _ bool, _ PasswordRetrievalFn) error {
		return nil
	})
	return b, nil
}

func authReject(
	_ context.Context,
	_ AuthConn,
	_ tls.ConnectionState,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
	_ *identmap.Conf,
) (*AuthBehaviors, error) {
	b := &AuthBehaviors{}
	b.SetRoleMapper(UseProvidedIdentity)
	b.SetAuthenticator(func(_ context.Context, _ security.SQLUsername, _ bool, _ PasswordRetrievalFn) error {
		return errors.New("authentication rejected by configuration")
	})
	return b, nil
}
