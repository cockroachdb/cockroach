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
	RegisterAuthMethod("password", authPassword, hba.ConnAny, nil)

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
	RegisterAuthMethod("reject", authReject, hba.ConnAny, nil)

	// The "trust" method accepts any connection attempt that matches
	// the current rule.
	RegisterAuthMethod("trust", authTrust, hba.ConnAny, nil)
}

// AuthMethod defines a method for authentication of a connection.
type AuthMethod func(
	ctx context.Context,
	c AuthConn,
	tlsState tls.ConnectionState,
	pwRetrieveFn PasswordRetrievalFn,
	pwValidUntilFn PasswordValidUntilFn,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error)

// PasswordRetrievalFn defines a method to retrieve the hashed
// password for the user logging in.
type PasswordRetrievalFn = func(context.Context) ([]byte, error)

// PasswordValidUntilFn defines a method to retrieve the expiration time
// of the user's password.
type PasswordValidUntilFn = func(context.Context) (*tree.DTimestamp, error)

func authPassword(
	ctx context.Context,
	c AuthConn,
	_ tls.ConnectionState,
	pwRetrieveFn PasswordRetrievalFn,
	pwValidUntilFn PasswordValidUntilFn,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
) (security.UserAuthHook, error) {
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
	hashedPassword, err := pwRetrieveFn(ctx)
	if err != nil {
		return nil, err
	}
	if len(hashedPassword) == 0 {
		c.LogAuthInfof(ctx, "user has no password defined")
	}

	validUntil, err := pwValidUntilFn(ctx)
	if err != nil {
		return nil, err
	}
	if validUntil != nil {
		if validUntil.Sub(timeutil.Now()) < 0 {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_CREDENTIALS_EXPIRED, nil)
			return nil, errors.New("password is expired")
		}
	}

	return security.UserAuthPasswordHook(
		false /*insecure*/, password, hashedPassword,
	), nil
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
	_ PasswordRetrievalFn,
	_ PasswordValidUntilFn,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
) (security.UserAuthHook, error) {
	if len(tlsState.PeerCertificates) == 0 {
		return nil, errors.New("no TLS peer certificates, but required for auth")
	}
	// Normalize the username contained in the certificate.
	tlsState.PeerCertificates[0].Subject.CommonName = tree.Name(
		tlsState.PeerCertificates[0].Subject.CommonName,
	).Normalize()
	return security.UserAuthCertHook(false /*insecure*/, &tlsState)
}

func authCertPassword(
	ctx context.Context,
	c AuthConn,
	tlsState tls.ConnectionState,
	pwRetrieveFn PasswordRetrievalFn,
	pwValidUntilFn PasswordValidUntilFn,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	var fn AuthMethod
	if len(tlsState.PeerCertificates) == 0 {
		c.LogAuthInfof(ctx, "no client certificate, proceeding with password authentication")
		fn = authPassword
	} else {
		c.LogAuthInfof(ctx, "client presented certificate, proceeding with certificate validation")
		fn = authCert
	}
	return fn(ctx, c, tlsState, pwRetrieveFn, pwValidUntilFn, execCfg, entry)
}

func authTrust(
	_ context.Context,
	_ AuthConn,
	_ tls.ConnectionState,
	_ PasswordRetrievalFn,
	_ PasswordValidUntilFn,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
) (security.UserAuthHook, error) {
	return func(_ context.Context, _ security.SQLUsername, _ bool) (func(), error) { return nil, nil }, nil
}

func authReject(
	_ context.Context,
	_ AuthConn,
	_ tls.ConnectionState,
	_ PasswordRetrievalFn,
	_ PasswordValidUntilFn,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
) (security.UserAuthHook, error) {
	return func(_ context.Context, _ security.SQLUsername, _ bool) (func(), error) {
		return nil, errors.New("authentication rejected by configuration")
	}, nil
}
