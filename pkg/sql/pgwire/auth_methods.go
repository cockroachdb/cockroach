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
		// First step: send a cleartext authentication request to the client.
		if err := c.SendAuthRequest(authCleartextPassword, nil /* data */); err != nil {
			return err
		}

		// While waiting for the client response, concurrently
		// load the credentials from storage (or cache).
		// Note: if this fails, we can't return the error right away,
		// because we need to consume the client response first. This
		// will be handled below.
		hashedPassword, pwValidUntil, pwRetrievalErr := pwRetrieveFn(ctx)

		// Wait for the password response from the client.
		pwdData, err := c.GetPwdData()
		if err != nil {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
			if pwRetrievalErr != nil {
				return errors.CombineErrors(err, pwRetrievalErr)
			}
			return err
		}
		// Now process the password retrieval error, if any.
		if pwRetrievalErr != nil {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_USER_RETRIEVAL_ERROR, pwRetrievalErr)
			return pwRetrievalErr
		}

		// Extract the password response from the client.
		password, err := passwordString(pwdData)
		if err != nil {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
			return err
		}
		// Inform operators looking at logs if there's something amiss.
		if len(hashedPassword) == 0 {
			c.LogAuthInfof(ctx, "user has no password defined")
			// NB: the failure reason will be automatically handled by the fallback
			// in auth.go (and report CREDENTIALS_INVALID).
		}

		// Expiration check.
		if pwValidUntil != nil {
			if pwValidUntil.Sub(timeutil.Now()) < 0 {
				c.LogAuthFailed(ctx, eventpb.AuthFailReason_CREDENTIALS_EXPIRED, nil)
				return errors.New("password is expired")
			}
		}

		// Now check the cleartext password against the retrieved credentials.
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
