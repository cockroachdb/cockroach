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
	"math"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/sessionrevival"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
	"github.com/xdg-go/scram"
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

	// The "scram-sha-256" authentication method uses the 5-way SCRAM
	// handshake to negotiate password authn with the client. It hides
	// the password from the network connection and is non-replayable.
	RegisterAuthMethod("scram-sha-256", authScram, hba.ConnAny,
		chainOptions(
			requireClusterVersion(clusterversion.SCRAMAuthentication),
			NoOptionsAllowed))

	// The "cert-scram-sha-256" method is alike to "cert-password":
	// it allows either a client certificate, or a valid 5-way SCRAM handshake.
	RegisterAuthMethod("cert-scram-sha-256", authCertScram, hba.ConnAny,
		chainOptions(
			requireClusterVersion(clusterversion.SCRAMAuthentication),
			NoOptionsAllowed))

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

var _ AuthMethod = authPassword
var _ AuthMethod = authScram
var _ AuthMethod = authCert
var _ AuthMethod = authCertPassword
var _ AuthMethod = authCertScram
var _ AuthMethod = authTrust
var _ AuthMethod = authReject
var _ AuthMethod = authSessionRevivalToken([]byte{})

// authPassword is the AuthMethod constructor for HBA method
// "password": authenticate using a cleartext password received from
// the client.
func authPassword(
	_ context.Context,
	c AuthConn,
	_ tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
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
		return passwordAuthenticator(ctx, systemIdentity, clientConnection, pwRetrieveFn, c, execCfg)
	})
	return b, nil
}

var errExpiredPassword = errors.New("password is expired")

// passwordAuthenticator is the authenticator function for the
// behavior constructed by authPassword().
func passwordAuthenticator(
	ctx context.Context,
	systemIdentity security.SQLUsername,
	clientConnection bool,
	pwRetrieveFn PasswordRetrievalFn,
	c AuthConn,
	execCfg *sql.ExecutorConfig,
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
	expired, hashedPassword, pwRetrievalErr := pwRetrieveFn(ctx)

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

	// Expiration check.
	//
	// NB: This check is advisory and could be omitted; the retrieval
	// function ensures that the returned hashedPassword is
	// security.MissingPasswordHash when the credentials have expired,
	// so the credential check below would fail anyway.
	if expired {
		c.LogAuthFailed(ctx, eventpb.AuthFailReason_CREDENTIALS_EXPIRED, nil)
		return errExpiredPassword
	} else if hashedPassword.Method() == security.HashMissingPassword {
		c.LogAuthInfof(ctx, "user has no password defined")
		// NB: the failure reason will be automatically handled by the fallback
		// in auth.go (and report CREDENTIALS_INVALID).
	}

	// Now check the cleartext password against the retrieved credentials.
	err = security.UserAuthPasswordHook(
		false /*insecure*/, password, hashedPassword,
	)(ctx, systemIdentity, clientConnection)

	if err == nil {
		// Password authentication succeeded using cleartext.  If the
		// stored hash was encoded using crdb-bcrypt, we might want to
		// upgrade it to SCRAM instead.
		//
		// This auto-conversion is a CockroachDB-specific feature, which
		// pushes clusters upgraded from a previous version into using
		// SCRAM-SHA-256.
		sql.MaybeUpgradeStoredPasswordHash(ctx,
			execCfg,
			systemIdentity,
			password, hashedPassword)
	}

	return err
}

func passwordString(pwdData []byte) (string, error) {
	// Make a string out of the byte array.
	if bytes.IndexByte(pwdData, 0) != len(pwdData)-1 {
		return "", fmt.Errorf("expected 0-terminated byte array")
	}
	return string(pwdData[:len(pwdData)-1]), nil
}

// authScram is the AuthMethod constructor for HBA method
// "scram-sha-256": authenticate using a 5-way SCRAM handshake with
// the client.
// It is also the fallback constructor for HBA method
// "cert-scram-sha-256", when the SQL client does not provide a TLS
// client certificate.
func authScram(
	ctx context.Context,
	c AuthConn,
	_ tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
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
		return scramAuthenticator(ctx, systemIdentity, clientConnection, pwRetrieveFn, c, execCfg)
	})
	return b, nil
}

// scramAuthenticator is the authenticator function for the
// behavior constructed by authScram().
func scramAuthenticator(
	ctx context.Context,
	systemIdentity security.SQLUsername,
	clientConnection bool,
	pwRetrieveFn PasswordRetrievalFn,
	c AuthConn,
	execCfg *sql.ExecutorConfig,
) error {
	// First step: send a SCRAM authentication request to the client.
	// We do this with an auth request with the request type SASL,
	// and a payload containing the list of supported SCRAM methods.
	//
	// NB: SCRAM-SHA-256-PLUS is not supported, see
	// https://github.com/cockroachdb/cockroach/issues/74300
	// There is one nul byte to terminate the first string,
	// then another nul byte to terminate the list.
	const supportedMethods = "SCRAM-SHA-256\x00\x00"
	if err := c.SendAuthRequest(authReqSASL, []byte(supportedMethods)); err != nil {
		return err
	}

	// While waiting for the client response, concurrently
	// load the credentials from storage (or cache).
	// Note: if this fails, we can't return the error right away,
	// because we need to consume the client response first. This
	// will be handled below.
	expired, hashedPassword, pwRetrievalErr := pwRetrieveFn(ctx)

	scramServer, _ := scram.SHA256.NewServer(func(user string) (creds scram.StoredCredentials, err error) {
		// NB: the username passed in the SCRAM exchange (the user
		// parameter in this callback) is ignored by PostgreSQL servers;
		// see auth-scram.c, read_client_first_message().
		//
		// Therefore, we can't assume that SQL client drivers populate anything
		// useful there. So we ignore it too.

		// We still need to check whether the credentials loaded above
		// are valid. We place this check in this callback because it
		// only needs to happen after the SCRAM handshake actually needs
		// to know the credentials.
		if expired {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_CREDENTIALS_EXPIRED, nil)
			return creds, errExpiredPassword
		} else if hashedPassword.Method() != security.HashSCRAMSHA256 {
			const credentialsNotSCRAM = "user password hash not in SCRAM format"
			c.LogAuthInfof(ctx, credentialsNotSCRAM)
			return creds, errors.New(credentialsNotSCRAM)
		}

		// The method check above ensures this cast is always valid.
		ok, creds := security.GetSCRAMStoredCredentials(hashedPassword)
		if !ok {
			return creds, errors.AssertionFailedf("programming error: hash method is SCRAM but no stored credentials")
		}
		return creds, nil
	})

	handshake := scramServer.NewConversation()

	initial := true
	for {
		if handshake.Done() {
			break
		}

		// Receive a response from the client.
		resp, err := c.GetPwdData()
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

		var input []byte
		if initial {
			// Quoth postgres, backend/auth.go:
			//
			// The first SASLInitialResponse message is different from the others.
			// It indicates which SASL mechanism the client selected, and contains
			// an optional Initial Client Response payload. The subsequent
			// SASLResponse messages contain just the SASL payload.
			//
			rb := pgwirebase.ReadBuffer{Msg: resp}
			reqMethod, err := rb.GetString()
			if err != nil {
				c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
				return err
			}
			if reqMethod != "SCRAM-SHA-256" {
				c.LogAuthInfof(ctx, "client requests unknown scram method %q", reqMethod)
				err := unimplemented.NewWithIssue(74300, "channel binding not supported")
				// We need to manually report the unimplemented error because it is not
				// passed through to the client as-is (authn errors are hidden behind
				// a generic "authn failed" error).
				sqltelemetry.RecordError(ctx, err, &execCfg.Settings.SV)
				return err
			}
			inputLen, err := rb.GetUint32()
			if err != nil {
				c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
				return err
			}
			// PostgreSQL ignores input from clients that pass -1 as length,
			// but does not treat it as invalid.
			if inputLen < math.MaxUint32 {
				input, err = rb.GetBytes(int(inputLen))
				if err != nil {
					c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
					return err
				}
			}
			initial = false
		} else {
			input = resp
		}

		// Feed the client message to the state machine.
		got, err := handshake.Step(string(input))
		if err != nil {
			c.LogAuthInfof(ctx, "scram handshake error: %v", err)
			break
		}
		// Decide which response to send to the client.
		reqType := authReqSASLContinue
		if handshake.Done() {
			// This is the last message.
			reqType = authReqSASLFin
		}
		// Send the response to the client.
		if err := c.SendAuthRequest(reqType, []byte(got)); err != nil {
			return err
		}
	}

	// Did authentication succeed?
	if !handshake.Valid() {
		return security.NewErrPasswordUserAuthFailed(systemIdentity)
	}

	return nil // auth success!
}

// authCert is the AuthMethod constructor for HBA method "cert":
// authenticate using TLS client certificates.
// It is also the fallback constructor for HBA methods "cert-password"
// and "cert-scram-sha-256" when the SQL client provides a TLS client
// certificate.
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

// authCertPassword is the AuthMethod constructor for HBA method
// "cert-password": authenticate EITHER using a TLS client cert OR a
// password exchange.
//
// TLS client cert authn is used iff the client presents a TLS client cert.
// Otherwise, the password authentication protocol is chosen
// depending on the format of the stored credentials: SCRAM is preferred
// if possible, otherwise fallback to cleartext.
// See the documentation for authAutoSelectPasswordProtocol() below.
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
		c.LogAuthInfof(ctx, "client did not present TLS certificate")
		if AutoSelectPasswordAuth.Get(&execCfg.Settings.SV) {
			// We don't call c.LogAuthInfo here; this is done in
			// authAutoSelectPasswordProtocol() below.
			fn = authAutoSelectPasswordProtocol
		} else {
			c.LogAuthInfof(ctx, "proceeding with password authentication")
			fn = authPassword
		}
	} else {
		c.LogAuthInfof(ctx, "client presented certificate, proceeding with certificate validation")
		fn = authCert
	}
	return fn(ctx, c, tlsState, execCfg, entry, identMap)
}

// AutoSelectPasswordAuth determines whether CockroachDB automatically promotes the password
// protocol when a SCRAM hash is detected in the stored credentials.
//
// It is exported for use in tests.
var AutoSelectPasswordAuth = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"server.user_login.cert_password_method.auto_scram_promotion.enabled",
	"whether to automatically promote cert-password authentication to use SCRAM",
	true,
).WithPublic()

// authAutoSelectPasswordProtocol is the AuthMethod constructor used
// for HBA method "cert-password" when the SQL client does not provide
// a TLS client certificate.
//
// It uses the effective format of the stored hash password to decide
// the hash protocol: if the stored hash uses the SCRAM encoding,
// SCRAM-SHA-256 is used (which is a safer handshake); otherwise,
// cleartext password authentication is used.
func authAutoSelectPasswordProtocol(
	_ context.Context,
	c AuthConn,
	_ tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
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
		// Request information about the password hash.
		expired, hashedPassword, err := pwRetrieveFn(ctx)
		// Note: we could be checking 'expired' and 'err' here, and exit
		// early. However, we already have code paths to do just that in
		// each authenticator, so we might as well use them. To do this,
		// we capture the same information into the closure that the
		// authenticator will call anyway.
		newpwfn := func(ctx context.Context) (bool, security.PasswordHash, error) { return expired, hashedPassword, err }

		// Was the password using the bcrypt hash encoding?
		if err == nil && hashedPassword.Method() == security.HashBCrypt {
			// Yes: we have no choice but to request a cleartext password.
			c.LogAuthInfof(ctx, "found stored crdb-bcrypt credentials; requesting cleartext password")
			return passwordAuthenticator(ctx, systemIdentity, clientConnection, newpwfn, c, execCfg)
		}

		// Error, no credentials or stored SCRAM hash: use the
		// SCRAM-SHA-256 logic.
		//
		// Note: we use SCRAM as a fallback as an additional security
		// measure: if the password retrieval fails due to a transient
		// error, we don't want the fallback to force the client to
		// transmit a password in clear.
		c.LogAuthInfof(ctx, "no crdb-bcrypt credentials found; proceeding with SCRAM-SHA-256")
		return scramAuthenticator(ctx, systemIdentity, clientConnection, newpwfn, c, execCfg)
	})
	return b, nil
}

// authCertPassword is the AuthMethod constructor for HBA method
// "cert-scram-sha-256": authenticate EITHER using a TLS client cert
// OR a valid SCRAM exchange.
// TLS client cert authn is used iff the client presents a TLS client cert.
func authCertScram(
	ctx context.Context,
	c AuthConn,
	tlsState tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
	identMap *identmap.Conf,
) (*AuthBehaviors, error) {
	var fn AuthMethod
	if len(tlsState.PeerCertificates) == 0 {
		c.LogAuthInfof(ctx, "no client certificate, proceeding with SCRAM authentication")
		fn = authScram
	} else {
		c.LogAuthInfof(ctx, "client presented certificate, proceeding with certificate validation")
		fn = authCert
	}
	return fn(ctx, c, tlsState, execCfg, entry, identMap)
}

// authTrust is the AuthMethod constructor for HBA method "trust":
// always allow the client, do not perform authentication.
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

// authReject is the AuthMethod constructor for HBA method "reject":
// never allow the client.
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

// authSessionRevivalToken is the AuthMethod constructor for the CRDB-specific
// session revival token.
func authSessionRevivalToken(token []byte) AuthMethod {
	return func(
		_ context.Context,
		c AuthConn,
		_ tls.ConnectionState,
		execCfg *sql.ExecutorConfig,
		_ *hba.Entry,
		_ *identmap.Conf,
	) (*AuthBehaviors, error) {
		b := &AuthBehaviors{}
		b.SetRoleMapper(UseProvidedIdentity)
		b.SetAuthenticator(func(ctx context.Context, user security.SQLUsername, _ bool, _ PasswordRetrievalFn) error {
			c.LogAuthInfof(ctx, "session revival token detected; attempting to use it")
			if !sql.AllowSessionRevival.Get(&execCfg.Settings.SV) || execCfg.Codec.ForSystemTenant() {
				return errors.New("session revival tokens are not supported on this cluster")
			}
			cm, err := execCfg.RPCContext.SecurityContext.GetCertificateManager()
			if err != nil {
				return err
			}
			if err := sessionrevival.ValidateSessionRevivalToken(cm, user, token); err != nil {
				return errors.Wrap(err, "invalid session revival token")
			}
			return nil
		})
		return b, nil
	}
}
