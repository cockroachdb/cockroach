// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/distinguishedname"
	"github.com/cockroachdb/cockroach/pkg/security/password"
	"github.com/cockroachdb/cockroach/pkg/security/sessionrevival"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/go-ldap/ldap/v3"
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
	RegisterAuthMethod("scram-sha-256", authScram, hba.ConnAny, NoOptionsAllowed)

	// The "cert-scram-sha-256" method is alike to "cert-password":
	// it allows either a client certificate, or a valid 5-way SCRAM handshake.
	RegisterAuthMethod("cert-scram-sha-256", authCertScram, hba.ConnAny, NoOptionsAllowed)

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
	sessionUser username.SQLUsername,
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
var _ AuthMethod = authJwtToken
var _ AuthMethod = AuthLDAP

// authPassword is the AuthMethod constructor for HBA method
// "password": authenticate using a cleartext password received from
// the client.
func authPassword(
	_ context.Context,
	c AuthConn,
	user username.SQLUsername,
	_ tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	_ *hba.Entry,
	_ *identmap.Conf,
) (*AuthBehaviors, error) {
	b := &AuthBehaviors{}
	b.SetRoleMapper(UseProvidedIdentity)
	b.SetAuthenticator(func(
		ctx context.Context,
		systemIdentity string,
		clientConnection bool,
		pwRetrieveFn PasswordRetrievalFn,
		_ *ldap.DN,
	) error {
		return passwordAuthenticator(ctx, user, clientConnection, pwRetrieveFn, c, execCfg)
	})
	return b, nil
}

var errExpiredPassword = errors.New("password is expired")

// passwordAuthenticator is the authenticator function for the
// behavior constructed by authPassword().
func passwordAuthenticator(
	ctx context.Context,
	user username.SQLUsername,
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
	passwordStr, err := passwordString(pwdData)
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
	} else if hashedPassword.Method() == password.HashMissingPassword {
		c.LogAuthInfof(ctx, "user has no password defined")
		// NB: the failure reason will be automatically handled by the fallback
		// in auth.go (and report CREDENTIALS_INVALID).
	}

	metrics := c.GetTenantSpecificMetrics()
	// Now check the cleartext password against the retrieved credentials.
	if err := security.UserAuthPasswordHook(
		false, passwordStr, hashedPassword, metrics.ConnsWaitingToHash,
	)(ctx, user.Normalized(), clientConnection); err != nil {
		if errors.HasType(err, &security.PasswordUserAuthError{}) {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_CREDENTIALS_INVALID, err)
		}
		return err
	}

	// Password authentication succeeded using cleartext.  If the stored hash
	// was encoded using crdb-bcrypt, we might want to upgrade it to SCRAM
	// instead. Conversely, if the stored hash was encoded using SCRAM, we might
	// want to downgrade it to crdb-bcrypt.
	//
	// This auto-conversion is a CockroachDB-specific feature, which pushes
	// clusters upgraded from a previous version into using SCRAM-SHA-256, and
	// makes it easy to rollback from SCRAM-SHA-256 if there are issues.
	sql.MaybeConvertStoredPasswordHash(ctx,
		execCfg,
		user,
		passwordStr, hashedPassword)

	return nil
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
	user username.SQLUsername,
	_ tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	_ *hba.Entry,
	_ *identmap.Conf,
) (*AuthBehaviors, error) {
	b := &AuthBehaviors{}
	b.SetRoleMapper(UseProvidedIdentity)
	b.SetAuthenticator(func(
		ctx context.Context,
		systemIdentity string,
		clientConnection bool,
		pwRetrieveFn PasswordRetrievalFn,
		_ *ldap.DN,
	) error {
		return scramAuthenticator(ctx, user, clientConnection, pwRetrieveFn, c, execCfg)
	})
	return b, nil
}

// scramAuthenticator is the authenticator function for the
// behavior constructed by authScram().
func scramAuthenticator(
	ctx context.Context,
	systemIdentity username.SQLUsername,
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
		} else if hashedPassword.Method() != password.HashSCRAMSHA256 {
			credentialsNotSCRAMErr := errors.New("user password hash not in SCRAM format")
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, credentialsNotSCRAMErr)
			return creds, credentialsNotSCRAMErr
		}

		// The method check above ensures this cast is always valid.
		ok, creds := password.GetSCRAMStoredCredentials(hashedPassword)
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
			reqMethod, err := rb.GetUnsafeString()
			if err != nil {
				c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
				return err
			}
			if reqMethod != "SCRAM-SHA-256" {
				c.LogAuthInfof(ctx, redact.Sprintf("client requests unknown scram method %q", redact.SafeString(reqMethod)))
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
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, errors.Wrap(err, "scram handshake error"))
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
	_ username.SQLUsername,
	tlsState tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	hbaEntry *hba.Entry,
	identMap *identmap.Conf,
) (*AuthBehaviors, error) {
	b := &AuthBehaviors{}
	b.SetRoleMapper(HbaMapper(hbaEntry, identMap))
	b.SetAuthenticator(func(
		ctx context.Context,
		systemIdentity string,
		clientConnection bool,
		pwRetrieveFn PasswordRetrievalFn,
		roleSubject *ldap.DN,
	) error {
		if len(tlsState.PeerCertificates) == 0 {
			return errors.New("no TLS peer certificates, but required for auth")
		}
		// Normalize the username contained in the certificate.
		tlsState.PeerCertificates[0].Subject.CommonName = tree.Name(
			tlsState.PeerCertificates[0].Subject.CommonName,
		).Normalize()

		cm, err := execCfg.RPCContext.SecurityContext.GetCertificateManager()
		if err != nil {
			log.Ops.Warningf(ctx, "failed to get cert manager info: %v", err)
		}

		hook, err := security.UserAuthCertHook(
			false, /*insecure*/
			&tlsState,
			execCfg.RPCContext.TenantID,
			cm,
			roleSubject,
			security.ClientCertSubjectRequired.Get(&execCfg.Settings.SV),
		)
		if err != nil {
			return err
		}
		return hook(ctx, systemIdentity, clientConnection)
	})
	if len(tlsState.PeerCertificates) > 0 && hbaEntry.GetOption("map") != "" {
		// The common name in the certificate is set as the system identity in case we have an HBAEntry for db user.
		b.SetReplacementIdentity(
			lexbase.NormalizeName(tlsState.PeerCertificates[0].Subject.CommonName),
		)
	}
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
	sessionUser username.SQLUsername,
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
	return fn(ctx, c, sessionUser, tlsState, execCfg, entry, identMap)
}

// AutoSelectPasswordAuth determines whether CockroachDB automatically promotes the password
// protocol when a SCRAM hash is detected in the stored credentials.
//
// It is exported for use in tests.
var AutoSelectPasswordAuth = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.user_login.cert_password_method.auto_scram_promotion.enabled",
	"whether to automatically promote cert-password authentication to use SCRAM",
	true,
	settings.WithPublic)

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
	user username.SQLUsername,
	_ tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	_ *hba.Entry,
	_ *identmap.Conf,
) (*AuthBehaviors, error) {
	b := &AuthBehaviors{}
	b.SetRoleMapper(UseProvidedIdentity)
	b.SetAuthenticator(func(
		ctx context.Context,
		systemIdentity string,
		clientConnection bool,
		pwRetrieveFn PasswordRetrievalFn,
		_ *ldap.DN,
	) error {
		// Request information about the password hash.
		expired, hashedPassword, pwRetrieveErr := pwRetrieveFn(ctx)
		// Note: we could be checking 'expired' and 'err' here, and exit
		// early. However, we already have code paths to do just that in
		// each authenticator, so we might as well use them. To do this,
		// we capture the same information into the closure that the
		// authenticator will call anyway.
		newpwfn := func(ctx context.Context) (bool, password.PasswordHash, error) {
			return expired, hashedPassword, pwRetrieveErr
		}

		// Was the password using the bcrypt hash encoding?
		if pwRetrieveErr == nil && hashedPassword.Method() == password.HashBCrypt {
			// Yes: we have no choice but to request a cleartext password.
			c.LogAuthInfof(ctx, "found stored crdb-bcrypt credentials; requesting cleartext password")
			return passwordAuthenticator(ctx, user, clientConnection, newpwfn, c, execCfg)
		}

		if pwRetrieveErr == nil && hashedPassword.Method() == password.HashSCRAMSHA256 {
			autoDowngradePasswordHashesBool := security.AutoDowngradePasswordHashes.Get(&execCfg.Settings.SV)
			autoRehashOnCostChangeBool := security.AutoRehashOnSCRAMCostChange.Get(&execCfg.Settings.SV)
			configuredHashMethod := security.GetConfiguredPasswordHashMethod(&execCfg.Settings.SV)
			configuredSCRAMCost := security.SCRAMCost.Get(&execCfg.Settings.SV)

			if autoDowngradePasswordHashesBool && configuredHashMethod == password.HashBCrypt {
				// If the cluster is configured to automatically downgrade from SCRAM to
				// bcrypt, then we also request the cleartext password.
				c.LogAuthInfof(ctx, "found stored SCRAM-SHA-256 credentials but cluster is configured to downgrade to bcrypt; requesting cleartext password")
				return passwordAuthenticator(ctx, user, clientConnection, newpwfn, c, execCfg)
			}

			if autoRehashOnCostChangeBool && configuredHashMethod == password.HashSCRAMSHA256 {
				ok, creds := password.GetSCRAMStoredCredentials(hashedPassword)
				if !ok {
					return errors.AssertionFailedf("programming error: password retrieved but invalid scram hash")
				}
				if int64(creds.Iters) != configuredSCRAMCost {
					// If the cluster is configured to automatically re-hash the SCRAM
					// password when the default cost is changed, then we also request the
					// cleartext password.
					c.LogAuthInfof(ctx, "found stored SCRAM-SHA-256 credentials but cluster is configured to re-hash after SCRAM cost change; requesting cleartext password")
					return passwordAuthenticator(ctx, user, clientConnection, newpwfn, c, execCfg)
				}
			}
		}

		// Error, no credentials or stored SCRAM hash: use the
		// SCRAM-SHA-256 logic.
		//
		// Note: we use SCRAM as a fallback as an additional security
		// measure: if the password retrieval fails due to a transient
		// error, we don't want the fallback to force the client to
		// transmit a password in clear.
		c.LogAuthInfof(ctx, "no crdb-bcrypt credentials found; proceeding with SCRAM-SHA-256")
		return scramAuthenticator(ctx, user, clientConnection, newpwfn, c, execCfg)
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
	sessionUser username.SQLUsername,
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
	return fn(ctx, c, sessionUser, tlsState, execCfg, entry, identMap)
}

// authTrust is the AuthMethod constructor for HBA method "trust":
// always allow the client, do not perform authentication.
func authTrust(
	_ context.Context,
	_ AuthConn,
	_ username.SQLUsername,
	_ tls.ConnectionState,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
	_ *identmap.Conf,
) (*AuthBehaviors, error) {
	b := &AuthBehaviors{}
	b.SetRoleMapper(UseProvidedIdentity)
	b.SetAuthenticator(func(_ context.Context, _ string, _ bool, _ PasswordRetrievalFn, _ *ldap.DN) error {
		return nil
	})
	return b, nil
}

// authReject is the AuthMethod constructor for HBA method "reject":
// never allow the client.
func authReject(
	_ context.Context,
	c AuthConn,
	_ username.SQLUsername,
	_ tls.ConnectionState,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
	_ *identmap.Conf,
) (*AuthBehaviors, error) {
	b := &AuthBehaviors{}
	b.SetRoleMapper(UseProvidedIdentity)
	b.SetAuthenticator(func(
		ctx context.Context, _ string, _ bool, _ PasswordRetrievalFn, _ *ldap.DN,
	) error {
		err := errors.New("authentication rejected by configuration")
		c.LogAuthFailed(ctx, eventpb.AuthFailReason_LOGIN_DISABLED, err)
		return err
	})
	return b, nil
}

// authSessionRevivalToken is the AuthMethod constructor for the CRDB-specific
// session revival token.
// The session revival token is passed in the crdb:session_revival_token_base64
// field during initial connection. This value is then extracted, base64 decoded
// and verified.
// This field is only expected to be used in instances where we have a SQL proxy.
// The SQL proxy prevents the end customer from sending this field.  In the future,
// We may decide to pass the token in the password field with a boolean field to
// indicate the contents of the password field is a sessionRevivalToken as an
// additional method. This could reduce the risk of the sessionRevivalToken being
// logged accidentally. This risk is already fairly low because it should only be
// passed by the SQL proxy.
func authSessionRevivalToken(token []byte) AuthMethod {
	return func(
		_ context.Context,
		c AuthConn,
		user username.SQLUsername,
		_ tls.ConnectionState,
		execCfg *sql.ExecutorConfig,
		_ *hba.Entry,
		_ *identmap.Conf,
	) (*AuthBehaviors, error) {
		b := &AuthBehaviors{}
		b.SetRoleMapper(UseProvidedIdentity)
		b.SetAuthenticator(func(
			ctx context.Context, systemIdentity string, _ bool, _ PasswordRetrievalFn, _ *ldap.DN,
		) error {
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

// JWTVerifier is an interface for the `jwtauthccl` library to add JWT login support.
// This interface has a method that validates whether a given JWT token is a proper
// credential for a given user to login.
type JWTVerifier interface {
	ValidateJWTLogin(_ context.Context, _ *cluster.Settings,
		_ username.SQLUsername,
		_ []byte,
		_ *identmap.Conf,
	) (detailedErrorMsg redact.RedactableString, authError error)

	// RetrieveIdentity retrieves the user identity from the JWT.
	//
	// If a user identity is provided as input, it matches it against the token
	// principals. In case of a match, it returns the matched user and no
	// error. Otherwise, it returns the input user along with the error.
	//
	// If a user identity is not provided as input, and there is a single token
	// principal to match against, it returns this user identity and no error.
	// If there are multiple matches, then it returns an error.
	RetrieveIdentity(
		_ context.Context, _ username.SQLUsername, _ []byte, _ *identmap.Conf,
	) (retrievedUser username.SQLUsername, authError error)
}

var jwtVerifier JWTVerifier

type noJWTConfigured struct{}

func (c *noJWTConfigured) ValidateJWTLogin(
	_ context.Context, _ *cluster.Settings, _ username.SQLUsername, _ []byte, _ *identmap.Conf,
) (detailedErrorMsg redact.RedactableString, authError error) {
	return "", errors.New("JWT token authentication requires CCL features")
}

func (c *noJWTConfigured) RetrieveIdentity(
	_ context.Context, u username.SQLUsername, _ []byte, _ *identmap.Conf,
) (retrievedUser username.SQLUsername, authError error) {
	return u, errors.New("JWT token authentication requires CCL features")
}

// ConfigureJWTAuth is a hook for the `jwtauthccl` library to add JWT login support. It's called to
// setup the JWTVerifier just as it is needed.
var ConfigureJWTAuth = func(
	serverCtx context.Context,
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	clusterUUID uuid.UUID,
) JWTVerifier {
	return &noJWTConfigured{}
}

// authJwtToken is the AuthMethod constructor for the CRDB-specific
// jwt auth token.
// The method is triggered when the client passes a specific option indicating
// that it is passing a token in the password field. The token is then extracted
// from the password field and verified.
// The decision was made to pass the token in the password field instead of in
// the options parameter as some drivers may insecurely handle options parameters.
// In contrast, all drivers SHOULD know not to log the password, for example.
func authJwtToken(
	sctx context.Context,
	c AuthConn,
	user username.SQLUsername,
	_ tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	_ *hba.Entry,
	identMap *identmap.Conf,
) (*AuthBehaviors, error) {
	// Initialize the jwt verifier if it hasn't been already.
	if jwtVerifier == nil {
		jwtVerifier = ConfigureJWTAuth(sctx, execCfg.AmbientCtx, execCfg.Settings, execCfg.NodeInfo.LogicalClusterID())
	}
	b := &AuthBehaviors{}
	b.SetRoleMapper(UseProvidedIdentity)
	b.SetAuthenticator(func(
		ctx context.Context, systemIdentity string, clientConnection bool, pwRetrieveFn PasswordRetrievalFn, _ *ldap.DN,
	) error {
		c.LogAuthInfof(ctx, "JWT token detected; attempting to use it")
		if !clientConnection {
			err := errors.New("JWT token authentication is only available for client connections")
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
			return err
		}
		// Request password from client.
		if err := c.SendAuthRequest(authCleartextPassword, nil /* data */); err != nil {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
			return err
		}
		// Wait for the password response from the client.
		pwdData, err := c.GetPwdData()
		if err != nil {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
			return err
		}

		// Extract the token response from the password field.
		token, err := passwordString(pwdData)
		if err != nil {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
			return err
		}
		// If there is no token send the Password Auth Failed error to make the client prompt for a password.
		if len(token) == 0 {
			return security.NewErrPasswordUserAuthFailed(user)
		}
		if detailedErrors, authError := jwtVerifier.ValidateJWTLogin(ctx, execCfg.Settings, user, []byte(token), identMap); authError != nil {
			errForLog := authError
			if detailedErrors != "" {
				errForLog = errors.Join(errForLog, errors.Newf("%s", detailedErrors))
			}
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_CREDENTIALS_INVALID, errForLog)
			return authError
		}
		return nil
	})
	return b, nil
}

// LDAPManager is an interface for `ldapauthccl` pkg to add ldap login(authN)
// and groups sync(authZ) support.
type LDAPManager interface {
	// FetchLDAPUserDN extracts the user distinguished name for the sql session
	// user performing a lookup for the user on ldap server using options provided
	// in the hba conf and supplied sql username in db connection string.
	FetchLDAPUserDN(_ context.Context, _ *cluster.Settings,
		_ username.SQLUsername,
		_ *hba.Entry,
		_ *identmap.Conf,
	) (userDN *ldap.DN, detailedErrorMsg redact.RedactableString, authError error)
	// ValidateLDAPLogin validates whether the password supplied could be used to
	// bind to ldap server with the ldap user DN(provided as systemIdentityDN
	// being the "externally-defined" system identity).
	ValidateLDAPLogin(_ context.Context, _ *cluster.Settings,
		_ *ldap.DN,
		_ username.SQLUsername,
		_ string,
		_ *hba.Entry,
		_ *identmap.Conf,
	) (detailedErrorMsg redact.RedactableString, authError error)
	// FetchLDAPGroups retrieves ldap groups for the supplied ldap user
	// DN(provided as systemIdentityDN being the "externally-defined" system
	// identity) performing a group search with the options provided in the hba
	// conf and filtering for the groups which have the user DN as its member.
	FetchLDAPGroups(_ context.Context, _ *cluster.Settings,
		_ *ldap.DN,
		_ username.SQLUsername,
		_ *hba.Entry,
		_ *identmap.Conf,
	) (ldapGroups []*ldap.DN, detailedErrorMsg redact.RedactableString, authError error)
}

// ldapManager is a singleton global pgwire object which gets initialized from
// authLDAP method whenever an LDAP auth attempt happens. It depends on ldapccl
// module to be imported properly to override its default ConfigureLDAPAuth
// constructor.
var ldapManager = struct {
	sync.Once
	m LDAPManager
}{}

type noLDAPConfigured struct{}

func (c *noLDAPConfigured) FetchLDAPUserDN(
	_ context.Context, _ *cluster.Settings, _ username.SQLUsername, _ *hba.Entry, _ *identmap.Conf,
) (retrievedUserDN *ldap.DN, detailedErrorMsg redact.RedactableString, authError error) {
	return nil, "", errors.New("LDAP based authentication requires CCL features")
}

func (c *noLDAPConfigured) ValidateLDAPLogin(
	_ context.Context,
	_ *cluster.Settings,
	_ *ldap.DN,
	_ username.SQLUsername,
	_ string,
	_ *hba.Entry,
	_ *identmap.Conf,
) (detailedErrorMsg redact.RedactableString, authError error) {
	return "", errors.New("LDAP based authentication requires CCL features")
}

func (c *noLDAPConfigured) FetchLDAPGroups(
	_ context.Context,
	_ *cluster.Settings,
	_ *ldap.DN,
	_ username.SQLUsername,
	_ *hba.Entry,
	_ *identmap.Conf,
) (ldapGroups []*ldap.DN, detailedErrorMsg redact.RedactableString, authError error) {
	return nil, "", errors.New("LDAP based authorization requires CCL features")
}

// ConfigureLDAPAuth is a hook for the `ldapauthccl` library to add LDAP login
// support. It's called to setup the LDAPManager just as it is needed.
var ConfigureLDAPAuth = func(
	serverCtx context.Context,
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	clusterUUID uuid.UUID,
) LDAPManager {
	return &noLDAPConfigured{}
}

// AuthLDAP is the AuthMethod constructor for the CRDB-specific ldap auth
// mechanism. The "LDAP" method requires a clear text password which will be
// used to bind with a LDAP server. The remaining connection parameters are
// provided in hba conf options.
//
// Care should be taken by administrators to only accept this auth method over
// secure connections, e.g. those encrypted using SSL.
func AuthLDAP(
	sCtx context.Context,
	c AuthConn,
	sessionUser username.SQLUsername,
	_ tls.ConnectionState,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
	identMap *identmap.Conf,
) (*AuthBehaviors, error) {
	ldapManager.Do(func() {
		if ldapManager.m == nil {
			ldapManager.m = ConfigureLDAPAuth(sCtx, execCfg.AmbientCtx, execCfg.Settings, execCfg.NodeInfo.LogicalClusterID())
		}
	})
	b := &AuthBehaviors{}
	b.SetRoleMapper(UseSpecifiedIdentity(sessionUser))

	ldapUserDN, detailedErrors, authError := ldapManager.m.FetchLDAPUserDN(sCtx, execCfg.Settings, sessionUser, entry, identMap)
	if authError != nil {
		errForLog := authError
		if detailedErrors != "" {
			errForLog = errors.Join(errForLog, errors.Newf("%s", detailedErrors))
		}
		c.LogAuthFailed(sCtx, eventpb.AuthFailReason_USER_RETRIEVAL_ERROR, errForLog)
		return b, authError
	} else {
		// The DN of user from LDAP server is set as the system identity DN which
		// can then be used for authenticator & authorizer AuthBehaviors fn.
		b.SetReplacementIdentity(ldapUserDN.String())
	}

	b.SetAuthenticator(func(
		ctx context.Context, systemIdentity string, clientConnection bool, _ PasswordRetrievalFn, _ *ldap.DN,
	) error {
		c.LogAuthInfof(ctx, "LDAP password provided; attempting to bind to domain")

		// Verify that the systemIdentity is what we expect.
		if ldapUserDN.String() != systemIdentity {
			err := errors.Newf("LDAP user DN mismatch, expected user DN: %s, obtained systemIdentity: %s", ldapUserDN.String(), systemIdentity)
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
			return err
		}

		if !clientConnection {
			err := errors.New("LDAP authentication is only available for client connections")
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
			return err
		}
		// Request password from client.
		if err := c.SendAuthRequest(authCleartextPassword, nil /* data */); err != nil {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
			return err
		}
		// Wait for the password response from the client.
		pwdData, err := c.GetPwdData()
		if err != nil {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
			return err
		}

		// Extract the LDAP password.
		ldapPwd, err := passwordString(pwdData)
		if err != nil {
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_PRE_HOOK_ERROR, err)
			return err
		}
		// If there is no ldap pwd, send the Password Auth Failed error to make the
		// client prompt for a password.
		if len(ldapPwd) == 0 {
			return security.NewErrPasswordUserAuthFailed(sessionUser)
		}
		if detailedErrors, authError := ldapManager.m.ValidateLDAPLogin(
			ctx, execCfg.Settings, ldapUserDN, sessionUser, ldapPwd, entry, identMap,
		); authError != nil {
			errForLog := authError
			if detailedErrors != "" {
				errForLog = errors.Join(errForLog, errors.Newf("%s", detailedErrors))
			}
			c.LogAuthFailed(ctx, eventpb.AuthFailReason_CREDENTIALS_INVALID, errForLog)
			return authError
		}
		return nil
	})

	if entry.GetOption("ldapgrouplistfilter") != "" {
		b.SetAuthorizer(func(ctx context.Context, systemIdentity string, clientConnection bool) error {
			c.LogAuthInfof(ctx, "LDAP authentication succeeded; attempting authorization")

			// Verify that the systemIdentity is what we expect.
			if ldapUserDN.String() != systemIdentity {
				err := errors.Newf("LDAP user DN mismatch, expected user DN: %s, obtained systemIdentity: %s", ldapUserDN.String(), systemIdentity)
				c.LogAuthFailed(ctx, eventpb.AuthFailReason_AUTHORIZATION_ERROR, err)
				return err
			}

			if ldapGroups, detailedErrors, authError := ldapManager.m.FetchLDAPGroups(
				ctx, execCfg.Settings, ldapUserDN, sessionUser, entry, identMap,
			); authError != nil {
				errForLog := errors.Wrapf(authError, "LDAP authorization: error retrieving ldap groups for authorization")
				if detailedErrors != "" {
					errForLog = errors.Join(errForLog, errors.Newf("%s", detailedErrors))
				}
				c.LogAuthFailed(ctx, eventpb.AuthFailReason_AUTHORIZATION_ERROR, errForLog)
				return authError
			} else {
				c.LogAuthInfof(ctx, redact.Sprintf("LDAP authorization sync succeeded; attempting to assign roles for LDAP groups: %s", ldapGroups))
				// Parse and apply transformation to LDAP group DNs for roles granter.
				sqlRoles := make([]username.SQLUsername, 0, len(ldapGroups))
				for _, ldapGroup := range ldapGroups {
					// Extract the CN from the LDAP group DN to use as the SQL role.
					sqlRole, found, err := distinguishedname.ExtractCNAsSQLUsername(ldapGroup)
					if err != nil {
						err := errors.Wrapf(err, "LDAP authorization: error finding matching SQL role for group %s", ldapGroup.String())
						c.LogAuthFailed(ctx, eventpb.AuthFailReason_AUTHORIZATION_ERROR, err)
						return err
					}
					if !found {
						c.LogAuthInfof(ctx, redact.Sprintf("skipping role assignment for group %s since there is no common name", ldapGroup.String()))
						continue
					}
					sqlRoles = append(sqlRoles, sqlRole)
				}

				// Assign roles to the user.
				if err := sql.EnsureUserOnlyBelongsToRoles(ctx, execCfg, sessionUser, sqlRoles); err != nil {
					err = errors.Wrapf(err, "LDAP authorization: error assigning roles to user %s", sessionUser)
					c.LogAuthFailed(ctx, eventpb.AuthFailReason_AUTHORIZATION_ERROR, err)
					return err
				}
				return nil
			}
		})
	}
	return b, nil
}
