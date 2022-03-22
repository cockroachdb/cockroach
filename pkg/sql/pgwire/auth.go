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
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

const (
	// authOK is the pgwire auth response code for successful authentication
	// during the connection handshake.
	authOK int32 = 0
	// authCleartextPassword is the pgwire auth response code to request
	// a plaintext password during the connection handshake.
	authCleartextPassword int32 = 3

	// authReqSASL is the begin request for a SCRAM handshake.
	authReqSASL int32 = 10
	// authReqSASLContinue is the continue request for a SCRAM handshake.
	authReqSASLContinue int32 = 11
	// authReqSASLFin is the final message for a SCRAM handshake.
	authReqSASLFin int32 = 12
)

type authOptions struct {
	// insecure indicates that all connections for existing users must
	// be allowed to go through. A password, if presented, must be
	// accepted.
	insecure bool
	// connType is the actual type of client connection (e.g. local,
	// hostssl, hostnossl).
	connType hba.ConnType
	// connDetails is the event payload common to all auth/session events.
	connDetails eventpb.CommonConnectionDetails

	// auth is the current HBA configuration as returned by
	// (*Server).GetAuthenticationConfiguration().
	auth *hba.Conf
	// identMap is used in conjunction with the HBA configuration to
	// allow system usernames (e.g. GSSAPI principals or X.509 CN's) to
	// be dynamically mapped to database usernames.
	identMap *identmap.Conf
	// ie is the server-wide internal executor, used to
	// retrieve entries from system.users.
	ie *sql.InternalExecutor

	// The following fields are only used by tests.

	// testingSkipAuth requires to skip authentication, not even
	// allowing a password exchange.
	// Note that this different from insecure auth: with no auth, no
	// password is accepted (a protocol error is given if one is
	// presented); with insecure auth; _any_ is accepted.
	testingSkipAuth bool
	// testingAuthHook, if provided, replaces the logic in
	// handleAuthentication().
	testingAuthHook func(ctx context.Context) error
}

// handleAuthentication checks the connection's user. Errors are sent to the
// client and also returned.
//
// TODO(knz): handleAuthentication should discuss with the client to arrange
// authentication and update c.sessionArgs with the authenticated user's name,
// if different from the one given initially.
func (c *conn) handleAuthentication(
	ctx context.Context, ac AuthConn, authOpt authOptions, execCfg *sql.ExecutorConfig,
) (connClose func(), _ error) {
	if authOpt.testingSkipAuth {
		return nil, nil
	}
	if authOpt.testingAuthHook != nil {
		return nil, authOpt.testingAuthHook(ctx)
	}

	// Retrieve the authentication method.
	tlsState, hbaEntry, authMethod, err := c.findAuthenticationMethod(authOpt)
	if err != nil {
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_METHOD_NOT_FOUND, err)
		return nil, c.sendError(ctx, execCfg, pgerror.WithCandidateCode(err, pgcode.InvalidAuthorizationSpecification))
	}

	ac.SetAuthMethod(hbaEntry.Method.String())
	ac.LogAuthInfof(ctx, "HBA rule: %s", hbaEntry.Input)

	// Populate the AuthMethod with per-connection information so that it
	// can compose the next layer of behaviors that we're going to apply
	// to the incoming connection.
	behaviors, err := authMethod(ctx, ac, tlsState, execCfg, hbaEntry, authOpt.identMap)
	connClose = behaviors.ConnClose
	if err != nil {
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_UNKNOWN, err)
		return connClose, c.sendError(ctx, execCfg, pgerror.WithCandidateCode(err, pgcode.InvalidAuthorizationSpecification))
	}

	// Choose the system identity that we'll use below for mapping
	// externally-provisioned principals to database users.
	var systemIdentity security.SQLUsername
	if found, ok := behaviors.ReplacementIdentity(); ok {
		systemIdentity = found
		ac.SetSystemIdentity(systemIdentity)
	} else {
		systemIdentity = c.sessionArgs.User
	}

	// Delegate to the AuthMethod's MapRole to choose the actual
	// database user that a successful authentication will result in.
	if err := c.chooseDbRole(ctx, ac, behaviors.MapRole, systemIdentity); err != nil {
		log.Warningf(ctx, "unable to map incoming identity %q to any database user: %+v", systemIdentity, err)
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_USER_NOT_FOUND, err)
		return connClose, c.sendError(ctx, execCfg, pgerror.WithCandidateCode(err, pgcode.InvalidAuthorizationSpecification))
	}

	// Once chooseDbRole() returns, we know that the actual DB username
	// will be present in c.sessionArgs.User.
	dbUser := c.sessionArgs.User

	// Check that the requested user exists and retrieve the hashed
	// password in case password authentication is needed.
	exists, canLoginSQL, _, isSuperuser, defaultSettings, pwRetrievalFn, err :=
		sql.GetUserSessionInitInfo(
			ctx,
			execCfg,
			authOpt.ie,
			dbUser,
			c.sessionArgs.SessionDefaults["database"],
		)
	if err != nil {
		log.Warningf(ctx, "user retrieval failed for user=%q: %+v", dbUser, err)
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_USER_RETRIEVAL_ERROR, err)
		return connClose, c.sendError(ctx, execCfg, pgerror.WithCandidateCode(err, pgcode.InvalidAuthorizationSpecification))
	}
	c.sessionArgs.IsSuperuser = isSuperuser

	if !exists {
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_USER_NOT_FOUND, nil)
		return connClose, c.sendError(ctx, execCfg, pgerror.WithCandidateCode(security.NewErrPasswordUserAuthFailed(dbUser), pgcode.InvalidAuthorizationSpecification))
	}

	if !canLoginSQL {
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_LOGIN_DISABLED, nil)
		return connClose, c.sendError(ctx, execCfg, pgerror.Newf(pgcode.InvalidAuthorizationSpecification, "%s does not have login privilege", dbUser))
	}

	// At this point, we know that the requested user exists and is
	// allowed to log in. Now we can delegate to the selected AuthMethod
	// implementation to complete the authentication.
	if err := behaviors.Authenticate(ctx, systemIdentity, true /* public */, pwRetrievalFn); err != nil {
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_CREDENTIALS_INVALID, err)
		return connClose, c.sendError(ctx, execCfg, pgerror.WithCandidateCode(err, pgcode.InvalidAuthorizationSpecification))
	}

	// Add all the defaults to this session's defaults. If there is an
	// error (e.g., a setting that no longer exists, or bad input),
	// log a warning instead of preventing login.
	// The defaultSettings array is ordered by precedence. This means that if
	// SessionDefaults already has an entry for a given setting name, then
	// it should not be replaced.
	for _, settingEntry := range defaultSettings {
		for _, setting := range settingEntry.Settings {
			keyVal := strings.SplitN(setting, "=", 2)
			if len(keyVal) != 2 {
				log.Ops.Warningf(ctx, "%s has malformed default setting: %q", dbUser, setting)
				continue
			}
			if err := sql.CheckSessionVariableValueValid(ctx, execCfg.Settings, keyVal[0], keyVal[1]); err != nil {
				log.Ops.Warningf(ctx, "%s has invalid default setting: %v", dbUser, err)
				continue
			}
			if _, ok := c.sessionArgs.SessionDefaults[keyVal[0]]; !ok {
				c.sessionArgs.SessionDefaults[keyVal[0]] = keyVal[1]
			}
		}
	}

	ac.LogAuthOK(ctx)
	return connClose, nil
}

func (c *conn) authOKMessage() error {
	c.msgBuilder.initMsg(pgwirebase.ServerMsgAuth)
	c.msgBuilder.putInt32(authOK)
	return c.msgBuilder.finishMsg(c.conn)
}

// chooseDbRole uses the provided RoleMapper to map an incoming
// system identity to an actual database role. If a mapping is present,
// the sessionArgs.User field will be updated.
//
// TODO(#sql-security): The docs for the pg_ident.conf file state that
// if there are multiple mappings for an incoming system-user, the
// session should act with the union of all roles granted to the mapped
// database users. We're going to go with a first-one-wins approach
// until the session can have multiple roles.
func (c *conn) chooseDbRole(
	ctx context.Context, ac AuthConn, mapper RoleMapper, systemIdentity security.SQLUsername,
) error {
	if mapped, err := mapper(ctx, systemIdentity); err != nil {
		return err
	} else if len(mapped) == 0 {
		return errors.Newf("system identity %q did not map to a database role", systemIdentity.Normalized())
	} else {
		c.sessionArgs.User = mapped[0]
		ac.SetDbUser(mapped[0])
	}
	return nil
}

func (c *conn) findAuthenticationMethod(
	authOpt authOptions,
) (tlsState tls.ConnectionState, hbaEntry *hba.Entry, methodFn AuthMethod, err error) {
	if authOpt.insecure {
		// Insecure connections always use "trust" no matter what, and the
		// remaining of the configuration is ignored.
		methodFn = authTrust
		hbaEntry = &insecureEntry
		return
	}
	if c.sessionArgs.SessionRevivalToken != nil {
		methodFn = authSessionRevivalToken(c.sessionArgs.SessionRevivalToken)
		c.sessionArgs.SessionRevivalToken = nil
		hbaEntry = &sessionRevivalEntry
		return
	}

	// Look up the method from the HBA configuration.
	var mi methodInfo
	mi, hbaEntry, err = c.lookupAuthenticationMethodUsingRules(authOpt.connType, authOpt.auth)
	if err != nil {
		return
	}
	methodFn = mi.fn

	// Check that this method can be used over this connection type.
	if authOpt.connType&mi.validConnTypes == 0 {
		err = errors.Newf("method %q required for this user, but unusable over this connection type",
			hbaEntry.Method.Value)
		return
	}

	// If the client is using SSL, retrieve the TLS state to provide as
	// input to the method.
	if authOpt.connType == hba.ConnHostSSL {
		tlsConn, ok := c.conn.(*readTimeoutConn).Conn.(*tls.Conn)
		if !ok {
			err = errors.AssertionFailedf("server reports hostssl conn without TLS state")
			return
		}
		tlsState = tlsConn.ConnectionState()
	}

	return
}

func (c *conn) lookupAuthenticationMethodUsingRules(
	connType hba.ConnType, auth *hba.Conf,
) (mi methodInfo, entry *hba.Entry, err error) {
	var ip net.IP
	if connType != hba.ConnLocal {
		// Extract the IP address of the client.
		tcpAddr, ok := c.sessionArgs.RemoteAddr.(*net.TCPAddr)
		if !ok {
			err = errors.AssertionFailedf("client address type %T unsupported", c.sessionArgs.RemoteAddr)
			return
		}
		ip = tcpAddr.IP
	}

	// Look up the method.
	for i := range auth.Entries {
		entry = &auth.Entries[i]
		var connMatch bool
		connMatch, err = entry.ConnMatches(connType, ip)
		if err != nil {
			// TODO(knz): Determine if an error should be reported
			// upon unknown address formats.
			// See: https://github.com/cockroachdb/cockroach/issues/43716
			return
		}
		if !connMatch {
			// The address does not match.
			continue
		}
		if !entry.UserMatches(c.sessionArgs.User) {
			// The user does not match.
			continue
		}
		return entry.MethodFn.(methodInfo), entry, nil
	}

	// No match.
	err = errors.Errorf("no %s entry for host %q, user %q", serverHBAConfSetting, ip, c.sessionArgs.User)
	return
}

// authenticatorIO is the interface used by the connection to pass password data
// to the authenticator and expect an authentication decision from it.
type authenticatorIO interface {
	// sendPwdData is used to push authentication data into the authenticator.
	// This call is blocking; authenticators are supposed to consume data hastily
	// once they've requested it.
	sendPwdData(data []byte) error
	// noMorePwdData is used to inform the authenticator that the client is not
	// sending any more authentication data. This method can be called multiple
	// times.
	noMorePwdData()
	// authResult blocks for an authentication decision. This call also informs
	// the authenticator that no more auth data is coming from the client;
	// noMorePwdData() is called internally.
	authResult() error
}

// AuthConn is the interface used by the authenticator for interacting with the
// pgwire connection.
type AuthConn interface {
	// SendAuthRequest send a request for authentication information. After
	// calling this, the authenticator needs to call GetPwdData() quickly, as the
	// connection's goroutine will be blocked on providing us the requested data.
	SendAuthRequest(authType int32, data []byte) error
	// GetPwdData returns authentication info that was previously requested with
	// SendAuthRequest. The call blocks until such data is available.
	// An error is returned if the client connection dropped or if the client
	// didn't respect the protocol. After an error has been returned, GetPwdData()
	// cannot be called any more.
	GetPwdData() ([]byte, error)
	// AuthOK declares that authentication succeeded and provides a
	// unqualifiedIntSizer, to be returned by authenticator.authResult(). Future
	// authenticator.sendPwdData() calls fail.
	AuthOK(context.Context)
	// AuthFail declares that authentication has failed and provides an error to
	// be returned by authenticator.authResult(). Future
	// authenticator.sendPwdData() calls fail. The error has already been written
	// to the client connection.
	AuthFail(err error)

	// SetAuthMethod sets the authentication method for subsequent
	// logging messages.
	SetAuthMethod(method string)
	// SetDbUser updates the AuthConn with the actual database username
	// the connection has authenticated to.
	SetDbUser(dbUser security.SQLUsername)
	// SetSystemIdentity updates the AuthConn with an externally-defined
	// identity for the connection. This is useful for "ambient"
	// authentication mechanisms, such as GSSAPI.
	SetSystemIdentity(systemIdentity security.SQLUsername)
	// LogAuthInfof logs details about the progress of the
	// authentication.
	LogAuthInfof(ctx context.Context, format string, args ...interface{})
	// LogAuthFailed logs details about an authentication failure.
	LogAuthFailed(ctx context.Context, reason eventpb.AuthFailReason, err error)
	// LogAuthOK logs when the authentication handshake has completed.
	LogAuthOK(ctx context.Context)
}

// authPipe is the implementation for the authenticator and AuthConn interfaces.
// A single authPipe will serve as both an AuthConn and an authenticator; the
// two represent the two "ends" of the pipe and we'll pass data between them.
type authPipe struct {
	c   *conn // Only used for writing, not for reading.
	log bool

	connDetails eventpb.CommonConnectionDetails
	authDetails eventpb.CommonSessionDetails
	authMethod  string

	ch chan []byte
	// writerDone is a channel closed by noMorePwdData().
	// Nil if noMorePwdData().
	writerDone chan struct{}
	readerDone chan authRes
}

type authRes struct {
	err error
}

func newAuthPipe(
	c *conn, logAuthn bool, authOpt authOptions, systemIdentity security.SQLUsername,
) *authPipe {
	ap := &authPipe{
		c:           c,
		log:         logAuthn,
		connDetails: authOpt.connDetails,
		authDetails: eventpb.CommonSessionDetails{
			SystemIdentity: systemIdentity.Normalized(),
			Transport:      authOpt.connType.String(),
		},
		ch:         make(chan []byte),
		writerDone: make(chan struct{}),
		readerDone: make(chan authRes, 1),
	}
	return ap
}

var _ authenticatorIO = &authPipe{}
var _ AuthConn = &authPipe{}

func (p *authPipe) sendPwdData(data []byte) error {
	select {
	case p.ch <- data:
		return nil
	case <-p.readerDone:
		return pgwirebase.NewProtocolViolationErrorf("unexpected auth data")
	}
}

func (p *authPipe) noMorePwdData() {
	if p.writerDone == nil {
		return
	}
	// A reader blocked in GetPwdData() gets unblocked with an error.
	close(p.writerDone)
	p.writerDone = nil
}

// GetPwdData is part of the AuthConn interface.
func (p *authPipe) GetPwdData() ([]byte, error) {
	select {
	case data := <-p.ch:
		return data, nil
	case <-p.writerDone:
		return nil, pgwirebase.NewProtocolViolationErrorf("client didn't send required auth data")
	}
}

// AuthOK is part of the AuthConn interface.
func (p *authPipe) AuthOK(ctx context.Context) {
	p.readerDone <- authRes{err: nil}
}

func (p *authPipe) AuthFail(err error) {
	p.readerDone <- authRes{err: err}
}

func (p *authPipe) SetAuthMethod(method string) {
	p.authMethod = method
}

func (p *authPipe) SetDbUser(dbUser security.SQLUsername) {
	p.authDetails.User = dbUser.Normalized()
}

func (p *authPipe) SetSystemIdentity(systemIdentity security.SQLUsername) {
	p.authDetails.SystemIdentity = systemIdentity.Normalized()
}

func (p *authPipe) LogAuthOK(ctx context.Context) {
	if p.log {
		ev := &eventpb.ClientAuthenticationOk{
			CommonConnectionDetails: p.connDetails,
			CommonSessionDetails:    p.authDetails,
			Method:                  p.authMethod,
		}
		log.StructuredEvent(ctx, ev)
	}
}

func (p *authPipe) LogAuthInfof(ctx context.Context, format string, args ...interface{}) {
	if p.log {
		ev := &eventpb.ClientAuthenticationInfo{
			CommonConnectionDetails: p.connDetails,
			CommonSessionDetails:    p.authDetails,
			Info:                    fmt.Sprintf(format, args...),
			Method:                  p.authMethod,
		}
		log.StructuredEvent(ctx, ev)
	}
}

func (p *authPipe) LogAuthFailed(
	ctx context.Context, reason eventpb.AuthFailReason, detailedErr error,
) {
	if p.log {
		var errStr string
		if detailedErr != nil {
			errStr = detailedErr.Error()
		}
		ev := &eventpb.ClientAuthenticationFailed{
			CommonConnectionDetails: p.connDetails,
			CommonSessionDetails:    p.authDetails,
			Reason:                  reason,
			Detail:                  errStr,
			Method:                  p.authMethod,
		}
		log.StructuredEvent(ctx, ev)
	}
}

// authResult is part of the authenticator interface.
func (p *authPipe) authResult() error {
	p.noMorePwdData()
	res := <-p.readerDone
	return res.err
}

// SendAuthRequest is part of the AuthConn interface.
func (p *authPipe) SendAuthRequest(authType int32, data []byte) error {
	c := p.c
	c.msgBuilder.initMsg(pgwirebase.ServerMsgAuth)
	c.msgBuilder.putInt32(authType)
	c.msgBuilder.write(data)
	return c.msgBuilder.finishMsg(c.conn)
}
