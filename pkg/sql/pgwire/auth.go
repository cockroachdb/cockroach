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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
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

	sendError := func(err error) error {
		_ /* err */ = writeErr(ctx, &execCfg.Settings.SV, err, &c.msgBuilder, c.conn)
		return err
	}

	// Check that the requested user exists and retrieve the hashed
	// password in case password authentication is needed.
	exists, canLogin, pwRetrievalFn, validUntilFn, err := sql.GetUserHashedPassword(
		ctx,
		execCfg,
		authOpt.ie,
		c.sessionArgs.User,
	)
	if err != nil {
		log.Warningf(ctx, "user retrieval failed for user=%q: %+v", c.sessionArgs.User, err)
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_USER_RETRIEVAL_ERROR, err)
		return nil, sendError(err)
	}

	if !exists {
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_USER_NOT_FOUND, nil)
		return nil, sendError(errors.Errorf(security.ErrPasswordUserAuthFailed, c.sessionArgs.User))
	}

	if !canLogin {
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_LOGIN_DISABLED, nil)
		return nil, sendError(errors.Errorf(
			"%s does not have login privilege", c.sessionArgs.User))
	}

	// Retrieve the authentication method.
	tlsState, hbaEntry, methodFn, err := c.findAuthenticationMethod(authOpt)
	if err != nil {
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_METHOD_NOT_FOUND, err)
		return nil, sendError(err)
	}

	ac.SetAuthMethod(hbaEntry.Method.String())
	ac.LogAuthInfof(ctx, "HBA rule: %s", hbaEntry.Input)

	// Ask the method to authenticate.
	authenticationHook, err := methodFn(ctx, ac, tlsState, pwRetrievalFn,
		validUntilFn, execCfg, hbaEntry)

	if err != nil {
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_METHOD_NOT_FOUND, err)
		return nil, sendError(err)
	}

	if connClose, err = authenticationHook(ctx, c.sessionArgs.User, true /* public */); err != nil {
		ac.LogAuthFailed(ctx, eventpb.AuthFailReason_CREDENTIALS_INVALID, err)
		return connClose, sendError(err)
	}

	ac.LogAuthOK(ctx)
	c.msgBuilder.initMsg(pgwirebase.ServerMsgAuth)
	c.msgBuilder.putInt32(authOK)
	return connClose, c.msgBuilder.finishMsg(c.conn)
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
	//
	// The auth result is either an unqualifiedIntSizer (in case the auth
	// succeeded) or an auth error.
	authResult() (unqualifiedIntSizer, error)
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
	AuthOK(context.Context, unqualifiedIntSizer)
	// AuthFail declares that authentication has failed and provides an error to
	// be returned by authenticator.authResult(). Future
	// authenticator.sendPwdData() calls fail. The error has already been written
	// to the client connection.
	AuthFail(err error)

	// SetAuthMethod sets the authentication method for subsequent
	// logging messages.
	SetAuthMethod(method string)
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
	intSizer unqualifiedIntSizer
	err      error
}

func newAuthPipe(c *conn, logAuthn bool, authOpt authOptions, user security.SQLUsername) *authPipe {
	ap := &authPipe{
		c:           c,
		log:         logAuthn,
		connDetails: authOpt.connDetails,
		authDetails: eventpb.CommonSessionDetails{
			Transport: authOpt.connType.String(),
			User:      user.Normalized(),
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
func (p *authPipe) AuthOK(ctx context.Context, intSizer unqualifiedIntSizer) {
	p.readerDone <- authRes{intSizer: intSizer}
}

func (p *authPipe) AuthFail(err error) {
	p.readerDone <- authRes{err: err}
}

func (p *authPipe) SetAuthMethod(method string) {
	p.authMethod = method
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
func (p *authPipe) authResult() (unqualifiedIntSizer, error) {
	p.noMorePwdData()
	res := <-p.readerDone
	return res.intSizer, res.err
}

// SendAuthRequest is part of the AuthConn interface.
func (p *authPipe) SendAuthRequest(authType int32, data []byte) error {
	c := p.c
	c.msgBuilder.initMsg(pgwirebase.ServerMsgAuth)
	c.msgBuilder.putInt32(authType)
	c.msgBuilder.write(data)
	return c.msgBuilder.finishMsg(c.conn)
}
