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
	"net"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
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
	skipAuth bool                            // test-only
	authHook func(ctx context.Context) error // test-only
	insecure bool
	auth     *hba.Conf
	ie       *sql.InternalExecutor
}

// handleAuthentication checks the connection's user. Errors are sent to the
// client and also returned.
//
// TODO(knz): handleAuthentication should discuss with the client to arrange
// authentication and update c.sessionArgs with the authenticated user's name,
// if different from the one given initially.
func (c *conn) handleAuthentication(
	ctx context.Context,
	ac AuthConn,
	insecure bool,
	ie *sql.InternalExecutor,
	auth *hba.Conf,
	execCfg *sql.ExecutorConfig,
) error {
	sendError := func(err error) error {
		_ /* err */ = writeErr(ctx, &execCfg.Settings.SV, err, &c.msgBuilder, c.conn)
		return err
	}

	// Check that the requested user exists and retrieve the hashed
	// password in case password authentication is needed.
	exists, hashedPassword, err := sql.GetUserHashedPassword(
		ctx, ie, &c.metrics.SQLMemMetrics, c.sessionArgs.User,
	)
	if err != nil {
		return sendError(err)
	}
	if !exists {
		return sendError(errors.Errorf(security.ErrPasswordUserAuthFailed, c.sessionArgs.User))
	}

	if tlsConn, ok := c.conn.(*readTimeoutConn).Conn.(*tls.Conn); ok {
		tlsState := tlsConn.ConnectionState()
		var methodFn AuthMethod
		var hbaEntry *hba.Entry

		if auth == nil {
			methodFn = authCertPassword
		} else if c.sessionArgs.User == security.RootUser {
			// If a hba.conf file is specified, hard code the root user to always use
			// cert auth. This prevents users from shooting themselves in the foot and
			// making root not able to login, thus disallowing anyone from fixing the
			// hba.conf file.
			methodFn = authCert
		} else {
			addr, _, err := net.SplitHostPort(c.conn.RemoteAddr().String())
			if err != nil {
				return sendError(err)
			}
			ip := net.ParseIP(addr)
			for _, entry := range auth.Entries {
				switch a := entry.Address.(type) {
				case *net.IPNet:
					if !a.Contains(ip) {
						continue
					}
				case hba.String:
					if !a.IsSpecial("all") {
						return sendError(errors.Errorf("unexpected %s address: %q", serverHBAConfSetting, a.Value))
					}
				default:
					return sendError(errors.Errorf("unexpected address type %T", a))
				}
				match := false
				for _, u := range entry.User {
					if u.IsSpecial("all") {
						match = true
						break
					}
					if u.Value == c.sessionArgs.User {
						match = true
						break
					}
				}
				if !match {
					continue
				}
				methodFn = hbaAuthMethods[entry.Method]
				if methodFn == nil {
					return sendError(errors.Errorf("unknown auth method %s", entry.Method))
				}
				hbaEntry = &entry
				break
			}
			if methodFn == nil {
				return sendError(errors.Errorf("no %s entry for host %q, user %q", serverHBAConfSetting, addr, c.sessionArgs.User))
			}
		}

		authenticationHook, err := methodFn(ac, tlsState, insecure, hashedPassword, execCfg, hbaEntry)
		if err != nil {
			return sendError(err)
		}
		if err := authenticationHook(c.sessionArgs.User, true /* public */); err != nil {
			return sendError(err)
		}
	}

	c.msgBuilder.initMsg(pgwirebase.ServerMsgAuth)
	c.msgBuilder.putInt32(authOK)
	return c.msgBuilder.finishMsg(c.conn)
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
	AuthOK(unqualifiedIntSizer)
	// AuthFail declares that authentication has failed and provides an error to
	// be returned by authenticator.authResult(). Future
	// authenticator.sendPwdData() calls fail. The error has already been written
	// to the client connection.
	AuthFail(err error)
}

// authPipe is the implementation for the authenticator and AuthConn interfaces.
// A single authPipe will serve as both an AuthConn and an authenticator; the
// two represent the two "ends" of the pipe and we'll pass data between them.
type authPipe struct {
	c *conn // Only used for writing, not for reading.

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

func newAuthPipe(c *conn) *authPipe {
	ap := &authPipe{
		c:          c,
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
func (p *authPipe) AuthOK(intSizer unqualifiedIntSizer) {
	p.readerDone <- authRes{intSizer: intSizer}
}

func (p *authPipe) AuthFail(err error) {
	p.readerDone <- authRes{err: err}
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
