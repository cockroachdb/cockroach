// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var nilThrottleHook = func(state throttler.AttemptStatus) error {
	return nil
}

func TestAuthenticateOK(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cli, srv := net.Pipe()
	be := pgproto3.NewBackend(pgproto3.NewChunkReader(srv), srv)
	fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(cli), cli)

	proxyBackendKeyData := &pgproto3.BackendKeyData{ProcessID: 1, SecretKey: 1}
	crdbBackendKeyData := &pgproto3.BackendKeyData{ProcessID: 2, SecretKey: 2}
	go func() {
		// First the frontend gets back the proxy's BackendKeyData.
		err := be.Send(crdbBackendKeyData)
		assert.NoError(t, err)
		beMsg, err := fe.Receive()
		assert.NoError(t, err)
		assert.Equal(t, beMsg, proxyBackendKeyData)
		// Then the frontend gets ReadyForQuery.
		err = be.Send(&pgproto3.ReadyForQuery{})
		assert.NoError(t, err)
		beMsg, err = fe.Receive()
		assert.NoError(t, err)
		assert.Equal(t, beMsg, &pgproto3.ReadyForQuery{})
	}()

	receivedCrdbBackendKeyData, err := authenticate(srv, cli, proxyBackendKeyData, nilThrottleHook)
	require.NoError(t, err)
	require.Equal(t, crdbBackendKeyData, receivedCrdbBackendKeyData)
}

func TestAuthenticateClearText(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cli, srv := net.Pipe()
	be := pgproto3.NewBackend(pgproto3.NewChunkReader(srv), srv)
	fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(cli), cli)

	go func() {
		err := be.Send(&pgproto3.AuthenticationCleartextPassword{})
		require.NoError(t, err)
		beMsg, err := fe.Receive()
		require.NoError(t, err)
		require.Equal(t, beMsg, &pgproto3.AuthenticationCleartextPassword{})

		err = fe.Send(&pgproto3.PasswordMessage{Password: "password"})
		require.NoError(t, err)
		feMsg, err := be.Receive()
		require.NoError(t, err)
		require.Equal(t, feMsg, &pgproto3.PasswordMessage{Password: "password"})

		err = be.Send(&pgproto3.AuthenticationOk{})
		require.NoError(t, err)
		beMsg, err = fe.Receive()
		require.NoError(t, err)
		require.Equal(t, beMsg, &pgproto3.AuthenticationOk{})

		err = be.Send(&pgproto3.ParameterStatus{Name: "Server Version", Value: "1.3"})
		require.NoError(t, err)
		beMsg, err = fe.Receive()
		require.NoError(t, err)
		require.Equal(t, beMsg, &pgproto3.ParameterStatus{Name: "Server Version", Value: "1.3"})

		err = be.Send(&pgproto3.ReadyForQuery{})
		require.NoError(t, err)
		beMsg, err = fe.Receive()
		require.NoError(t, err)
		require.Equal(t, beMsg, &pgproto3.ReadyForQuery{})
	}()

	_, err := authenticate(srv, cli, nil /* proxyBackendKeyData */, nilThrottleHook)
	require.NoError(t, err)
}

func TestAuthenticateThrottled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	server := func(t *testing.T, be *pgproto3.Backend) {
		require.NoError(t, be.Send(&pgproto3.AuthenticationCleartextPassword{}))

		msg, err := be.Receive()
		require.NoError(t, err)
		require.Equal(t, msg, &pgproto3.PasswordMessage{Password: "password"})

		require.NoError(t, be.Send(&pgproto3.ErrorResponse{Message: "wrong password"}))
	}

	client := func(t *testing.T, fe *pgproto3.Frontend) {
		msg, err := fe.Receive()
		require.NoError(t, err)
		require.Equal(t, msg, &pgproto3.AuthenticationCleartextPassword{})

		require.NoError(t, fe.Send(&pgproto3.PasswordMessage{Password: "password"}))

		msg, err = fe.Receive()
		require.NoError(t, err)
		require.Equal(t, msg, &pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     "08C00",
			Message:  "codeProxyRefusedConnection: too many failed authentication attempts",
			Hint:     throttledErrorHint,
		})

		// Try reading from the connection. This check ensures authorize
		// swallowed the OK/Error response from the sql server.
		_, err = fe.Receive()
		require.Error(t, err)
	}

	proxyToServer, serverToProxy := net.Pipe()
	proxyToClient, clientToProxy := net.Pipe()
	sqlServer := pgproto3.NewBackend(pgproto3.NewChunkReader(serverToProxy), serverToProxy)
	sqlClient := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientToProxy), clientToProxy)

	go server(t, sqlServer)
	go client(t, sqlClient)

	// The error returned from authenticate should be different from the error
	// received at the client.
	_, err := authenticate(
		proxyToClient,
		proxyToServer,
		nil, /* proxyBackendKeyData */
		func(status throttler.AttemptStatus) error {
			require.Equal(t, throttler.AttemptInvalidCredentials, status)
			return errors.New("request denied")
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "request denied")

	proxyToServer.Close()
	proxyToClient.Close()
}

func TestErrorFollowingAuthenticateNotThrottled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	server := func(t *testing.T, be *pgproto3.Backend) {
		require.NoError(t, be.Send(&pgproto3.AuthenticationCleartextPassword{}))

		msg, err := be.Receive()
		require.NoError(t, err)
		require.Equal(t, msg, &pgproto3.PasswordMessage{Password: "password"})

		require.NoError(t, be.Send(&pgproto3.ErrorResponse{
			Code:    pgcode.TooManyConnections.String(),
			Message: "sorry, too many clients already"}))
	}

	client := func(t *testing.T, fe *pgproto3.Frontend) {
		msg, err := fe.Receive()
		require.NoError(t, err)
		require.Equal(t, msg, &pgproto3.AuthenticationCleartextPassword{})

		require.NoError(t, fe.Send(&pgproto3.PasswordMessage{Password: "password"}))

		// Try reading from the connection. This check ensures authorize
		// swallowed the OK/Error response from the sql server.
		msg, err = fe.Receive()
		require.NoError(t, err)
		require.Equal(t, msg, &pgproto3.ErrorResponse{
			Code:    pgcode.TooManyConnections.String(),
			Message: "sorry, too many clients already"})
	}

	proxyToServer, serverToProxy := net.Pipe()
	proxyToClient, clientToProxy := net.Pipe()
	sqlServer := pgproto3.NewBackend(pgproto3.NewChunkReader(serverToProxy), serverToProxy)
	sqlClient := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientToProxy), clientToProxy)

	go server(t, sqlServer)
	go client(t, sqlClient)

	var throttleCallCount int
	_, err := authenticate(proxyToClient, proxyToServer, nil, /* proxyBackendKeyData */
		func(status throttler.AttemptStatus) error {
			throttleCallCount++
			require.Equal(t, throttler.AttemptOK, status)
			return nil
		})
	require.Equal(t, 1, throttleCallCount)
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"codeAuthFailed: authentication failed: sorry, too many clients already")

	proxyToServer.Close()
	proxyToClient.Close()
}

func TestAuthenticateError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cli, srv := net.Pipe()
	be := pgproto3.NewBackend(pgproto3.NewChunkReader(srv), srv)
	fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(cli), cli)

	go func() {
		err := be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "foo"})
		require.NoError(t, err)
		beMsg, err := fe.Receive()
		require.NoError(t, err)
		require.Equal(t, beMsg, &pgproto3.ErrorResponse{Severity: "FATAL", Code: "foo"})
	}()

	_, err := authenticate(srv, cli, nil /* proxyBackendKeyData */, nilThrottleHook)
	require.Error(t, err)
	require.Equal(t, codeAuthFailed, getErrorCode(err))
}

func TestAuthenticateUnexpectedMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	cli, srv := net.Pipe()
	be := pgproto3.NewBackend(pgproto3.NewChunkReader(srv), srv)
	fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(cli), cli)

	go func() {
		err := be.Send(&pgproto3.BindComplete{})
		require.NoError(t, err)
		_, err = fe.Receive()
		require.Error(t, err)
	}()

	_, err := authenticate(srv, cli, nil /* proxyBackendKeyData */, nilThrottleHook)

	srv.Close()

	require.Error(t, err)
	require.Equal(t, codeBackendDisconnected, getErrorCode(err))
}

func TestReadTokenAuthResult(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	t.Run("unexpected message", func(t *testing.T) {
		cli, srv := net.Pipe()

		go func() {
			buf, err := (&pgproto3.BindComplete{}).Encode(nil)
			require.NoError(t, err)
			_, err = srv.Write(buf)
			require.NoError(t, err)
		}()

		_, err := readTokenAuthResult(cli)
		require.Error(t, err)
		require.Equal(t, codeBackendDisconnected, getErrorCode(err))
	})

	t.Run("error_response", func(t *testing.T) {
		cli, srv := net.Pipe()

		go func() {
			buf, err := (&pgproto3.ErrorResponse{Severity: "FATAL", Code: "foo"}).Encode(nil)
			require.NoError(t, err)
			_, err = srv.Write(buf)
			require.NoError(t, err)
		}()

		_, err := readTokenAuthResult(cli)
		require.Error(t, err)
		require.Equal(t, codeAuthFailed, getErrorCode(err))
	})

	t.Run("successful", func(t *testing.T) {
		cli, srv := net.Pipe()
		crdbBackendKeyData := &pgproto3.BackendKeyData{ProcessID: 42, SecretKey: 99}

		go func() {
			buf, err := (&pgproto3.AuthenticationOk{}).Encode(nil)
			require.NoError(t, err)
			_, err = srv.Write(buf)
			require.NoError(t, err)

			buf, err = (&pgproto3.ParameterStatus{Name: "Server Version", Value: "1.3"}).Encode(nil)
			require.NoError(t, err)
			_, err = srv.Write(buf)
			require.NoError(t, err)

			buf, err = crdbBackendKeyData.Encode(nil)
			require.NoError(t, err)
			_, err = srv.Write(buf)
			require.NoError(t, err)

			buf, err = (&pgproto3.ReadyForQuery{}).Encode(nil)
			require.NoError(t, err)
			_, err = srv.Write(buf)
			require.NoError(t, err)
		}()

		receivedCrdbBackendKeyData, err := readTokenAuthResult(cli)
		require.NoError(t, err)
		require.Equal(t, crdbBackendKeyData, receivedCrdbBackendKeyData)
	})
}
