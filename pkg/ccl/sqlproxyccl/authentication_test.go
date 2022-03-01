// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/throttler"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

var nilThrottleHook = func(state throttler.AttemptStatus) error {
	return nil
}

func TestAuthenticateOK(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cli, srv := net.Pipe()
	be := pgproto3.NewBackend(pgproto3.NewChunkReader(srv), srv)
	fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(cli), cli)

	go func() {
		err := be.Send(&pgproto3.ReadyForQuery{})
		require.NoError(t, err)
		beMsg, err := fe.Receive()
		require.NoError(t, err)
		require.Equal(t, beMsg, &pgproto3.ReadyForQuery{})
	}()

	require.NoError(t, authenticate(srv, cli, nilThrottleHook))
}

func TestAuthenticateClearText(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	require.NoError(t, authenticate(srv, cli, nilThrottleHook))
}

func TestAuthenticateThrottled(t *testing.T) {
	defer leaktest.AfterTest(t)()

	server := func(t *testing.T, be *pgproto3.Backend, authResponse pgproto3.BackendMessage) {
		require.NoError(t, be.Send(&pgproto3.AuthenticationCleartextPassword{}))

		msg, err := be.Receive()
		require.NoError(t, err)
		require.Equal(t, msg, &pgproto3.PasswordMessage{Password: "password"})

		require.NoError(t, be.Send(authResponse))
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
			Code:     "08004",
			Message:  "codeProxyRefusedConnection: connection attempt throttled",
			Hint:     throttledErrorHint,
		})

		// Try reading from the connection. This check ensures authorize
		// swallowed the OK/Error response from the sql server.
		_, err = fe.Receive()
		require.Error(t, err)
	}

	type testCase struct {
		name           string
		result         pgproto3.BackendMessage
		expectedStatus throttler.AttemptStatus
	}
	for _, tc := range []testCase{
		{
			name:           "AuthenticationOkay",
			result:         &pgproto3.AuthenticationOk{},
			expectedStatus: throttler.AttemptOK,
		},
		{
			name:           "AuthenticationError",
			result:         &pgproto3.ErrorResponse{Message: "wrong password"},
			expectedStatus: throttler.AttemptInvalidCredentials,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxyToServer, serverToProxy := net.Pipe()
			proxyToClient, clientToProxy := net.Pipe()
			sqlServer := pgproto3.NewBackend(pgproto3.NewChunkReader(serverToProxy), serverToProxy)
			sqlClient := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientToProxy), clientToProxy)

			go server(t, sqlServer, &pgproto3.AuthenticationOk{})
			go client(t, sqlClient)

			err := authenticate(proxyToClient, proxyToServer, func(status throttler.AttemptStatus) error {
				require.Equal(t, throttler.AttemptOK, status)
				return throttledError
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "connection attempt throttled")

			proxyToServer.Close()
			proxyToClient.Close()
		})
	}
}

func TestAuthenticateError(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	err := authenticate(srv, cli, nilThrottleHook)
	require.Error(t, err)
	codeErr := (*codeError)(nil)
	require.True(t, errors.As(err, &codeErr))
	require.Equal(t, codeAuthFailed, codeErr.code)
}

func TestAuthenticateUnexpectedMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cli, srv := net.Pipe()
	be := pgproto3.NewBackend(pgproto3.NewChunkReader(srv), srv)
	fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(cli), cli)

	go func() {
		err := be.Send(&pgproto3.BindComplete{})
		require.NoError(t, err)
		_, err = fe.Receive()
		require.Error(t, err)
	}()

	err := authenticate(srv, cli, nilThrottleHook)

	srv.Close()

	require.Error(t, err)
	codeErr := (*codeError)(nil)
	require.True(t, errors.As(err, &codeErr))
	require.Equal(t, codeBackendDisconnected, codeErr.code)
}

func TestReadTokenAuthResult(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("unexpected message", func(t *testing.T) {
		cli, srv := net.Pipe()

		go func() {
			_, err := srv.Write((&pgproto3.BindComplete{}).Encode(nil))
			require.NoError(t, err)
		}()

		err := readTokenAuthResult(cli)
		require.Error(t, err)
		codeErr := (*codeError)(nil)
		require.True(t, errors.As(err, &codeErr))
		require.Equal(t, codeBackendDisconnected, codeErr.code)
	})

	t.Run("error_response", func(t *testing.T) {
		cli, srv := net.Pipe()

		go func() {
			_, err := srv.Write((&pgproto3.ErrorResponse{Severity: "FATAL", Code: "foo"}).Encode(nil))
			require.NoError(t, err)
		}()

		err := readTokenAuthResult(cli)
		require.Error(t, err)
		codeErr := (*codeError)(nil)
		require.True(t, errors.As(err, &codeErr))
		require.Equal(t, codeAuthFailed, codeErr.code)
	})

	t.Run("successful", func(t *testing.T) {
		cli, srv := net.Pipe()

		go func() {
			_, err := srv.Write((&pgproto3.AuthenticationOk{}).Encode(nil))
			require.NoError(t, err)

			_, err = srv.Write((&pgproto3.ParameterStatus{Name: "Server Version", Value: "1.3"}).Encode(nil))
			require.NoError(t, err)

			_, err = srv.Write((&pgproto3.BackendKeyData{ProcessID: uint32(42)}).Encode(nil))
			require.NoError(t, err)

			_, err = srv.Write((&pgproto3.ReadyForQuery{}).Encode(nil))
			require.NoError(t, err)
		}()

		require.NoError(t, readTokenAuthResult(cli))
	})
}
