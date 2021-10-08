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

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

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

	require.NoError(t, authenticate(srv, cli))
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

	require.NoError(t, authenticate(srv, cli))
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

	err := authenticate(srv, cli)
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
		beMsg, err := fe.Receive()
		require.NoError(t, err)
		require.Equal(t, beMsg, &pgproto3.BindComplete{})
	}()

	err := authenticate(srv, cli)
	require.Error(t, err)
	codeErr := (*codeError)(nil)
	require.True(t, errors.As(err, &codeErr))
	require.Equal(t, codeBackendDisconnected, codeErr.code)
}
