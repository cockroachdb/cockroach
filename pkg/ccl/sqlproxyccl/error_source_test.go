// Copyright 2022 The Cockroach Authors.
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
	"github.com/stretchr/testify/require"
)

type fakeConn struct {
	net.Conn
	size       int
	readError  error
	writeError error
}

func (conn *fakeConn) Read(b []byte) (n int, err error) {
	return conn.size, conn.readError
}

func (conn *fakeConn) Write(b []byte) (n int, err error) {
	return conn.size, conn.writeError
}

func TestWrapConnectionError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		marker error
		code   errorCode
	}
	tests := []testCase{
		{errClientRead, codeClientReadFailed},
		{errClientWrite, codeClientWriteFailed},
		{errServerRead, codeBackendReadFailed},
		{errServerWrite, codeBackendWriteFailed},
		{errors.New("some random error"), 0},
	}
	for _, tc := range tests {
		var code errorCode
		err := wrapConnectionError(errors.Mark(errors.New("some inner error"), tc.marker))
		if err != nil {
			codeErr := &codeError{}
			require.True(t, errors.As(err, &codeErr))
			code = codeErr.code
		}
		require.Equal(t, code, tc.code)
	}
}

func TestErrorSourceConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	newConn := func(size int, readError error, writeError error) net.Conn {
		return &errorSourceConn{
			Conn: &fakeConn{
				size:       size,
				readError:  readError,
				writeError: writeError,
			},
			readErrMarker:  errClientRead,
			writeErrMarker: errClientWrite,
		}
	}

	t.Run("WrapReadError", func(t *testing.T) {
		internalErr := errors.New("some connection error")
		conn := newConn(4, internalErr, nil)

		size, err := conn.Read([]byte{})
		require.Equal(t, size, 4)
		require.True(t, errors.Is(err, internalErr))
		require.True(t, errors.Is(err, errClientRead))
	})

	t.Run("WrapWriteError", func(t *testing.T) {
		internalErr := errors.New("some connection error")
		conn := newConn(4, nil, internalErr)

		size, err := conn.Write([]byte{})
		require.Equal(t, size, 4)
		require.True(t, errors.Is(err, internalErr))
		require.True(t, errors.Is(err, errClientWrite))
	})

	t.Run("OkayRead", func(t *testing.T) {
		conn := newConn(4, nil, nil)

		size, err := conn.Read([]byte{})
		require.Equal(t, size, 4)
		require.NoError(t, err)
	})

	t.Run("OkayWrite", func(t *testing.T) {
		conn := newConn(4, nil, nil)

		size, err := conn.Read([]byte{})
		require.Equal(t, size, 4)
		require.NoError(t, err)
	})
}
