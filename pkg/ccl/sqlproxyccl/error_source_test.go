// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
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

func (conn *fakeConn) Read(_ []byte) (n int, err error) {
	return conn.size, conn.readError
}

func (conn *fakeConn) Write(_ []byte) (n int, err error) {
	return conn.size, conn.writeError
}

func TestWrapConnectionError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	type testCase struct {
		marker error
		code   errorCode
	}
	tests := []testCase{
		{errClientRead, codeClientReadFailed},
		{errClientWrite, codeClientWriteFailed},
		{errServerRead, codeBackendReadFailed},
		{errServerWrite, codeBackendWriteFailed},
		{errors.New("some random error"), codeNone},
	}
	for _, tc := range tests {
		err := wrapConnectionError(errors.Mark(errors.New("some inner error"), tc.marker))
		require.Equal(t, tc.code, getErrorCode(err))
	}
}

func TestErrorSourceConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
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
