// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package interceptor_test

import (
	"bytes"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

// Note that the tests here are shallow. For detailed ones, see the tests for
// the internal interceptor in base_test.go.
func TestPGConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	q, err := (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)
	require.NoError(t, err)

	t.Run("net.Conn/Write", func(t *testing.T) {
		external, proxy := net.Pipe()

		c := interceptor.NewPGConn(proxy)
		errCh := writeAsync(t, c, q)

		bc := interceptor.NewBackendConn(external)
		msg, err := bc.ReadMsg()
		require.NoError(t, err)
		rmsg, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "SELECT 1", rmsg.String)

		err = <-errCh
		require.Nil(t, err)
	})

	t.Run("pgInterceptor/ForwardMsg", func(t *testing.T) {
		external, proxy := net.Pipe()
		errCh := writeAsync(t, external, q)
		dst := new(bytes.Buffer)

		c := interceptor.NewPGConn(proxy)

		n, err := c.ForwardMsg(dst)
		require.NoError(t, err)
		require.Equal(t, 14, n)
		require.Equal(t, 14, dst.Len())

		err = <-errCh
		require.Nil(t, err)
	})
}

func TestPGConn_ToFrontendConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	q, err := (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(nil)
	require.NoError(t, err)

	external, proxy := net.Pipe()
	errCh := writeAsync(t, external, q)

	fc := interceptor.NewPGConn(proxy).ToFrontendConn()
	msg, err := fc.ReadMsg()
	require.NoError(t, err)
	rmsg, ok := msg.(*pgproto3.ReadyForQuery)
	require.True(t, ok)
	require.Equal(t, byte('I'), rmsg.TxStatus)

	err = <-errCh
	require.Nil(t, err)
}

func TestPGConn_ToBackendConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	q, err := (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)
	require.NoError(t, err)

	external, proxy := net.Pipe()
	errCh := writeAsync(t, external, q)

	bc := interceptor.NewPGConn(proxy).ToBackendConn()
	msg, err := bc.ReadMsg()
	require.NoError(t, err)
	rmsg, ok := msg.(*pgproto3.Query)
	require.True(t, ok)
	require.Equal(t, "SELECT 1", rmsg.String)

	err = <-errCh
	require.Nil(t, err)
}
