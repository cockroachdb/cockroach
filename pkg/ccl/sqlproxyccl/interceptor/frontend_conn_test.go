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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

// TestFrontendConn tests the FrontendConn. Note that the tests here are shallow.
// For detailed ones, see the tests for the internal interceptor in base_test.go.
func TestFrontendConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	q, err := (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(nil)
	require.NoError(t, err)

	t.Run("PeekMsg returns the right message type", func(t *testing.T) {
		w, r := net.Pipe()
		errCh := writeAsync(t, w, q)

		fi := interceptor.NewFrontendConn(r)
		require.NotNil(t, fi)

		typ, size, err := fi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, pgwirebase.ServerMsgReady, typ)
		require.Equal(t, 6, size)

		err = <-errCh
		require.Nil(t, err)
	})

	t.Run("ReadMsg decodes the message correctly", func(t *testing.T) {
		w, r := net.Pipe()
		errCh := writeAsync(t, w, q)

		fi := interceptor.NewFrontendConn(r)
		require.NotNil(t, fi)

		msg, err := fi.ReadMsg()
		require.NoError(t, err)
		rmsg, ok := msg.(*pgproto3.ReadyForQuery)
		require.True(t, ok)
		require.Equal(t, byte('I'), rmsg.TxStatus)

		err = <-errCh
		require.Nil(t, err)
	})

	t.Run("ForwardMsg forwards data to dst", func(t *testing.T) {
		w, r := net.Pipe()
		errCh := writeAsync(t, w, q)
		dst := new(bytes.Buffer)

		fi := interceptor.NewFrontendConn(r)
		require.NotNil(t, fi)

		n, err := fi.ForwardMsg(dst)
		require.NoError(t, err)
		require.Equal(t, 6, n)
		require.Equal(t, 6, dst.Len())

		err = <-errCh
		require.Nil(t, err)
	})
}
