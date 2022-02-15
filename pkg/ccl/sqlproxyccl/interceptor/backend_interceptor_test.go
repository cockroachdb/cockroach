// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package interceptor_test

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

// TestBackendInterceptor tests the BackendInterceptor. Note that the tests
// here are shallow. For detailed ones, see the tests for the internal
// interceptor in base_test.go.
func TestBackendInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)

	t.Run("bufSize too small", func(t *testing.T) {
		bi, err := interceptor.NewBackendInterceptor(nil /* src */, nil /* dst */, 1)
		require.Error(t, err)
		require.Nil(t, bi)
	})

	t.Run("PeekMsg returns the right message type", func(t *testing.T) {
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)

		bi, err := interceptor.NewBackendInterceptor(src, nil /* dst */, 16)
		require.NoError(t, err)
		require.NotNil(t, bi)

		typ, size, err := bi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, pgwirebase.ClientMsgSimpleQuery, typ)
		require.Equal(t, 9, size)

		bi.Close()
		typ, size, err = bi.PeekMsg()
		require.EqualError(t, err, interceptor.ErrInterceptorClosed.Error())
		require.Equal(t, pgwirebase.ClientMessageType(0), typ)
		require.Equal(t, 0, size)
	})

	t.Run("WriteMsg writes data to dst", func(t *testing.T) {
		dst := new(bytes.Buffer)
		bi, err := interceptor.NewBackendInterceptor(nil /* src */, dst, 10)
		require.NoError(t, err)
		require.NotNil(t, bi)

		// This is a backend interceptor, so writing goes to the server.
		toSend := &pgproto3.Query{String: "SELECT 1"}
		n, err := bi.WriteMsg(toSend)
		require.NoError(t, err)
		require.Equal(t, 14, n)
		require.Equal(t, 14, dst.Len())

		bi.Close()
		n, err = bi.WriteMsg(toSend)
		require.EqualError(t, err, interceptor.ErrInterceptorClosed.Error())
		require.Equal(t, 0, n)
	})

	t.Run("ReadMsg decodes the message correctly", func(t *testing.T) {
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)

		bi, err := interceptor.NewBackendInterceptor(src, nil /* dst */, 16)
		require.NoError(t, err)
		require.NotNil(t, bi)

		msg, err := bi.ReadMsg()
		require.NoError(t, err)
		rmsg, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "SELECT 1", rmsg.String)

		bi.Close()
		msg, err = bi.ReadMsg()
		require.EqualError(t, err, interceptor.ErrInterceptorClosed.Error())
		require.Nil(t, msg)
	})

	t.Run("ForwardMsg forwards data to dst", func(t *testing.T) {
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)
		dst := new(bytes.Buffer)

		bi, err := interceptor.NewBackendInterceptor(src, dst, 16)
		require.NoError(t, err)
		require.NotNil(t, bi)

		n, err := bi.ForwardMsg()
		require.NoError(t, err)
		require.Equal(t, 14, n)
		require.Equal(t, 14, dst.Len())

		bi.Close()
		n, err = bi.ForwardMsg()
		require.EqualError(t, err, interceptor.ErrInterceptorClosed.Error())
		require.Equal(t, 0, n)
	})
}
