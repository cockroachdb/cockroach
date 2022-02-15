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

// TestFrontendInterceptor tests the FrontendInterceptor. Note that the tests
// here are shallow. For detailed ones, see the tests for the internal
// interceptor in base_test.go.
func TestFrontendInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(nil)

	buildSrc := func(t *testing.T, count int) *bytes.Buffer {
		t.Helper()
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)
		return src
	}

	t.Run("bufSize too small", func(t *testing.T) {
		fi, err := interceptor.NewFrontendInterceptor(nil /* src */, 1 /* bufSize */)
		require.Error(t, err)
		require.Nil(t, fi)
	})

	t.Run("PeekMsg returns the right message type", func(t *testing.T) {
		src := buildSrc(t, 1)

		fi, err := interceptor.NewFrontendInterceptor(src, 16 /* bufSize */)
		require.NoError(t, err)
		require.NotNil(t, fi)

		typ, size, err := fi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, pgwirebase.ServerMsgReady, typ)
		require.Equal(t, 6, size)
	})

	t.Run("WriteMsg writes data to dst", func(t *testing.T) {
		dst := new(bytes.Buffer)
		fi, err := interceptor.NewFrontendInterceptor(nil /* src */, 10 /* bufSize */)
		require.NoError(t, err)
		require.NotNil(t, fi)

		// This is a frontend interceptor, so writing goes to the client.
		toSend := &pgproto3.ReadyForQuery{TxStatus: 'I'}
		n, err := fi.WriteMsg(dst, toSend)
		require.NoError(t, err)
		require.Equal(t, 6, n)
		require.Equal(t, 6, dst.Len())
	})

	t.Run("ReadMsg decodes the message correctly", func(t *testing.T) {
		src := buildSrc(t, 1)

		fi, err := interceptor.NewFrontendInterceptor(src, 16 /* bufSize */)
		require.NoError(t, err)
		require.NotNil(t, fi)

		msg, err := fi.ReadMsg()
		require.NoError(t, err)
		rmsg, ok := msg.(*pgproto3.ReadyForQuery)
		require.True(t, ok)
		require.Equal(t, byte('I'), rmsg.TxStatus)
	})

	t.Run("ForwardMsg forwards data to dst", func(t *testing.T) {
		src := buildSrc(t, 1)
		dst := new(bytes.Buffer)

		fi, err := interceptor.NewFrontendInterceptor(src, 16 /* bufSize */)
		require.NoError(t, err)
		require.NotNil(t, fi)

		n, err := fi.ForwardMsg(dst)
		require.NoError(t, err)
		require.Equal(t, 6, n)
		require.Equal(t, 6, dst.Len())
	})
}
