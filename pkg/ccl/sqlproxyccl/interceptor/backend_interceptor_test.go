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

	buildSrc := func(t *testing.T, count int) *bytes.Buffer {
		t.Helper()
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)
		return src
	}

	t.Run("PeekMsg returns the right message type", func(t *testing.T) {
		src := buildSrc(t, 1)

		bi := interceptor.NewBackendInterceptor(src)
		require.NotNil(t, bi)

		typ, size, err := bi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, pgwirebase.ClientMsgSimpleQuery, typ)
		require.Equal(t, 14, size)
	})

	t.Run("ReadMsg decodes the message correctly", func(t *testing.T) {
		src := buildSrc(t, 1)

		bi := interceptor.NewBackendInterceptor(src)
		require.NotNil(t, bi)

		msg, err := bi.ReadMsg()
		require.NoError(t, err)
		rmsg, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "SELECT 1", rmsg.String)
	})

	t.Run("ForwardMsg forwards data to dst", func(t *testing.T) {
		src := buildSrc(t, 1)
		dst := new(bytes.Buffer)

		bi := interceptor.NewBackendInterceptor(src)
		require.NotNil(t, bi)

		n, err := bi.ForwardMsg(dst)
		require.NoError(t, err)
		require.Equal(t, 14, n)
		require.Equal(t, 14, dst.Len())
	})
}
