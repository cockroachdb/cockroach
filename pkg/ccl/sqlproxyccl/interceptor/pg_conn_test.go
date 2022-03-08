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
	"io"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

// Note that the tests here are shallow. For detailed ones, see the tests for
// the internal interceptor in base_test.go.
func TestPGConn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)

	writeAsync := func(t *testing.T, w io.Writer) <-chan error {
		t.Helper()
		errCh := make(chan error, 1)
		go func() {
			_, err := w.Write(q)
			errCh <- err
		}()
		return errCh
	}

	t.Run("net.Conn/Write", func(t *testing.T) {
		external, proxy := net.Pipe()

		c := interceptor.NewPGConn(proxy)
		errCh := writeAsync(t, c)

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
		errCh := writeAsync(t, external)
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
