// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package listenerutil

import (
	"context"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReusableListener(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	lr := NewListenerRegistry()
	defer lr.Close()
	ln := lr.MustGetOrCreate(t, 0)

	// connect accepts from the socket and also connects to it to write and then
	// read a single byte. The returned error is that of the connecting goroutine;
	// an error in Accept() is ignored (but others are returned).
	connect := func() error {
		return ctxgroup.GoAndWait(ctx,
			func(ctx context.Context) (rerr error) {
				defer func() {
					rerr = errors.Wrap(rerr, "connecter")
				}()
				c, err := net.Dial("tcp", ln.Addr().String())
				if err != nil {
					return err
				}
				if _, err := c.Write([]byte{'x'}); err != nil {
					return errors.Wrap(err, "write")
				}
				sl := make([]byte, 1)
				if _, err := c.Read(sl); err != nil {
					return errors.Wrap(err, "read")
				}
				if sl[0] != 'x' {
					return errors.Errorf("reply-read: didn't expect %v", sl)
				}
				_ = c.Close()
				return nil
			},
			func(ctx context.Context) (rerr error) {
				defer func() {
					rerr = errors.Wrap(rerr, "accepter")
				}()
				c, err := ln.Accept()
				if err != nil {
					t.Logf("ignoring error from Accept: %s", err)
					return nil
				}
				sl := make([]byte, 1)
				if _, err := c.Read(sl); err != nil {
					return errors.Wrap(err, "read")
				}
				if sl[0] != 'x' {
					return errors.Errorf("read: didn't expect %v", sl)
				}
				if _, err := c.Write([]byte{'x'}); err != nil {
					return errors.Wrap(err, "reply-write")
				}
				_ = c.Close()
				return nil
			},
		)
	}

	require.NoError(t, connect())
	require.NoError(t, ln.Close())
	err := connect()
	require.Error(t, err)
	t.Logf("the expected error is: %v", err)
	require.NoError(t, lr.MustGet(t, 0).Reopen())
	require.NoError(t, connect())
}
