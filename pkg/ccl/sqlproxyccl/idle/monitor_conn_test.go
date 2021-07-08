// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package idle

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestMonitorConn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mon := NewMonitor(ctx, time.Second)

	conn := &testConn{Addr: "addr"}
	wrapped := newMonitorConn(mon, conn)
	defer func() {
		_ = wrapped.Close()
	}()

	wrapped.deadline = 1
	_, _ = wrapped.Read(nil)
	require.Equal(t, int64(0), wrapped.deadline)

	wrapped.deadline = 1
	_, _ = wrapped.Write(nil)
	require.Equal(t, int64(0), wrapped.deadline)
}

type testConn struct {
	net.Conn
	Addr string
}

func (c *testConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{Name: c.Addr, Net: "unix"}
}

func (c *testConn) Read(b []byte) (n int, err error) {
	return len(b), nil
}

func (c *testConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (c *testConn) Close() error {
	return nil
}
