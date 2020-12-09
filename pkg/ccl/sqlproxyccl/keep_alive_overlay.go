// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"net"
)

type keepAliveConn struct {
	net.Conn
	terminated     bool
	keepAliveError error
}

func (c *keepAliveConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if c.terminated {
		err = NewErrorf(
			CodeExpiredClientConnection, "expired client conn: %v",
			c.keepAliveError,
		)
	}
	return
}
func (c *keepAliveConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	if c.terminated {
		err = NewErrorf(
			CodeExpiredClientConnection, "expired client conn: %v",
			c.keepAliveError,
		)
	}
	return
}

// KeepAliveOverlay upgrades the connection with one that get closed when
// the keep alive loop function terminates.
func KeepAliveOverlay(
	ctx context.Context, conn net.Conn, keepAliveLoop func(ctx context.Context) error,
) net.Conn {
	if keepAliveLoop != nil {
		conn := &keepAliveConn{Conn: conn}
		go func() {
			conn.keepAliveError = keepAliveLoop(ctx)
			conn.terminated = true
			// This closes the backend connection which also currently results in
			// closing the frontend connection.
			conn.Conn.Close()
		}()
		return conn
	}
	return conn
}
