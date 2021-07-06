// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package idle

import (
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// DisconnectConnection is a wrapper around net.Conn that disconnects if
// connection is idle. The idle time is only counted while the client is
// waiting, blocked on Read.
type DisconnectConnection struct {
	net.Conn
	timeout time.Duration
	mu      struct {
		syncutil.Mutex
		lastDeadlineSetAt time.Time
	}
}

var errNotSupported = errors.Errorf(
	"Not supported for DisconnectConnection",
)

func (c *DisconnectConnection) updateDeadline() error {
	now := timeutil.Now()
	// If it has been more than 1% of the timeout duration - advance the deadline.
	c.mu.Lock()
	defer c.mu.Unlock()
	if now.Sub(c.mu.lastDeadlineSetAt) > c.timeout/100 {
		c.mu.lastDeadlineSetAt = now

		if err := c.Conn.SetReadDeadline(now.Add(c.timeout)); err != nil {
			return err
		}
	}
	return nil
}

// Read reads data from the connection with timeout.
func (c *DisconnectConnection) Read(b []byte) (n int, err error) {
	if err := c.updateDeadline(); err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}

// Write writes data to the connection and sets the read timeout.
func (c *DisconnectConnection) Write(b []byte) (n int, err error) {
	// The Write for the connection is not blocking (or can block only temporary
	// in case of flow control). For idle connections, the Read will be the call
	// that will block and stay blocked until the backend doesn't send something.
	// However, it is theoretically possible, that the traffic is only going in
	// one direction - from the proxy to the backend, in which case we will call
	// repeatedly Write but stay blocked on the Read. For that specific case - the
	// write pushes further out the read deadline so the read doesn't timeout.
	if err := c.updateDeadline(); err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

// SetDeadline is unsupported as it will interfere with the reads.
func (c *DisconnectConnection) SetDeadline(t time.Time) error {
	return errNotSupported
}

// SetReadDeadline is unsupported as it will interfere with the reads.
func (c *DisconnectConnection) SetReadDeadline(t time.Time) error {
	return errNotSupported
}

// SetWriteDeadline is unsupported as it will interfere with the reads.
func (c *DisconnectConnection) SetWriteDeadline(t time.Time) error {
	return errNotSupported
}

// DisconnectOverlay upgrades the connection to one that closes when
// idle for more than timeout duration. Timeout of zero will turn off
// the idle disconnect code.
func DisconnectOverlay(conn net.Conn, timeout time.Duration) net.Conn {
	if timeout != 0 {
		return &DisconnectConnection{Conn: conn, timeout: timeout}
	}
	return conn
}
