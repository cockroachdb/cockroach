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
	"sync/atomic"
)

// monitorConn is a wrapper around net.Conn that intercepts reads/writes and
// registers the activity so that the idle monitor will not trigger connection
// closure. This is accomplished by using the "deadline" field as the central
// point of coordination between the monitor and this connection. The monitor
// atomically sets the deadline each time it wakes up, and the connection
// atomically clears it, by setting it to zero when activity occurs. If no
// activity occurs (and the connection is not clearing the deadline), then
// eventually the deadline expires and the monitor reports the connection as
// idle.
type monitorConn struct {
	net.Conn
	monitor  *Monitor
	deadline int64
}

// newMonitorConn wraps the given connection with a monitorConn.
func newMonitorConn(monitor *Monitor, conn net.Conn) *monitorConn {
	return &monitorConn{Conn: conn, monitor: monitor}
}

// Read reads data from the connection with timeout.
func (c *monitorConn) Read(b []byte) (n int, err error) {
	c.clearDeadline()
	return c.Conn.Read(b)
}

// Write writes data to the connection and sets the read timeout.
func (c *monitorConn) Write(b []byte) (n int, err error) {
	c.clearDeadline()
	return c.Conn.Write(b)
}

// Close removes this connection from the monitor and passes through the call to
// the wrapped connection.
func (c *monitorConn) Close() error {
	c.monitor.removeConn(c)
	return c.Conn.Close()
}

// clearDeadline atomically sets the deadline field to zero in order to prevent
// the monitor from declaring the connection as idle.
func (c *monitorConn) clearDeadline() {
	deadline := atomic.LoadInt64(&c.deadline)
	if deadline == 0 {
		return
	}
	atomic.StoreInt64(&c.deadline, 0)
}
