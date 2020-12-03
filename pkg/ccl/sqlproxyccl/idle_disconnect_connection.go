// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// IdleDisconnectConnection is a wrapper around net.Conn that disconnects if connection is idle
type IdleDisconnectConnection struct {
	net.Conn
	timeout time.Duration
}

var notSupportedError = errors.Errorf("Not supported for IdleDisconnectConnection")

// Read reads data from the connection with timeout.
func (c IdleDisconnectConnection) Read(b []byte) (n int, err error) {
	if err := c.Conn.SetReadDeadline(timeutil.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}

// Write writes data to the connection and sets the read timeout.
func (c IdleDisconnectConnection) Write(b []byte) (n int, err error) {
	if err := c.Conn.SetReadDeadline(timeutil.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

// SetDeadline is unsupported as it will interfere with the reads.
func (c IdleDisconnectConnection) SetDeadline(t time.Time) error {
	return notSupportedError
}

// SetReadDeadline is unsupported as it will interfere with the reads.
func (c IdleDisconnectConnection) SetReadDeadline(t time.Time) error {
	return notSupportedError
}

// SetWriteDeadline is unsupported as it will interfere with the reads.
func (c IdleDisconnectConnection) SetWriteDeadline(t time.Time) error {
	return notSupportedError
}

// NewIdleDisconnectConnection creates a new Idle disconnect connection.
func NewIdleDisconnectConnection(conn net.Conn, timeout time.Duration) *IdleDisconnectConnection {
	return &IdleDisconnectConnection{Conn: conn, timeout: timeout}
}
