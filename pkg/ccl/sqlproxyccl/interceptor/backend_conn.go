// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package interceptor

import (
	"io"
	"net"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/jackc/pgproto3/v2"
)

// BackendConn is a server interceptor for the Postgres backend protocol.
// This will be used for the connection between client and proxy.
type BackendConn struct {
	net.Conn
	interceptor *pgInterceptor
}

// NewBackendConn creates a BackendConn using the default buffer size of 8KB.
func NewBackendConn(conn net.Conn) *BackendConn {
	return &BackendConn{
		Conn:        conn,
		interceptor: newPgInterceptor(conn, defaultBufferSize),
	}
}

// PeekMsg returns the header of the current pgwire message without advancing
// the interceptor.
//
// See pgInterceptor.PeekMsg for more information.
func (c *BackendConn) PeekMsg() (typ pgwirebase.ClientMessageType, size int, err error) {
	byteType, size, err := c.interceptor.PeekMsg()
	return pgwirebase.ClientMessageType(byteType), size, err
}

// ReadMsg decodes the current pgwire message and returns a FrontendMessage.
// This also advances the interceptor to the next message.
//
// See pgInterceptor.ReadMsg for more information.
func (c *BackendConn) ReadMsg() (msg pgproto3.FrontendMessage, err error) {
	msgBytes, err := c.interceptor.ReadMsg()
	if err != nil {
		return nil, err
	}
	// errWriter is used here because Receive must not Write.
	return pgproto3.NewBackend(newChunkReader(msgBytes), &errWriter{}).Receive()
}

// ForwardMsg sends the current pgwire message to the destination without any
// decoding, and advances the interceptor to the next message.
//
// See pgInterceptor.ForwardMsg for more information.
func (c *BackendConn) ForwardMsg(dst io.Writer) (n int, err error) {
	return c.interceptor.ForwardMsg(dst)
}
