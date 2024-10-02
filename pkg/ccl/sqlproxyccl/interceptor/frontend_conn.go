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

// FrontendConn is a client interceptor for the Postgres frontend protocol.
// This will be used for the connection between proxy and server.
type FrontendConn struct {
	net.Conn
	interceptor *pgInterceptor
}

// NewFrontendConn creates a FrontendConn using the default buffer size of 8KB.
func NewFrontendConn(conn net.Conn) *FrontendConn {
	return &FrontendConn{
		Conn:        conn,
		interceptor: newPgInterceptor(conn, defaultBufferSize),
	}
}

// PeekMsg returns the header of the current pgwire message without advancing
// the interceptor.
//
// See pgInterceptor.PeekMsg for more information.
func (c *FrontendConn) PeekMsg() (typ pgwirebase.ServerMessageType, size int, err error) {
	byteType, size, err := c.interceptor.PeekMsg()
	return pgwirebase.ServerMessageType(byteType), size, err
}

// ReadMsg decodes the current pgwire message and returns a BackendMessage.
// This also advances the interceptor to the next message.
//
// See pgInterceptor.ReadMsg for more information.
func (c *FrontendConn) ReadMsg() (msg pgproto3.BackendMessage, err error) {
	msgBytes, err := c.interceptor.ReadMsg()
	if err != nil {
		return nil, err
	}
	// errWriter is used here because Receive must not Write.
	return pgproto3.NewFrontend(newChunkReader(msgBytes), &errWriter{}).Receive()
}

// ForwardMsg sends the current pgwire message to the destination without any
// decoding, and advances the interceptor to the next message.
//
// See pgInterceptor.ForwardMsg for more information.
func (c *FrontendConn) ForwardMsg(dst io.Writer) (n int, err error) {
	return c.interceptor.ForwardMsg(dst)
}
