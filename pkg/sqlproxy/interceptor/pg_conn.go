// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package interceptor

import "net"

// PGConn wraps both net.Conn and pgInterceptor as one object. This is the
// net.Conn version for pgInterceptor.
type PGConn struct {
	net.Conn
	*pgInterceptor
}

// NewPGConn creates a PGConn using a default buffer size of 8KB.
func NewPGConn(conn net.Conn) *PGConn {
	return &PGConn{
		Conn:          conn,
		pgInterceptor: newPgInterceptor(conn, defaultBufferSize),
	}
}

// ToFrontendConn converts a PGConn to a FrontendConn. Callers should be aware
// of the underlying type of net.Conn before calling this, or else there will be
// an error during parsing.
func (c *PGConn) ToFrontendConn() *FrontendConn {
	return &FrontendConn{Conn: c.Conn, interceptor: c.pgInterceptor}
}

// ToBackendConn converts a PGConn to a BackendConn. Callers should be aware
// of the underlying type of net.Conn before calling this, or else there will be
// an error during parsing.
func (c *PGConn) ToBackendConn() *BackendConn {
	return &BackendConn{Conn: c.Conn, interceptor: c.pgInterceptor}
}
