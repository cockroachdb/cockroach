// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
