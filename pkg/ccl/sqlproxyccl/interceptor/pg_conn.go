// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package interceptor

import "net"

// PGConn ...
// TODO(jaylim-crl): Comment and tests.
type PGConn struct {
	net.Conn
	*pgInterceptor
}

// NewPGConn ...
// TODO(jaylim-crl): Comment and tests.
func NewPGConn(conn net.Conn) *PGConn {
	return &PGConn{
		Conn:          conn,
		pgInterceptor: newPgInterceptor(conn, defaultBufferSize),
	}
}
