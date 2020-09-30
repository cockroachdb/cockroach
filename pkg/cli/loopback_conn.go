// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/lib/pq"
)

// newLoopbackSQLConn establishes an in-memory connection to the SQL
// server. Its authentication method is always "trust" so the
// password or other credentials do not matter.
func newLoopbackSQLConn(s *server.Server, username string) *sqlConn {
	// The URL is using the DSN format because we want to pass a host value
	// starting with a "/". This short-cuts all the IP-related checks in
	// lib/pq.
	//
	// The particular value of host/port doesn't matter: it is passed as
	// address into our loopbackDialer's Dial() method below, which
	// ignores it.
	//
	// We do care about the username value though.
	url := "user=" + username + " password=unused host=/nonexistent port=1234"
	conn := makeSQLConn(url)
	conn.dialer = &loopbackDialer{s}
	return conn
}

// loopbackDialer makes it possible for lib/pq to use
// (*server.Server).ConnectLocalSQL().
type loopbackDialer struct {
	s *server.Server
}

var _ pq.Dialer = (*loopbackDialer)(nil)
var _ pq.DialerContext = (*loopbackDialer)(nil)

// Dial is part of the pq.Dialer interface.
func (d *loopbackDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

// DialContext is part of the pq.DialerContext interface.
func (d *loopbackDialer) DialContext(
	ctx context.Context, network, address string,
) (net.Conn, error) {
	return d.s.ConnectLocalSQL(ctx)
}

// DialTimeout is part of the pq.Dialer interface.
func (d *loopbackDialer) DialTimeout(
	network, address string, timeout time.Duration,
) (conn net.Conn, err error) {
	err = contextutil.RunWithTimeout(
		context.Background(), "pq-loopback-dial", timeout,
		func(ctx context.Context) (fnErr error) {
			conn, fnErr = d.DialContext(ctx, network, address)
			return
		})
	return
}
