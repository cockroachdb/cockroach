// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/lib/pq"
)

// useLoopbackListener is a test-only variable that controls whether
// the test SQLConn* methods use a loopback connection (via
// *netutil.LoopbackListener) or a regular TCP connection.
//
// In theory, the loopback listener results in faster tests because it
// bypasses the TCP stack in the OS and also does not need to create
// TLS certs on disk or use the TLS protocol at all.
//
// In practice, we have to disable it for now because it causes
// certain tests to fail with unclear errors. It appears that server
// shutdowns does not properly close the resulting *gosql.DB objects.
//
// TODO(#107747): re-enable this.
const useLoopbackListener = false

// openTestSQLConn is a test helper that supports the SQLConn* methods
// of serverutils.ApplicationLayerInterface.
func openTestSQLConn(
	userName, dbName string,
	tenantName roachpb.TenantName,
	stopper *stop.Stopper,
	// When useLoopbackListener is set, only this is used:
	pgL *netutil.LoopbackListener,
	// When useLoopbackListener is not set, this is used:
	sqlAddr string,
	insecure bool,
) (*gosql.DB, error) {
	cleanupFn := func() {}
	var goDB *gosql.DB

	opts := url.Values{}
	if tenantName != "" && !strings.HasPrefix(dbName, "cluster:") {
		opts.Add("options", fmt.Sprintf("-ccluster=%s", tenantName))
	}
	if insecure || useLoopbackListener {
		opts.Add("sslmode", "disable")
	}

	if useLoopbackListener {
		pgurl := url.URL{
			Scheme:   "postgres",
			User:     url.User(userName),
			Host:     "unused",
			Path:     dbName,
			RawQuery: opts.Encode(),
		}
		// TODO(sql): consider using pgx for tests instead of lib/pq.
		connector, err := pq.NewConnector(pgurl.String())
		if err != nil {
			return nil, err
		}
		connector.Dialer(testDialer{pgL})
		goDB = gosql.OpenDB(connector)
	} else /* useLoopbackListener == false */ {
		var pgURL url.URL
		var err error
		pgURL, cleanupFn, err = sqlutils.PGUrlE(sqlAddr, "openTestSQLConn", url.User(userName))
		if err != nil {
			return nil, err
		}
		pgURL.Path = dbName

		// Add the common query options decided above to those prepared by
		// PGUrlE().
		qv := pgURL.Query()
		for k, v := range opts {
			qv[k] = v
		}
		pgURL.RawQuery = qv.Encode()

		// Open the connection.
		goDB, err = gosql.Open("postgres", pgURL.String())
		if err != nil {
			cleanupFn()
			return nil, err
		}
	}

	// Ensure the connection is closed at the end of the test.
	stopper.AddCloser(stop.CloserFn(func() {
		err := goDB.Close()
		if log.V(2) {
			log.Infof(context.Background(), "closing test SQL connection: %v", err)
		}
		cleanupFn()
	}))
	return goDB, nil
}

type testDialer struct{ pgL *netutil.LoopbackListener }

// Dial implements the pq.Dialer interface.
func (d testDialer) Dial(_, _ string) (net.Conn, error) {
	return d.pgL.Connect(context.Background())
}

// DialContext implements the pq.Dialer interface.
func (d testDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	return d.pgL.Connect(ctx)
}

// DialTimeout implements the pq.Dialer interface.
func (d testDialer) DialTimeout(_, _ string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.pgL.Connect(ctx)
}
