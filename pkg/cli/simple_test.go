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
	gosql "database/sql"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// First test: establish a SQL connection while cluster is available,
// then switch down some nodes, then verify that some queries indeed
// fail with the configured timeout.
func TestUnavailableClusterAfterConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlConn := tc.ServerConn(0)

	// Establish conn.
	if _, err := sqlConn.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	// Stop the server. This should make the system unavailable.
	tc.StopServer(1)
	tc.StopServer(2)

	err := contextutil.RunWithTimeout(ctx, t.Name(), 3*time.Second, func(ctx context.Context) error {
		_, err := sqlConn.ExecContext(ctx, `SELECT * FROM system.users WHERE username = 'notexistent'`)
		return err
	})
	if _, ok := err.(*contextutil.TimeoutError); !ok {
		t.Fatalf("expected dimeout error, got (%T) %v", err, err)
	}
}

// Second test: make cluster unavailable, then establish non-admin
// user connection with password. As this requires a query to
// system.users, we're expecting that query to fail.
func TestUnavailableClusterBeforeConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// Call GetAuthenticatedHTTPClient to create a new user in system.user
	// with password "abc"
	if _, err := tc.Server(0).GetAuthenticatedHTTPClient(false /*isAdmin*/); err != nil {
		t.Fatal(err)
	}

	// Make the cluster unavailable.
	tc.StopServer(1)
	tc.StopServer(2)

	userURL, cleanupFn := sqlutils.PGUrlWithOptionalClientCerts(t,
		tc.Server(0).ServingSQLAddr(),
		t.Name(),
		url.UserPassword("authentic_user_noadmin", "abc"),
		false /* withClientCerts */)
	defer cleanupFn()

	// Override the timeout built into the SQL driver so we are only
	// subject to what the server thinks.
	userURL.RawQuery += "&connect_timeout=0"

	sqlConn, err := gosql.Open("postgres", userURL.String())
	if err != nil {
		t.Fatal(err) // No error expected yet - pq does late binding.
	}

	// Run something to establish the connection. We're expecting
	// a timeout because a non-root conn is hitting system.users, which
	// should now be unavailable.
	err = contextutil.RunWithTimeout(ctx, t.Name(), 3*time.Second, func(ctx context.Context) error {
		_, err := sqlConn.ExecContext(ctx, `SELECT 1`)
		return err
	})
	if _, ok := err.(*contextutil.TimeoutError); !ok {
		t.Fatalf("expected dimeout error, got (%T) %v", err, err)
	}

}
