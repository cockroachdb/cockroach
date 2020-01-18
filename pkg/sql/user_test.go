// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

// TestGetUserHashedPasswordTimeout verifies that user login attempts
// fail with a suitable timeout when some system range(s) are
// unavailable.
//
// To achieve this it creates a 2-node cluster, moves all ranges
// from node 1 to node 2, then stops node 2, then attempts
// to connect to node 1.
func TestGetUserHashedPasswordTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short flag")
	}

	ctx := context.Background()

	// Two nodes, two localities. We need the localities to pin the
	// ranges to a particular node velow.
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ServerArgsPerNode: map[int]base.TestServerArgs{
				0: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r1"}}}},
				1: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}}}},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Note: we are using fmt.Fprintln here and below instead of t.Logf
	// so as to ensure that these messages are properly interleaved with
	// the server logs in case of failure.
	fmt.Fprintln(os.Stderr, "-- moving system ranges to 2nd node --")

	// Disable replication (num replicas = 1) and move all ranges to r2.
	if err := tc.Server(0).(*server.TestServer).Server.RunLocalSQL(ctx,
		func(ctx context.Context, ie *sql.InternalExecutor) error {
			rows, err := ie.Query(ctx, "get-zones", nil,
				"SELECT target FROM crdb_internal.zones")
			if err != nil {
				return err
			}
			for _, row := range rows {
				zone := string(*row[0].(*tree.DString))
				if _, err := ie.Exec(ctx, "set-zone", nil,
					fmt.Sprintf("ALTER %s CONFIGURE ZONE USING num_replicas = 1, constraints = '[+region=r2]'", zone)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	// Wait for ranges to move to node 2.
	testutils.SucceedsSoon(t, func() error {
		// Kick the replication queues, to speed up the rebalancing process.
		for i := 0; i < tc.NumServers(); i++ {
			if err := tc.Server(i).GetStores().(*storage.Stores).VisitStores(func(s *storage.Store) error {
				return s.ForceReplicationScanAndProcess()
			}); err != nil {
				t.Fatal(err)
			}
		}
		row := tc.ServerConn(0).QueryRow(`SELECT min(unnest(replicas)) FROM crdb_internal.ranges`)
		var nodeID int
		if err := row.Scan(&nodeID); err != nil {
			t.Fatal(err)
		}
		if nodeID != 2 {
			return errors.New("not rebalanced, still waiting")
		}
		return nil
	})

	// Make a user that must use a password to authenticate.
	// Default privileges on defaultdb are needed to run simple queries.
	if _, err := tc.ServerConn(0).Exec(`
CREATE USER foo WITH PASSWORD 'testabc';
GRANT ALL ON DATABASE defaultdb TO foo`); err != nil {
		t.Fatal(err)
	}

	// We'll attempt connections on gateway node 0.
	userURL, cleanupFn := sqlutils.PGUrlWithOptionalClientCerts(t,
		tc.Server(0).ServingSQLAddr(), t.Name(), url.UserPassword("foo", "testabc"), false /* withClientCerts */)
	defer cleanupFn()
	rootURL, rootCleanupFn := sqlutils.PGUrl(t,
		tc.Server(0).ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer rootCleanupFn()

	// Override the timeout built into pgx so we are only subject to
	// what the server thinks.
	userURL.RawQuery += "&connect_timeout=0"
	rootURL.RawQuery += "&connect_timeout=0"

	fmt.Fprintln(os.Stderr, "-- sanity checks --")

	// We use a closure here and below to ensure the defers are run
	// before the rest of the test.

	func() {
		// Sanity check: verify that secure mode is enabled: password is
		// required. If this part fails, this means the test cluster is
		// not properly configured, and the remainder of the test below
		// would report false positives.
		unauthURL := userURL
		unauthURL.User = url.User("foo")
		dbSQL, err := pgxConn(t, unauthURL)
		if err == nil {
			defer func() { _ = dbSQL.Close() }()
		}
		if !testutils.IsError(err, "password authentication failed for user foo") {
			t.Fatalf("expected password error, got %v", err)
		}
	}()

	func() {
		// Sanity check: verify that the new user is able to log in with password.
		dbSQL, err := pgxConn(t, userURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = dbSQL.Close() }()
		row := dbSQL.QueryRow("SELECT current_user")
		var username string
		if err := row.Scan(&username); err != nil {
			t.Fatal(err)
		}
		if username != "foo" {
			t.Fatalf("invalid username: expected foo, got %q", username)
		}
	}()

	// Configure the login timeout to just 1s.
	if _, err := tc.ServerConn(0).Exec(`SET CLUSTER SETTING server.user_login.timeout = '1s'`); err != nil {
		t.Fatal(err)
	}

	fmt.Fprintln(os.Stderr, "-- shutting down node 2 --")

	// This is intended to make the system ranges unavailable.
	tc.StopServer(1)

	fmt.Fprintln(os.Stderr, "-- expect timeout --")

	func() {
		// Now attempt to connect again. We're expecting a timeout within 5 seconds.
		start := timeutil.Now()
		dbSQL, err := pgxConn(t, userURL)
		if err == nil {
			defer func() { _ = dbSQL.Close() }()
		}
		if !testutils.IsError(err, "internal error while retrieving user account") {
			t.Fatalf("expected error during connection, got %v", err)
		}
		timeoutDur := timeutil.Now().Sub(start)
		if timeoutDur > 5*time.Second {
			t.Fatalf("timeout lasted for more than 5 second (%s)", timeoutDur)
		}
	}()

	fmt.Fprintln(os.Stderr, "-- no timeout for root --")

	func() {
		dbSQL, err := pgxConn(t, rootURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = dbSQL.Close() }()
		// A simple query must work for 'root' even without a system range available.
		if _, err := dbSQL.Exec("SELECT 1"); err != nil {
			t.Fatal(err)
		}
	}()

}

func pgxConn(t *testing.T, connURL url.URL) (*pgx.Conn, error) {
	pgxConfig, err := pgx.ParseConnectionString(connURL.String())
	if err != nil {
		t.Fatal(err)
	}

	// Override the conninfo to avoid a bunch of pg_catalog
	// queries when the connection is being set up.
	pgxConfig.CustomConnInfo = func(c *pgx.Conn) (*pgtype.ConnInfo, error) {
		return c.ConnInfo, nil
	}

	return pgx.Connect(pgxConfig)
}
