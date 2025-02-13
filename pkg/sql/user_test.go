// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestUserLoginAfterGC sets an artificially low gc.ttl on system.role_members,
// then verifies that the role membership cache still works. This is valuable to
// test since we change the read timestamp of the transaction that populates the
// role membership cache.
func TestUserLoginAfterGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a user.
	_, err := db.Exec(`CREATE USER newuser WITH password '123'`)
	require.NoError(t, err)

	_, err = db.Exec(`GRANT admin TO newuser`)
	require.NoError(t, err)

	// Sleep so that the system.role_members modification time is 2s in the past.
	time.Sleep(2 * time.Second)

	// Force a table GC with a threshold of 500ms in the past.
	err = s.ForceTableGC(ctx, "system", "role_members", s.Clock().Now().Add(-int64(500*time.Millisecond), 0))
	require.NoError(t, err)

	// Verify that newuser can still log in.
	newUserURL, cleanup := pgurlutils.PGUrlWithOptionalClientCerts(
		t, s.AdvSQLAddr(), t.Name(), url.UserPassword("newuser", "123"), false, /* withClientCerts */
		"")
	defer cleanup()

	newUserConn, err := sqltestutils.PGXConn(t, newUserURL)
	require.NoError(t, err)
	defer func() { _ = newUserConn.Close(ctx) }()

	_, err = newUserConn.Exec(ctx, `SHOW GRANTS FOR newuser`)
	require.NoError(t, err)
}

// TestGetUserTimeout verifies that user login attempts
// fail with a suitable timeout when some system range(s) are
// unavailable.
//
// To achieve this it creates a 2-node cluster, moves all ranges
// from node 1 to node 2, then stops node 2, then attempts
// to connect to node 1.
func TestGetUserTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// unavailableCh is used by the replica command filter
	// to conditionally block requests and simulate unavailability.
	var unavailableCh atomic.Value
	closedCh := make(chan struct{})
	close(closedCh)
	unavailableCh.Store(closedCh)
	knobs := &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, _ *kvpb.BatchRequest) *kvpb.Error {
			select {
			case <-unavailableCh.Load().(chan struct{}):
			case <-ctx.Done():
			}
			return nil
		},
	}
	params := base.TestServerArgs{Knobs: base.TestingKnobs{Store: knobs}}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Make a user that must use a password to authenticate.
	// Default privileges on defaultdb are needed to run simple queries.
	if _, err := db.Exec(`
CREATE USER foo WITH PASSWORD 'testabc';
GRANT ALL ON DATABASE defaultdb TO foo;
GRANT admin TO foo`); err != nil {
		t.Fatal(err)
	}

	// We'll attempt connections on gateway node 0.
	fooURL, fooCleanupFn := pgurlutils.PGUrlWithOptionalClientCerts(t,
		s.AdvSQLAddr(), t.Name(), url.UserPassword("foo", "testabc"), false, "" /* withClientCerts */)
	defer fooCleanupFn()
	barURL, barCleanupFn := pgurlutils.PGUrlWithOptionalClientCerts(t,
		s.AdvSQLAddr(), t.Name(), url.UserPassword("bar", "testabc"), false, "" /* withClientCerts */)
	defer barCleanupFn()
	rootURL, rootCleanupFn := pgurlutils.PGUrl(t,
		s.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer rootCleanupFn()

	// Override the timeout built into pgx so we are only subject to
	// what the server thinks.
	fooURL.RawQuery += "&connect_timeout=0"
	barURL.RawQuery += "&connect_timeout=0"
	rootURL.RawQuery += "&connect_timeout=0"

	t.Log("-- sanity checks --")

	// We use a closure here and below to ensure the defers are run
	// before the rest of the test.

	func() {
		// Sanity check: verify that secure mode is enabled: password is
		// required. If this part fails, this means the test cluster is
		// not properly configured, and the remainder of the test below
		// would report false positives.
		unauthURL := fooURL
		unauthURL.User = url.User("foo")
		dbSQL, err := sqltestutils.PGXConn(t, unauthURL)
		if err == nil {
			defer func() { _ = dbSQL.Close(ctx) }()
		}
		if !testutils.IsError(err, "failed SASL auth") {
			t.Fatalf("expected password error, got %v", err)
		}
	}()

	func() {
		// Sanity check: verify that the new user is able to log in with password.
		dbSQL, err := sqltestutils.PGXConn(t, fooURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = dbSQL.Close(ctx) }()
		row := dbSQL.QueryRow(ctx, "SELECT current_user")
		var username string
		if err := row.Scan(&username); err != nil {
			t.Fatal(err)
		}
		if username != "foo" {
			t.Fatalf("invalid username: expected foo, got %q", username)
		}
	}()

	// Configure the login timeout to just 200ms.
	if _, err := db.Exec(`SET CLUSTER SETTING server.user_login.timeout = '200ms'`); err != nil {
		t.Fatal(err)
	}

	// Cache our privilege check for `SHOW is_superuser` and the
	// underlying query to a virtual table.
	dbSQL, err := sqltestutils.PGXConn(t, fooURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = dbSQL.Close(ctx)
	}()
	var isSuperuser string
	require.NoError(t, dbSQL.QueryRow(ctx, "SHOW is_superuser").Scan(&isSuperuser))
	require.Equal(t, "on", isSuperuser)

	func() {
		t.Log("-- make ranges unavailable --")

		ch := make(chan struct{})
		unavailableCh.Store(ch)
		defer close(ch)

		t.Log("-- expect no timeout because of cache --")

		func() {
			// Now attempt to connect again. Since a previous authentication attempt
			// for this user occurred, the auth-related info should be cached, so
			// authentication should work.
			dbSQL, err := sqltestutils.PGXConn(t, fooURL)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = dbSQL.Close(ctx) }()
			// A simple query must work even without a system range available.
			if _, err := dbSQL.Exec(ctx, "SELECT 1"); err != nil {
				t.Fatal(err)
			}
			var isSuperuser string
			require.NoError(t, dbSQL.QueryRow(ctx, "SHOW is_superuser").Scan(&isSuperuser))
			require.Equal(t, "on", isSuperuser)
		}()

		t.Log("-- expect timeout --")

		func() {
			// Now attempt to connect with a different user. We're expecting a timeout
			// within 5 seconds.
			start := timeutil.Now()
			dbSQL, err := sqltestutils.PGXConn(t, barURL)
			if err == nil {
				defer func() { _ = dbSQL.Close(ctx) }()
			}
			if !testutils.IsError(err, "internal error while retrieving user account") {
				t.Fatalf("expected error during connection, got %v", err)
			}
			timeoutDur := timeutil.Since(start)
			if timeoutDur > 5*time.Second {
				t.Fatalf("timeout lasted for more than 5 second (%s)", timeoutDur)
			}
		}()

		t.Log("-- no timeout for root --")

		func() {
			dbSQL, err := sqltestutils.PGXConn(t, rootURL)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = dbSQL.Close(ctx) }()
			// A simple query must work for 'root' even without a system range available.
			if _, err := dbSQL.Exec(ctx, "SELECT 1"); err != nil {
				t.Fatal(err)
			}
			var isSuperuser string
			require.NoError(t, dbSQL.QueryRow(ctx, "SHOW is_superuser").Scan(&isSuperuser))
			require.Equal(t, "on", isSuperuser)
		}()

		t.Log("-- re-enable range, revoke admin for user --")
	}()

	t.Log("-- removing foo as admin --")
	_, err = db.Exec(`REVOKE admin FROM foo`)
	require.NoError(t, err)

	func() {
		t.Log("-- make ranges unavailable --")

		ch := make(chan struct{})
		unavailableCh.Store(ch)
		defer close(ch)

		func() {
			// Now attempt to connect with foo. We're expecting a timeout within 5
			// seconds as the membership cache is invalid.
			start := timeutil.Now()
			dbSQL, err := sqltestutils.PGXConn(t, fooURL)
			if err == nil {
				defer func() { _ = dbSQL.Close(ctx) }()
			}
			if !testutils.IsError(err, "internal error while retrieving user account memberships") {
				t.Fatalf("expected error during connection, got %v", err)
			}
			timeoutDur := timeutil.Since(start)
			if timeoutDur > 5*time.Second {
				t.Fatalf("timeout lasted for more than 5 second (%s)", timeoutDur)
			}
		}()
	}()
}
