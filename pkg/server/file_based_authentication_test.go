// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows

package server

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

// TestEmergencyAuthentication checks that the HTTP interface can still
// be made reachable via the emergency authentication mechanism
// even when the majority of the cluster is unavailable.
func TestEmergencyAuthentication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.UnderShort(t, "long test")

	defer log.Scope(t).Close(t)

	authDir, cleanup := testutils.TempDir(t)
	defer cleanup()

	authFile := filepath.Join(authDir, "myauth")

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			EmergencyAuthenticationSessions: authFile,
		},
	})
	defer tc.Stopper().Stop(ctx)

	s0 := tc.Server(0).(*TestServer)

	t.Logf("waiting for up-replication")
	// Wait for the newly created table to spread over all nodes.
	testutils.SucceedsSoon(t, func() error {
		return s0.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
			_, err := ie.Exec(ctx, "wait-replication", nil, `
SELECT -- wait for up-replication.
       IF(array_length(replicas, 1) != 3,
          crdb_internal.force_error('UU000', 'not ready: ' || array_length(replicas, 1)::string || ' replicas'),
          0)
  FROM crdb_internal.ranges`)
			if err != nil && !testutils.IsError(err, "not ready") {
				t.Fatal(err)
			}
			return err
		})
	})

	t.Logf("creating admin users")
	// Create a second admin user. admin1 is already created by TestCluster.
	if err := s0.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
		for _, stmt := range []string{
			`CREATE USER admin2 WITH PASSWORD 'abc'`,
			`GRANT admin TO admin2`,
		} {
			if _, err := ie.Exec(ctx, "create-admin2", nil, stmt); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	udebug := debugURL(s0)

	t.Logf("sanity check: ensure that admin1 can use the cluster")
	// Get cookie for admin1.
	client1, _, err := s0.getAuthenticatedHTTPClientAndCookie(
		security.MakeSQLUsernameFromPreNormalizedString("admin1"), true /* isAdmin */)
	if err != nil {
		t.Fatal(err)
	}
	// Sanity check: check that admin1 can do things.
	{
		resp, err := client1.Get(udebug)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status code %d; got %d", http.StatusOK, resp.StatusCode)
		}
	}

	t.Logf("stopping 2/3 nodes")
	tc.StopServer(1)
	tc.StopServer(2)

	t.Logf("expecting timeout for admin1")
	// Try to use client1. We expect the test to time out here.
	// This timeout is not going to happen immediately: there is a short
	// amount of time after the other nodes have shut down, where the first
	// node still has a valid lease on the first range and can read data from
	// it. Wait until this has expired, by waiting until the HTTP request
	// actually reaches its timeout.
	testutils.SucceedsSoon(t, func() error {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctxWithTimeout, "GET", udebug, nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := client1.Do(req)
		if err != nil {
			if !errors.Is(ctxWithTimeout.Err(), context.DeadlineExceeded) {
				t.Fatal(err)
			}
			// Timeout reached: all good.
			return nil
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return errors.New("HTTP service still ready")
		}
		return nil
	})

	t.Logf("verifying that admin2 is shut off from the cluster")
	func() {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		_, err := s0.authentication.UserLogin(ctxWithTimeout, &serverpb.UserLoginRequest{
			Username: "admin2",
			Password: "abc",
		})
		if err == nil {
			t.Fatalf("expected RPC error, got nil")
		}
	}()

	t.Logf("creating and loading the emergency credentials")
	admin2 := security.MakeSQLUsernameFromPreNormalizedString("admin2")
	_, authLine, httpCookie, err := GenerateEmergencyCredentials(admin2, true /* isAdmin */, 1*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// Write the credential to the file server-side.
	if err := ioutil.WriteFile(authFile, []byte(authLine+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Trigger a reload on the server.
	if err := unix.Kill(unix.Getpid(), unix.SIGHUP); err != nil {
		t.Fatal(err)
	}

	t.Logf("attempting a new connection with the special credential")
	adminUrl, err := url.Parse(s0.AdminURL())
	if err != nil {
		t.Fatal(err)
	}
	client1.Jar.SetCookies(adminUrl, []*http.Cookie{httpCookie})

	testutils.SucceedsSoon(t, func() error {
		// Add the new cookie to the client jar.
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctxWithTimeout, "GET", udebug, nil)
		if err != nil {
			return err
		}

		resp, err := client1.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return errors.Newf("expected HTTP %v, got %v", http.StatusOK, resp.StatusCode)
		}
		return nil
	})
}
