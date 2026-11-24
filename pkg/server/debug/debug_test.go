// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package debug_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// debugURL returns the root debug URL.
func debugURL(s serverutils.ApplicationLayerInterface, path string) *serverutils.TestURL {
	return s.AdminURL().WithPath(debug.Endpoint).WithPath(path)
}

// TestAdminDebugExpVar verifies that cmdline and memstats variables are
// available via the /debug/vars link.
func TestAdminDebugExpVar(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSharedProcessModeButDoesntYet(
			base.TestTenantProbabilistic, 113187,
		),
	})
	defer s.Stopper().Stop(context.Background())

	ts := s.ApplicationLayer()

	jI, err := srvtestutils.GetJSON(ts, debugURL(ts, "vars").String())
	if err != nil {
		t.Fatalf("failed to fetch JSON: %v", err)
	}
	j := jI.(map[string]interface{})
	if _, ok := j["cmdline"]; !ok {
		t.Error("cmdline not found in JSON response")
	}
	if _, ok := j["memstats"]; !ok {
		t.Error("memstats not found in JSON response")
	}
}

// TestAdminDebugMetrics verifies that cmdline and memstats variables are
// available via the /debug/metrics link.
func TestAdminDebugMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSharedProcessModeButDoesntYet(
			base.TestTenantProbabilistic, 113187,
		),
	})
	defer s.Stopper().Stop(context.Background())

	ts := s.ApplicationLayer()

	jI, err := srvtestutils.GetJSON(ts, debugURL(ts, "metrics").String())
	if err != nil {
		t.Fatalf("failed to fetch JSON: %v", err)
	}
	j := jI.(map[string]interface{})
	if _, ok := j["cmdline"]; !ok {
		t.Error("cmdline not found in JSON response")
	}
	if _, ok := j["memstats"]; !ok {
		t.Error("memstats not found in JSON response")
	}
}

// TestAdminDebugPprof verifies that pprof tools are available.
// via the /debug/pprof/* links.
func TestAdminDebugPprof(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSharedProcessModeButDoesntYet(
			base.TestTenantProbabilistic, 113187,
		),
	})
	defer s.Stopper().Stop(context.Background())

	ts := s.ApplicationLayer()

	body, err := srvtestutils.GetText(ts, debugURL(ts, "pprof/block?debug=1").String())
	if err != nil {
		t.Fatal(err)
	}
	if exp := "contention:\ncycles/second="; !bytes.Contains(body, []byte(exp)) {
		t.Errorf("expected %s to contain %s", body, exp)
	}
}

// TestAdminDebugTrace verifies that the net/trace endpoints are available via
// /debug/requests.
func TestAdminDebugTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSharedProcessModeButDoesntYet(
			base.TestTenantProbabilistic, 113187,
		),
	})
	defer s.Stopper().Stop(context.Background())

	ts := s.ApplicationLayer()

	tc := []struct {
		segment, search string
	}{
		{"requests", "<title>/debug/requests</title>"},
	}

	for _, c := range tc {
		body, err := srvtestutils.GetText(ts, debugURL(ts, c.segment).String())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(body, []byte(c.search)) {
			t.Errorf("expected %s to be contained in %s", c.search, body)
		}
	}
}

func TestAdminDebugAuth(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

	url := debugURL(ts, "").String()

	// Unauthenticated.
	client, err := ts.GetUnauthenticatedHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status code %d; got %d", http.StatusUnauthorized, resp.StatusCode)
	}

	// Authenticated as non-admin.
	client, err = ts.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status code %d; got %d", http.StatusUnauthorized, resp.StatusCode)
	}

	// Authenticated as admin.
	client, err = ts.GetAuthenticatedHTTPClient(true, serverutils.SingleTenantSession)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status code %d; got %d", http.StatusOK, resp.StatusCode)
	}
}

// TestDebugLSMViz is a smoke test for /debug/lsm-viz/<store>. The endpoint
// reconstructs a view of the engine's LSM via pebble's tool package, which
// goes through test-only helpers (NewVersionForTesting / ResetForTesting)
// that have carried stricter invariants than the production-path organizer
// and have broken silently across versions (#170896, #170897). This test
// catches the "code path is fundamentally broken" failure mode by exercising
// the endpoint end-to-end against a real on-disk store.
func TestDebugLSMViz(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// The on-disk testserver setup and shutdown alone can run for tens of
	// seconds under race instrumentation, which combined with the other tests
	// in this bundle exceeds the 60s race-mode test timeout. The handler
	// itself is fast (~250µs per call) — this is purely a budgeting issue.
	skip.UnderRace(t, "on-disk testserver setup + Stopper.Quiesce exceeds the race-mode test budget")

	tempDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{{Path: tempDir}},
	})
	defer s.Stopper().Stop(context.Background())

	// /debug/lsm-viz/<storeID> is registered on the system tenant's debug
	// server because stores belong to the system tenant.
	ts := s.SystemLayer()

	// Write some data and flush so the LSM has at least one SSTable; the
	// pebble lsm tool errors out if the MANIFEST contains no edits that
	// add or delete files.
	sqlDB := sqlutils.MakeSQLRunner(ts.SQLConn(t))
	sqlDB.Exec(t, "CREATE TABLE t (id INT PRIMARY KEY, v STRING)")
	sqlDB.Exec(t,
		"INSERT INTO t SELECT i, repeat('x', 200) FROM generate_series(1, 1000) AS g(i)")
	require.NoError(t,
		s.StorageLayer().GetStores().(*kvserver.Stores).VisitStores(
			func(store *kvserver.Store) error {
				return store.StateEngine().Flush()
			}))

	client, err := ts.GetAdminHTTPClient()
	require.NoError(t, err)

	url := debugURL(ts, "lsm-viz/1").String()
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equalf(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	bodyStr := string(body)
	// On success, the lsm tool produces an HTML page embedding the LSM as
	// JSON. On error, the handler writes "error analyzing LSM" into the
	// body (with a 200 status, so check the body content rather than the
	// status code).
	require.Containsf(t, bodyStr, "<!DOCTYPE html>",
		"expected HTML response, got: %s", bodyStr)
	require.NotContainsf(t, bodyStr, "error analyzing LSM",
		"handler returned an error: %s", bodyStr)
}

// TestAdminDebugRedirect verifies that the /debug/ endpoint is redirected to on
// incorrect /debug/ paths.
func TestAdminDebugRedirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

	expURL := debugURL(ts, "/")
	// Drops the `?cluster=` query param if present.
	expURL.RawQuery = ""

	origURL := debugURL(ts, "/incorrect")

	// Must be admin to access debug endpoints
	client, err := ts.GetAdminHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		// Don't follow redirects automatically. This error is a special
		// case in the `CheckRedirect` docs that forwards the last response
		// instead of following the redirect.
		return http.ErrUseLastResponse
	}

	resp, err := client.Get(origURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMovedPermanently {
		t.Errorf("expected status code %d; got %d", http.StatusMovedPermanently, resp.StatusCode)
	}
	if redirectURL, err := resp.Location(); err != nil {
		t.Error(err)
	} else if foundURL := redirectURL.String(); foundURL != expURL.String() {
		t.Errorf("expected location %s; got %s", expURL, foundURL)
	}
}
