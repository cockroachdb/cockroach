// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package debug_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/storage/storageconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/goroutines"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

func TestOneMillionGorosProfile(t *testing.T) {
	logScope := log.Scope(t)

	//defer leaktest.AfterTest(t)()
	defer logScope.Close(t)

	ctx := context.Background()

	var h *goroutines.Handle
	opts := goroutines.Options{
		Initial:    10_000,
		Max:        1_200_000, // leave headroom for “doubling” trigger
		SpawnBatch: 20000,
		Name:       "pprof-debug2-stress",
	}

	args := base.TestServerArgs{}
	args.Knobs.Server = &server.TestingKnobs{
		GoroutineMixer:            &opts,
		GoroutineMixerHandle:      &h,
		EnvironmentSampleInterval: 1 * time.Second,
	}
	var myStoreSpec = storageconfig.Store{
		InMemory: true,
		Size:     storageconfig.BytesSize(512 << 20),
		Path:     logScope.GetDirectory(),
	}
	args.StoreSpecs = []base.StoreSpec{myStoreSpec}

	s, db, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)
	defer db.Close()

	require.NotNil(t, h)

	_, err := db.Exec("select 1")
	require.NoError(t, err)

	h.GrowBy(40_000) // total 50_000

	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 50_000 {
			return errors.Errorf("expected 50_000 running goroutines, got %d", h.Running())
		}
		return nil
	})

	h.Double() // triggers internal dumper-on-doubling scenarios

	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 100_000 {
			return errors.Errorf("expected 100_000 running goroutines, got %d", h.Running())
		}
		return nil
	})

	h.Double()

	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 200_000 {
			return errors.Errorf("expected 200_000 running goroutines, got %d", h.Running())
		}
		return nil
	})

	h.GrowBy(50_000) // total 250_000

	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 250_000 {
			return errors.Errorf("expected 250_000 running goroutines, got %d", h.Running())
		}
		return nil
	})

	h.Double()

	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 500_000 {
			return errors.Errorf("expected 500_000 running goroutines, got %d", h.Running())
		}
		return nil
	})

	h.Double()

	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 1_000_000 {
			return errors.Errorf("expected 1_000_000 running goroutines, got %d", h.Running())
		}
		return nil
	})

	time.Sleep(5 * time.Second)

	body, err := srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String())
	require.NoError(t, err)
	fmt.Println(getMetric(body, "sys_go_stop_other_ns"))

	ts := s.ApplicationLayer()
	start := time.Now()
	body, err = getText(ts, debugURL(ts, "pprof/goroutine?debug=3").String())
	require.NoError(t, err)
	fmt.Printf("Profile length: %d, elapsed time: %s\n", len(body), time.Since(start))

	profilePath := filepath.Join(logScope.GetDirectory(), "goroutine_dump_test_profile.txt")
	err = os.WriteFile(profilePath, body, 0644)
	require.NoError(t, err)

	body, err = srvtestutils.GetText(s, s.AdminURL().WithPath(apiconstants.StatusPrefix+"vars").String())
	require.NoError(t, err)
	fmt.Println(getMetric(body, "sys_go_stop_other_ns"))

	t.Fatalf("foo")

	// ... now hit /debug/pprof/goroutine?debug=2 etc ...
}

// grep -E -v "HELP|TYPE" | grep metricName | awk '{print $2}'
func getMetric(body []byte, metricName string) []string {
	lines := strings.Split(string(body), "\n")
	// Equivalent to: grep -E -v "HELP|TYPE" (but only for prometheus comment lines)
	helpType := regexp.MustCompile(`^#\s*(?:HELP|TYPE)\b`)

	out := make([]string, 0)
	for _, line := range lines {
		if helpType.MatchString(line) {
			continue
		}
		if !strings.Contains(line, metricName) {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) >= 2 {
			out = append(out, fields[1])
		}
	}
	return out
}

func getText(ts serverutils.ApplicationLayerInterface, url string) ([]byte, error) {
	httpClient, err := ts.GetAdminHTTPClient()
	if err != nil {
		return nil, err
	}
	httpClient.Timeout = 5 * time.Minute
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
