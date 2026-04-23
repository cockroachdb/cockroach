// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestNoFallbackParam(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	httpClient, err := s.SystemLayer().GetUnauthenticatedHTTPClient()
	require.NoError(t, err)
	defer httpClient.CloseIdleConnections()

	endpoint := s.SystemLayer().AdminURL().String() + "/health"
	t.Run("requests for a non-existent tenant return an error when fallback is true", func(t *testing.T) {
		resp, err := httpClient.Get(endpoint + "?cluster=doesnotexist&nofallback=true")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, resp.StatusCode, http.StatusServiceUnavailable)
	})
	t.Run("requests for a non-existent tenant fall backs to default tenant when fallback is false", func(t *testing.T) {
		resp, err := httpClient.Get(endpoint + "?cluster=doesnotexist&nofallback=false")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, resp.StatusCode, http.StatusOK)
	})
	t.Run("requests for a non-existent tenant fall backs to default tenant when fallback is not specified", func(t *testing.T) {
		resp, err := httpClient.Get(endpoint + "?cluster=doesnotexist")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, resp.StatusCode, http.StatusOK)
	})
}

// TestClusterQueryParamSetsTenantCookie verifies that requests with
// ?cluster=<name> cause the server to set the tenant select cookie in
// the response. This enables the Single Page App to route subsequent
// requests (which don't carry ?cluster=) to the same tenant.
func TestClusterQueryParamSetsTenantCookie(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	_, _, err := s.TenantController().StartSharedProcessTenant(ctx,
		base.TestSharedProcessTenantArgs{TenantName: "apptenant"},
	)
	require.NoError(t, err)

	httpClient, err := s.SystemLayer().GetUnauthenticatedHTTPClient()
	require.NoError(t, err)
	defer httpClient.CloseIdleConnections()
	// Don't follow redirects so we can inspect response cookies.
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	baseURL := s.SystemLayer().AdminURL().String()

	t.Run("cluster param sets tenant cookie", func(t *testing.T) {
		resp, err := httpClient.Get(baseURL + "/health?cluster=system")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var found bool
		for _, c := range resp.Cookies() {
			if c.Name == authserver.TenantSelectCookieName {
				require.Equal(t, "system", c.Value)
				found = true
			}
		}
		require.True(t, found, "expected tenant cookie to be set")
	})

	t.Run("cluster param sets tenant cookie for non-system tenant", func(t *testing.T) {
		resp, err := httpClient.Get(baseURL + "/health?cluster=apptenant")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var found bool
		for _, c := range resp.Cookies() {
			if c.Name == authserver.TenantSelectCookieName {
				require.Equal(t, "apptenant", c.Value)
				found = true
			}
		}
		require.True(t, found, "expected tenant cookie to be set for non-system tenant")
	})

	t.Run("no cluster param does not set tenant cookie", func(t *testing.T) {
		resp, err := httpClient.Get(baseURL + "/health")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		for _, c := range resp.Cookies() {
			if c.Name == authserver.TenantSelectCookieName {
				t.Fatal("tenant cookie should not be set without cluster param")
			}
		}
	})

	t.Run("cluster param overrides existing tenant cookie", func(t *testing.T) {
		req, err := http.NewRequestWithContext(
			ctx, "GET", baseURL+"/health?cluster=system", nil,
		)
		require.NoError(t, err)
		req.AddCookie(&http.Cookie{Name: authserver.TenantSelectCookieName, Value: "other"})
		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		var found bool
		for _, c := range resp.Cookies() {
			if c.Name == authserver.TenantSelectCookieName {
				require.Equal(t, "system", c.Value)
				found = true
			}
		}
		require.True(t, found, "expected tenant cookie to be updated")
	})

	t.Run("invalid cluster does not set tenant cookie", func(t *testing.T) {
		resp, err := httpClient.Get(baseURL + "/health?cluster=doesnotexist")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		for _, c := range resp.Cookies() {
			if c.Name == authserver.TenantSelectCookieName && c.Value != "" {
				t.Fatalf(
					"tenant cookie should not be set for invalid cluster, got value %q",
					c.Value,
				)
			}
		}
	})
}
