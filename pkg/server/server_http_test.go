// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestHSTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	httpClient, err := s.GetUnauthenticatedHTTPClient()
	require.NoError(t, err)
	defer httpClient.CloseIdleConnections()

	secureClient, err := s.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession)
	require.NoError(t, err)
	defer secureClient.CloseIdleConnections()

	urlsToTest := []string{"/", "/_status/vars", "/index.html"}

	adminURLHTTPS := s.AdminURL().String()
	adminURLHTTP := strings.Replace(adminURLHTTPS, "https", "http", 1)

	for _, u := range urlsToTest {
		resp, err := httpClient.Get(adminURLHTTP + u)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Empty(t, resp.Header.Get(hstsHeaderKey))

		resp, err = secureClient.Get(adminURLHTTPS + u)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Empty(t, resp.Header.Get(hstsHeaderKey))
	}

	_, err = db.Exec("SET cluster setting server.hsts.enabled = true")
	require.NoError(t, err)

	for _, u := range urlsToTest {
		resp, err := httpClient.Get(adminURLHTTP + u)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, resp.Header.Get(hstsHeaderKey), hstsHeaderValue)

		resp, err = secureClient.Get(adminURLHTTPS + u)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, resp.Header.Get(hstsHeaderKey), hstsHeaderValue)
	}
	_, err = db.Exec("SET cluster setting server.hsts.enabled = false")
	require.NoError(t, err)

	for _, u := range urlsToTest {
		resp, err := httpClient.Get(adminURLHTTP + u)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Empty(t, resp.Header.Get(hstsHeaderKey))

		resp, err = secureClient.Get(adminURLHTTPS + u)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Empty(t, resp.Header.Get(hstsHeaderKey))
	}
}

func TestVirtualClustersSingleTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	httpClient, err := s.GetUnauthenticatedHTTPClient()
	require.NoError(t, err)
	defer httpClient.CloseIdleConnections()

	secureClient, err := s.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession)
	require.NoError(t, err)
	defer secureClient.CloseIdleConnections()

	adminURLHTTPS := s.AdminURL().String()
	adminURLHTTP := strings.Replace(adminURLHTTPS, "https", "http", 1)

	resp, err := httpClient.Get(adminURLHTTP + "/virtual_clusters")
	require.NoError(t, err)
	defer resp.Body.Close()
	read, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "application/json", resp.Header.Get("content-type"))
	require.Equal(t, `{"virtual_clusters":[]}`, string(read))

	resp, err = secureClient.Get(adminURLHTTPS + "/virtual_clusters")
	require.NoError(t, err)
	defer resp.Body.Close()
	read, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "application/json", resp.Header.Get("content-type"))
	require.Equal(t, `{"virtual_clusters":[]}`, string(read))
}

func TestVirtualClustersMultiTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.SharedTestTenantAlwaysEnabled,
	})
	defer s.Stopper().Stop(ctx)
	appServer := s.ApplicationLayer()

	httpClient, err := appServer.GetUnauthenticatedHTTPClient()
	require.NoError(t, err)
	defer httpClient.CloseIdleConnections()

	secureClient, err := appServer.GetAuthenticatedHTTPClient(false, serverutils.MultiTenantSession)
	require.NoError(t, err)
	defer secureClient.CloseIdleConnections()

	adminURLHTTPS := appServer.AdminURL()
	adminURLHTTPS.Scheme = "http"
	adminURLHTTPS.Path = adminURLHTTPS.Path + "/virtual_clusters"

	resp, err := httpClient.Get(adminURLHTTPS.String())
	require.NoError(t, err)
	defer resp.Body.Close()
	read, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "application/json", resp.Header.Get("content-type"))
	require.Equal(t, `{"virtual_clusters":[]}`, string(read))

	adminURLHTTPS.Scheme = "https"
	resp, err = secureClient.Get(adminURLHTTPS.String())
	require.NoError(t, err)
	defer resp.Body.Close()
	read, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "application/json", resp.Header.Get("content-type"))
	require.Equal(t, `{"virtual_clusters":["test-tenant"]}`, string(read))

	secureClient.Jar.SetCookies(adminURLHTTPS.URL, []*http.Cookie{
		{
			Name: "session",
			Value: authserver.CreateAggregatedSessionCookieValue([]authserver.SessionCookieValue{
				authserver.MakeSessionCookieValue("system", "session=abcd1234"),
				authserver.MakeSessionCookieValue("app", "session=efgh5678"),
			}),
		},
	})

	adminURLHTTPS.Scheme = "https"
	resp, err = secureClient.Get(adminURLHTTPS.String())
	require.NoError(t, err)
	defer resp.Body.Close()
	read, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "application/json", resp.Header.Get("content-type"))
	require.Equal(t, `{"virtual_clusters":["system","app"]}`, string(read))
}
