package server

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
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

	httpClient, err := s.GetHTTPClient()
	require.NoError(t, err)
	defer httpClient.CloseIdleConnections()

	secureClient, err := s.GetAuthenticatedHTTPClient(false)
	require.NoError(t, err)
	defer secureClient.CloseIdleConnections()

	urlsToTest := []string{"/", "/_status/vars", "/index.html"}

	adminURLHTTPS := s.AdminURL()
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

	_, err = db.Exec("SET cluster setting security.hsts.enabled = true")
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
	_, err = db.Exec("SET cluster setting security.hsts.enabled = false")
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

func TestRedirectWhenForwardedHTTPS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	httpClient, err := s.GetHTTPClient()
	require.NoError(t, err)
	defer httpClient.CloseIdleConnections()

	secureClient, err := s.GetAuthenticatedHTTPClient(false)
	require.NoError(t, err)
	defer secureClient.CloseIdleConnections()

	urlsToTest := []string{"/", "/_status/vars", "/index.html"}

	adminURLHTTPS := s.AdminURL()
	adminURLHTTP := strings.Replace(adminURLHTTPS, "https", "http", 1)

	for _, u := range urlsToTest {
		req, err := http.NewRequest("GET", adminURLHTTP+u, nil)
		require.NoError(t, err)
		req.Header.Set("X-Forwarded-Proto", "https")

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, resp.StatusCode, 307)

		resp, err = secureClient.Get(adminURLHTTPS + u)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, resp.StatusCode, 200)
	}

	_, err = db.Exec("SET cluster setting server.accept_https_forwarded.enabled = true")
	require.NoError(t, err)

	for _, u := range urlsToTest {
		req, err := http.NewRequest("GET", adminURLHTTP+u, nil)
		require.NoError(t, err)
		req.Header.Set("X-Forwarded-For", "https")

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, resp.StatusCode, 200)

		resp, err = secureClient.Get(adminURLHTTPS + u)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, resp.StatusCode, 200)
	}
	_, err = db.Exec("SET cluster setting server.accept_https_forwarded.enabled = false")
	require.NoError(t, err)

	for _, u := range urlsToTest {
		req, err := http.NewRequest("GET", adminURLHTTP+u, nil)
		require.NoError(t, err)
		req.Header.Set("X-Forwarded-Proto", "https")

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, resp.StatusCode, 307)

		resp, err = secureClient.Get(adminURLHTTPS + u)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, resp.StatusCode, 200)
	}
}

func Test_getForwardedProtocol(t *testing.T) {
	tests := []struct {
		name string
		args http.Header
		want string
	}{
		{
			"empty header",
			http.Header{},
			"",
		},
		{
			"x-forwarded-proto header contains https",
			http.Header{
				"x-forwarded-proto": []string{"https"},
			},
			"https",
		},
		{
			"x-forwarded-proto header is empty",
			http.Header{
				"x-forwarded-proto": []string{""},
			},
			"",
		},
		{
			"forwarded header contains proto",
			http.Header{
				"forwarded": []string{"for=192.0.2.60;proto=https;by=203.0.113.43"},
			},
			"https",
		},
		{
			"forwarded header contains proto at head",
			http.Header{
				"forwarded": []string{"proto=https;by=203.0.113.43"},
			},
			"https",
		},
		{
			"forwarded header contains proto at tail",
			http.Header{
				"forwarded": []string{"for=192.0.2.60;by=203.0.113.43;proto=https"},
			},
			"https",
		},
		{
			"forwarded header contains proto only",
			http.Header{
				"forwarded": []string{"proto=https"},
			},
			"https",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getForwardedProtocol(tt.args.h); got != tt.want {
				t.Errorf("getForwardedProtocol() = %v, want %v", got, tt.want)
			}
		})
	}
}
