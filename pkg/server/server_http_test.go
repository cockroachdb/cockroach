// Copyright 2022 The Cockroach Authors.
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
