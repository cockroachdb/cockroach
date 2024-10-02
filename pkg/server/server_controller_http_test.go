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
