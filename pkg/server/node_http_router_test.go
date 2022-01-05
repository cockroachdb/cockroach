// Copyright 2021 The Cockroach Authors.
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
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRouteToNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 2, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	routesToTest := []struct {
		name                        string
		path                        string
		requireAuth                 bool
		requireAdmin                bool
		sourceServerID              int
		nodeIDRequestedInCookie     string
		nodeIDRequestedInQueryParam string
		expectStatusCode            int
		expectRegex                 *regexp.Regexp
	}{
		{
			name:                    "local _status/vars on node 2",
			path:                    "/_status/vars",
			sourceServerID:          1,
			nodeIDRequestedInCookie: "local",
			expectStatusCode:        200,
			expectRegex:             regexp.MustCompile(`ranges_underreplicated{store="2"}`),
		},
		{
			name:                    "remote _status/vars on node 2 from node 1 using cookie",
			path:                    "/_status/vars",
			sourceServerID:          0,
			nodeIDRequestedInCookie: "2",
			expectStatusCode:        200,
			expectRegex:             regexp.MustCompile(`ranges_underreplicated{store="2"}`),
		},
		{
			name:                    "remote _status/vars on node 1 from node 2 using cookie",
			path:                    "/_status/vars",
			sourceServerID:          1,
			nodeIDRequestedInCookie: "1",
			expectStatusCode:        200,
			expectRegex:             regexp.MustCompile(`ranges_underreplicated{store="1"}`),
		},
		{
			name:                        "remote _status/vars on node 2 from node 1 using query param",
			path:                        "/_status/vars",
			sourceServerID:              0,
			nodeIDRequestedInQueryParam: "2",
			expectStatusCode:            200,
			expectRegex:                 regexp.MustCompile(`ranges_underreplicated{store="2"}`),
		},
		{
			name:                        "query param overrides cookie",
			path:                        "/_status/vars",
			sourceServerID:              0,
			nodeIDRequestedInCookie:     "local",
			nodeIDRequestedInQueryParam: "2",
			expectStatusCode:            200,
			expectRegex:                 regexp.MustCompile(`ranges_underreplicated{store="2"}`),
		},
		{
			name:                    "remote / root HTML on node 2 from node 1 using cookie",
			path:                    "/",
			sourceServerID:          0,
			nodeIDRequestedInCookie: "2",
			expectStatusCode:        200,
			// The root HTTP endpoint returns the "Binary built without web
			// UI" response in tests so it's a bit tricky to look for any more
			// detail here.
			expectRegex: regexp.MustCompile(`<!DOCTYPE html>`),
		},
		{
			name:                    "unathenticated remote request for statements endpoint should fail",
			path:                    "/_status/statements",
			sourceServerID:          0,
			nodeIDRequestedInCookie: "2",
			expectStatusCode:        401,
		},
		{
			name:                    "authenticated remote request for statements endpoint should succeed",
			path:                    "/_status/statements",
			requireAuth:             true,
			requireAdmin:            true,
			sourceServerID:          0,
			nodeIDRequestedInCookie: "2",
			expectStatusCode:        200,
			expectRegex:             regexp.MustCompile(`"statements": \[`),
		},
		{
			name:                    "malformed nodeID returns 400",
			path:                    "/_status/vars",
			sourceServerID:          0,
			nodeIDRequestedInCookie: "bad_node_id",
			expectStatusCode:        400,
		},
		{
			name:                    "unknown nodeID returns 400",
			path:                    "/_status/vars",
			sourceServerID:          0,
			nodeIDRequestedInCookie: "34",
			expectStatusCode:        400,
		},
	}

	for _, rt := range routesToTest {
		t.Run(rt.name, func(t *testing.T) {
			s := tc.Server(rt.sourceServerID)
			require.Equal(t, rt.sourceServerID+1, int(s.NodeID()))
			client, err := s.GetHTTPClient()
			if rt.requireAuth {
				client, err = s.GetAuthenticatedHTTPClient(rt.requireAdmin)
			}
			require.NoError(t, err)

			// Authenticated client will already have the Jar created.
			if client.Jar == nil {
				client.Jar, err = cookiejar.New(&cookiejar.Options{})
				require.NoError(t, err)
			}
			adminURL, err := url.Parse(s.AdminURL())
			require.NoError(t, err)
			client.Jar.SetCookies(adminURL, []*http.Cookie{{Name: RemoteNodeID, Value: rt.nodeIDRequestedInCookie}})

			requestPath := s.AdminURL() + rt.path
			if rt.nodeIDRequestedInQueryParam != "" {
				requestPath += fmt.Sprintf("?%s=%s", RemoteNodeID, rt.nodeIDRequestedInQueryParam)
			}
			resp, err := client.Get(requestPath)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, rt.expectStatusCode, resp.StatusCode)
			if rt.expectStatusCode >= 400 && rt.expectStatusCode != 401 {
				// We should be resetting the cookie on all errors to prevent
				// the user from getting stuck. Unauthorized errors are
				// omitted because the user can generally take action there
				// and log in.
				require.Equal(t, resp.Cookies()[0].Name, RemoteNodeID)
				require.Equal(t, resp.Cookies()[0].Value, "")
			}
			bodyBytes, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			if rt.expectRegex != nil {
				require.Regexp(t, rt.expectRegex, string(bodyBytes))
			}
		})
	}

	t.Run("route to shutdown node should fail with error status", func(t *testing.T) {
		tc.Server(1).Stopper().Stop(context.Background())
		s := tc.Server(0)
		client, err := s.GetHTTPClient()
		require.NoError(t, err)

		resp, err := client.Get(s.AdminURL() + fmt.Sprintf("/_status/vars?%s=%s", RemoteNodeID, "2"))
		require.NoError(t, err)
		// We expect some error here. It's difficult to know what
		// will happen because in different stress scenarios, the
		// node may still have an address available, in which
		// case we get a 502, or a 400 if the ID can't be resolved.
		require.Greater(t, resp.StatusCode, 399)
	})
}
