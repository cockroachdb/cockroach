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
			// The root HTTP endpoint returns the "Binary built without web UI"
			// response in tests so it's a bit tricky to look for anymore detail here
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
			client.Jar.SetCookies(adminURL, []*http.Cookie{{Name: NodeRoutingIDKey, Value: rt.nodeIDRequestedInCookie}})

			requestPath := s.AdminURL() + rt.path
			if rt.nodeIDRequestedInQueryParam != "" {
				requestPath += fmt.Sprintf("?%s=%s", NodeRoutingIDKey, rt.nodeIDRequestedInQueryParam)
			}
			resp, err := client.Get(requestPath)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, rt.expectStatusCode, resp.StatusCode)
			bodyBytes, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			if rt.expectRegex != nil {
				require.Regexp(t, rt.expectRegex, string(bodyBytes))
			}
		})
	}
}
