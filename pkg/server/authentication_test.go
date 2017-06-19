// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package server

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type ctxI interface {
	GetHTTPClient() (http.Client, error)
	HTTPRequestScheme() string
}

var _ ctxI = insecureCtx{}
var _ ctxI = (*base.Config)(nil)

type insecureCtx struct{}

func (insecureCtx) GetHTTPClient() (http.Client, error) {
	return http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}, nil
}

func (insecureCtx) HTTPRequestScheme() string {
	return "https"
}

// Verify client certificate enforcement and user whitelisting.
func TestSSLEnforcement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// HTTPS with client certs for security.RootUser.
	rootCertsContext := testutils.NewTestBaseContext(security.RootUser)
	// HTTPS with client certs for security.NodeUser.
	nodeCertsContext := testutils.NewNodeTestBaseContext()
	// HTTPS with client certs for TestUser.
	testCertsContext := testutils.NewTestBaseContext(TestUser)
	// HTTPS without client certs. The user does not matter.
	noCertsContext := insecureCtx{}
	// Plain http.
	insecureContext := testutils.NewTestBaseContext(TestUser)
	insecureContext.Insecure = true

	kvGet := &roachpb.GetRequest{}
	kvGet.Key = roachpb.Key("/")

	for _, tc := range []struct {
		path string
		ctx  ctxI
		code int // http response code
	}{
		// /ui/: basic file server: no auth.
		{"", rootCertsContext, http.StatusOK},
		{"", nodeCertsContext, http.StatusOK},
		{"", testCertsContext, http.StatusOK},
		{"", noCertsContext, http.StatusOK},
		{"", insecureContext, http.StatusPermanentRedirect},

		// /_admin/: server.adminServer: no auth.
		{adminPrefix + "health", rootCertsContext, http.StatusOK},
		{adminPrefix + "health", nodeCertsContext, http.StatusOK},
		{adminPrefix + "health", testCertsContext, http.StatusOK},
		{adminPrefix + "health", noCertsContext, http.StatusOK},
		{adminPrefix + "health", insecureContext, http.StatusPermanentRedirect},

		// /debug/: server.adminServer: no auth.
		{debugEndpoint + "vars", rootCertsContext, http.StatusOK},
		{debugEndpoint + "vars", nodeCertsContext, http.StatusOK},
		{debugEndpoint + "vars", testCertsContext, http.StatusOK},
		{debugEndpoint + "vars", noCertsContext, http.StatusOK},
		{debugEndpoint + "vars", insecureContext, http.StatusPermanentRedirect},

		// /_status/nodes: server.statusServer: no auth.
		{statusPrefix + "nodes", rootCertsContext, http.StatusOK},
		{statusPrefix + "nodes", nodeCertsContext, http.StatusOK},
		{statusPrefix + "nodes", testCertsContext, http.StatusOK},
		{statusPrefix + "nodes", noCertsContext, http.StatusOK},
		{statusPrefix + "nodes", insecureContext, http.StatusPermanentRedirect},

		// /ts/: ts.Server: no auth.
		{ts.URLPrefix, rootCertsContext, http.StatusNotFound},
		{ts.URLPrefix, nodeCertsContext, http.StatusNotFound},
		{ts.URLPrefix, testCertsContext, http.StatusNotFound},
		{ts.URLPrefix, noCertsContext, http.StatusNotFound},
		{ts.URLPrefix, insecureContext, http.StatusPermanentRedirect},
	} {
		t.Run("", func(t *testing.T) {
			client, err := tc.ctx.GetHTTPClient()
			if err != nil {
				t.Fatal(err)
			}
			// Avoid automatically following redirects.
			client.CheckRedirect = func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			}
			url := url.URL{
				Scheme: tc.ctx.HTTPRequestScheme(),
				Host:   s.(*TestServer).Cfg.HTTPAddr,
				Path:   tc.path,
			}
			resp, err := client.Get(url.String())
			if err != nil {
				t.Fatal(err)
			}

			defer resp.Body.Close()
			if resp.StatusCode != tc.code {
				t.Errorf("expected status code %d, got %d", tc.code, resp.StatusCode)
				u, err := resp.Location()
				t.Errorf("orig=%s url=%s err=%v", tc.path, u, err)
			}
		})
	}
}
