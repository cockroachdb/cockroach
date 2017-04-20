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
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/protobuf/proto"
)

func doHTTPReq(
	t *testing.T, client http.Client, method, url string, body proto.Message,
) (*http.Response, error) {
	var b io.Reader
	if body != nil {
		buf, err := protoutil.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		b = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, url, b)
	if err != nil {
		t.Fatalf("%s %s: error building request: %s", method, url, err)
	}
	if b != nil {
		req.Header.Add(httputil.ContentTypeHeader, httputil.ProtoContentType)
	}

	return client.Do(req)
}

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

	testCases := []struct {
		method, path string
		body         proto.Message
		ctx          ctxI
		success      bool // request sent successfully (may be non-200)
		code         int  // http response code
	}{
		// /ui/: basic file server: no auth.
		{"GET", "", nil, rootCertsContext, true, http.StatusOK},
		{"GET", "", nil, nodeCertsContext, true, http.StatusOK},
		{"GET", "", nil, testCertsContext, true, http.StatusOK},
		{"GET", "", nil, noCertsContext, true, http.StatusOK},
		{"GET", "", nil, insecureContext, true, http.StatusPermanentRedirect},

		// /_admin/: server.adminServer: no auth.
		{"GET", adminPrefix + "health", nil, rootCertsContext, true, http.StatusOK},
		{"GET", adminPrefix + "health", nil, nodeCertsContext, true, http.StatusOK},
		{"GET", adminPrefix + "health", nil, testCertsContext, true, http.StatusOK},
		{"GET", adminPrefix + "health", nil, noCertsContext, true, http.StatusOK},
		{"GET", adminPrefix + "health", nil, insecureContext, true, http.StatusPermanentRedirect},

		// /debug/: server.adminServer: no auth.
		{"GET", debugEndpoint + "vars", nil, rootCertsContext, true, http.StatusOK},
		{"GET", debugEndpoint + "vars", nil, nodeCertsContext, true, http.StatusOK},
		{"GET", debugEndpoint + "vars", nil, testCertsContext, true, http.StatusOK},
		{"GET", debugEndpoint + "vars", nil, noCertsContext, true, http.StatusOK},
		{"GET", debugEndpoint + "vars", nil, insecureContext, true, http.StatusPermanentRedirect},

		// /_status/nodes: server.statusServer: no auth.
		{"GET", statusPrefix + "nodes", nil, rootCertsContext, true, http.StatusOK},
		{"GET", statusPrefix + "nodes", nil, nodeCertsContext, true, http.StatusOK},
		{"GET", statusPrefix + "nodes", nil, testCertsContext, true, http.StatusOK},
		{"GET", statusPrefix + "nodes", nil, noCertsContext, true, http.StatusOK},
		{"GET", statusPrefix + "nodes", nil, insecureContext, true, http.StatusPermanentRedirect},

		// /ts/: ts.Server: no auth.
		{"GET", ts.URLPrefix, nil, rootCertsContext, true, http.StatusNotFound},
		{"GET", ts.URLPrefix, nil, nodeCertsContext, true, http.StatusNotFound},
		{"GET", ts.URLPrefix, nil, testCertsContext, true, http.StatusNotFound},
		{"GET", ts.URLPrefix, nil, noCertsContext, true, http.StatusNotFound},
		{"GET", ts.URLPrefix, nil, insecureContext, true, http.StatusPermanentRedirect},
	}

	for tcNum, tc := range testCases {
		client, err := tc.ctx.GetHTTPClient()
		if err != nil {
			t.Fatalf("[%d]: failed to get http client: %v", tcNum, err)
		}
		// Avoid automatically following redirects.
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		}
		url := fmt.Sprintf(
			"%s://%s%s", tc.ctx.HTTPRequestScheme(),
			s.(*TestServer).Cfg.HTTPAddr, tc.path)
		resp, err := doHTTPReq(t, client, tc.method, url, tc.body)
		if (err == nil) != tc.success {
			t.Errorf("[%d]: expected success=%t, got err=%v", tcNum, tc.success, err)
		}
		if err != nil {
			continue
		}

		defer resp.Body.Close()
		if resp.StatusCode != tc.code {
			t.Errorf("[%d]: expected status code %d, got %d", tcNum, tc.code, resp.StatusCode)
			u, _ := resp.Location()
			t.Errorf("orig=%s url=%s", tc.path, u)
		}
	}
}
