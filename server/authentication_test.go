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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func doHTTPReq(t *testing.T, client *http.Client, method, url string) (*http.Response, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("%s %s: error building request: %s", method, url, err)
	}

	return client.Do(req)
}

// Verify client certificate enforcement.
func TestSSLEnforcement(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := StartTestServer(t)
	defer s.Stop()

	// HTTPS with client certs.
	certsContext := testutils.NewRootTestBaseContext()
	// HTTPS without client certs.
	noCertsContext := testutils.NewRootTestBaseContext()
	noCertsContext.Certs = ""
	// Plain http.
	insecureContext := testutils.NewRootTestBaseContext()
	insecureContext.Insecure = true

	testCases := []struct {
		method, key string
		ctx         *base.Context
		success     bool // request sent successfully (may be non-200)
		code        int  // http response code
	}{
		// /ui/: basic file server: no auth.
		{"GET", "/index.html", certsContext, true, http.StatusOK},
		{"GET", "/index.html", noCertsContext, true, http.StatusOK},
		{"GET", "/index.html", insecureContext, false, -1},

		// /_admin/: server.adminServer: no auth.
		{"GET", healthPath, certsContext, true, http.StatusOK},
		{"GET", healthPath, noCertsContext, true, http.StatusOK},
		{"GET", healthPath, insecureContext, false, -1},

		// /debug/: server.adminServer: no auth.
		{"GET", debugEndpoint + "vars", certsContext, true, http.StatusOK},
		{"GET", debugEndpoint + "vars", noCertsContext, true, http.StatusOK},
		{"GET", debugEndpoint + "vars", insecureContext, false, -1},

		// /_status/nodes: server.statusServer: no auth.
		{"GET", statusNodeKeyPrefix, certsContext, true, http.StatusOK},
		{"GET", statusNodeKeyPrefix, noCertsContext, true, http.StatusOK},
		{"GET", statusNodeKeyPrefix, insecureContext, false, -1},

		// /ts/: ts.Server: no auth.
		{"GET", ts.URLPrefix, certsContext, true, http.StatusNotFound},
		{"GET", ts.URLPrefix, noCertsContext, true, http.StatusNotFound},
		{"GET", ts.URLPrefix, insecureContext, false, -1},

		// /kv/db/: kv.DBServer. These are proto reqs, but we can at least get past auth.
		{"GET", kv.DBPrefix + "Get", certsContext, true, http.StatusBadRequest},
		{"GET", kv.DBPrefix + "Get", noCertsContext, true, http.StatusUnauthorized},
		{"GET", kv.DBPrefix + "Get", insecureContext, false, -1},

		// /sql/: sql.Server. These are proto reqs, but we can at least get past auth.
		{"GET", driver.Endpoint + "Get", certsContext, true, http.StatusNotFound},
		{"GET", driver.Endpoint + "Get", noCertsContext, true, http.StatusUnauthorized},
		{"GET", driver.Endpoint + "Get", insecureContext, false, -1},
	}

	for tcNum, tc := range testCases {
		client, err := tc.ctx.GetHTTPClient()
		if err != nil {
			t.Fatalf("[%d]: failed to get http client: %v", tcNum, err)
		}
		resp, err := doHTTPReq(t, client, tc.method,
			fmt.Sprintf("%s://%s%s", tc.ctx.RequestScheme(), s.ServingAddr(), tc.key))
		if (err == nil) != tc.success {
			t.Fatalf("[%d]: expected success=%t, got err=%v", tcNum, tc.success, err)
		}
		if err != nil {
			continue
		}

		defer resp.Body.Close()
		if resp.StatusCode != tc.code {
			t.Errorf("[%d]: expected status code %d, got %d", tcNum, tc.code, resp.StatusCode)
		}
	}
}
