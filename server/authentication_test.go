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

	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/ts"
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
	s := StartTestServer(t)
	defer s.Stop()
	testCases := []struct {
		method, key   string
		certsStatus   int // Status code for https with client certs.
		noCertsStatus int // Status code for https without client certs.
	}{
		// /ui/: basic file server: no auth.
		{"GET", "/index.html", http.StatusOK, http.StatusOK},

		// /_admin/: server.adminServer: no auth.
		{"GET", healthPath, http.StatusOK, http.StatusOK},

		// /debug/: server.adminServer: no auth.
		{"GET", debugEndpoint + "vars", http.StatusOK, http.StatusOK},

		// /_status/: server.statusServer: no auth.
		{"GET", statusKeyPrefix, http.StatusOK, http.StatusOK},

		// /kv/rest/: kv.RESTServer. Key cannot be empty. Response is NotFound since the key does not exist.
		{"GET", kv.EntryPrefix + "foo", http.StatusNotFound, http.StatusUnauthorized},

		// /kv/db/: kv.DBServer. These are proto reqs, but we can at least get past auth.
		{"GET", kv.DBPrefix + "Get", http.StatusBadRequest, http.StatusUnauthorized},

		// /ts/: ts.Server.
		{"GET", ts.URLPrefix, http.StatusNotFound, http.StatusUnauthorized},
	}

	// HTTPS with client certs.
	certsContext := testutils.NewTestBaseContext()
	client, err := certsContext.GetHTTPClient()
	if err != nil {
		t.Fatalf("error initializing http client: %s", err)
	}
	for tcNum, tc := range testCases {
		resp, err := doHTTPReq(t, client, tc.method,
			fmt.Sprintf("%s://%s%s", certsContext.RequestScheme(), s.ServingAddr(), tc.key))
		if err != nil {
			t.Fatalf("[%d]: error issuing request: %s", tcNum, err)
		}

		defer resp.Body.Close()
		if resp.StatusCode != tc.certsStatus {
			t.Errorf("[%d]: expected status code %d, got %d", tcNum, tc.certsStatus, resp.StatusCode)
		}
	}

	// HTTPS without client certs.
	noCertsContext := testutils.NewTestBaseContext()
	noCertsContext.Certs = ""
	client, err = noCertsContext.GetHTTPClient()
	if err != nil {
		t.Fatalf("error initializing http client: %s", err)
	}
	for tcNum, tc := range testCases {
		resp, err := doHTTPReq(t, client, tc.method,
			fmt.Sprintf("%s://%s%s", noCertsContext.RequestScheme(), s.ServingAddr(), tc.key))
		if err != nil {
			t.Fatalf("[%d]: error issuing request: %s", tcNum, err)
		}

		defer resp.Body.Close()
		if resp.StatusCode != tc.noCertsStatus {
			t.Errorf("[%d]: expected status code %d, got %d", tcNum, tc.noCertsStatus, resp.StatusCode)
		}
	}

	// Plain http.
	insecureContext := testutils.NewTestBaseContext()
	insecureContext.Insecure = true
	client, err = insecureContext.GetHTTPClient()
	if err != nil {
		t.Fatalf("error initializing http client: %s", err)
	}
	for tcNum, tc := range testCases {
		resp, err := doHTTPReq(t, client, tc.method,
			fmt.Sprintf("%s://%s%s", insecureContext.RequestScheme(), s.ServingAddr(), tc.key))
		// We're talking http to a https server. We don't even make it to a response.
		if err == nil {
			defer resp.Body.Close()
			t.Errorf("[%d]: unexpected success", tcNum)
		}
	}
}
