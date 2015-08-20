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
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	gogoproto "github.com/gogo/protobuf/proto"
)

func doHTTPReq(t *testing.T, client *http.Client, method, url string, body gogoproto.Message) (*http.Response, error) {
	var b io.Reader
	if body != nil {
		buf, err := gogoproto.Marshal(body)
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
		req.Header.Add(util.ContentTypeHeader, util.ProtoContentType)
	}

	return client.Do(req)
}

func kvGetForUser(context *base.Context) gogoproto.Message {
	ret := &proto.GetRequest{}
	ret.User = context.User
	ret.Key = proto.Key("/")
	return ret
}

func sqlForUser(context *base.Context) gogoproto.Message {
	ret := &driver.Request{}
	ret.User = context.User
	return ret
}

// Verify client certificate enforcement and user whitelisting.
func TestSSLEnforcement(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := StartTestServer(t)
	defer s.Stop()

	// HTTPS with client certs for "root".
	certsContext := testutils.NewRootTestBaseContext()
	// HTTPS with client certs for testuser.
	testCertsContext := testutils.NewRootTestBaseContext()
	testCertsContext.User = TestUser
	// HTTPS without client certs.
	noCertsContext := testutils.NewRootTestBaseContext()
	noCertsContext.Certs = ""
	// Plain http.
	insecureContext := testutils.NewRootTestBaseContext()
	insecureContext.Insecure = true

	testCases := []struct {
		method, key string
		body        gogoproto.Message
		ctx         *base.Context
		success     bool // request sent successfully (may be non-200)
		code        int  // http response code
	}{
		// /ui/: basic file server: no auth.
		{"GET", "/index.html", nil, certsContext, true, http.StatusOK},
		{"GET", "/index.html", nil, testCertsContext, true, http.StatusOK},
		{"GET", "/index.html", nil, noCertsContext, true, http.StatusOK},
		{"GET", "/index.html", nil, insecureContext, false, -1},

		// /_admin/: server.adminServer: no auth.
		{"GET", healthPath, nil, certsContext, true, http.StatusOK},
		{"GET", healthPath, nil, testCertsContext, true, http.StatusOK},
		{"GET", healthPath, nil, noCertsContext, true, http.StatusOK},
		{"GET", healthPath, nil, insecureContext, false, -1},

		// /debug/: server.adminServer: no auth.
		{"GET", debugEndpoint + "vars", nil, certsContext, true, http.StatusOK},
		{"GET", debugEndpoint + "vars", nil, testCertsContext, true, http.StatusOK},
		{"GET", debugEndpoint + "vars", nil, noCertsContext, true, http.StatusOK},
		{"GET", debugEndpoint + "vars", nil, insecureContext, false, -1},

		// /_status/nodes: server.statusServer: no auth.
		{"GET", statusNodesPrefix, nil, certsContext, true, http.StatusOK},
		{"GET", statusNodesPrefix, nil, testCertsContext, true, http.StatusOK},
		{"GET", statusNodesPrefix, nil, noCertsContext, true, http.StatusOK},
		{"GET", statusNodesPrefix, nil, insecureContext, false, -1},

		// /ts/: ts.Server: no auth.
		{"GET", ts.URLPrefix, nil, certsContext, true, http.StatusNotFound},
		{"GET", ts.URLPrefix, nil, testCertsContext, true, http.StatusNotFound},
		{"GET", ts.URLPrefix, nil, noCertsContext, true, http.StatusNotFound},
		{"GET", ts.URLPrefix, nil, insecureContext, false, -1},

		// /kv/db/: kv.DBServer. These are proto reqs. The important field is header.User.
		{"POST", kv.DBPrefix + "Get", kvGetForUser(certsContext), certsContext,
			true, http.StatusOK},
		{"POST", kv.DBPrefix + "Get", kvGetForUser(testCertsContext), testCertsContext,
			true, http.StatusUnauthorized},
		{"POST", kv.DBPrefix + "Get", kvGetForUser(noCertsContext), noCertsContext,
			true, http.StatusUnauthorized},
		{"POST", kv.DBPrefix + "Get", kvGetForUser(insecureContext), insecureContext, false, -1},

		// /sql/: sql.Server. These are proto reqs. The important field is header.User.
		{"POST", driver.Endpoint + driver.Execute.String(), sqlForUser(certsContext),
			certsContext, true, http.StatusOK},
		{"POST", driver.Endpoint + driver.Execute.String(), sqlForUser(testCertsContext),
			testCertsContext, true, http.StatusOK},
		{"POST", driver.Endpoint + driver.Execute.String(), sqlForUser(noCertsContext),
			noCertsContext, true, http.StatusUnauthorized},
		{"POST", driver.Endpoint + driver.Execute.String(), sqlForUser(insecureContext),
			insecureContext, false, -1},
	}

	for tcNum, tc := range testCases {
		client, err := tc.ctx.GetHTTPClient()
		if err != nil {
			t.Fatalf("[%d]: failed to get http client: %v", tcNum, err)
		}
		resp, err := doHTTPReq(t, client, tc.method,
			fmt.Sprintf("%s://%s%s", tc.ctx.RequestScheme(), s.ServingAddr(), tc.key),
			tc.body)
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
