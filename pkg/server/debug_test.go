// Copyright 2017 The Cockroach Authors.
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

package server

import (
	"net/http"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDebugRemote(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test servers listen on a local address only by default. Listen on :0 to
	// force listening on a non-local address. We can't use certs because the
	// test certs are only valid for localhost.
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{HTTPAddr: ":0", Insecure: true})
	defer s.Stopper().Stop()
	ts := s.(*TestServer)

	httpClient, err := ts.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	// Be a good citizen and cleanup after ourselves.
	defer func() {
		if err := os.Unsetenv("COCKROACH_REMOTE_DEBUG"); err != nil {
			t.Fatal(err)
		}
	}()

	testCases := []struct {
		remoteDebug string
		status      int
	}{
		{"any", http.StatusOK},
		{"TRUE", http.StatusOK},
		{"t", http.StatusOK},
		{"1", http.StatusOK},
		{"local", http.StatusUnauthorized},
		{"false", http.StatusUnauthorized},
		{"unrecognized", http.StatusUnauthorized},
	}
	for _, c := range testCases {
		envutil.ClearEnvCache()
		if err := os.Setenv("COCKROACH_REMOTE_DEBUG", c.remoteDebug); err != nil {
			t.Fatal(err)
		}

		resp, err := httpClient.Get(ts.AdminURL() + debugEndpoint)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()

		if c.status != resp.StatusCode {
			t.Fatalf("expected %d, but got %d", c.status, resp.StatusCode)
		}
	}
}
