// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// startStatusServer launches a new status server using minimal engine
// and local database setup. Returns the new http test server, which
// should be cleaned up by caller via httptest.Server.Close(). The
// Cockroach KV client address is set to the address of the test server.
func startStatusServer() (*httptest.Server, *util.Stopper) {
	stopper := util.NewStopper()
	db, err := BootstrapCluster("cluster-1", engine.NewInMem(proto.Attributes{}, 1<<20), stopper)
	if err != nil {
		log.Fatal(err)
	}
	status := newStatusServer(db, nil)
	mux := http.NewServeMux()
	status.registerHandlers(mux)
	httpServer := httptest.NewServer(mux)
	stopper.AddCloser(httpServer)
	return httpServer, stopper
}

// TestStatusLocalStacks verifies that goroutine stack traces are available
// via the /_status/local/stacks endpoint.
func TestStatusLocalStacks(t *testing.T) {
	s, stopper := startStatusServer()
	defer stopper.Stop()
	body, err := getText(s.URL + statusLocalStacksKey)
	if err != nil {
		t.Fatal(err)
	}
	// Verify match with at least two goroutine stacks.
	if matches, err := regexp.Match("(?s)goroutine [0-9]+.*goroutine [0-9]+.*", body); !matches || err != nil {
		t.Errorf("expected match: %t; err nil: %s", matches, err)
	}
}

// TestStatusJson verifies that status endpoints return expected
// Json results. The content type of the responses is always
// "application/json".
func TestStatusJson(t *testing.T) {
	s, stopper := startStatusServer()
	defer stopper.Stop()

	type TestCase struct {
		keyPrefix string
		expected  string
	}

	testCases := []TestCase{
		{statusKeyPrefix, "{}"},
		{statusNodesKeyPrefix, "\"nodes\": null"},
	}
	// Test the /_status/local/stacks endpoint only in a go release branch.
	if !strings.HasPrefix(runtime.Version(), "devel") {
		testCases = append(testCases, TestCase{statusLocalKeyPrefix, `{
  "buildInfo": {
    "goVersion": "go[0-9\.]+",
    "tag": "",
    "time": "",
    "dependencies": ""
  }
}`})
	}

	for _, spec := range testCases {
		contentTypes := []string{"application/json", "application/x-protobuf", "text/yaml"}
		for _, contentType := range contentTypes {
			req, err := http.NewRequest("GET", s.URL+spec.keyPrefix, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Accept", contentType)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("Unpexected status code: %v", resp.StatusCode)
			}
			contentType = resp.Header.Get(util.ContentTypeHeader)
			if contentType != "application/json" {
				t.Errorf("Unpexected content type: %v", contentType)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if matches, err := regexp.Match(spec.expected, body); !matches || err != nil {
				t.Errorf("expected match: %t; err nil: %s", matches, err)
			}
		}
	}
}
