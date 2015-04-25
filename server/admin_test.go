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
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// startAdminServer launches a new admin server using minimal engine
// and local database setup. Returns the new http test server, which
// should be cleaned up by caller via httptest.Server.Close(). The
// Cockroach KV client address is set to the address of the test server.
func startAdminServer() (string, *util.Stopper) {
	stopper := util.NewStopper()
	db, err := BootstrapCluster("cluster-1", engine.NewInMem(proto.Attributes{}, 1<<20), testContext, stopper)
	if err != nil {
		log.Fatal(err)
	}
	admin := newAdminServer(db, stopper)
	mux := http.NewServeMux()
	admin.registerHandlers(mux)
	httpServer := httptest.NewTLSServer(mux)
	stopper.AddCloser(httpServer)

	if strings.HasPrefix(httpServer.URL, "http://") {
		testContext.Addr = strings.TrimPrefix(httpServer.URL, "http://")
	} else if strings.HasPrefix(httpServer.URL, "https://") {
		testContext.Addr = strings.TrimPrefix(httpServer.URL, "https://")
	}
	return httpServer.URL, stopper
}

// getText fetches the HTTP response body as text in the form of a
// byte slice from the specified URL.
func getText(url string) ([]byte, error) {
	client, err := testutils.NewTestHTTPClient()
	if err != nil {
		return nil, err
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

// getJSON fetches the JSON from the specified URL and returns
// it as unmarshaled JSON. Returns an error on any failure to fetch
// or unmarshal response body.
func getJSON(url string) (interface{}, error) {
	body, err := getText(url)
	if err != nil {
		return nil, err
	}
	var jI interface{}
	if err := json.Unmarshal(body, &jI); err != nil {
		return nil, err
	}
	return jI, nil
}

// TestAdminDebugExpVar verifies that cmdline and memstats variables are
// available via the /debug/vars link.
func TestAdminDebugExpVar(t *testing.T) {
	url, stopper := startAdminServer()
	defer stopper.Stop()

	jI, err := getJSON(url + debugEndpoint + "vars")
	if err != nil {
		t.Fatalf("failed to fetch JSON: %v", err)
	}
	j := jI.(map[string]interface{})
	if _, ok := j["cmdline"]; !ok {
		t.Error("cmdline not found in JSON response")
	}
	if _, ok := j["memstats"]; !ok {
		t.Error("memstats not found in JSON response")
	}
}

// TestAdminDebugPprof verifies that pprof tools are available.
// via the /debug/pprof/* links.
func TestAdminDebugPprof(t *testing.T) {
	url, stopper := startAdminServer()
	defer stopper.Stop()

	body, err := getText(url + debugEndpoint + "pprof/block")
	if err != nil {
		t.Fatal(err)
	}
	if matches, err := regexp.MatchString(".*contention:\ncycles/second=.*", string(body)); !matches || err != nil {
		t.Errorf("expected match: %t; err nil: %v", matches, err)
	}
}
