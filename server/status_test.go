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
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/log"
)

// startStatusServer launches a new status server using minimal engine
// and local database setup. Returns the new http test server, which
// should be cleaned up by caller via httptest.Server.Close(). The
// Cockroach KV client address is set to the address of the test server.
func startStatusServer() *httptest.Server {
	db, err := BootstrapCluster("cluster-1", engine.NewInMem(proto.Attributes{}, 1<<20))
	if err != nil {
		log.Fatal(err)
	}
	status := newStatusServer(db, nil)
	mux := http.NewServeMux()
	status.RegisterHandlers(mux)
	httpServer := httptest.NewServer(mux)
	return httpServer
}

// TestStatusLocalStacks verifies that goroutine stack traces are available
// via the /_status/local/stacks endpoint.
func TestStatusLocalStacks(t *testing.T) {
	s := startStatusServer()
	defer s.Close()
	body, err := getText(s.URL + statusLocalStacksKey)
	if err != nil {
		t.Fatal(err)
	}
	// Verify match with at least two goroutine stacks.
	if matches, err := regexp.MatchString("(?s)goroutine [0-9]+.*goroutine [0-9]+.*", string(body)); !matches || err != nil {
		t.Errorf("expected match: %t; err nil: %s", matches, err)
	}
}

// TestStatusLocal verifies that local node info is exported.
// via the /_status/local/ endpoint.
func TestStatusLocal(t *testing.T) {
	// If not on a go release branch, the below compare will fail.
	if strings.HasPrefix(runtime.Version(), "devel") {
		t.Skip()
	}
	s := startStatusServer()
	defer s.Close()
	body, err := getText(s.URL + statusLocalKeyPrefix)
	if err != nil {
		t.Fatal(err)
	}
	// Verify indentation match with at least two goroutine stacks.
	pat := `{
  "buildInfo": {
    "goVersion": "go[0-9\.]+",
    "tag": "",
    "time": "",
    "dependencies": ""
  }
}`
	if matches, err := regexp.MatchString(pat, string(body)); !matches || err != nil {
		t.Errorf("expected match on %s; got %s: %v", pat, string(body), err)
	}
}
