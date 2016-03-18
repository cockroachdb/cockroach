// Copyright 2016 The Cockroach Authors.
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
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestSetupReportingURLs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := new(Server) // don't actually need a testserver here

	if err := s.SetupRportingURLs(); err != nil {
		t.Fatal(err)
	}
	if s.parsedUpdatesURL == nil {
		t.Fatal("updates url should be set")
	}
}

func TestCheckVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	updateChecks := int32(0)
	uuid := ""
	version := ""

	recorder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		atomic.AddInt32(&updateChecks, 1)
		uuid = r.URL.Query().Get("uuid")
		version = r.URL.Query().Get("version")
	}))

	s := StartTestServer(t)
	s.parsedUpdatesURL, _ = url.Parse(recorder.URL)
	s.checkForUpdates()
	recorder.Close()
	s.Stop()

	if expected, actual := int32(1), atomic.LoadInt32(&updateChecks); actual != expected {
		t.Fatalf("expected %v update checks, got %v", expected, actual)
	}

	if expected, actual := s.node.ClusterID.String(), uuid; expected != actual {
		t.Errorf("expected uuid %v, got %v", expected, actual)
	}

	if expected, actual := util.GetBuildInfo().Tag, version; expected != actual {
		t.Errorf("expected version tag %v, got %v", expected, actual)
	}
}
