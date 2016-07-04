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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/build"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func stubURL(target **url.URL, stubURL *url.URL) func() {
	realURL := *target
	*target = stubURL
	return func() {
		*target = realURL
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
	u, err := url.Parse(recorder.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer stubURL(&updatesURL, u)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	s.(*TestServer).checkForUpdates()
	recorder.Close()
	s.Stopper().Stop()

	if expected, actual := int32(1), atomic.LoadInt32(&updateChecks); actual != expected {
		t.Fatalf("expected %v update checks, got %v", expected, actual)
	}

	if expected, actual := s.(*TestServer).node.ClusterID.String(), uuid; expected != actual {
		t.Errorf("expected uuid %v, got %v", expected, actual)
	}

	if expected, actual := build.GetInfo().Tag, version; expected != actual {
		t.Errorf("expected version tag %v, got %v", expected, actual)
	}
}

func TestReportUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	usageReports := int32(0)
	uuid := ""
	reported := reportingInfo{}

	recorder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		atomic.AddInt32(&usageReports, 1)
		uuid = r.URL.Query().Get("uuid")
		if err := json.NewDecoder(r.Body).Decode(&reported); err != nil {
			t.Fatal(err)
		}
	}))
	u, err := url.Parse(recorder.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer stubURL(&reportingURL, u)()

	params := base.TestServerArgs{StoresPerNode: 2}
	s, _, _ := serverutils.StartServer(t, params)
	ts := s.(*TestServer)

	if err := ts.WaitForInitialSplits(); err != nil {
		t.Fatal(err)
	}

	node := ts.node.recorder.GetStatusSummary()
	ts.reportUsage()

	ts.Stopper().Stop() // stopper will wait for the update/report loop to finish too.
	recorder.Close()

	keyCounts := make(map[roachpb.StoreID]int)
	rangeCounts := make(map[roachpb.StoreID]int)
	totalKeys := 0
	totalRanges := 0

	for _, store := range node.StoreStatuses {
		if keys, ok := store.Metrics["keycount"]; ok {
			totalKeys += int(keys)
			keyCounts[store.Desc.StoreID] = int(keys)
		} else {
			t.Fatal("keycount not in metrics")
		}
		if replicas, ok := store.Metrics["replicas"]; ok {
			totalRanges += int(replicas)
			rangeCounts[store.Desc.StoreID] = int(replicas)
		} else {
			t.Fatal("replicas not in metrics")
		}
	}

	if expected, actual := int32(1), atomic.LoadInt32(&usageReports); expected != actual {
		t.Fatalf("expected %v reports, got %v", expected, actual)
	}
	if expected, actual := ts.node.ClusterID.String(), uuid; expected != actual {
		t.Errorf("expected cluster id %v got %v", expected, actual)
	}
	if expected, actual := ts.node.Descriptor.NodeID, reported.Node.NodeID; expected != actual {
		t.Errorf("expected node id %v got %v", expected, actual)
	}
	if minExpected, actual := totalKeys, reported.Node.KeyCount; minExpected > actual {
		t.Errorf("expected node keys at least %v got %v", minExpected, actual)
	}
	if minExpected, actual := totalRanges, reported.Node.RangeCount; minExpected > actual {
		t.Errorf("expected node ranges at least %v got %v", minExpected, actual)
	}
	if minExpected, actual := params.StoresPerNode, len(reported.Stores); minExpected > actual {
		t.Errorf("expected at least %v stores got %v", minExpected, actual)
	}

	for _, store := range reported.Stores {
		if minExpected, actual := keyCounts[store.StoreID], store.KeyCount; minExpected > actual {
			t.Errorf("expected at least %v keys in store %v got %v", minExpected, store.StoreID, actual)
		}
		if minExpected, actual := rangeCounts[store.StoreID], store.RangeCount; minExpected > actual {
			t.Errorf("expected at least %v ranges in store %v got %v", minExpected, store.StoreID, actual)
		}
	}

}
