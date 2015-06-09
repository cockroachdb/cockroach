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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// startStatusServer launches a new status server using minimal engine
// and local database setup. Returns the new http test server, which
// should be cleaned up by caller via httptest.Server.Close(). The
// Cockroach KV client address is set to the address of the test server.
func startStatusServer() (*httptest.Server, *util.Stopper) {
	stopper := util.NewStopper()
	db, err := BootstrapCluster("cluster-1", []engine.Engine{engine.NewInMem(proto.Attributes{}, 1<<20)}, stopper)
	if err != nil {
		log.Fatal(err)
	}
	status := newStatusServer(db, nil)
	httpServer := httptest.NewUnstartedServer(status.router)
	tlsConfig, err := testContext.GetServerTLSConfig()
	if err != nil {
		log.Fatal(err)
	}
	httpServer.TLS = tlsConfig
	httpServer.StartTLS()
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
	s := StartTestServer(t)
	defer s.Stop()

	type TestCase struct {
		keyPrefix string
		expected  string
	}

	addr, err := s.Gossip().GetNodeIDAddress(s.Gossip().GetNodeID())
	if err != nil {
		t.Fatal(err)
	}
	testCases := []TestCase{
		{statusKeyPrefix, "{}"},
	}
	// Test the /_status/local/ endpoint only in a go release branch.
	if !strings.HasPrefix(runtime.Version(), "devel") {
		testCases = append(testCases, TestCase{statusLocalKeyPrefix, fmt.Sprintf(`{
  "address": {
    "network": "%s",
    "string": "%s"
  },
  "buildInfo": {
    "goVersion": "go[0-9\.]+",
    "tag": "",
    "time": "",
    "dependencies": ""
  }
}`, addr.Network(), addr.String())})
	} else {
		testCases = append(testCases, TestCase{statusLocalKeyPrefix, fmt.Sprintf(`{
  "address": {
    "network": "%s",
    "string": "%s"
  }
}`, addr.Network(), addr.String())})
	}

	httpClient, err := testContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range testCases {
		contentTypes := []string{"application/json", "application/x-protobuf", "text/yaml"}
		for _, contentType := range contentTypes {
			req, err := http.NewRequest("GET", testContext.RequestScheme()+"://"+s.ServingAddr()+spec.keyPrefix, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Accept", contentType)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("unexpected status code: %v", resp.StatusCode)
			}
			returnedContentType := resp.Header.Get(util.ContentTypeHeader)
			if returnedContentType != "application/json" {
				t.Errorf("unexpected content type: %v", returnedContentType)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if matches, err := regexp.Match(spec.expected, body); !matches || err != nil {
				t.Errorf("expected match %s; got %s; err nil: %s", spec.expected, body, err)
			}
		}
	}
}

// TestStatusGossipJson ensures that the output response for the full gossip
// info contains the required fields.
func TestStatusGossipJson(t *testing.T) {
	s := StartTestServer(t)
	defer s.Stop()

	type prefixedInfo struct {
		Key string                 `json:"Key"`
		Val []storage.PrefixConfig `json:"Val"`
	}

	type rangeDescriptorInfo struct {
		Key string                `json:"Key"`
		Val proto.RangeDescriptor `json:"Val"`
	}

	type keyValueStringPair struct {
		Key string `json:"Key"`
		Val string `json:"Val"`
	}

	type keyValuePair struct {
		Key string                 `json:"Key"`
		Val map[string]interface{} `json:"Val"`
	}

	type infos struct {
		Infos struct {
			Accounting  *prefixedInfo        `json:"accounting"`
			FirstRange  *rangeDescriptorInfo `json:"first-range"`
			Permissions *prefixedInfo        `json:"permissions"`
			Zones       *prefixedInfo        `json:"zones"`
			ClusterID   *keyValueStringPair  `json:"cluster-id"`
			Node1       *keyValuePair        `json:"node:1"`
		} `json:"infos"`
	}

	httpClient, err := testContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	contentTypes := []string{"application/json", "application/x-protobuf", "text/yaml"}
	for _, contentType := range contentTypes {
		req, err := http.NewRequest("GET", testContext.RequestScheme()+"://"+s.ServingAddr()+statusGossipKeyPrefix, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Accept", contentType)

		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("unexpected status code: %v", resp.StatusCode)
		}
		returnedContentType := resp.Header.Get(util.ContentTypeHeader)
		if returnedContentType != "application/json" {
			t.Errorf("unexpected content type: %v", returnedContentType)
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		data := &infos{}
		if err = json.Unmarshal(body, &data); err != nil {
			t.Fatal(err)
		}
		if data.Infos.Accounting == nil {
			t.Errorf("no accounting info returned: %v,", body)
		}
		if data.Infos.FirstRange == nil {
			t.Errorf("no first-range info returned: %v", body)
		}
		if data.Infos.Permissions == nil {
			t.Errorf("no permission info returned: %v", body)
		}
		if data.Infos.Zones == nil {
			t.Errorf("no zone info returned: %v", body)
		}
		if data.Infos.ClusterID == nil {
			t.Errorf("no clusterID info returned: %v", body)
		}
		if data.Infos.Node1 == nil {
			t.Errorf("no node 1 info returned: %v", body)
		}
	}
}

// getRequest returns the the results of a get request to the test server with
// the given path.  It returns the contents of the body of the result.
func getRequest(t *testing.T, ts *TestServer, path string) []byte {
	httpClient, err := testContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("GET", testContext.RequestScheme()+"://"+ts.ServingAddr()+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("unexpected status code: %v", resp.StatusCode)
	}
	returnedContentType := resp.Header.Get(util.ContentTypeHeader)
	if returnedContentType != "application/json" {
		t.Errorf("unexpected content type: %v", returnedContentType)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// startServerAndGetStatus will startup a server with a short scan interval,
// wait for the scan to completed, fetch the request status based on the
// keyPrefix. The test server and fetched status are returned. The caller is
// responsible to stop the server.
// TODO(Bram): Add more nodes.
func startServerAndGetStatus(t *testing.T, keyPrefix string) (*TestServer, []byte) {
	ts := &TestServer{}
	ts.Ctx = NewTestContext()
	ts.Ctx.ScanInterval = time.Duration(5 * time.Millisecond)
	ts.StoresPerNode = 3
	if err := ts.Start(); err != nil {
		t.Fatal(err)
	}

	// Make sure the node is spun up and that a full scan of the ranges in the
	// stores is complete.  The best way to do that is to wait twice.
	ts.node.waitForScanCompletion()
	ts.node.waitForScanCompletion()

	body := getRequest(t, ts, keyPrefix)
	return ts, body
}

func TestStatusLocalLog(t *testing.T) {
	dir, err := ioutil.TempDir("", "local_log_test")
	if err != nil {
		t.Fatal(err)
	}
	log.EnableLogFileOutput(dir)
	defer func() {
		log.DisableLogFileOutput()
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}()
	ts, body := startServerAndGetStatus(t, statusLocalLogKeyPrefix)
	defer ts.Stop()

	type logsWrapper struct {
		Data []log.FileInfo `json:"d"`
	}
	logs := logsWrapper{}
	if err := json.Unmarshal(body, &logs); err != nil {
		t.Fatal(err)
	}
	if l := len(logs.Data); l != 3 {
		t.Fatalf("expected 3 log files; got %d", l)
	}
	for i, pat := range []string{`.*log.ERROR.*`, `.*log.INFO.*`, `.*log.WARNING.*`} {
		if ok, err := regexp.MatchString(pat, logs.Data[i].Name); !ok || err != nil {
			t.Errorf("expected log file %s to match %q: %s", logs.Data[i].Name, pat, err)
		}
	}

	// Log an error which we expect to show up on every log file.
	log.Errorf("TestStatusLocalLog test message")

	// Fetch a each listed log directly.
	type logWrapper struct {
		Data []proto.LogEntry `json:"d"`
	}
	// Check each individual log can be fetched and is non-empty.
	for _, log := range logs.Data {
		body = getRequest(t, ts, fmt.Sprintf("%s%s", statusLocalLogKeyPrefix, log.Name))
		logW := logWrapper{}
		if err := json.Unmarshal(body, &logW); err != nil {
			t.Fatal(err)
		}
		var found bool
		for i := len(logW.Data) - 1; i >= 0; i-- {
			if logW.Data[i].Format == "TestStatusLocalLog test message" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("exected to find test message in %v", logW.Data)
		}
	}
}

// TestNodeStatusResponse verifies that node status returns the expected
// results.
func TestNodeStatusResponse(t *testing.T) {
	ts, body := startServerAndGetStatus(t, statusNodeKeyPrefix)
	defer ts.Stop()

	// First fetch all the node statuses.
	type nsWrapper struct {
		Data []proto.NodeStatus `json:"d"`
	}
	wrapper := nsWrapper{}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		t.Fatal(err)
	}
	nodeStatuses := wrapper.Data

	if len(nodeStatuses) != 1 {
		t.Errorf("too many node statuses returned - expected:1 actual:%d", len(nodeStatuses))
	}
	if !reflect.DeepEqual(ts.node.Descriptor, nodeStatuses[0].Desc) {
		t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", ts.node.Descriptor, nodeStatuses[0].Desc)
	}

	// Now fetch each one individually. Loop through the nodeStatuses to use the
	// ids only.
	for _, oldNodeStatus := range nodeStatuses {
		nodeStatus := &proto.NodeStatus{}
		requestBody := getRequest(t, ts, fmt.Sprintf("%s%s", statusNodeKeyPrefix, oldNodeStatus.Desc.NodeID))
		if err := json.Unmarshal(requestBody, &nodeStatus); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ts.node.Descriptor, nodeStatus.Desc) {
			t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", ts.node.Descriptor, nodeStatus.Desc)
		}
	}
}

// TestStoreStatusResponse verifies that node status returns the expected
// results.
func TestStoreStatusResponse(t *testing.T) {
	ts, body := startServerAndGetStatus(t, statusStoreKeyPrefix)
	defer ts.Stop()
	type ssWrapper struct {
		Data []proto.StoreStatus `json:"d"`
	}
	wrapper := ssWrapper{}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		t.Fatal(err)
	}
	storeStatuses := wrapper.Data

	if len(storeStatuses) != ts.node.lSender.GetStoreCount() {
		t.Errorf("too many node statuses returned - expected:%d, actual:%d", ts.node.lSender.GetStoreCount(), len(storeStatuses))
	}
	for _, storeStatus := range storeStatuses {
		storeID := storeStatus.Desc.StoreID
		store, err := ts.node.lSender.GetStore(storeID)
		if err != nil {
			t.Fatal(err)
		}
		desc, err := store.Descriptor()
		if err != nil {
			t.Fatal(err)
		}
		// The capacities fluctuate a lot, so drop them for the deep equal.
		desc.Capacity = proto.StoreCapacity{}
		storeStatus.Desc.Capacity = proto.StoreCapacity{}
		if !reflect.DeepEqual(*desc, storeStatus.Desc) {
			t.Errorf("store status descriptors are not equal\nexpected:%+v\nactual:%+v\n", *desc, storeStatus.Desc)
		}

		// Also fetch the each status individually.
		fetchedStoreStatus := &proto.StoreStatus{}
		requestBody := getRequest(t, ts, fmt.Sprintf("%s%s", statusStoreKeyPrefix, storeStatus.Desc.StoreID))
		if err := json.Unmarshal(requestBody, &fetchedStoreStatus); err != nil {
			t.Fatal(err)
		}
		fetchedStoreStatus.Desc.Capacity = proto.StoreCapacity{}
		if !reflect.DeepEqual(*desc, fetchedStoreStatus.Desc) {
			t.Errorf("store status descriptors are not equal\nexpected:%+v\nactual:%+v\n", *desc, fetchedStoreStatus.Desc)
		}
	}
}

// TestMetricsRecording verifies that Node statistics are periodically recorded
// as time series data.
func TestMetricsRecording(t *testing.T) {
	tsrv := &TestServer{}
	tsrv.Ctx = NewTestContext()
	tsrv.Ctx.MetricsFrequency = 5 * time.Millisecond
	if err := tsrv.Start(); err != nil {
		t.Fatal(err)
	}
	defer tsrv.Stop()

	checkTimeSeriesKey := func(now int64, keyName string) error {
		key := ts.MakeDataKey(keyName, "", ts.Resolution10s, now)
		data := &proto.InternalTimeSeriesData{}
		return tsrv.db.GetProto(key, data)
	}

	// Verify that metrics for the current timestamp are recorded. This should
	// be true very quickly.
	util.SucceedsWithin(t, time.Second, func() error {
		now := tsrv.Clock().PhysicalNow()
		if err := checkTimeSeriesKey(now, "cr.store.livebytes.1"); err != nil {
			return err
		}
		if err := checkTimeSeriesKey(now, "cr.node.sys.allocbytes.1"); err != nil {
			return err
		}
		return nil
	})
}
