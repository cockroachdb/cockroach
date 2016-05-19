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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

// TestStatusLocalStacks verifies that goroutine stack traces are available
// via the /_status/stacks/local endpoint.
func TestStatusLocalStacks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()

	// Verify match with at least two goroutine stacks.
	re := regexp.MustCompile("(?s)goroutine [0-9]+.*goroutine [0-9]+.*")

	if body, err := getText(s.Ctx.HTTPRequestScheme() + "://" + s.HTTPAddr() + "/_status/stacks/local"); err != nil {
		t.Fatal(err)
	} else if !re.Match(body) {
		t.Errorf("expected %s to match %s", body, re)
	}

	if body, err := getText(s.Ctx.HTTPRequestScheme() + "://" + s.HTTPAddr() + "/_status/stacks/1"); err != nil {
		t.Fatal(err)
	} else if !re.Match(body) {
		t.Errorf("expected %s to match %s", body, re)
	}
}

// ignoreWhitespace replaces all whitespace sequences with regex whitespace
// matches so you can match sequences without format mattering.
func ignoreWhitespace(text string) string {
	return regexp.MustCompile(`\s+`).ReplaceAllString(text, `\s*`)
}

// TestStatusJson verifies that status endpoints return expected Json results.
// The content type of the responses is always util.JSONContentType.
func TestStatusJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		{statusNodesPrefix, "\\\"nodes\\\":"},
	}
	expectedResult := fmt.Sprintf(ignoreWhitespace(`{
		"node_id": 1,
		"address": {
			"network_field": "%s",
			"address_field": "%s"
		},
		"build_info": {
			"go_version": "%s",
			"tag": "unknown",
			"cgo_compiler": ".*",
			"platform": ".*"
		}
	}`), addr.Network(), addr.String(), regexp.QuoteMeta(runtime.Version()))
	testCases = append(testCases, TestCase{"/health", expectedResult})
	testCases = append(testCases, TestCase{"/_status/details/local", expectedResult})
	testCases = append(testCases, TestCase{"/_status/details/1", expectedResult})

	httpClient, err := s.Ctx.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range testCases {
		contentTypes := []string{util.JSONContentType, util.ProtoContentType, util.YAMLContentType}
		for i, contentType := range contentTypes {
			util.SucceedsSoon(t, func() error {
				req, err := http.NewRequest("GET", s.Ctx.HTTPRequestScheme()+"://"+s.HTTPAddr()+spec.keyPrefix, nil)
				if err != nil {
					return err
				}
				req.Header.Set(util.AcceptHeader, contentType)
				resp, err := httpClient.Do(req)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					return fmt.Errorf("%d. endpoint %s unexpected status code: %v", i, spec.keyPrefix, resp.StatusCode)
				}
				returnedContentType := resp.Header.Get(util.ContentTypeHeader)
				if returnedContentType != util.JSONContentType {
					return fmt.Errorf("%d. endpoint %s unexpected content type: %v", i, spec.keyPrefix, returnedContentType)
				}
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return err
				}
				if re := regexp.MustCompile(spec.expected); !re.Match(body) {
					return fmt.Errorf("%d. endpoint %s expected match %s; got %s", i, spec.keyPrefix, spec.expected, body)
				}
				return nil
			})
		}
	}
}

// TestStatusGossipJson ensures that the output response for the full gossip
// info contains the required fields.
func TestStatusGossipJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()

	httpClient, err := s.Ctx.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	contentTypes := []string{util.JSONContentType, util.ProtoContentType, util.YAMLContentType}
	for _, contentType := range contentTypes {
		req, err := http.NewRequest("GET", s.Ctx.HTTPRequestScheme()+"://"+s.HTTPAddr()+"/_status/gossip/local", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(util.AcceptHeader, contentType)

		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("unexpected status code: %v", resp.StatusCode)
		}
		returnedContentType := resp.Header.Get(util.ContentTypeHeader)
		if returnedContentType != util.JSONContentType {
			t.Errorf("unexpected content type: %v", returnedContentType)
		}
		data := gossip.InfoStatus{}
		if err = jsonpb.Unmarshal(resp.Body, &data); err != nil {
			t.Fatal(err)
		}
		if _, ok := data.Infos["first-range"]; !ok {
			t.Errorf("no first-range info returned: %v", data)
		}
		if _, ok := data.Infos["cluster-id"]; !ok {
			t.Errorf("no clusterID info returned: %v", data)
		}
		if _, ok := data.Infos["node:1"]; !ok {
			t.Errorf("no node 1 info returned: %v", data)
		}
		if _, ok := data.Infos["system-db"]; !ok {
			t.Errorf("no system config info returned: %v", data)
		}
	}
}

var retryOptions = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxRetries:     4,
	Multiplier:     2,
}

// getRequestReader returns the io.ReadCloser from a get request to the test
// server with the given path. The returned closer should be closed by the
// caller.
func getRequestReader(t *testing.T, ts TestServer, path string) io.ReadCloser {
	httpClient, err := ts.Ctx.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	url := ts.Ctx.HTTPRequestScheme() + "://" + ts.HTTPAddr() + path
	for r := retry.Start(retryOptions); r.Next(); {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(util.AcceptHeader, util.JSONContentType)
		resp, err := httpClient.Do(req)
		if err != nil {
			log.Infof("could not GET %s - %s", url, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Infof("could not read body for %s - %s", url, err)
				continue
			}
			log.Infof("could not GET %s - statuscode: %d - body: %s", url, resp.StatusCode, body)
			continue
		}
		returnedContentType := resp.Header.Get(util.ContentTypeHeader)
		if returnedContentType != util.JSONContentType {
			log.Infof("unexpected content type: %v", returnedContentType)
			continue
		}
		log.Infof("OK response from %s", url)
		return resp.Body
	}
	t.Fatalf("There was an error retrieving %s", url)
	return nil
}

// getRequest returns the results of a get request to the test server with
// the given path. It returns the contents of the body of the result.
func getRequest(t *testing.T, ts TestServer, path string) []byte {
	respBody := getRequestReader(t, ts, path)
	defer respBody.Close()
	body, err := ioutil.ReadAll(respBody)
	if err != nil {
		log.Infof("could not read body for %s - %s", path, err)
		return nil
	}
	return body
}

// getRequestProto unmarshals the result of a get request to the test server
// with the given path.
func getRequestProto(t *testing.T, ts TestServer, path string, v proto.Message) error {
	respBody := getRequestReader(t, ts, path)
	defer respBody.Close()
	return jsonpb.Unmarshal(respBody, v)
}

// startServer will start a server with a short scan interval, wait for
// the scan to complete, and return the server. The caller is
// responsible for stopping the server.
func startServer(t *testing.T) TestServer {
	var ts TestServer
	ts.Ctx = NewTestContext()
	ts.StoresPerNode = 3
	if err := ts.Start(); err != nil {
		t.Fatalf("failed to start test server: %s", err)
	}

	// Make sure the range is spun up with an arbitrary read command. We do not
	// expect a specific response.
	if _, err := ts.db.Get("a"); err != nil {
		t.Fatal(err)
	}

	// Make sure the node status is available. This is done by forcing stores to
	// publish their status, synchronizing to the event feed with a canary
	// event, and then forcing the server to write summaries immediately.
	if err := ts.node.computePeriodicMetrics(); err != nil {
		t.Fatalf("error publishing store statuses: %s", err)
	}

	if err := ts.WriteSummaries(); err != nil {
		t.Fatalf("error writing summaries: %s", err)
	}

	return ts
}

// TestStatusLocalLogs checks to ensure that local/logfiles,
// local/logfiles/{filename}, local/log and local/log/{level} function
// correctly.
func TestStatusLocalLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	ts := startServer(t)
	defer ts.Stop()

	// Log an error which we expect to show up on every log file.
	timestamp := timeutil.Now().UnixNano()
	log.Errorf("TestStatusLocalLogFile test message-Error")
	timestampE := timeutil.Now().UnixNano()
	log.Warningf("TestStatusLocalLogFile test message-Warning")
	timestampEW := timeutil.Now().UnixNano()
	log.Infof("TestStatusLocalLogFile test message-Info")
	timestampEWI := timeutil.Now().UnixNano()

	type logsWrapper struct {
		Data []log.FileInfo `json:"d"`
	}
	var logs logsWrapper
	if err := json.Unmarshal(getRequest(t, ts, "/_status/logfiles/local"), &logs); err != nil {
		t.Fatal(err)
	}
	if a, e := len(logs.Data), 3; a != e {
		t.Fatalf("expected %d log files; got %d", e, a)
	}
	for i, name := range []string{"log.ERROR", "log.INFO", "log.WARNING"} {
		if !strings.Contains(logs.Data[i].Name, name) {
			t.Errorf("expected log file name %s to contain %q", logs.Data[i].Name, name)
		}
	}

	// Fetch the full list of log entries.
	type logWrapper struct {
		Data []log.Entry `json:"d"`
	}

	// Check each individual log can be fetched and is non-empty.
	var foundInfo, foundWarning, foundError bool
	for _, file := range logs.Data {
		var log logWrapper
		if err := json.Unmarshal(getRequest(t, ts, fmt.Sprintf("/_status/logfiles/local/%s", file.Name)), &log); err != nil {
			t.Fatal(err)
		}
		for _, entry := range log.Data {
			switch entry.Message {
			case "TestStatusLocalLogFile test message-Error":
				foundError = true
			case "TestStatusLocalLogFile test message-Warning":
				foundWarning = true
			case "TestStatusLocalLogFile test message-Info":
				foundInfo = true
			}
		}
	}

	if !(foundInfo && foundWarning && foundError) {
		t.Errorf("expected to find test messages in %v", logs.Data)
	}

	testCases := []struct {
		Level           log.Severity
		MaxEntities     int
		StartTimestamp  int64
		EndTimestamp    int64
		Pattern         string
		ExpectedError   bool
		ExpectedWarning bool
		ExpectedInfo    bool
	}{
		// Test filtering by log severity.
		{log.InfoLog, 0, 0, 0, "", true, true, true},
		{log.WarningLog, 0, 0, 0, "", true, true, false},
		{log.ErrorLog, 0, 0, 0, "", true, false, false},
		// Test entry limit. Ignore Info/Warning/Error filters.
		{log.InfoLog, 1, timestamp, timestampEWI, "", false, false, false},
		{log.InfoLog, 2, timestamp, timestampEWI, "", false, false, false},
		{log.InfoLog, 3, timestamp, timestampEWI, "", false, false, false},
		// Test filtering in different timestamp windows.
		{log.InfoLog, 0, timestamp, timestamp, "", false, false, false},
		{log.InfoLog, 0, timestamp, timestampE, "", true, false, false},
		{log.InfoLog, 0, timestampE, timestampEW, "", false, true, false},
		{log.InfoLog, 0, timestampEW, timestampEWI, "", false, false, true},
		{log.InfoLog, 0, timestamp, timestampEW, "", true, true, false},
		{log.InfoLog, 0, timestampE, timestampEWI, "", false, true, true},
		{log.InfoLog, 0, timestamp, timestampEWI, "", true, true, true},
		// Test filtering by regexp pattern.
		{log.InfoLog, 0, 0, 0, "Info", false, false, true},
		{log.InfoLog, 0, 0, 0, "Warning", false, true, false},
		{log.InfoLog, 0, 0, 0, "Error", true, false, false},
		{log.InfoLog, 0, 0, 0, "Info|Error|Warning", true, true, true},
		{log.InfoLog, 0, 0, 0, "Nothing", false, false, false},
	}

	for i, testCase := range testCases {
		var url bytes.Buffer
		fmt.Fprintf(&url, "/_status/logs/local?level=%s", testCase.Level.Name())
		if testCase.MaxEntities > 0 {
			fmt.Fprintf(&url, "&max=%d", testCase.MaxEntities)
		}
		if testCase.StartTimestamp > 0 {
			fmt.Fprintf(&url, "&starttime=%d", testCase.StartTimestamp)
		}
		if testCase.StartTimestamp > 0 {
			fmt.Fprintf(&url, "&endtime=%d", testCase.EndTimestamp)
		}
		if len(testCase.Pattern) > 0 {
			fmt.Fprintf(&url, "&pattern=%s", testCase.Pattern)
		}

		var log logWrapper
		path := url.String()
		if err := json.Unmarshal(getRequest(t, ts, path), &log); err != nil {
			t.Fatal(err)
		}

		if testCase.MaxEntities > 0 {
			if a, e := len(log.Data), testCase.MaxEntities; a != e {
				t.Errorf("%d expected %d entries, got %d: \n%+v", i, e, a, log.Data)
			}
		} else {
			var actualInfo, actualWarning, actualError bool
			var formats bytes.Buffer
			for _, entry := range log.Data {
				fmt.Fprintf(&formats, "%s\n", entry.Message)

				switch entry.Message {
				case "TestStatusLocalLogFile test message-Error":
					actualError = true
				case "TestStatusLocalLogFile test message-Warning":
					actualWarning = true
				case "TestStatusLocalLogFile test message-Info":
					actualInfo = true
				}
			}

			if !(testCase.ExpectedInfo == actualInfo &&
				testCase.ExpectedWarning == actualWarning &&
				testCase.ExpectedError == actualError) {

				t.Errorf(
					"%d: expected info, warning, error: (%t, %t, %t) from %s, got:\n%s",
					i,
					testCase.ExpectedInfo,
					testCase.ExpectedWarning,
					testCase.ExpectedError,
					path,
					formats.String(),
				)
			}
		}
	}
}

// TestNodeStatusResponse verifies that node status returns the expected
// results.
func TestNodeStatusResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := startServer(t)
	defer ts.Stop()

	// First fetch all the node statuses.
	wrapper := NodesResponse{}
	if err := getRequestProto(t, ts, statusNodesPrefix, &wrapper); err != nil {
		t.Fatal(err)
	}
	nodeStatuses := wrapper.Nodes

	if len(nodeStatuses) != 1 {
		t.Errorf("too many node statuses returned - expected:1 actual:%d", len(nodeStatuses))
	}
	if !reflect.DeepEqual(ts.node.Descriptor, nodeStatuses[0].Desc) {
		t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", ts.node.Descriptor, nodeStatuses[0].Desc)
	}

	// Now fetch each one individually. Loop through the nodeStatuses to use the
	// ids only.
	for _, oldNodeStatus := range nodeStatuses {
		nodeStatus := status.NodeStatus{}
		if err := getRequestProto(t, ts, PathForNodeStatus(oldNodeStatus.Desc.NodeID.String()), &nodeStatus); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ts.node.Descriptor, nodeStatus.Desc) {
			t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", ts.node.Descriptor, nodeStatus.Desc)
		}
	}
}

// TestMetricsRecording verifies that Node statistics are periodically recorded
// as time series data.
func TestMetricsRecording(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tsrv := TestServer{}
	tsrv.Ctx = NewTestContext()
	tsrv.Ctx.MetricsSampleInterval = 5 * time.Millisecond
	if err := tsrv.Start(); err != nil {
		t.Fatal(err)
	}
	defer tsrv.Stop()

	checkTimeSeriesKey := func(now int64, keyName string) error {
		key := ts.MakeDataKey(keyName, "", ts.Resolution10s, now)
		data := roachpb.InternalTimeSeriesData{}
		return tsrv.db.GetProto(key, &data)
	}

	// Verify that metrics for the current timestamp are recorded. This should
	// be true very quickly.
	util.SucceedsSoon(t, func() error {
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

// TestMetricsEndpoint retrieves the metrics endpoint, which is currently only
// used for development purposes. The metrics within the response are verified
// in other tests.
func TestMetricsEndpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := startServer(t)
	defer s.Stop()

	nodeID := s.Gossip().GetNodeID()
	url := fmt.Sprintf("%s/%s", statusMetricsPrefix, nodeID)
	getRequest(t, s, url)
}

func TestRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := startServer(t)
	defer ts.Stop()

	var response RangesResponse
	if err := getRequestProto(t, ts, statusRangesPrefix+"local", &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Ranges) == 0 {
		t.Errorf("didn't get any ranges")
	}
	for _, ri := range response.Ranges {
		// Do some simple validation based on the fact that this is a
		// single-node cluster.
		if ri.RaftState != "StateLeader" {
			t.Errorf("expected to be raft leader but was %s", ri.RaftState)
		}
		expReplica := roachpb.ReplicaDescriptor{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 1,
		}
		if len(ri.Desc.Replicas) != 1 || ri.Desc.Replicas[0] != expReplica {
			t.Errorf("unexpected replica list %+v", ri.Desc.Replicas)
		}
	}
}
