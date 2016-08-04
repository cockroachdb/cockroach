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
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/build"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// TestStatusLocalStacks verifies that goroutine stack traces are available
// via the /_status/stacks/local endpoint.
func TestStatusLocalStacks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	// Verify match with at least two goroutine stacks.
	re := regexp.MustCompile("(?s)goroutine [0-9]+.*goroutine [0-9]+.*")

	if body, err := getText(s.AdminURL() + "/_status/stacks/local"); err != nil {
		t.Fatal(err)
	} else if !re.Match(body) {
		t.Errorf("expected %s to match %s", body, re)
	}

	if body, err := getText(s.AdminURL() + "/_status/stacks/1"); err != nil {
		t.Fatal(err)
	} else if !re.Match(body) {
		t.Errorf("expected %s to match %s", body, re)
	}
}

// TestStatusJson verifies that status endpoints return expected Json results.
// The content type of the responses is always util.JSONContentType.
func TestStatusJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	ts := s.(*TestServer)

	nodeID := ts.Gossip().GetNodeID()
	addr, err := ts.Gossip().GetNodeIDAddress(nodeID)
	if err != nil {
		t.Fatal(err)
	}

	var nodes serverpb.NodesResponse
	util.SucceedsSoon(t, func() error {
		if err := getRequestProto(t, s, statusNodesPrefix, &nodes); err != nil {
			t.Fatal(err)
		}

		if len(nodes.Nodes) == 0 {
			return errors.Errorf("expected non-empty node list, got: %v", nodes)
		}
		return nil
	})

	for _, path := range []string{
		"/health",
		"/_status/details/local",
		"/_status/details/" + strconv.FormatUint(uint64(nodeID), 10),
	} {
		var details serverpb.DetailsResponse
		if err := getRequestProto(t, s, path, &details); err != nil {
			t.Fatal(err)
		}
		if a, e := details.NodeID, nodeID; a != e {
			t.Errorf("expected: %d, got: %d", e, a)
		}
		if a, e := details.Address, *addr; a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
		if a, e := details.BuildInfo, build.GetInfo(); a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
	}
}

// TestStatusGossipJson ensures that the output response for the full gossip
// info contains the required fields.
func TestStatusGossipJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	var data gossip.InfoStatus
	if err := getRequestProto(t, s, "/_status/gossip/local", &data); err != nil {
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

var retryOptions = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxRetries:     4,
	Multiplier:     2,
}

// getRequestReader returns the io.ReadCloser from a get request to the test
// server with the given path. The returned closer should be closed by the
// caller.
func getRequestReader(t *testing.T, ts serverutils.TestServerInterface, path string) io.ReadCloser {
	httpClient, err := ts.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	url := ts.AdminURL() + path
	for r := retry.Start(retryOptions); r.Next(); {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(util.AcceptHeader, util.JSONContentType)
		resp, err := httpClient.Do(req)
		if err != nil {
			log.Infof(context.Background(), "could not GET %s - %s", url, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Infof(context.Background(), "could not read body for %s - %s", url, err)
				continue
			}
			log.Infof(context.Background(), "could not GET %s - statuscode: %d - body: %s", url, resp.StatusCode, body)
			continue
		}
		returnedContentType := resp.Header.Get(util.ContentTypeHeader)
		if returnedContentType != util.JSONContentType {
			log.Infof(context.Background(), "unexpected content type: %v", returnedContentType)
			continue
		}
		log.Infof(context.Background(), "OK response from %s", url)
		return resp.Body
	}
	t.Fatalf("There was an error retrieving %s", url)
	return nil
}

// getRequest returns the results of a get request to the test server with
// the given path. It returns the contents of the body of the result.
func getRequest(t *testing.T, ts serverutils.TestServerInterface, path string) []byte {
	respBody := getRequestReader(t, ts, path)
	defer respBody.Close()
	body, err := ioutil.ReadAll(respBody)
	if err != nil {
		log.Infof(context.Background(), "could not read body for %s - %s", path, err)
		return nil
	}
	return body
}

// getRequestProto unmarshals the result of a get request to the test server
// with the given path.
func getRequestProto(t *testing.T, ts serverutils.TestServerInterface, path string, v proto.Message) error {
	respBody := getRequestReader(t, ts, path)
	defer respBody.Close()
	return jsonpb.Unmarshal(respBody, v)
}

// startServer will start a server with a short scan interval, wait for
// the scan to complete, and return the server. The caller is
// responsible for stopping the server.
func startServer(t *testing.T) serverutils.TestServerInterface {
	ts, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{StoresPerNode: 3})

	// Make sure the range is spun up with an arbitrary read command. We do not
	// expect a specific response.
	if _, err := kvDB.Get("a"); err != nil {
		t.Fatal(err)
	}

	// Make sure the node status is available. This is done by forcing stores to
	// publish their status, synchronizing to the event feed with a canary
	// event, and then forcing the server to write summaries immediately.
	if err := ts.(*TestServer).node.computePeriodicMetrics(); err != nil {
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
	if log.V(3) {
		t.Skip("Test only works with low verbosity levels")
	}
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
	defer ts.Stopper().Stop()

	// Log an error which we expect to show up on every log file.
	timestamp := timeutil.Now().UnixNano()
	log.Errorf(context.Background(), "TestStatusLocalLogFile test message-Error")
	timestampE := timeutil.Now().UnixNano()
	log.Warningf(context.Background(), "TestStatusLocalLogFile test message-Warning")
	timestampEW := timeutil.Now().UnixNano()
	log.Infof(context.Background(), "TestStatusLocalLogFile test message-Info")
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
	s := startServer(t)
	defer s.Stopper().Stop()
	ts := s.(*TestServer)

	// First fetch all the node statuses.
	wrapper := serverpb.NodesResponse{}
	if err := getRequestProto(t, s, statusNodesPrefix, &wrapper); err != nil {
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
		if err := getRequestProto(t, s, PathForNodeStatus(oldNodeStatus.Desc.NodeID.String()), &nodeStatus); err != nil {
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
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		MetricsSampleInterval: 5 * time.Millisecond})
	defer s.Stopper().Stop()

	checkTimeSeriesKey := func(now int64, keyName string) error {
		key := ts.MakeDataKey(keyName, "", ts.Resolution10s, now)
		data := roachpb.InternalTimeSeriesData{}
		return kvDB.GetProto(key, &data)
	}

	// Verify that metrics for the current timestamp are recorded. This should
	// be true very quickly.
	util.SucceedsSoon(t, func() error {
		now := s.Clock().PhysicalNow()
		if err := checkTimeSeriesKey(now, "cr.store.livebytes.1"); err != nil {
			return err
		}
		if err := checkTimeSeriesKey(now, "cr.node.sys.go.allocbytes.1"); err != nil {
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
	defer s.Stopper().Stop()

	nodeID := s.(*TestServer).Gossip().GetNodeID()
	url := fmt.Sprintf("%s/%s", statusMetricsPrefix, nodeID)
	getRequest(t, s, url)
}

func TestRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := startServer(t)
	defer ts.Stopper().Stop()

	// Perform a scan to ensure that all the raft groups are initialized.
	if _, err := ts.(*TestServer).db.Scan(keys.LocalMax, roachpb.KeyMax, 0); err != nil {
		t.Fatal(err)
	}

	var response serverpb.RangesResponse
	if err := getRequestProto(t, ts, statusRangesPrefix+"local", &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Ranges) == 0 {
		t.Errorf("didn't get any ranges")
	}
	for _, ri := range response.Ranges {
		// Do some simple validation based on the fact that this is a
		// single-node cluster.
		if ri.RaftState != "StateLeader" && ri.RaftState != "StateDormant" {
			t.Errorf("expected to be Raft leader or dormant, but was '%s'", ri.RaftState)
		}
		expReplica := roachpb.ReplicaDescriptor{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 1,
		}
		if len(ri.State.Desc.Replicas) != 1 || ri.State.Desc.Replicas[0] != expReplica {
			t.Errorf("unexpected replica list %+v", ri.State.Desc.Replicas)
		}
		if ri.State.Lease == nil {
			t.Error("expected a nontrivial Lease")
		}
		if ri.State.LastIndex == 0 {
			t.Error("expected positive LastIndex")
		}
	}
}

func TestRaftDebug(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := startServer(t)
	defer s.Stopper().Stop()

	var resp serverpb.RaftDebugResponse
	if err := getRequestProto(t, s, statusRaftEndpoint, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Ranges) == 0 {
		t.Errorf("didn't get any ranges")
	}
}

// TestStatusVars verifies that prometheus metrics are available via the
// /_status/vars endpoint.
func TestStatusVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	if body, err := getText(s.AdminURL() + "/_status/vars"); err != nil {
		t.Fatal(err)
	} else if !bytes.Contains(body, []byte("# TYPE sql_bytesout counter\nsql_bytesout")) {
		t.Errorf("expected sql_bytesout, got: %s", body)
	}
}

func TestSpanStatsResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := startServer(t)
	defer ts.Stopper().Stop()

	httpClient, err := ts.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	var response serverpb.SpanStatsResponse
	request := serverpb.SpanStatsRequest{
		NodeID:   "1",
		StartKey: []byte(roachpb.RKeyMin),
		EndKey:   []byte(roachpb.RKeyMax),
	}

	url := ts.AdminURL() + statusPrefix + "span"
	if err := util.PostJSON(httpClient, url, &request, &response); err != nil {
		t.Fatal(err)
	}
	if a, e := int(response.RangeCount), ExpectedInitialRangeCount(); a != e {
		t.Errorf("expected %d ranges, found %d", e, a)
	}
}

func TestSpanStatsGRPCResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := startServer(t)
	defer ts.Stopper().Stop()

	rpcStopper := stop.NewStopper()
	defer rpcStopper.Stop()
	rpcContext := rpc.NewContext(ts.RPCContext().Context, ts.Clock(), rpcStopper)
	request := serverpb.SpanStatsRequest{
		NodeID:   "1",
		StartKey: []byte(roachpb.RKeyMin),
		EndKey:   []byte(roachpb.RKeyMax),
	}

	url := ts.ServingAddr()
	conn, err := rpcContext.GRPCDial(url)
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	response, err := client.SpanStats(context.Background(), &request)
	if err != nil {
		t.Fatal(err)
	}
	if a, e := int(response.RangeCount), ExpectedInitialRangeCount(); a != e {
		t.Errorf("expected %d ranges, found %d", e, a)
	}
}
