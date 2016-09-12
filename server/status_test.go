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
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
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
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

func getStatusJSONProto(ts serverutils.TestServerInterface, path string, response proto.Message) error {
	return serverutils.GetJSONProto(ts, statusPrefix+path, response)
}

// TestStatusLocalStacks verifies that goroutine stack traces are available
// via the /_status/stacks/local endpoint.
func TestStatusLocalStacks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	// Verify match with at least two goroutine stacks.
	re := regexp.MustCompile("(?s)goroutine [0-9]+.*goroutine [0-9]+.*")

	var stacks serverpb.JSONResponse
	for _, nodeID := range []string{"local", "1"} {
		if err := getStatusJSONProto(s, "stacks/"+nodeID, &stacks); err != nil {
			t.Fatal(err)
		}
		if !re.Match(stacks.Data) {
			t.Errorf("expected %s to match %s", stacks.Data, re)
		}
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
		if err := getStatusJSONProto(s, "nodes", &nodes); err != nil {
			t.Fatal(err)
		}

		if len(nodes.Nodes) == 0 {
			return errors.Errorf("expected non-empty node list, got: %v", nodes)
		}
		return nil
	})

	for _, path := range []string{
		"/health",
		statusPrefix + "details/local",
		statusPrefix + "details/" + strconv.FormatUint(uint64(nodeID), 10),
	} {
		var details serverpb.DetailsResponse
		if err := serverutils.GetJSONProto(s, path, &details); err != nil {
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
	if err := getStatusJSONProto(s, "gossip/local", &data); err != nil {
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

// startServer will start a server with a short scan interval, wait for
// the scan to complete, and return the server. The caller is
// responsible for stopping the server.
func startServer(t *testing.T) *TestServer {
	tsI, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
		},
	})

	ts := tsI.(*TestServer)

	// Make sure the range is spun up with an arbitrary read command. We do not
	// expect a specific response.
	if _, err := kvDB.Get("a"); err != nil {
		t.Fatal(err)
	}

	// Make sure the node status is available. This is done by forcing stores to
	// publish their status, synchronizing to the event feed with a canary
	// event, and then forcing the server to write summaries immediately.
	if err := ts.node.computePeriodicMetrics(0); err != nil {
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

	var wrapper serverpb.LogFilesListResponse
	if err := getStatusJSONProto(ts, "logfiles/local", &wrapper); err != nil {
		t.Fatal(err)
	}
	if a, e := len(wrapper.Files), 3; a != e {
		t.Fatalf("expected %d log files; got %d", e, a)
	}
	for i, name := range []string{"log.ERROR", "log.INFO", "log.WARNING"} {
		if !strings.Contains(wrapper.Files[i].Name, name) {
			t.Errorf("expected log file name %s to contain %q", wrapper.Files[i].Name, name)
		}
	}

	// Check each individual log can be fetched and is non-empty.
	var foundInfo, foundWarning, foundError bool
	for _, file := range wrapper.Files {
		var wrapper serverpb.LogEntriesResponse
		if err := getStatusJSONProto(ts, "logfiles/local/"+file.Name, &wrapper); err != nil {
			t.Fatal(err)
		}
		for _, entry := range wrapper.Entries {
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
		t.Errorf("expected to find test messages in %v", wrapper.Files)
	}

	type levelPresence struct {
		Error, Warning, Info bool
	}

	testCases := []struct {
		Level          log.Severity
		MaxEntities    int
		StartTimestamp int64
		EndTimestamp   int64
		Pattern        string
		levelPresence
	}{
		// Test filtering by log severity.
		{log.Severity_INFO, 0, 0, 0, "", levelPresence{true, true, true}},
		{log.Severity_WARNING, 0, 0, 0, "", levelPresence{true, true, false}},
		{log.Severity_ERROR, 0, 0, 0, "", levelPresence{true, false, false}},
		// // Test entry limit. Ignore Info/Warning/Error filters.
		{log.Severity_INFO, 1, timestamp, timestampEWI, "", levelPresence{false, false, false}},
		{log.Severity_INFO, 2, timestamp, timestampEWI, "", levelPresence{false, false, false}},
		{log.Severity_INFO, 3, timestamp, timestampEWI, "", levelPresence{false, false, false}},
		// Test filtering in different timestamp windows.
		{log.Severity_INFO, 0, timestamp, timestamp, "", levelPresence{false, false, false}},
		{log.Severity_INFO, 0, timestamp, timestampE, "", levelPresence{true, false, false}},
		{log.Severity_INFO, 0, timestampE, timestampEW, "", levelPresence{false, true, false}},
		{log.Severity_INFO, 0, timestampEW, timestampEWI, "", levelPresence{false, false, true}},
		{log.Severity_INFO, 0, timestamp, timestampEW, "", levelPresence{true, true, false}},
		{log.Severity_INFO, 0, timestampE, timestampEWI, "", levelPresence{false, true, true}},
		{log.Severity_INFO, 0, timestamp, timestampEWI, "", levelPresence{true, true, true}},
		// Test filtering by regexp pattern.
		{log.Severity_INFO, 0, 0, 0, "Info", levelPresence{false, false, true}},
		{log.Severity_INFO, 0, 0, 0, "Warning", levelPresence{false, true, false}},
		{log.Severity_INFO, 0, 0, 0, "Error", levelPresence{true, false, false}},
		{log.Severity_INFO, 0, 0, 0, "Info|Error|Warning", levelPresence{true, true, true}},
		{log.Severity_INFO, 0, 0, 0, "Nothing", levelPresence{false, false, false}},
	}

	for i, testCase := range testCases {
		var url bytes.Buffer
		fmt.Fprintf(&url, "logs/local?level=%s", testCase.Level.Name())
		if testCase.MaxEntities > 0 {
			fmt.Fprintf(&url, "&max=%d", testCase.MaxEntities)
		}
		if testCase.StartTimestamp > 0 {
			fmt.Fprintf(&url, "&start_time=%d", testCase.StartTimestamp)
		}
		if testCase.StartTimestamp > 0 {
			fmt.Fprintf(&url, "&end_time=%d", testCase.EndTimestamp)
		}
		if len(testCase.Pattern) > 0 {
			fmt.Fprintf(&url, "&pattern=%s", testCase.Pattern)
		}

		var wrapper serverpb.LogEntriesResponse
		path := url.String()
		if err := getStatusJSONProto(ts, path, &wrapper); err != nil {
			t.Fatal(err)
		}

		if testCase.MaxEntities > 0 {
			if a, e := len(wrapper.Entries), testCase.MaxEntities; a != e {
				t.Errorf("%d expected %d entries, got %d: \n%+v", i, e, a, wrapper.Entries)
			}
		} else {
			var actual levelPresence
			var logsBuf bytes.Buffer
			for _, entry := range wrapper.Entries {
				fmt.Fprintln(&logsBuf, entry.Message)

				switch entry.Message {
				case "TestStatusLocalLogFile test message-Error":
					actual.Error = true
				case "TestStatusLocalLogFile test message-Warning":
					actual.Warning = true
				case "TestStatusLocalLogFile test message-Info":
					actual.Info = true
				}
			}

			if testCase.levelPresence != actual {
				t.Errorf("%d: expected %+v at %s, got:\n%s", i, testCase, path, logsBuf.String())
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

	// First fetch all the node statuses.
	wrapper := serverpb.NodesResponse{}
	if err := getStatusJSONProto(s, "nodes", &wrapper); err != nil {
		t.Fatal(err)
	}
	nodeStatuses := wrapper.Nodes

	if len(nodeStatuses) != 1 {
		t.Errorf("too many node statuses returned - expected:1 actual:%d", len(nodeStatuses))
	}
	if !reflect.DeepEqual(s.node.Descriptor, nodeStatuses[0].Desc) {
		t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", s.node.Descriptor, nodeStatuses[0].Desc)
	}

	// Now fetch each one individually. Loop through the nodeStatuses to use the
	// ids only.
	for _, oldNodeStatus := range nodeStatuses {
		nodeStatus := status.NodeStatus{}
		if err := getStatusJSONProto(s, "nodes/"+oldNodeStatus.Desc.NodeID.String(), &nodeStatus); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(s.node.Descriptor, nodeStatus.Desc) {
			t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", s.node.Descriptor, nodeStatus.Desc)
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

	if _, err := getText(s, s.AdminURL()+statusPrefix+"metrics/"+s.Gossip().GetNodeID().String()); err != nil {
		t.Fatal(err)
	}
}

func TestRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := startServer(t)
	defer ts.Stopper().Stop()

	// Perform a scan to ensure that all the raft groups are initialized.
	if _, err := ts.db.Scan(keys.LocalMax, roachpb.KeyMax, 0); err != nil {
		t.Fatal(err)
	}

	var response serverpb.RangesResponse
	if err := getStatusJSONProto(ts, "ranges/local", &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Ranges) == 0 {
		t.Errorf("didn't get any ranges")
	}
	for _, ri := range response.Ranges {
		// Do some simple validation based on the fact that this is a
		// single-node cluster.
		if ri.RaftState.State != "StateLeader" && ri.RaftState.State != "StateDormant" {
			t.Errorf("expected to be Raft leader or dormant, but was '%s'", ri.RaftState.State)
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
	if err := getStatusJSONProto(s, "raft", &resp); err != nil {
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

	if body, err := getText(s, s.AdminURL()+statusPrefix+"vars"); err != nil {
		t.Fatal(err)
	} else if !bytes.Contains(body, []byte("# TYPE sql_bytesout counter\nsql_bytesout")) {
		t.Errorf("expected sql_bytesout, got: %s", body)
	}
}

func TestSpanStatsResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("")
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
	t.Skip("")
	ts := startServer(t)
	defer ts.Stopper().Stop()

	rpcStopper := stop.NewStopper()
	defer rpcStopper.Stop()
	rpcContext := rpc.NewContext(context.TODO(), ts.RPCContext().Context, ts.Clock(), rpcStopper)
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
