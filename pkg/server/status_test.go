// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/catalog"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getStatusJSONProto(
	ts serverutils.TestServerInterface, path string, response protoutil.Message,
) error {
	return serverutils.GetJSONProto(ts, statusPrefix+path, response)
}

func postStatusJSONProto(
	ts serverutils.TestServerInterface, path string, request, response protoutil.Message,
) error {
	return serverutils.PostJSONProto(ts, statusPrefix+path, request, response)
}

func getStatusJSONProtoWithAdminOption(
	ts serverutils.TestServerInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	return serverutils.GetJSONProtoWithAdminOption(ts, statusPrefix+path, response, isAdmin)
}

// TestStatusLocalStacks verifies that goroutine stack traces are available
// via the /_status/stacks/local endpoint.
func TestStatusLocalStacks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

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
// The content type of the responses is always httputil.JSONContentType.
func TestStatusJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	nodeID := ts.Gossip().NodeID.Get()
	addr, err := ts.Gossip().GetNodeIDAddress(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	sqlAddr, err := ts.Gossip().GetNodeIDSQLAddress(nodeID)
	if err != nil {
		t.Fatal(err)
	}

	var nodes serverpb.NodesResponse
	testutils.SucceedsSoon(t, func() error {
		if err := getStatusJSONProto(s, "nodes", &nodes); err != nil {
			t.Fatal(err)
		}

		if len(nodes.Nodes) == 0 {
			return errors.Errorf("expected non-empty node list, got: %v", nodes)
		}
		return nil
	})

	for _, path := range []string{
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
		if a, e := details.SQLAddress, *sqlAddr; a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
		if a, e := details.BuildInfo, build.GetInfo(); a != e {
			t.Errorf("expected: %v, got: %v", e, a)
		}
	}
}

// TestHealthTelemetry confirms that hits on some status endpoints increment
// feature telemetry counters.
func TestHealthTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	rows, err := db.Query("SELECT * FROM crdb_internal.feature_usage WHERE feature_name LIKE 'monitoring%' AND usage_count > 0;")
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatal(err)
	}

	initialCounts := make(map[string]int)
	for rows.Next() {
		var featureName string
		var usageCount int

		if err := rows.Scan(&featureName, &usageCount); err != nil {
			t.Fatal(err)
		}

		initialCounts[featureName] = usageCount
	}

	var details serverpb.DetailsResponse
	if err := serverutils.GetJSONProto(s, "/health", &details); err != nil {
		t.Fatal(err)
	}
	if _, err := getText(s, s.AdminURL()+statusPrefix+"vars"); err != nil {
		t.Fatal(err)
	}

	expectedCounts := map[string]int{
		"monitoring.prometheus.vars": 1,
		"monitoring.health.details":  1,
	}

	rows2, err := db.Query("SELECT feature_name, usage_count FROM crdb_internal.feature_usage WHERE feature_name LIKE 'monitoring%' AND usage_count > 0;")
	defer func() {
		if err := rows2.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatal(err)
	}

	for rows2.Next() {
		var featureName string
		var usageCount int

		if err := rows2.Scan(&featureName, &usageCount); err != nil {
			t.Fatal(err)
		}

		usageCount -= initialCounts[featureName]
		if count, ok := expectedCounts[featureName]; ok {
			if count != usageCount {
				t.Fatalf("expected %d count for feature %s, got %d", count, featureName, usageCount)
			}
			delete(expectedCounts, featureName)
		}
	}

	if len(expectedCounts) > 0 {
		t.Fatalf("%d expected telemetry counters not emitted", len(expectedCounts))
	}
}

// TestStatusGossipJson ensures that the output response for the full gossip
// info contains the required fields.
func TestStatusGossipJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

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

// TestStatusEngineStatsJson ensures that the output response for the engine
// stats contains the required fields.
func TestStatusEngineStatsJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	s, err := serverutils.StartServerRaw(base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stopper().Stop(context.Background())

	var engineStats serverpb.EngineStatsResponse
	if err := getStatusJSONProto(s, "enginestats/local", &engineStats); err != nil {
		t.Fatal(err)
	}
	if len(engineStats.Stats) != 1 {
		t.Fatal(errors.Errorf("expected one engine stats, got: %v", engineStats))
	}

	if engineStats.Stats[0].EngineType == enginepb.EngineTypePebble ||
		engineStats.Stats[0].EngineType == enginepb.EngineTypeDefault {
		// Pebble does not have RocksDB style TickersAnd Histogram.
		return
	}

	tickers := engineStats.Stats[0].TickersAndHistograms.Tickers
	if len(tickers) == 0 {
		t.Fatal(errors.Errorf("expected non-empty tickers list, got: %v", tickers))
	}
	allTickersZero := true
	for _, ticker := range tickers {
		if ticker != 0 {
			allTickersZero = false
		}
	}
	if allTickersZero {
		t.Fatal(errors.Errorf("expected some tickers nonzero, got: %v", tickers))
	}

	histograms := engineStats.Stats[0].TickersAndHistograms.Histograms
	if len(histograms) == 0 {
		t.Fatal(errors.Errorf("expected non-empty histograms list, got: %v", histograms))
	}
	allHistogramsZero := true
	for _, histogram := range histograms {
		if histogram.Max == 0 {
			allHistogramsZero = false
		}
	}
	if allHistogramsZero {
		t.Fatal(errors.Errorf("expected some histograms nonzero, got: %v", histograms))
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
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				// Now that we allow same node rebalances, disable it in these tests,
				// as they dont expect replicas to move.
				DisableReplicaRebalancing: true,
			},
		},
	})

	ts := tsI.(*TestServer)

	// Make sure the range is spun up with an arbitrary read command. We do not
	// expect a specific response.
	if _, err := kvDB.Get(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}

	// Make sure the node status is available. This is done by forcing stores to
	// publish their status, synchronizing to the event feed with a canary
	// event, and then forcing the server to write summaries immediately.
	if err := ts.node.computePeriodicMetrics(context.Background(), 0); err != nil {
		t.Fatalf("error publishing store statuses: %s", err)
	}

	if err := ts.WriteSummaries(); err != nil {
		t.Fatalf("error writing summaries: %s", err)
	}

	return ts
}

func newRPCTestContext(ts *TestServer, cfg *base.Config) *rpc.Context {
	rpcContext := rpc.NewContext(rpc.ContextOptions{
		TenantID:   roachpb.SystemTenantID,
		AmbientCtx: log.AmbientContext{Tracer: ts.ClusterSettings().Tracer},
		Config:     cfg,
		Clock:      ts.Clock(),
		Stopper:    ts.Stopper(),
		Settings:   ts.ClusterSettings(),
	})
	// Ensure that the RPC client context validates the server cluster ID.
	// This ensures that a test where the server is restarted will not let
	// its test RPC client talk to a server started by an unrelated concurrent test.
	rpcContext.ClusterID.Set(context.Background(), ts.ClusterID())
	return rpcContext
}

// TestStatusGetFiles tests the GetFiles endpoint.
func TestStatusGetFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tempDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	storeSpec := base.StoreSpec{Path: tempDir}

	tsI, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			storeSpec,
		},
	})
	ts := tsI.(*TestServer)
	defer ts.Stopper().Stop(context.Background())

	rootConfig := testutils.NewTestBaseContext(security.RootUser)
	rpcContext := newRPCTestContext(ts, rootConfig)

	url := ts.ServingRPCAddr()
	nodeID := ts.NodeID()
	conn, err := rpcContext.GRPCDialNode(url, nodeID, rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	// Test fetching heap files.
	t.Run("heap", func(t *testing.T) {
		const testFilesNo = 3
		for i := 0; i < testFilesNo; i++ {
			testHeapDir := filepath.Join(storeSpec.Path, "logs", base.HeapProfileDir)
			testHeapFile := filepath.Join(testHeapDir, fmt.Sprintf("heap%d.pprof", i))
			if err := os.MkdirAll(testHeapDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}
			if err := ioutil.WriteFile(testHeapFile, []byte(fmt.Sprintf("I'm heap file %d", i)), 0644); err != nil {
				t.Fatal(err)
			}
		}

		request := serverpb.GetFilesRequest{
			NodeId: "local", Type: serverpb.FileType_HEAP, Patterns: []string{"*"}}
		response, err := client.GetFiles(context.Background(), &request)
		if err != nil {
			t.Fatal(err)
		}

		if a, e := len(response.Files), testFilesNo; a != e {
			t.Errorf("expected %d files(s), found %d", e, a)
		}

		for i, file := range response.Files {
			expectedFileName := fmt.Sprintf("heap%d.pprof", i)
			if file.Name != expectedFileName {
				t.Fatalf("expected file name %s, found %s", expectedFileName, file.Name)
			}
			expectedFileContents := []byte(fmt.Sprintf("I'm heap file %d", i))
			if !bytes.Equal(file.Contents, expectedFileContents) {
				t.Fatalf("expected file contents %s, found %s", expectedFileContents, file.Contents)
			}
		}
	})

	// Test fetching goroutine files.
	t.Run("goroutines", func(t *testing.T) {
		const testFilesNo = 3
		for i := 0; i < testFilesNo; i++ {
			testGoroutineDir := filepath.Join(storeSpec.Path, "logs", base.GoroutineDumpDir)
			testGoroutineFile := filepath.Join(testGoroutineDir, fmt.Sprintf("goroutine_dump%d.txt.gz", i))
			if err := os.MkdirAll(testGoroutineDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}
			if err := ioutil.WriteFile(testGoroutineFile, []byte(fmt.Sprintf("Goroutine dump %d", i)), 0644); err != nil {
				t.Fatal(err)
			}
		}

		request := serverpb.GetFilesRequest{
			NodeId: "local", Type: serverpb.FileType_GOROUTINES, Patterns: []string{"*"}}
		response, err := client.GetFiles(context.Background(), &request)
		if err != nil {
			t.Fatal(err)
		}

		if a, e := len(response.Files), testFilesNo; a != e {
			t.Errorf("expected %d files(s), found %d", e, a)
		}

		for i, file := range response.Files {
			expectedFileName := fmt.Sprintf("goroutine_dump%d.txt.gz", i)
			if file.Name != expectedFileName {
				t.Fatalf("expected file name %s, found %s", expectedFileName, file.Name)
			}
			expectedFileContents := []byte(fmt.Sprintf("Goroutine dump %d", i))
			if !bytes.Equal(file.Contents, expectedFileContents) {
				t.Fatalf("expected file contents %s, found %s", expectedFileContents, file.Contents)
			}
		}
	})

	// Testing path separators in pattern.
	t.Run("path separators", func(t *testing.T) {
		request := serverpb.GetFilesRequest{NodeId: "local", ListOnly: true,
			Type: serverpb.FileType_HEAP, Patterns: []string{"pattern/with/separators"}}
		_, err = client.GetFiles(context.Background(), &request)
		if !testutils.IsError(err, "invalid pattern: cannot have path seperators") {
			t.Errorf("GetFiles: path separators allowed in pattern")
		}
	})

	// Testing invalid filetypes.
	t.Run("filetypes", func(t *testing.T) {
		request := serverpb.GetFilesRequest{NodeId: "local", ListOnly: true,
			Type: -1, Patterns: []string{"*"}}
		_, err = client.GetFiles(context.Background(), &request)
		if !testutils.IsError(err, "unknown file type: -1") {
			t.Errorf("GetFiles: invalid file type allowed")
		}
	})
}

// TestStatusLocalLogs checks to ensure that local/logfiles,
// local/logfiles/{filename} and local/log function
// correctly.
func TestStatusLocalLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if log.V(3) {
		skip.IgnoreLint(t, "Test only works with low verbosity levels")
	}

	s := log.ScopeWithoutShowLogs(t)
	defer s.Close(t)

	ts := startServer(t)
	defer ts.Stopper().Stop(context.Background())

	// Log an error of each main type which we expect to be able to retrieve.
	// The resolution of our log timestamps is such that it's possible to get
	// two subsequent log messages with the same timestamp. This test will fail
	// when that occurs. By adding a small sleep in here after each timestamp to
	// ensures this isn't the case and that the log filtering doesn't filter out
	// the log entires we're looking for. The value of 20 μs was chosen because
	// the log timestamps have a fidelity of 10 μs and thus doubling that should
	// be a sufficient buffer.
	// See util/log/clog.go formatHeader() for more details.
	const sleepBuffer = time.Microsecond * 20
	timestamp := timeutil.Now().UnixNano()
	time.Sleep(sleepBuffer)
	log.Errorf(context.Background(), "TestStatusLocalLogFile test message-Error")
	time.Sleep(sleepBuffer)
	timestampE := timeutil.Now().UnixNano()
	time.Sleep(sleepBuffer)
	log.Warningf(context.Background(), "TestStatusLocalLogFile test message-Warning")
	time.Sleep(sleepBuffer)
	timestampEW := timeutil.Now().UnixNano()
	time.Sleep(sleepBuffer)
	log.Infof(context.Background(), "TestStatusLocalLogFile test message-Info")
	time.Sleep(sleepBuffer)
	timestampEWI := timeutil.Now().UnixNano()

	var wrapper serverpb.LogFilesListResponse
	if err := getStatusJSONProto(ts, "logfiles/local", &wrapper); err != nil {
		t.Fatal(err)
	}
	if a, e := len(wrapper.Files), 1; a != e {
		t.Fatalf("expected %d log files; got %d", e, a)
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
		MaxEntities    int
		StartTimestamp int64
		EndTimestamp   int64
		Pattern        string
		levelPresence
	}{
		// Test filtering by log severity.
		// // Test entry limit. Ignore Info/Warning/Error filters.
		{1, timestamp, timestampEWI, "", levelPresence{false, false, false}},
		{2, timestamp, timestampEWI, "", levelPresence{false, false, false}},
		{3, timestamp, timestampEWI, "", levelPresence{false, false, false}},
		// Test filtering in different timestamp windows.
		{0, timestamp, timestamp, "", levelPresence{false, false, false}},
		{0, timestamp, timestampE, "", levelPresence{true, false, false}},
		{0, timestampE, timestampEW, "", levelPresence{false, true, false}},
		{0, timestampEW, timestampEWI, "", levelPresence{false, false, true}},
		{0, timestamp, timestampEW, "", levelPresence{true, true, false}},
		{0, timestampE, timestampEWI, "", levelPresence{false, true, true}},
		{0, timestamp, timestampEWI, "", levelPresence{true, true, true}},
		// Test filtering by regexp pattern.
		{0, 0, 0, "Info", levelPresence{false, false, true}},
		{0, 0, 0, "Warning", levelPresence{false, true, false}},
		{0, 0, 0, "Error", levelPresence{true, false, false}},
		{0, 0, 0, "Info|Error|Warning", levelPresence{true, true, true}},
		{0, 0, 0, "Nothing", levelPresence{false, false, false}},
	}

	for i, testCase := range testCases {
		var url bytes.Buffer
		fmt.Fprintf(&url, "logs/local?level=")
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

// TestStatusLogRedaction checks that the log file retrieval RPCs
// honor the redaction flags.
func TestStatusLogRedaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		redactableLogs     bool // logging flag
		redact             bool // RPC request flag
		keepRedactable     bool // RPC request flag
		expectedMessage    string
		expectedRedactable bool // redactable bit in result entries
	}{
		// Note: all 2^3 combinations of (redactableLogs, redact,
		// keepRedactable) must be tested below.

		// redact=false, keepredactable=false results in an unsafe "flat"
		// format regardless of whether there were markers in the log
		// file.
		{false, false, false, `THISISSAFE THISISUNSAFE`, false},
		// keepredactable=true, if there were no markers to start with
		// (redactableLogs=false), introduces markers around the entire
		// message to indicate it's not known to be safe.
		{false, false, true, `‹THISISSAFE THISISUNSAFE›`, true},
		// redact=true must be conservative and redact everything out if
		// there were no markers to start with (redactableLogs=false).
		{false, true, false, `‹×›`, false},
		{false, true, true, `‹×›`, false},
		// redact=false, keepredactable=false results in an unsafe "flat"
		// format regardless of whether there were markers in the log
		// file.
		{true, false, false, `THISISSAFE THISISUNSAFE`, false},
		// keepredactable=true, redact=false, keeps whatever was in the
		// log file.
		{true, false, true, `THISISSAFE ‹THISISUNSAFE›`, true},
		// if there were markers in the log to start with, redact=true
		// removes only the unsafe information.
		{true, true, false, `THISISSAFE ‹×›`, false},
		// Whether or not to keep the redactable markers has no influence
		// on the output of redaction, just on the presence of the
		// "redactable" marker. In any case no information is leaked.
		{true, true, true, `THISISSAFE ‹×›`, true},
	}

	testutils.RunTrueAndFalse(t, "redactableLogs",
		func(t *testing.T, redactableLogs bool) {
			s := log.ScopeWithoutShowLogs(t)
			defer s.Close(t)

			// Apply the redactable log boolean for this test.
			defer log.TestingSetRedactable(redactableLogs)()

			ts := startServer(t)
			defer ts.Stopper().Stop(context.Background())

			// Log something.
			log.Infof(context.Background(), "THISISSAFE %s", "THISISUNSAFE")

			// Determine the log file name.
			var wrapper serverpb.LogFilesListResponse
			if err := getStatusJSONProto(ts, "logfiles/local", &wrapper); err != nil {
				t.Fatal(err)
			}
			// We expect only the main log.
			if a, e := len(wrapper.Files), 1; a != e {
				t.Fatalf("expected %d log files; got %d: %+v", e, a, wrapper.Files)
			}
			file := wrapper.Files[0]
			// Assert that the log that's present is not a stderr log.
			if strings.Contains("stderr", file.Name) {
				t.Fatalf("expected main log, found %v", file.Name)
			}

			for _, tc := range testData {
				if tc.redactableLogs != redactableLogs {
					continue
				}
				t.Run(fmt.Sprintf("redact=%v,keepredactable=%v", tc.redact, tc.keepRedactable),
					func(t *testing.T) {
						// checkEntries asserts that the redaction results are
						// those expected in tc.
						checkEntries := func(entries []log.Entry) {
							foundMessage := false
							for _, entry := range entries {
								if !strings.HasSuffix(entry.File, "status_test.go") {
									continue
								}
								foundMessage = true

								assert.Equal(t, tc.expectedMessage, entry.Message)
							}
							if !foundMessage {
								t.Fatalf("did not find expected message from test in log")
							}
						}

						// Retrieve the log entries with the configured flags using
						// the LogFiles() RPC.
						logFilesURL := fmt.Sprintf("logfiles/local/%s?redact=%v&keep_redactable=%v",
							file.Name, tc.redact, tc.keepRedactable)
						var wrapper serverpb.LogEntriesResponse
						if err := getStatusJSONProto(ts, logFilesURL, &wrapper); err != nil {
							t.Fatal(err)
						}
						checkEntries(wrapper.Entries)

						// If the test specifies redact=false, check that a non-admin
						// user gets a privilege error.
						if !tc.redact {
							err := getStatusJSONProtoWithAdminOption(ts, logFilesURL, &wrapper, false /* isAdmin */)
							if !testutils.IsError(err, "status: 403") {
								t.Fatalf("expected privilege error, got %v", err)
							}
						}

						// Retrieve the log entries using the Logs() RPC.
						logsURL := fmt.Sprintf("logs/local?redact=%v&keep_redactable=%v",
							tc.redact, tc.keepRedactable)
						var wrapper2 serverpb.LogEntriesResponse
						if err := getStatusJSONProto(ts, logsURL, &wrapper2); err != nil {
							t.Fatal(err)
						}
						checkEntries(wrapper2.Entries)

						// If the test specifies redact=false, check that a non-admin
						// user gets a privilege error.
						if !tc.redact {
							err := getStatusJSONProtoWithAdminOption(ts, logsURL, &wrapper2, false /* isAdmin */)
							if !testutils.IsError(err, "status: 403") {
								t.Fatalf("expected privilege error, got %v", err)
							}
						}
					})
			}
		})
}

// TestNodeStatusResponse verifies that node status returns the expected
// results.
func TestNodeStatusResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := startServer(t)
	defer s.Stopper().Stop(context.Background())

	// First fetch all the node statuses.
	wrapper := serverpb.NodesResponse{}
	if err := getStatusJSONProto(s, "nodes", &wrapper); err != nil {
		t.Fatal(err)
	}
	nodeStatuses := wrapper.Nodes

	if len(nodeStatuses) != 1 {
		t.Errorf("too many node statuses returned - expected:1 actual:%d", len(nodeStatuses))
	}
	if !proto.Equal(&s.node.Descriptor, &nodeStatuses[0].Desc) {
		t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", s.node.Descriptor, nodeStatuses[0].Desc)
	}

	// Now fetch each one individually. Loop through the nodeStatuses to use the
	// ids only.
	for _, oldNodeStatus := range nodeStatuses {
		nodeStatus := statuspb.NodeStatus{}
		if err := getStatusJSONProto(s, "nodes/"+oldNodeStatus.Desc.NodeID.String(), &nodeStatus); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(&s.node.Descriptor, &nodeStatus.Desc) {
			t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", s.node.Descriptor, nodeStatus.Desc)
		}
	}
}

// TestMetricsRecording verifies that Node statistics are periodically recorded
// as time series data.
func TestMetricsRecording(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Verify that metrics for the current timestamp are recorded. This should
	// be true very quickly even though DefaultMetricsSampleInterval is large,
	// because the server writes an entry eagerly on startup.
	testutils.SucceedsSoon(t, func() error {
		now := s.Clock().PhysicalNow()

		var data roachpb.InternalTimeSeriesData
		for _, keyName := range []string{
			"cr.store.livebytes.1",
			"cr.node.sys.go.allocbytes.1",
		} {
			key := ts.MakeDataKey(keyName, "", ts.Resolution10s, now)
			if err := kvDB.GetProto(ctx, key, &data); err != nil {
				return err
			}
		}
		return nil
	})
}

// TestMetricsEndpoint retrieves the metrics endpoint, which is currently only
// used for development purposes. The metrics within the response are verified
// in other tests.
func TestMetricsEndpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := startServer(t)
	defer s.Stopper().Stop(context.Background())

	if _, err := getText(s, s.AdminURL()+statusPrefix+"metrics/"+s.Gossip().NodeID.String()); err != nil {
		t.Fatal(err)
	}
}

// TestMetricsMetadata ensures that the server's recorder return metrics and
// that each metric has a Name, Help, Unit, and DisplayUnit defined.
func TestMetricsMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := startServer(t)
	defer s.Stopper().Stop(context.Background())

	metricsMetadata := s.recorder.GetMetricsMetadata()

	if len(metricsMetadata) < 200 {
		t.Fatal("s.recorder.GetMetricsMetadata() failed sanity check; didn't return enough metrics.")
	}

	for _, v := range metricsMetadata {
		if v.Name == "" {
			t.Fatal("metric missing name.")
		}
		if v.Help == "" {
			t.Fatalf("%s missing Help.", v.Name)
		}
		if v.Measurement == "" {
			t.Fatalf("%s missing Measurement.", v.Name)
		}
		if v.Unit == 0 {
			t.Fatalf("%s missing Unit.", v.Name)
		}
	}
}

// TestChartCatalog ensures that the server successfully generates the chart catalog.
func TestChartCatalogGen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := startServer(t)
	defer s.Stopper().Stop(context.Background())

	metricsMetadata := s.recorder.GetMetricsMetadata()

	chartCatalog, err := catalog.GenerateCatalog(metricsMetadata)

	if err != nil {
		t.Fatal(err)
	}

	// Ensure each of the 8 constant sections of the chart catalog exist.
	if len(chartCatalog) != 8 {
		t.Fatal("Chart catalog failed to generate.")
	}

	for _, section := range chartCatalog {
		// Ensure that one of the chartSections has defined Subsections.
		if len(section.Subsections) == 0 {
			t.Fatalf(`Chart catalog has missing subsections in %v`, section)
		}
	}
}

// findUndefinedMetrics finds metrics listed in pkg/ts/catalog/chart_catalog.go
// that are not defined. This is most likely caused by a metric being removed.
func findUndefinedMetrics(c *catalog.ChartSection, metadata map[string]metric.Metadata) []string {
	var undefinedMetrics []string
	for _, ic := range c.Charts {
		for _, metric := range ic.Metrics {
			_, ok := metadata[metric.Name]
			if !ok {
				undefinedMetrics = append(undefinedMetrics, metric.Name)
			}
		}
	}

	for _, x := range c.Subsections {
		undefinedMetrics = append(undefinedMetrics, findUndefinedMetrics(x, metadata)...)
	}

	return undefinedMetrics
}

// deleteSeenMetrics removes all metrics in a section from the metricMetadata map.
func deleteSeenMetrics(c *catalog.ChartSection, metadata map[string]metric.Metadata, t *testing.T) {
	// if c.Title == "SQL" {
	// 	t.Log(c)
	// }
	for _, x := range c.Charts {
		if x.Title == "Connections" || x.Title == "Byte I/O" {
			t.Log(x)
		}

		for _, metric := range x.Metrics {
			if metric.Name == "sql.new_conns" || metric.Name == "sql.bytesin" {
				t.Logf("found %v\n", metric.Name)
			}
			_, ok := metadata[metric.Name]
			if ok {
				delete(metadata, metric.Name)
			}
		}
	}

	for _, x := range c.Subsections {
		deleteSeenMetrics(x, metadata, t)
	}
}

// TestChartCatalogMetric ensures that all metrics are included in at least one
// chart, and that every metric included in a chart is still part of the metrics
// registry.
func TestChartCatalogMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := startServer(t)
	defer s.Stopper().Stop(context.Background())

	metricsMetadata := s.recorder.GetMetricsMetadata()

	chartCatalog, err := catalog.GenerateCatalog(metricsMetadata)

	if err != nil {
		t.Fatal(err)
	}

	// Each metric referenced in the chartCatalog must have a definition in metricsMetadata
	var undefinedMetrics []string
	for _, cs := range chartCatalog {
		undefinedMetrics = append(undefinedMetrics, findUndefinedMetrics(&cs, metricsMetadata)...)
	}

	if len(undefinedMetrics) > 0 {
		t.Fatalf(`The following metrics need are no longer present and need to be removed
			from the chart catalog (pkg/ts/catalog/chart_catalog.go):%v`, undefinedMetrics)
	}

	// Each metric in metricsMetadata should have at least one entry in
	// chartCatalog, which we track by deleting the metric from metricsMetadata.
	for _, v := range chartCatalog {
		deleteSeenMetrics(&v, metricsMetadata, t)
	}

	if len(metricsMetadata) > 0 {
		var metricNames []string
		for metricName := range metricsMetadata {
			metricNames = append(metricNames, metricName)
		}
		sort.Strings(metricNames)
		t.Fatalf(`The following metrics need to be added to the chart catalog
		    (pkg/ts/catalog/chart_catalog.go): %v`, metricNames)
	}
}

func TestHotRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ts := startServer(t)
	defer ts.Stopper().Stop(context.Background())

	var hotRangesResp serverpb.HotRangesResponse
	if err := getStatusJSONProto(ts, "hotranges", &hotRangesResp); err != nil {
		t.Fatal(err)
	}
	if len(hotRangesResp.HotRangesByNodeID) == 0 {
		t.Fatalf("didn't get hot range responses from any nodes")
	}

	for nodeID, nodeResp := range hotRangesResp.HotRangesByNodeID {
		if len(nodeResp.Stores) == 0 {
			t.Errorf("didn't get any stores in hot range response from n%d: %v",
				nodeID, nodeResp.ErrorMessage)
		}
		for _, storeResp := range nodeResp.Stores {
			// Only the first store will actually have any ranges on it.
			if storeResp.StoreID != roachpb.StoreID(1) {
				continue
			}
			lastQPS := math.MaxFloat64
			if len(storeResp.HotRanges) == 0 {
				t.Errorf("didn't get any hot ranges in response from n%d,s%d: %v",
					nodeID, storeResp.StoreID, nodeResp.ErrorMessage)
			}
			for _, r := range storeResp.HotRanges {
				if r.Desc.RangeID == 0 || (len(r.Desc.StartKey) == 0 && len(r.Desc.EndKey) == 0) {
					t.Errorf("unexpected empty/unpopulated range descriptor: %+v", r.Desc)
				}
				if r.QueriesPerSecond > lastQPS {
					t.Errorf("unexpected increase in qps between ranges; prev=%.2f, current=%.2f, desc=%v",
						lastQPS, r.QueriesPerSecond, r.Desc)
				}
				lastQPS = r.QueriesPerSecond
			}
		}

	}
}

func TestRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer kvserver.EnableLeaseHistory(100)()
	ts := startServer(t)
	defer ts.Stopper().Stop(context.Background())

	// Perform a scan to ensure that all the raft groups are initialized.
	if _, err := ts.db.Scan(context.Background(), keys.LocalMax, roachpb.KeyMax, 0); err != nil {
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
		if ri.RaftState.State != "StateLeader" && ri.RaftState.State != raftStateDormant {
			t.Errorf("expected to be Raft leader or dormant, but was '%s'", ri.RaftState.State)
		}
		expReplica := roachpb.ReplicaDescriptor{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 1,
		}
		if len(ri.State.Desc.InternalReplicas) != 1 || ri.State.Desc.InternalReplicas[0] != expReplica {
			t.Errorf("unexpected replica list %+v", ri.State.Desc.InternalReplicas)
		}
		if ri.State.Lease == nil || ri.State.Lease.Empty() {
			t.Error("expected a nontrivial Lease")
		}
		if ri.State.LastIndex == 0 {
			t.Error("expected positive LastIndex")
		}
		if len(ri.LeaseHistory) == 0 {
			t.Error("expected at least one lease history entry")
		}
	}
}

func TestRaftDebug(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := startServer(t)
	defer s.Stopper().Stop(context.Background())

	var resp serverpb.RaftDebugResponse
	if err := getStatusJSONProto(s, "raft", &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Ranges) == 0 {
		t.Errorf("didn't get any ranges")
	}

	if len(resp.Ranges) < 3 {
		t.Errorf("expected more than 2 ranges, got %d", len(resp.Ranges))
	}

	reqURI := "raft"
	requestedIDs := []roachpb.RangeID{}
	for id := range resp.Ranges {
		if len(requestedIDs) == 0 {
			reqURI += "?"
		} else {
			reqURI += "&"
		}
		reqURI += fmt.Sprintf("range_ids=%d", id)
		requestedIDs = append(requestedIDs, id)
		if len(requestedIDs) >= 2 {
			break
		}
	}

	if err := getStatusJSONProto(s, reqURI, &resp); err != nil {
		t.Fatal(err)
	}

	// Make sure we get exactly two ranges back.
	if len(resp.Ranges) != 2 {
		t.Errorf("expected exactly two ranges in response, got %d", len(resp.Ranges))
	}

	// Make sure the ranges returned are those requested.
	for _, reqID := range requestedIDs {
		if _, ok := resp.Ranges[reqID]; !ok {
			t.Errorf("request URI was %s, but range ID %d not returned: %+v", reqURI, reqID, resp.Ranges)
		}
	}
}

// TestStatusVars verifies that prometheus metrics are available via the
// /_status/vars endpoint.
func TestStatusVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	if body, err := getText(s, s.AdminURL()+statusPrefix+"vars"); err != nil {
		t.Fatal(err)
	} else if !bytes.Contains(body, []byte("# TYPE sql_bytesout counter\nsql_bytesout")) {
		t.Errorf("expected sql_bytesout, got: %s", body)
	}
}

func TestSpanStatsResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ts := startServer(t)
	defer ts.Stopper().Stop(context.Background())

	httpClient, err := ts.GetAdminAuthenticatedHTTPClient()
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
	if err := httputil.PostJSON(httpClient, url, &request, &response); err != nil {
		t.Fatal(err)
	}
	initialRanges, err := ts.ExpectedInitialRangeCount()
	if err != nil {
		t.Fatal(err)
	}
	if a, e := int(response.RangeCount), initialRanges; a != e {
		t.Errorf("expected %d ranges, found %d", e, a)
	}
}

func TestSpanStatsGRPCResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	ts := startServer(t)
	defer ts.Stopper().Stop(ctx)

	rpcStopper := stop.NewStopper()
	defer rpcStopper.Stop(ctx)
	rpcContext := newRPCTestContext(ts, ts.RPCContext().Config)
	request := serverpb.SpanStatsRequest{
		NodeID:   "1",
		StartKey: []byte(roachpb.RKeyMin),
		EndKey:   []byte(roachpb.RKeyMax),
	}

	url := ts.ServingRPCAddr()
	nodeID := ts.NodeID()
	conn, err := rpcContext.GRPCDialNode(url, nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	response, err := client.SpanStats(ctx, &request)
	if err != nil {
		t.Fatal(err)
	}
	initialRanges, err := ts.ExpectedInitialRangeCount()
	if err != nil {
		t.Fatal(err)
	}
	if a, e := int(response.RangeCount), initialRanges; a != e {
		t.Fatalf("expected %d ranges, found %d", e, a)
	}
}

func TestNodesGRPCResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ts := startServer(t)
	defer ts.Stopper().Stop(context.Background())

	rootConfig := testutils.NewTestBaseContext(security.RootUser)
	rpcContext := newRPCTestContext(ts, rootConfig)
	var request serverpb.NodesRequest

	url := ts.ServingRPCAddr()
	nodeID := ts.NodeID()
	conn, err := rpcContext.GRPCDialNode(url, nodeID, rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	response, err := client.Nodes(context.Background(), &request)
	if err != nil {
		t.Fatal(err)
	}

	if a, e := len(response.Nodes), 1; a != e {
		t.Errorf("expected %d node(s), found %d", e, a)
	}
}

func TestCertificatesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ts := startServer(t)
	defer ts.Stopper().Stop(context.Background())

	var response serverpb.CertificatesResponse
	if err := getStatusJSONProto(ts, "certificates/local", &response); err != nil {
		t.Fatal(err)
	}

	// We expect 5 certificates: CA, node, and client certs for root, testuser, testuser2.
	if a, e := len(response.Certificates), 5; a != e {
		t.Errorf("expected %d certificates, found %d", e, a)
	}

	// Read the certificates from the embedded assets.
	caPath := filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert)
	nodePath := filepath.Join(security.EmbeddedCertsDir, security.EmbeddedNodeCert)

	caFile, err := securitytest.EmbeddedAssets.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}

	nodeFile, err := securitytest.EmbeddedAssets.ReadFile(nodePath)
	if err != nil {
		t.Fatal(err)
	}

	// The response is ordered: CA cert followed by node cert.
	cert := response.Certificates[0]
	if a, e := cert.Type, serverpb.CertificateDetails_CA; a != e {
		t.Errorf("wrong type %s, expected %s", a, e)
	} else if cert.ErrorMessage != "" {
		t.Errorf("expected cert without error, got %v", cert.ErrorMessage)
	} else if a, e := cert.Data, caFile; !bytes.Equal(a, e) {
		t.Errorf("mismatched contents: %s vs %s", a, e)
	}

	cert = response.Certificates[1]
	if a, e := cert.Type, serverpb.CertificateDetails_NODE; a != e {
		t.Errorf("wrong type %s, expected %s", a, e)
	} else if cert.ErrorMessage != "" {
		t.Errorf("expected cert without error, got %v", cert.ErrorMessage)
	} else if a, e := cert.Data, nodeFile; !bytes.Equal(a, e) {
		t.Errorf("mismatched contents: %s vs %s", a, e)
	}
}

func TestDiagnosticsResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	var resp diagnosticspb.DiagnosticReport
	if err := getStatusJSONProto(s, "diagnostics/local", &resp); err != nil {
		t.Fatal(err)
	}

	// The endpoint just serializes result of getReportingInfo() which is already
	// tested elsewhere, so simply verify that we have a non-empty reply.
	if expected, actual := s.NodeID(), resp.Node.NodeID; expected != actual {
		t.Fatalf("expected %v got %v", expected, actual)
	}
}

func TestRangeResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer kvserver.EnableLeaseHistory(100)()
	ts := startServer(t)
	defer ts.Stopper().Stop(context.Background())

	// Perform a scan to ensure that all the raft groups are initialized.
	if _, err := ts.db.Scan(context.Background(), keys.LocalMax, roachpb.KeyMax, 0); err != nil {
		t.Fatal(err)
	}

	var response serverpb.RangeResponse
	if err := getStatusJSONProto(ts, "range/1", &response); err != nil {
		t.Fatal(err)
	}

	// This is a single node cluster, so only expect a single response.
	if e, a := 1, len(response.ResponsesByNodeID); e != a {
		t.Errorf("got the wrong number of responses, expected %d, actual %d", e, a)
	}

	node1Response := response.ResponsesByNodeID[response.NodeID]

	// The response should come back as valid.
	if !node1Response.Response {
		t.Errorf("node1's response returned as false, expected true")
	}

	// The response should include just the one range.
	if e, a := 1, len(node1Response.Infos); e != a {
		t.Errorf("got the wrong number of ranges in the response, expected %d, actual %d", e, a)
	}

	info := node1Response.Infos[0]
	expReplica := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}

	// Check some other values.
	if len(info.State.Desc.InternalReplicas) != 1 || info.State.Desc.InternalReplicas[0] != expReplica {
		t.Errorf("unexpected replica list %+v", info.State.Desc.InternalReplicas)
	}

	if info.State.Lease == nil || info.State.Lease.Empty() {
		t.Error("expected a nontrivial Lease")
	}

	if info.State.LastIndex == 0 {
		t.Error("expected positive LastIndex")
	}

	if len(info.LeaseHistory) == 0 {
		t.Error("expected at least one lease history entry")
	}
}

func TestRemoteDebugModeSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
		},
	})
	ts := s.(*TestServer)
	defer ts.Stopper().Stop(context.Background())

	if _, err := db.Exec(`SET CLUSTER SETTING server.remote_debugging.mode = 'off'`); err != nil {
		t.Fatal(err)
	}

	// Create a split so that there's some records in the system.rangelog table.
	// The test needs them.
	if _, err := db.Exec(
		`create table t(x int primary key);
		alter table t split at values(1);`,
	); err != nil {
		t.Fatal(err)
	}

	// Verify that the remote debugging mode is respected for HTTP requests.
	// This needs to be wrapped in SucceedsSoon because settings changes have to
	// propagate through gossip and thus don't always take effect immediately.
	testutils.SucceedsSoon(t, func() error {
		for _, tc := range []struct {
			path     string
			response protoutil.Message
		}{
			{"gossip/local", &gossip.InfoStatus{}},
			{"allocator/node/local", &serverpb.AllocatorResponse{}},
			{"allocator/range/1", &serverpb.AllocatorResponse{}},
			{"logs/local", &serverpb.LogEntriesResponse{}},
			{"logfiles/local/cockroach.log", &serverpb.LogEntriesResponse{}},
		} {
			err := getStatusJSONProto(ts, tc.path, tc.response)
			if !testutils.IsError(err, "403 Forbidden") {
				return fmt.Errorf("expected '403 Forbidden' error, but %q returned %+v: %v",
					tc.path, tc.response, err)
			}
		}
		return nil
	})

	// But not for grpc requests. The fact that the above gets an error but these
	// don't indicate that the grpc gateway is correctly adding the necessary
	// metadata for differentiating between the two (and that we're correctly
	// interpreting said metadata).
	rootConfig := testutils.NewTestBaseContext(security.RootUser)
	rpcContext := newRPCTestContext(ts, rootConfig)
	url := ts.ServingRPCAddr()
	nodeID := ts.NodeID()
	conn, err := rpcContext.GRPCDialNode(url, nodeID, rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)
	if _, err := client.Gossip(ctx, &serverpb.GossipRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.Allocator(ctx, &serverpb.AllocatorRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.Allocator(ctx, &serverpb.AllocatorRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.AllocatorRange(ctx, &serverpb.AllocatorRangeRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.Logs(ctx, &serverpb.LogsRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.ListLocalSessions(ctx, &serverpb.ListSessionsRequest{}); err != nil {
		t.Error(err)
	}
	if _, err := client.ListSessions(ctx, &serverpb.ListSessionsRequest{}); err != nil {
		t.Error(err)
	}

	// Check that keys are properly omitted from the Ranges, HotRanges, and
	// RangeLog endpoints.
	var rangesResp serverpb.RangesResponse
	if err := getStatusJSONProto(ts, "ranges/local", &rangesResp); err != nil {
		t.Fatal(err)
	}
	if len(rangesResp.Ranges) == 0 {
		t.Errorf("didn't get any ranges")
	}
	for _, ri := range rangesResp.Ranges {
		if ri.Span.StartKey != omittedKeyStr || ri.Span.EndKey != omittedKeyStr ||
			ri.State.ReplicaState.Desc.StartKey != nil || ri.State.ReplicaState.Desc.EndKey != nil {
			t.Errorf("unexpected key value found in RangeInfo: %+v", ri)
		}
	}

	var hotRangesResp serverpb.HotRangesResponse
	if err := getStatusJSONProto(ts, "hotranges", &hotRangesResp); err != nil {
		t.Fatal(err)
	}
	if len(hotRangesResp.HotRangesByNodeID) == 0 {
		t.Errorf("didn't get hot range responses from any nodes")
	}
	for nodeID, nodeResp := range hotRangesResp.HotRangesByNodeID {
		if len(nodeResp.Stores) == 0 {
			t.Errorf("didn't get any stores in hot range response from n%d: %v",
				nodeID, nodeResp.ErrorMessage)
		}
		for _, storeResp := range nodeResp.Stores {
			// Only the first store will actually have any ranges on it.
			if storeResp.StoreID != roachpb.StoreID(1) {
				continue
			}
			if len(storeResp.HotRanges) == 0 {
				t.Errorf("didn't get any hot ranges in response from n%d,s%d: %v",
					nodeID, storeResp.StoreID, nodeResp.ErrorMessage)
			}
			for _, r := range storeResp.HotRanges {
				if r.Desc.StartKey != nil || r.Desc.EndKey != nil {
					t.Errorf("unexpected key value found in hot ranges range descriptor: %+v", r.Desc)
				}
			}
		}
	}

	var rangelogResp serverpb.RangeLogResponse
	if err := getAdminJSONProto(ts, "rangelog", &rangelogResp); err != nil {
		t.Fatal(err)
	}
	if len(rangelogResp.Events) == 0 {
		t.Errorf("didn't get any Events")
	}
	for _, event := range rangelogResp.Events {
		if event.Event.Info.NewDesc != nil {
			if event.Event.Info.NewDesc.StartKey != nil || event.Event.Info.NewDesc.EndKey != nil ||
				event.Event.Info.UpdatedDesc.StartKey != nil || event.Event.Info.UpdatedDesc.EndKey != nil {
				t.Errorf("unexpected key value found in rangelog event: %+v", event)
			}
		}
		if strings.Contains(event.PrettyInfo.NewDesc, "Min-System") ||
			strings.Contains(event.PrettyInfo.UpdatedDesc, "Min-System") {
			t.Errorf("unexpected key value found in rangelog event info: %+v", event.PrettyInfo)
		}
	}
}

func TestStatusAPITransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	thirdServer := testCluster.Server(2)
	pgURL, cleanupGoDB := sqlutils.PGUrl(
		t, thirdServer.ServingSQLAddr(), "CreateConnections" /* prefix */, url.User(security.RootUser))
	defer cleanupGoDB()
	firstServerProto := testCluster.Server(0)

	type testCase struct {
		query         string
		fingerprinted string
		count         int
		shouldRetry   bool
		numRows       int
	}

	testCases := []testCase{
		{query: `CREATE DATABASE roachblog`, count: 1, numRows: 0},
		{query: `SET database = roachblog`, count: 1, numRows: 0},
		{query: `CREATE TABLE posts (id INT8 PRIMARY KEY, body STRING)`, count: 1, numRows: 0},
		{
			query:         `INSERT INTO posts VALUES (1, 'foo')`,
			fingerprinted: `INSERT INTO posts VALUES (_, _)`,
			count:         1,
			numRows:       1,
		},
		{query: `SELECT * FROM posts`, count: 2, numRows: 1},
		{query: `BEGIN; SELECT * FROM posts; SELECT * FROM posts; COMMIT`, count: 3, numRows: 2},
		{
			query:         `BEGIN; SELECT crdb_internal.force_retry('2s'); SELECT * FROM posts; COMMIT;`,
			fingerprinted: `BEGIN; SELECT crdb_internal.force_retry(_); SELECT * FROM posts; COMMIT;`,
			shouldRetry:   true,
			count:         1,
			numRows:       2,
		},
		{
			query:         `BEGIN; SELECT crdb_internal.force_retry('5s'); SELECT * FROM posts; COMMIT;`,
			fingerprinted: `BEGIN; SELECT crdb_internal.force_retry(_); SELECT * FROM posts; COMMIT;`,
			shouldRetry:   true,
			count:         1,
			numRows:       2,
		},
	}

	appNameToTestCase := make(map[string]testCase)

	for i, tc := range testCases {
		appName := fmt.Sprintf("app%d", i)
		appNameToTestCase[appName] = tc

		// Create a brand new connection for each app, so that we don't pollute
		// transaction stats collection with `SET application_name` queries.
		sqlDB, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec(fmt.Sprintf(`SET application_name = "%s"`, appName)); err != nil {
			t.Fatal(err)
		}
		for c := 0; c < tc.count; c++ {
			if _, err := sqlDB.Exec(tc.query); err != nil {
				t.Fatal(err)
			}
		}
		if err := sqlDB.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Hit query endpoint.
	var resp serverpb.StatementsResponse
	if err := getStatusJSONProto(firstServerProto, "statements", &resp); err != nil {
		t.Fatal(err)
	}

	// Construct a map of all the statement IDs.
	statementIDs := make(map[roachpb.StmtID]bool, len(resp.Statements))
	for _, respStatement := range resp.Statements {
		statementIDs[respStatement.ID] = true
	}

	respAppNames := make(map[string]bool)
	for _, respTransaction := range resp.Transactions {
		appName := respTransaction.StatsData.App
		tc, found := appNameToTestCase[appName]
		if !found {
			// Ignore internal queries, they aren't relevant to this test.
			continue
		}
		respAppNames[appName] = true
		// Ensure all statementIDs comprised by the Transaction Response can be
		// linked to StatementIDs for statements in the response.
		for _, stmtID := range respTransaction.StatsData.StatementIDs {
			if _, found := statementIDs[stmtID]; !found {
				t.Fatalf("app: %s, expected stmtID: %s not found in StatementResponse.", appName, stmtID)
			}
		}
		stats := respTransaction.StatsData.Stats
		if tc.count != int(stats.Count) {
			t.Fatalf("app: %s, expected count %d, got %d", appName, tc.count, stats.Count)
		}
		if tc.shouldRetry && respTransaction.StatsData.Stats.MaxRetries == 0 {
			t.Fatalf("app: %s, expected retries, got none\n", appName)
		}

		// Sanity check numeric stat values
		if respTransaction.StatsData.Stats.CommitLat.Mean <= 0 {
			t.Fatalf("app: %s, unexpected mean for commit latency\n", appName)
		}
		if respTransaction.StatsData.Stats.RetryLat.Mean <= 0 && tc.shouldRetry {
			t.Fatalf("app: %s, expected retry latency mean to be non-zero as retries were involved\n", appName)
		}
		if respTransaction.StatsData.Stats.ServiceLat.Mean <= 0 {
			t.Fatalf("app: %s, unexpected mean for service latency\n", appName)
		}
		if respTransaction.StatsData.Stats.NumRows.Mean != float64(tc.numRows) {
			t.Fatalf("app: %s, unexpected number of rows observed. expected: %d, got %d\n",
				appName, tc.numRows, int(respTransaction.StatsData.Stats.NumRows.Mean))
		}
	}

	// Ensure we got transaction statistics for all the queries we sent.
	for appName := range appNameToTestCase {
		if _, found := respAppNames[appName]; !found {
			t.Fatalf("app: %s did not appear in the response\n", appName)
		}
	}
}

func TestStatusAPITransactionStatementIDsTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServerProto := testCluster.Server(0)
	thirdServerSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))
	testingApp := "testing"

	thirdServerSQL.Exec(t, `CREATE DATABASE db; CREATE TABLE db.t();`)
	thirdServerSQL.Exec(t, fmt.Sprintf(`SET application_name = "%s"`, testingApp))

	maxStmtIDsLen := int(sql.TxnStatsNumStmtIDsToRecord.Get(
		&firstServerProto.ExecutorConfig().(sql.ExecutorConfig).Settings.SV))

	// Construct 2 transaction queries that include an absurd number of statements.
	// These two queries have the same first 1000 statements, but should still have
	// different fingerprints, as fingerprints take into account all statementIDs
	// (unlike the statementIDs stored on the proto response, which are capped).
	testQuery1 := "BEGIN;"
	for i := 0; i < maxStmtIDsLen+1; i++ {
		testQuery1 += "SELECT * FROM db.t;"
	}
	testQuery2 := testQuery1 + "SELECT * FROM db.t; COMMIT;"
	testQuery1 += "COMMIT;"

	thirdServerSQL.Exec(t, testQuery1)
	thirdServerSQL.Exec(t, testQuery2)

	// Hit query endpoint.
	var resp serverpb.StatementsResponse
	if err := getStatusJSONProto(firstServerProto, "statements", &resp); err != nil {
		t.Fatal(err)
	}

	txnsFound := 0
	for _, respTransaction := range resp.Transactions {
		appName := respTransaction.StatsData.App
		if appName != testingApp {
			// Only testQuery1 and testQuery2 are relevant to this test.
			continue
		}

		txnsFound++
		if len(respTransaction.StatsData.StatementIDs) != maxStmtIDsLen {
			t.Fatalf("unexpected length of StatementIDs. expected:%d, got:%d",
				maxStmtIDsLen, len(respTransaction.StatsData.StatementIDs))
		}
	}
	if txnsFound != 2 {
		t.Fatalf("transactions were not disambiguated as expected. expected %d txns, got: %d",
			2, txnsFound)
	}
}

func TestStatusAPIStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServerProto := testCluster.Server(0)
	thirdServerSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))

	statements := []struct {
		stmt          string
		fingerprinted string
	}{
		{stmt: `CREATE DATABASE roachblog`},
		{stmt: `SET database = roachblog`},
		{stmt: `CREATE TABLE posts (id INT8 PRIMARY KEY, body STRING)`},
		{
			stmt:          `INSERT INTO posts VALUES (1, 'foo')`,
			fingerprinted: `INSERT INTO posts VALUES (_, _)`,
		},
		{stmt: `SELECT * FROM posts`},
	}

	for _, stmt := range statements {
		thirdServerSQL.Exec(t, stmt.stmt)
	}

	// Test that non-admin without VIEWACTIVITY privileges cannot access.
	var resp serverpb.StatementsResponse
	err := getStatusJSONProtoWithAdminOption(firstServerProto, "statements", &resp, false)
	if !testutils.IsError(err, "status: 403") {
		t.Fatalf("expected privilege error, got %v", err)
	}

	// Grant VIEWACTIVITY.
	thirdServerSQL.Exec(t, "ALTER USER $1 VIEWACTIVITY", authenticatedUserNameNoAdmin)

	// Hit query endpoint.
	if err := getStatusJSONProtoWithAdminOption(firstServerProto, "statements", &resp, false); err != nil {
		t.Fatal(err)
	}

	// See if the statements returned are what we executed.
	var expectedStatements []string
	for _, stmt := range statements {
		var expectedStmt = stmt.stmt
		if stmt.fingerprinted != "" {
			expectedStmt = stmt.fingerprinted
		}
		expectedStatements = append(expectedStatements, expectedStmt)
	}

	var statementsInResponse []string
	for _, respStatement := range resp.Statements {
		if respStatement.Key.KeyData.Failed {
			// We ignore failed statements here as the INSERT statement can fail and
			// be automatically retried, confusing the test success check.
			continue
		}
		if strings.HasPrefix(respStatement.Key.KeyData.App, catconstants.InternalAppNamePrefix) {
			// We ignore internal queries, these are not relevant for the
			// validity of this test.
			continue
		}
		if strings.HasPrefix(respStatement.Key.KeyData.Query, "ALTER USER") {
			// Ignore the ALTER USER ... VIEWACTIVITY statement.
			continue
		}
		statementsInResponse = append(statementsInResponse, respStatement.Key.KeyData.Query)
	}

	sort.Strings(expectedStatements)
	sort.Strings(statementsInResponse)

	if !reflect.DeepEqual(expectedStatements, statementsInResponse) {
		t.Fatalf("expected queries\n\n%v\n\ngot queries\n\n%v\n%s",
			expectedStatements, statementsInResponse, pretty.Sprint(resp))
	}
}

func TestListSessionsSecurity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ts := s.(*TestServer)
	defer ts.Stopper().Stop(context.Background())

	ctx := context.Background()

	for _, requestWithAdmin := range []bool{true, false} {
		t.Run(fmt.Sprintf("admin=%v", requestWithAdmin), func(t *testing.T) {
			myUser := authenticatedUserNameNoAdmin
			expectedErrOnListingRootSessions := "does not have permission to view sessions from user"
			if requestWithAdmin {
				myUser = authenticatedUserName
				expectedErrOnListingRootSessions = ""
			}

			// HTTP requests respect the authenticated username from the HTTP session.
			testCases := []struct {
				endpoint    string
				expectedErr string
			}{
				{"local_sessions", ""},
				{"sessions", ""},
				{fmt.Sprintf("local_sessions?username=%s", myUser), ""},
				{fmt.Sprintf("sessions?username=%s", myUser), ""},
				{"local_sessions?username=root", expectedErrOnListingRootSessions},
				{"sessions?username=root", expectedErrOnListingRootSessions},
			}
			for _, tc := range testCases {
				var response serverpb.ListSessionsResponse
				err := getStatusJSONProtoWithAdminOption(ts, tc.endpoint, &response, requestWithAdmin)
				if tc.expectedErr == "" {
					if err != nil || len(response.Errors) > 0 {
						t.Errorf("unexpected failure listing sessions from %s; error: %v; response errors: %v",
							tc.endpoint, err, response.Errors)
					}
				} else {
					respErr := "<no error>"
					if len(response.Errors) > 0 {
						respErr = response.Errors[0].Message
					}
					if !testutils.IsError(err, tc.expectedErr) &&
						!strings.Contains(respErr, tc.expectedErr) {
						t.Errorf("did not get expected error %q when listing sessions from %s: %v",
							tc.expectedErr, tc.endpoint, err)
					}
				}
			}
		})
	}

	// gRPC requests behave as root and thus are always allowed.
	rootConfig := testutils.NewTestBaseContext(security.RootUser)
	rpcContext := newRPCTestContext(ts, rootConfig)
	url := ts.ServingRPCAddr()
	nodeID := ts.NodeID()
	conn, err := rpcContext.GRPCDialNode(url, nodeID, rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	for _, user := range []string{"", authenticatedUserName, "root"} {
		request := &serverpb.ListSessionsRequest{Username: user}
		if resp, err := client.ListLocalSessions(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing local sessions for %q; error: %v; response errors: %v",
				user, err, resp.Errors)
		}
		if resp, err := client.ListSessions(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing sessions for %q; error: %v; response errors: %v",
				user, err, resp.Errors)
		}
	}
}

func TestCreateStatementDiagnosticsReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	req := &serverpb.CreateStatementDiagnosticsReportRequest{
		StatementFingerprint: "INSERT INTO test VALUES (_)",
	}
	var resp serverpb.CreateStatementDiagnosticsReportResponse
	if err := postStatusJSONProto(s, "stmtdiagreports", req, &resp); err != nil {
		t.Fatal(err)
	}

	var respGet serverpb.StatementDiagnosticsReportsResponse
	if err := getStatusJSONProto(s, "stmtdiagreports", &respGet); err != nil {
		t.Fatal(err)
	}

	if respGet.Reports[0].StatementFingerprint != req.StatementFingerprint {
		t.Fatal("statement diagnostics request was not persisted")
	}
}

func TestStatementDiagnosticsCompleted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	_, err := db.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	if err != nil {
		t.Fatal(err)
	}

	req := &serverpb.CreateStatementDiagnosticsReportRequest{
		StatementFingerprint: "INSERT INTO test VALUES (_)",
	}
	var resp serverpb.CreateStatementDiagnosticsReportResponse
	if err := postStatusJSONProto(s, "stmtdiagreports", req, &resp); err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}

	var respGet serverpb.StatementDiagnosticsReportsResponse
	if err := getStatusJSONProto(s, "stmtdiagreports", &respGet); err != nil {
		t.Fatal(err)
	}

	if respGet.Reports[0].Completed != true {
		t.Fatal("statement diagnostics was not captured")
	}

	var diagRespGet serverpb.StatementDiagnosticsResponse
	diagPath := fmt.Sprintf("stmtdiag/%d", respGet.Reports[0].StatementDiagnosticsId)
	if err := getStatusJSONProto(s, diagPath, &diagRespGet); err != nil {
		t.Fatal(err)
	}

	json := diagRespGet.Diagnostics.Trace
	if json == "" ||
		!strings.Contains(json, "traced statement") ||
		!strings.Contains(json, "statement execution committed the txn") {
		t.Fatal("statement diagnostics did not capture a trace")
	}
}

func TestJobStatusResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ts := startServer(t)
	defer ts.Stopper().Stop(context.Background())

	rootConfig := testutils.NewTestBaseContext(security.RootUser)
	rpcContext := newRPCTestContext(ts, rootConfig)

	url := ts.ServingRPCAddr()
	nodeID := ts.NodeID()
	conn, err := rpcContext.GRPCDialNode(url, nodeID, rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	request := &serverpb.JobStatusRequest{JobId: -1}
	response, err := client.JobStatus(context.Background(), request)
	require.Regexp(t, `job with ID -1 does not exist`, err)
	require.Nil(t, response)

	ctx := context.Background()
	job, err := ts.JobRegistry().(*jobs.Registry).CreateJobWithTxn(
		ctx,
		jobs.Record{
			Description: "testing",
			Statement:   "SELECT 1",
			Username:    "root",
			Details: jobspb.ImportDetails{
				Tables: []jobspb.ImportDetails_Table{
					{
						Desc: &descpb.TableDescriptor{
							ID: 1,
						},
					},
					{
						Desc: &descpb.TableDescriptor{
							ID: 2,
						},
					},
				},
				URIs: []string{"a", "b"},
			},
			Progress:      jobspb.ImportProgress{},
			DescriptorIDs: []descpb.ID{1, 2, 3},
		},
		nil)
	if err != nil {
		t.Fatal(err)
	}
	request.JobId = *job.ID()
	response, err = client.JobStatus(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, *job.ID(), response.Job.Id)
	require.Equal(t, job.Payload(), *response.Job.Payload)
	require.Equal(t, job.Progress(), *response.Job.Progress)
}
