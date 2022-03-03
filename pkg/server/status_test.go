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
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
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
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/catalog"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/kr/pretty"
	io_prometheus_client "github.com/prometheus/client_model/go"
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

func postStatusJSONProtoWithAdminOption(
	ts serverutils.TestServerInterface,
	path string,
	request, response protoutil.Message,
	isAdmin bool,
) error {
	return serverutils.PostJSONProtoWithAdminOption(ts, statusPrefix+path, request, response, isAdmin)
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

func newRPCTestContext(ctx context.Context, ts *TestServer, cfg *base.Config) *rpc.Context {
	var c base.NodeIDContainer
	ctx = logtags.AddTag(ctx, "n", &c)
	rpcContext := rpc.NewContext(ctx, rpc.ContextOptions{
		TenantID: roachpb.SystemTenantID,
		NodeID:   &c,
		Config:   cfg,
		Clock:    ts.Clock(),
		Stopper:  ts.Stopper(),
		Settings: ts.ClusterSettings(),
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

	rootConfig := testutils.NewTestBaseContext(security.RootUserName())
	rpcContext := newRPCTestContext(context.Background(), ts, rootConfig)

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

	// This test cares about the number of output files. Ensure
	// there's just one.
	defer s.SetupSingleFileLogging()()

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
			switch strings.TrimSpace(entry.Message) {
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

				switch strings.TrimSpace(entry.Message) {
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
		expectedMessage    string
		expectedRedactable bool // redactable bit in result entries
	}{
		// Note: all combinations of (redactableLogs, redact) must be tested below.

		// If there were no markers to start with (redactableLogs=false), we
		// introduce markers around the entire message to indicate it's not known to
		// be safe.
		{false, false, `‹THISISSAFE THISISUNSAFE›`, true},
		// redact=true must be conservative and redact everything out if
		// there were no markers to start with (redactableLogs=false).
		{false, true, `‹×›`, false},
		// redact=false keeps whatever was in the log file.
		{true, false, `THISISSAFE ‹THISISUNSAFE›`, true},
		// Whether or not to keep the redactable markers has no influence
		// on the output of redaction, just on the presence of the
		// "redactable" marker. In any case no information is leaked.
		{true, true, `THISISSAFE ‹×›`, true},
	}

	testutils.RunTrueAndFalse(t, "redactableLogs",
		func(t *testing.T, redactableLogs bool) {
			s := log.ScopeWithoutShowLogs(t)
			defer s.Close(t)

			// This test cares about the number of output files. Ensure
			// there's just one.
			defer s.SetupSingleFileLogging()()

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
				t.Run(fmt.Sprintf("redact=%v", tc.redact),
					func(t *testing.T) {
						// checkEntries asserts that the redaction results are
						// those expected in tc.
						checkEntries := func(entries []logpb.Entry) {
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
						logFilesURL := fmt.Sprintf("logfiles/local/%s?redact=%v", file.Name, tc.redact)
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
						logsURL := fmt.Sprintf("logs/local?redact=%v", tc.redact)
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

	wrapper := serverpb.NodesResponse{}

	// Check that the node statuses cannot be accessed via a non-admin account.
	if err := getStatusJSONProtoWithAdminOption(s, "nodes", &wrapper, false /* isAdmin */); !testutils.IsError(err, "status: 403") {
		t.Fatalf("expected privilege error, got %v", err)
	}

	// Now fetch all the node statuses as admin.
	if err := getStatusJSONProto(s, "nodes", &wrapper); err != nil {
		t.Fatal(err)
	}
	nodeStatuses := wrapper.Nodes

	if len(nodeStatuses) != 1 {
		t.Errorf("too many node statuses returned - expected:1 actual:%d", len(nodeStatuses))
	}
	if !s.node.Descriptor.Equal(&nodeStatuses[0].Desc) {
		t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", s.node.Descriptor, nodeStatuses[0].Desc)
	}

	// Now fetch each one individually. Loop through the nodeStatuses to use the
	// ids only.
	for _, oldNodeStatus := range nodeStatuses {
		nodeStatus := statuspb.NodeStatus{}
		nodeURL := "nodes/" + oldNodeStatus.Desc.NodeID.String()
		// Check that the node statuses cannot be accessed via a non-admin account.
		if err := getStatusJSONProtoWithAdminOption(s, nodeURL, &nodeStatus, false /* isAdmin */); !testutils.IsError(err, "status: 403") {
			t.Fatalf("expected privilege error, got %v", err)
		}

		// Now access that node's status.
		if err := getStatusJSONProto(s, nodeURL, &nodeStatus); err != nil {
			t.Fatal(err)
		}
		if !s.node.Descriptor.Equal(&nodeStatus.Desc) {
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

// walkAllSections invokes the visitor on each of the ChartSections nestled under
// the input one.
func walkAllSections(chartCatalog []catalog.ChartSection, visit func(c *catalog.ChartSection)) {
	for _, c := range chartCatalog {
		visit(&c)
		for _, ic := range c.Subsections {
			visit(ic)
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
		t.Errorf(`The following metrics need to be added to the chart catalog
		    (pkg/ts/catalog/chart_catalog.go): %v`, metricNames)
	}

	internalTSDBMetricNamesWithoutPrefix := map[string]struct{}{}
	for _, name := range catalog.AllInternalTimeseriesMetricNames() {
		name = strings.TrimPrefix(name, "cr.node.")
		name = strings.TrimPrefix(name, "cr.store.")
		internalTSDBMetricNamesWithoutPrefix[name] = struct{}{}
	}
	walkAllSections(chartCatalog, func(cs *catalog.ChartSection) {
		for _, chart := range cs.Charts {
			for _, metric := range chart.Metrics {
				if *metric.MetricType.Enum() != io_prometheus_client.MetricType_HISTOGRAM {
					continue
				}
				// We have a histogram. Make sure that it is properly represented in
				// AllInternalTimeseriesMetricNames(). It's not a complete check but good enough in
				// practice. Ideally we wouldn't require `histogramMetricsNames` and
				// the associated manual step when adding a histogram. See:
				// https://github.com/cockroachdb/cockroach/issues/64373
				_, ok := internalTSDBMetricNamesWithoutPrefix[metric.Name+"-p50"]
				if !ok {
					t.Errorf("histogram %s needs to be added to `catalog.histogramMetricsNames` manually",
						metric.Name)
				}
			}
		}
	})
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

func TestHotRanges2Response(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ts := startServer(t)
	defer ts.Stopper().Stop(context.Background())

	var hotRangesResp serverpb.HotRangesResponseV2
	if err := postStatusJSONProto(ts, "v2/hotranges", &serverpb.HotRangesRequest{}, &hotRangesResp); err != nil {
		t.Fatal(err)
	}
	if len(hotRangesResp.Ranges) == 0 {
		t.Fatalf("didn't get hot range responses from any nodes")
	}
	lastQPS := math.MaxFloat64
	for _, r := range hotRangesResp.Ranges {
		if r.RangeID == 0 {
			t.Errorf("unexpected empty range id: %d", r.RangeID)
		}
		if r.QPS > lastQPS {
			t.Errorf("unexpected increase in qps between ranges; prev=%.2f, current=%.2f", lastQPS, r.QPS)
		}
		lastQPS = r.QPS
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

func TestTenantRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	ts := startServer(t)
	defer ts.Stopper().Stop(ctx)

	t.Run("returns error when TenantID not set in ctx", func(t *testing.T) {
		rpcStopper := stop.NewStopper()
		defer rpcStopper.Stop(ctx)

		conn, err := ts.rpcContext.GRPCDialNode(ts.ServingRPCAddr(), ts.NodeID(), rpc.DefaultClass).Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		client := serverpb.NewStatusClient(conn)
		_, err = client.TenantRanges(ctx, &serverpb.TenantRangesRequest{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no tenant ID found in context")
	})
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
// /_status/vars and /_status/load endpoints.
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
	if body, err := getText(s, s.AdminURL()+statusPrefix+"load"); err != nil {
		t.Fatal(err)
	} else if !bytes.Contains(body, []byte("# TYPE sys_cpu_user_ns gauge\nsys_cpu_user_ns")) {
		t.Errorf("expected sys_cpu_user_ns, got: %s", body)
	}
}

// TestStatusVarsTxnMetrics verifies that the metrics from the /_status/vars
// endpoint for txns and the special cockroach_restart savepoint are correct.
func TestStatusVarsTxnMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer db.Close()
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec("BEGIN;" +
		"SAVEPOINT cockroach_restart;" +
		"SELECT 1;" +
		"RELEASE SAVEPOINT cockroach_restart;" +
		"ROLLBACK;"); err != nil {
		t.Fatal(err)
	}

	body, err := getText(s, s.AdminURL()+statusPrefix+"vars")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte("sql_txn_begin_count 1")) {
		t.Errorf("expected `sql_txn_begin_count 1`, got: %s", body)
	}
	if !bytes.Contains(body, []byte("sql_restart_savepoint_count 1")) {
		t.Errorf("expected `sql_restart_savepoint_count 1`, got: %s", body)
	}
	if !bytes.Contains(body, []byte("sql_restart_savepoint_release_count 1")) {
		t.Errorf("expected `sql_restart_savepoint_release_count 1`, got: %s", body)
	}
	if !bytes.Contains(body, []byte("sql_txn_commit_count 1")) {
		t.Errorf("expected `sql_txn_commit_count 1`, got: %s", body)
	}
	if !bytes.Contains(body, []byte("sql_txn_rollback_count 0")) {
		t.Errorf("expected `sql_txn_rollback_count 0`, got: %s", body)
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
	rpcContext := newRPCTestContext(ctx, ts, ts.RPCContext().Config)
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

	rootConfig := testutils.NewTestBaseContext(security.RootUserName())
	rpcContext := newRPCTestContext(context.Background(), ts, rootConfig)
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

func TestStatusAPICombinedTransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	params.Knobs.SpanConfig = &spanconfig.TestingKnobs{ManagerDisableJobCreation: true} // TODO(irfansharif): #74919.
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: params,
	})
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
			fingerprinted: `INSERT INTO posts VALUES (_, '_')`,
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
	if err := getStatusJSONProto(firstServerProto, "combinedstmts", &resp); err != nil {
		t.Fatal(err)
	}

	// Construct a map of all the statement fingerprint IDs.
	statementFingerprintIDs := make(map[roachpb.StmtFingerprintID]bool, len(resp.Statements))
	for _, respStatement := range resp.Statements {
		statementFingerprintIDs[respStatement.ID] = true
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
		// Ensure all statementFingerprintIDs comprised by the Transaction Response can be
		// linked to StatementFingerprintIDs for statements in the response.
		for _, stmtFingerprintID := range respTransaction.StatsData.StatementFingerprintIDs {
			if _, found := statementFingerprintIDs[stmtFingerprintID]; !found {
				t.Fatalf("app: %s, expected stmtFingerprintID: %d not found in StatementResponse.", appName, stmtFingerprintID)
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

	// Construct a map of all the statement fingerprint IDs.
	statementFingerprintIDs := make(map[roachpb.StmtFingerprintID]bool, len(resp.Statements))
	for _, respStatement := range resp.Statements {
		statementFingerprintIDs[respStatement.ID] = true
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
		// Ensure all statementFingerprintIDs comprised by the Transaction Response can be
		// linked to StatementFingerprintIDs for statements in the response.
		for _, stmtFingerprintID := range respTransaction.StatsData.StatementFingerprintIDs {
			if _, found := statementFingerprintIDs[stmtFingerprintID]; !found {
				t.Fatalf("app: %s, expected stmtFingerprintID: %d not found in StatementResponse.", appName, stmtFingerprintID)
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

func TestStatusAPITransactionStatementFingerprintIDsTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: params,
	})
	defer testCluster.Stopper().Stop(context.Background())

	firstServerProto := testCluster.Server(0)
	thirdServerSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))
	testingApp := "testing"

	thirdServerSQL.Exec(t, `CREATE DATABASE db; CREATE TABLE db.t();`)
	thirdServerSQL.Exec(t, fmt.Sprintf(`SET application_name = "%s"`, testingApp))

	maxStmtFingerprintIDsLen := int(sqlstats.TxnStatsNumStmtFingerprintIDsToRecord.Get(
		&firstServerProto.ExecutorConfig().(sql.ExecutorConfig).Settings.SV))

	// Construct 2 transaction queries that include an absurd number of statements.
	// These two queries have the same first 1000 statements, but should still have
	// different fingerprints, as fingerprints take into account all
	// statementFingerprintIDs (unlike the statementFingerprintIDs stored on the
	// proto response, which are capped).
	testQuery1 := "BEGIN;"
	for i := 0; i < maxStmtFingerprintIDsLen+1; i++ {
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
		if len(respTransaction.StatsData.StatementFingerprintIDs) != maxStmtFingerprintIDsLen {
			t.Fatalf("unexpected length of StatementFingerprintIDs. expected:%d, got:%d",
				maxStmtFingerprintIDsLen, len(respTransaction.StatsData.StatementFingerprintIDs))
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

	// Aug 30 2021 19:50:00 GMT+0000
	aggregatedTs := int64(1630353000)
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					AOSTClause:  "AS OF SYSTEM TIME '-1us'",
					StubTimeNow: func() time.Time { return timeutil.Unix(aggregatedTs, 0) },
				},
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // TODO(irfansharif): #74919.
				},
			},
		},
	})
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
			fingerprinted: `INSERT INTO posts VALUES (_, '_')`,
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

	testPath := func(path string, expectedStmts []string) {
		// Hit query endpoint.
		if err := getStatusJSONProtoWithAdminOption(firstServerProto, path, &resp, false); err != nil {
			t.Fatal(err)
		}

		// See if the statements returned are what we executed.
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
			if len(respStatement.Stats.SensitiveInfo.MostRecentPlanDescription.Name) == 0 {
				// Ensure that we populate the explain plan.
				t.Fatal("expected MostRecentPlanDescription to be populated")
			}
			statementsInResponse = append(statementsInResponse, respStatement.Key.KeyData.Query)
		}

		sort.Strings(expectedStmts)
		sort.Strings(statementsInResponse)

		if !reflect.DeepEqual(expectedStmts, statementsInResponse) {
			t.Fatalf("expected queries\n\n%v\n\ngot queries\n\n%v\n%s",
				expectedStmts, statementsInResponse, pretty.Sprint(resp))
		}
	}

	var expectedStatements []string
	for _, stmt := range statements {
		var expectedStmt = stmt.stmt
		if stmt.fingerprinted != "" {
			expectedStmt = stmt.fingerprinted
		}
		expectedStatements = append(expectedStatements, expectedStmt)
	}

	// Grant VIEWACTIVITY.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", authenticatedUserNameNoAdmin().Normalized()))

	// Test no params.
	testPath("statements", expectedStatements)
	// Test combined=true forwards to CombinedStatements
	testPath(fmt.Sprintf("statements?combined=true&start=%d", aggregatedTs+60), nil)

	// Remove VIEWACTIVITY so we can test with just the VIEWACTIVITYREDACTED role.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", authenticatedUserNameNoAdmin().Normalized()))
	// Grant VIEWACTIVITYREDACTED.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", authenticatedUserNameNoAdmin().Normalized()))

	// Test no params.
	testPath("statements", expectedStatements)
	// Test combined=true forwards to CombinedStatements
	testPath(fmt.Sprintf("statements?combined=true&start=%d", aggregatedTs+60), nil)
}

func TestStatusAPICombinedStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Aug 30 2021 19:50:00 GMT+0000
	aggregatedTs := int64(1630353000)
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					AOSTClause:  "AS OF SYSTEM TIME '-1us'",
					StubTimeNow: func() time.Time { return timeutil.Unix(aggregatedTs, 0) },
				},
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // TODO(irfansharif): #74919.
				},
			},
		},
	})
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
			fingerprinted: `INSERT INTO posts VALUES (_, '_')`,
		},
		{stmt: `SELECT * FROM posts`},
	}

	for _, stmt := range statements {
		thirdServerSQL.Exec(t, stmt.stmt)
	}

	var resp serverpb.StatementsResponse
	// Test that non-admin without VIEWACTIVITY privileges cannot access.
	err := getStatusJSONProtoWithAdminOption(firstServerProto, "combinedstmts", &resp, false)
	if !testutils.IsError(err, "status: 403") {
		t.Fatalf("expected privilege error, got %v", err)
	}

	testPath := func(path string, expectedStmts []string) {
		// Hit query endpoint.
		if err := getStatusJSONProtoWithAdminOption(firstServerProto, path, &resp, false); err != nil {
			t.Fatal(err)
		}

		// See if the statements returned are what we executed.
		var statementsInResponse []string
		for _, respStatement := range resp.Statements {
			if respStatement.Key.KeyData.Failed {
				// We ignore failed statements here as the INSERT statement can fail and
				// be automatically retried, confusing the test success check.
				continue
			}
			if strings.HasPrefix(respStatement.Key.KeyData.App, catconstants.InternalAppNamePrefix) {
				// CombinedStatementStats should filter out internal queries.
				t.Fatalf("unexpected internal query: %s", respStatement.Key.KeyData.Query)
			}
			if strings.HasPrefix(respStatement.Key.KeyData.Query, "ALTER USER") {
				// Ignore the ALTER USER ... VIEWACTIVITY statement.
				continue
			}

			if len(respStatement.Stats.SensitiveInfo.MostRecentPlanDescription.Name) == 0 {
				// Ensure that we populate the explain plan.
				t.Fatal("expected MostRecentPlanDescription to be populated")
			}

			statementsInResponse = append(statementsInResponse, respStatement.Key.KeyData.Query)
		}

		sort.Strings(expectedStmts)
		sort.Strings(statementsInResponse)

		if !reflect.DeepEqual(expectedStmts, statementsInResponse) {
			t.Fatalf("expected queries\n\n%v\n\ngot queries\n\n%v\n%s",
				expectedStmts, statementsInResponse, pretty.Sprint(resp))
		}
	}

	var expectedStatements []string
	for _, stmt := range statements {
		var expectedStmt = stmt.stmt
		if stmt.fingerprinted != "" {
			expectedStmt = stmt.fingerprinted
		}
		expectedStatements = append(expectedStatements, expectedStmt)
	}

	// Grant VIEWACTIVITY.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", authenticatedUserNameNoAdmin().Normalized()))

	// Test with no query params.
	testPath("combinedstmts", expectedStatements)

	oneMinAfterAggregatedTs := aggregatedTs + 60
	// Test with end = 1 min after aggregatedTs; should give the same results as get all.
	testPath(fmt.Sprintf("combinedstmts?end=%d", oneMinAfterAggregatedTs), expectedStatements)
	// Test with start = 1 hour before aggregatedTs  end = 1 min after aggregatedTs; should give same results as get all.
	testPath(fmt.Sprintf("combinedstmts?start=%d&end=%d", aggregatedTs-3600, oneMinAfterAggregatedTs), expectedStatements)
	// Test with start = 1 min after aggregatedTs; should give no results
	testPath(fmt.Sprintf("combinedstmts?start=%d", oneMinAfterAggregatedTs), nil)

	// Remove VIEWACTIVITY so we can test with just the VIEWACTIVITYREDACTED role.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", authenticatedUserNameNoAdmin().Normalized()))
	// Grant VIEWACTIVITYREDACTED.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", authenticatedUserNameNoAdmin().Normalized()))

	// Test with no query params.
	testPath("combinedstmts", expectedStatements)
	// Test with end = 1 min after aggregatedTs; should give the same results as get all.
	testPath(fmt.Sprintf("combinedstmts?end=%d", oneMinAfterAggregatedTs), expectedStatements)
	// Test with start = 1 hour before aggregatedTs  end = 1 min after aggregatedTs; should give same results as get all.
	testPath(fmt.Sprintf("combinedstmts?start=%d&end=%d", aggregatedTs-3600, oneMinAfterAggregatedTs), expectedStatements)
	// Test with start = 1 min after aggregatedTs; should give no results
	testPath(fmt.Sprintf("combinedstmts?start=%d", oneMinAfterAggregatedTs), nil)
}

func TestStatusAPIStatementDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// The liveness session might expire before the stress race can finish.
	skip.UnderStressRace(t, "expensive tests")

	// Aug 30 2021 19:50:00 GMT+0000
	aggregatedTs := int64(1630353000)
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLStatsKnobs: &sqlstats.TestingKnobs{
					AOSTClause:  "AS OF SYSTEM TIME '-1us'",
					StubTimeNow: func() time.Time { return timeutil.Unix(aggregatedTs, 0) },
				},
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true,
				},
			},
		},
	})
	defer testCluster.Stopper().Stop(context.Background())

	firstServerProto := testCluster.Server(0)
	thirdServerSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))

	statements := []string{
		`set application_name = 'first-app'`,
		`CREATE DATABASE roachblog`,
		`SET database = roachblog`,
		`CREATE TABLE posts (id INT8 PRIMARY KEY, body STRING)`,
		`INSERT INTO posts VALUES (1, 'foo')`,
		`INSERT INTO posts VALUES (2, 'foo')`,
		`INSERT INTO posts VALUES (3, 'foo')`,
		`SELECT * FROM posts`,
	}

	for _, stmt := range statements {
		thirdServerSQL.Exec(t, stmt)
	}
	fingerprintID := roachpb.ConstructStatementFingerprintID(`INSERT INTO posts VALUES (_, '_')`,
		false, true, `roachblog`)
	path := fmt.Sprintf(`stmtdetails/%v`, fingerprintID)

	var resp serverpb.StatementDetailsResponse
	// Test that non-admin without VIEWACTIVITY or VIEWACTIVITYREDACTED privileges cannot access.
	err := getStatusJSONProtoWithAdminOption(firstServerProto, path, &resp, false)
	if !testutils.IsError(err, "status: 403") {
		t.Fatalf("expected privilege error, got %v", err)
	}

	type resultValues struct {
		totalCount        int
		aggregatedTsCount int
		planHashCount     int
		appNames          []string
	}

	testPath := func(path string, expected resultValues) {
		err := getStatusJSONProtoWithAdminOption(firstServerProto, path, &resp, false)
		require.NoError(t, err)
		require.Equal(t, int64(expected.totalCount), resp.Statement.Stats.Count)
		require.Equal(t, expected.aggregatedTsCount, len(resp.StatementsPerAggregatedTs))
		require.Equal(t, expected.planHashCount, len(resp.StatementsPerPlanHash))
		require.Equal(t, expected.appNames, resp.Statement.AppNames)
	}

	// Grant VIEWACTIVITY.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", authenticatedUserNameNoAdmin().Normalized()))

	// Test with no query params.
	testPath(
		path,
		resultValues{
			totalCount:        3,
			aggregatedTsCount: 1,
			planHashCount:     1,
			appNames:          []string{"first-app"},
		})
	// Execute same fingerprint id statement on a different application
	statements = []string{
		`set application_name = 'second-app'`,
		`INSERT INTO posts VALUES (4, 'foo')`,
		`INSERT INTO posts VALUES (5, 'foo')`,
	}
	for _, stmt := range statements {
		thirdServerSQL.Exec(t, stmt)
	}

	oneMinAfterAggregatedTs := aggregatedTs + 60

	testData := []struct {
		path           string
		expectedResult resultValues
	}{
		{ // Test with no query params.
			path: path,
			expectedResult: resultValues{
				totalCount:        5,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app", "second-app"}},
		},
		{ // Test with end = 1 min after aggregatedTs; should give the same results as get all.
			path: fmt.Sprintf("%v?end=%d", path, oneMinAfterAggregatedTs),
			expectedResult: resultValues{
				totalCount:        5,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app", "second-app"}},
		},
		{ // Test with start = 1 hour before aggregatedTs  end = 1 min after aggregatedTs; should give same results as get all.
			path: fmt.Sprintf("%v?start=%d&end=%d", path, aggregatedTs-3600, oneMinAfterAggregatedTs),
			expectedResult: resultValues{
				totalCount:        5,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app", "second-app"}},
		},
		{ // Test with start = 1 min after aggregatedTs; should give no results.
			path: fmt.Sprintf("%v?start=%d", path, oneMinAfterAggregatedTs),
			expectedResult: resultValues{
				totalCount:        0,
				aggregatedTsCount: 0,
				planHashCount:     0,
				appNames:          []string{}},
		},
		{ // Test with one app_name.
			path: fmt.Sprintf("%v?app_names=first-app", path),
			expectedResult: resultValues{
				totalCount:        3,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app"}},
		},
		{ // Test with another app_name.
			path: fmt.Sprintf("%v?app_names=second-app", path),
			expectedResult: resultValues{
				totalCount:        2,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"second-app"}},
		},
		{ // Test with both app_names.
			path: fmt.Sprintf("%v?app_names=first-app&app_names=second-app", path),
			expectedResult: resultValues{
				totalCount:        5,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app", "second-app"}},
		},
		{ // Test with non-existing app_name.
			path: fmt.Sprintf("%v?app_names=non-existing", path),
			expectedResult: resultValues{
				totalCount:        0,
				aggregatedTsCount: 0,
				planHashCount:     0,
				appNames:          []string{}},
		},
		{ // Test with app_name, start and end time.
			path: fmt.Sprintf("%v?start=%d&end=%d&app_names=first-app&app_names=second-app", path, aggregatedTs-3600, oneMinAfterAggregatedTs),
			expectedResult: resultValues{
				totalCount:        5,
				aggregatedTsCount: 1,
				planHashCount:     1,
				appNames:          []string{"first-app", "second-app"}},
		},
	}

	for _, test := range testData {
		testPath(test.path, test.expectedResult)
	}

	// Remove VIEWACTIVITY so we can test with just the VIEWACTIVITYREDACTED role.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", authenticatedUserNameNoAdmin().Normalized()))
	// Grant VIEWACTIVITYREDACTED.
	thirdServerSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", authenticatedUserNameNoAdmin().Normalized()))

	for _, test := range testData {
		testPath(test.path, test.expectedResult)
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
			myUser := authenticatedUserNameNoAdmin()
			expectedErrOnListingRootSessions := "does not have permission to view sessions from user"
			if requestWithAdmin {
				myUser = authenticatedUserName()
				expectedErrOnListingRootSessions = ""
			}

			// HTTP requests respect the authenticated username from the HTTP session.
			testCases := []struct {
				endpoint    string
				expectedErr string
			}{
				{"local_sessions", ""},
				{"sessions", ""},
				{fmt.Sprintf("local_sessions?username=%s", myUser.Normalized()), ""},
				{fmt.Sprintf("sessions?username=%s", myUser.Normalized()), ""},
				{"local_sessions?username=" + security.RootUser, expectedErrOnListingRootSessions},
				{"sessions?username=" + security.RootUser, expectedErrOnListingRootSessions},
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
	rootConfig := testutils.NewTestBaseContext(security.RootUserName())
	rpcContext := newRPCTestContext(ctx, ts, rootConfig)
	url := ts.ServingRPCAddr()
	nodeID := ts.NodeID()
	conn, err := rpcContext.GRPCDialNode(url, nodeID, rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	for _, user := range []string{"", authenticatedUser, security.RootUser} {
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

func TestListActivitySecurity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ts := s.(*TestServer)
	defer ts.Stopper().Stop(ctx)

	expectedErrNoPermission := "this operation requires the VIEWACTIVITY or VIEWACTIVITYREDACTED role options"
	contentionMsg := &serverpb.ListContentionEventsResponse{}
	flowsMsg := &serverpb.ListDistSQLFlowsResponse{}
	getErrors := func(msg protoutil.Message) []serverpb.ListActivityError {
		switch r := msg.(type) {
		case *serverpb.ListContentionEventsResponse:
			return r.Errors
		case *serverpb.ListDistSQLFlowsResponse:
			return r.Errors
		default:
			t.Fatal("unexpected message type")
			return nil
		}
	}

	// HTTP requests respect the authenticated username from the HTTP session.
	testCases := []struct {
		endpoint                       string
		expectedErr                    string
		requestWithAdmin               bool
		requestWithViewActivityGranted bool
		response                       protoutil.Message
	}{
		{"local_contention_events", expectedErrNoPermission, false, false, contentionMsg},
		{"contention_events", expectedErrNoPermission, false, false, contentionMsg},
		{"local_contention_events", "", true, false, contentionMsg},
		{"contention_events", "", true, false, contentionMsg},
		{"local_contention_events", "", false, true, contentionMsg},
		{"contention_events", "", false, true, contentionMsg},
		{"local_distsql_flows", expectedErrNoPermission, false, false, flowsMsg},
		{"distsql_flows", expectedErrNoPermission, false, false, flowsMsg},
		{"local_distsql_flows", "", true, false, flowsMsg},
		{"distsql_flows", "", true, false, flowsMsg},
		{"local_distsql_flows", "", false, true, flowsMsg},
		{"distsql_flows", "", false, true, flowsMsg},
	}
	myUser := authenticatedUserNameNoAdmin().Normalized()
	for _, tc := range testCases {
		if tc.requestWithViewActivityGranted {
			// Note that for this query to work, it is crucial that
			// getStatusJSONProtoWithAdminOption below is called at least once,
			// on the previous test case, so that the user exists.
			_, err := db.Exec(fmt.Sprintf("ALTER USER %s VIEWACTIVITY", myUser))
			require.NoError(t, err)
		}
		err := getStatusJSONProtoWithAdminOption(s, tc.endpoint, tc.response, tc.requestWithAdmin)
		responseErrors := getErrors(tc.response)
		if tc.expectedErr == "" {
			if err != nil || len(responseErrors) > 0 {
				t.Errorf("unexpected failure listing the activity; error: %v; response errors: %v",
					err, responseErrors)
			}
		} else {
			respErr := "<no error>"
			if len(responseErrors) > 0 {
				respErr = responseErrors[0].Message
			}
			if !testutils.IsError(err, tc.expectedErr) &&
				!strings.Contains(respErr, tc.expectedErr) {
				t.Errorf("did not get expected error %q when listing the activity from %s: %v",
					tc.expectedErr, tc.endpoint, err)
			}
		}
		if tc.requestWithViewActivityGranted {
			_, err := db.Exec(fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", myUser))
			require.NoError(t, err)
		}
	}

	// gRPC requests behave as root and thus are always allowed.
	rootConfig := testutils.NewTestBaseContext(security.RootUserName())
	rpcContext := newRPCTestContext(ctx, ts, rootConfig)
	url := ts.ServingRPCAddr()
	nodeID := ts.NodeID()
	conn, err := rpcContext.GRPCDialNode(url, nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)
	{
		request := &serverpb.ListContentionEventsRequest{}
		if resp, err := client.ListLocalContentionEvents(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing local contention events; error: %v; response errors: %v",
				err, resp.Errors)
		}
		if resp, err := client.ListContentionEvents(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing contention events; error: %v; response errors: %v",
				err, resp.Errors)
		}
	}
	{
		request := &serverpb.ListDistSQLFlowsRequest{}
		if resp, err := client.ListLocalDistSQLFlows(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing local distsql flows; error: %v; response errors: %v",
				err, resp.Errors)
		}
		if resp, err := client.ListDistSQLFlows(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing distsql flows; error: %v; response errors: %v",
				err, resp.Errors)
		}
	}
}

func TestMergeDistSQLRemoteFlows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	flowIDs := make([]execinfrapb.FlowID, 4)
	for i := range flowIDs {
		flowIDs[i].UUID = uuid.FastMakeV4()
	}
	sort.Slice(flowIDs, func(i, j int) bool {
		return bytes.Compare(flowIDs[i].GetBytes(), flowIDs[j].GetBytes()) < 0
	})
	ts := make([]time.Time, 4)
	for i := range ts {
		ts[i] = timeutil.Now()
	}

	for _, tc := range []struct {
		a        []serverpb.DistSQLRemoteFlows
		b        []serverpb.DistSQLRemoteFlows
		expected []serverpb.DistSQLRemoteFlows
	}{
		// a is empty
		{
			a: []serverpb.DistSQLRemoteFlows{},
			b: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
						{NodeID: 2, Timestamp: ts[2], Status: serverpb.DistSQLRemoteFlows_QUEUED},
						{NodeID: 3, Timestamp: ts[3], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
			},
			expected: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
						{NodeID: 2, Timestamp: ts[2], Status: serverpb.DistSQLRemoteFlows_QUEUED},
						{NodeID: 3, Timestamp: ts[3], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
			},
		},
		// b is empty
		{
			a: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
						{NodeID: 2, Timestamp: ts[2], Status: serverpb.DistSQLRemoteFlows_QUEUED},
						{NodeID: 3, Timestamp: ts[3], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
			},
			b: []serverpb.DistSQLRemoteFlows{},
			expected: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
						{NodeID: 2, Timestamp: ts[2], Status: serverpb.DistSQLRemoteFlows_QUEUED},
						{NodeID: 3, Timestamp: ts[3], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
			},
		},
		// both non-empty with some intersections
		{
			a: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
						{NodeID: 2, Timestamp: ts[2], Status: serverpb.DistSQLRemoteFlows_QUEUED},
						{NodeID: 3, Timestamp: ts[3], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
				{
					FlowID: flowIDs[2],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 3, Timestamp: ts[3], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
				{
					FlowID: flowIDs[3],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0], Status: serverpb.DistSQLRemoteFlows_QUEUED},
					},
				},
			},
			b: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0], Status: serverpb.DistSQLRemoteFlows_QUEUED},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0], Status: serverpb.DistSQLRemoteFlows_QUEUED},
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
						{NodeID: 2, Timestamp: ts[2], Status: serverpb.DistSQLRemoteFlows_QUEUED},
					},
				},
				{
					FlowID: flowIDs[3],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
						{NodeID: 2, Timestamp: ts[2], Status: serverpb.DistSQLRemoteFlows_QUEUED},
					},
				},
			},
			expected: []serverpb.DistSQLRemoteFlows{
				{
					FlowID: flowIDs[0],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0], Status: serverpb.DistSQLRemoteFlows_QUEUED},
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
						{NodeID: 2, Timestamp: ts[2], Status: serverpb.DistSQLRemoteFlows_QUEUED},
						{NodeID: 3, Timestamp: ts[3], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
				{
					FlowID: flowIDs[1],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0], Status: serverpb.DistSQLRemoteFlows_QUEUED},
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
						{NodeID: 2, Timestamp: ts[2], Status: serverpb.DistSQLRemoteFlows_QUEUED},
					},
				},
				{
					FlowID: flowIDs[2],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 3, Timestamp: ts[3], Status: serverpb.DistSQLRemoteFlows_RUNNING},
					},
				},
				{
					FlowID: flowIDs[3],
					Infos: []serverpb.DistSQLRemoteFlows_Info{
						{NodeID: 0, Timestamp: ts[0], Status: serverpb.DistSQLRemoteFlows_QUEUED},
						{NodeID: 1, Timestamp: ts[1], Status: serverpb.DistSQLRemoteFlows_RUNNING},
						{NodeID: 2, Timestamp: ts[2], Status: serverpb.DistSQLRemoteFlows_QUEUED},
					},
				},
			},
		},
	} {
		require.Equal(t, tc.expected, mergeDistSQLRemoteFlows(tc.a, tc.b))
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

func TestCreateStatementDiagnosticsReportWithViewActivityOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := sqlutils.MakeSQLRunner(sqlDB)

	if err := getStatusJSONProtoWithAdminOption(s, "stmtdiagreports", &serverpb.CreateStatementDiagnosticsReportRequest{}, false); err != nil {
		if !testutils.IsError(err, "status: 403") {
			t.Fatalf("expected privilege error, got %v", err)
		}
	}

	// Grant VIEWACTIVITY and all test should work.
	db.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", authenticatedUserNameNoAdmin().Normalized()))
	req := &serverpb.CreateStatementDiagnosticsReportRequest{
		StatementFingerprint: "INSERT INTO test VALUES (_)",
	}
	var resp serverpb.CreateStatementDiagnosticsReportResponse
	if err := postStatusJSONProtoWithAdminOption(s, "stmtdiagreports", req, &resp, false); err != nil {
		t.Fatal(err)
	}
	var respGet serverpb.StatementDiagnosticsReportsResponse
	if err := getStatusJSONProtoWithAdminOption(s, "stmtdiagreports", &respGet, false); err != nil {
		t.Fatal(err)
	}
	if respGet.Reports[0].StatementFingerprint != req.StatementFingerprint {
		t.Fatal("statement diagnostics request was not persisted")
	}

	// Grant VIEWACTIVITYREDACTED and all test should get permission errors.
	db.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", authenticatedUserNameNoAdmin().Normalized()))

	if err := postStatusJSONProtoWithAdminOption(s, "stmtdiagreports", req, &resp, false); err != nil {
		if !testutils.IsError(err, "status: 403") {
			t.Fatalf("expected privilege error, got %v", err)
		}
	}
	if err := getStatusJSONProtoWithAdminOption(s, "stmtdiagreports", &respGet, false); err != nil {
		if !testutils.IsError(err, "status: 403") {
			t.Fatalf("expected privilege error, got %v", err)
		}
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
}

func TestStatementDiagnosticsDoesNotReturnExpiredRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := sqlutils.MakeSQLRunner(sqlDB)

	statementFingerprint := "INSERT INTO test VALUES (_)"
	expiresAfter := 5 * time.Millisecond

	// Create statement diagnostics request with defined expiry time.
	req := &serverpb.CreateStatementDiagnosticsReportRequest{
		StatementFingerprint: statementFingerprint,
		MinExecutionLatency:  500 * time.Millisecond,
		ExpiresAfter:         expiresAfter,
	}
	var resp serverpb.CreateStatementDiagnosticsReportResponse
	if err := postStatusJSONProto(s, "stmtdiagreports", req, &resp); err != nil {
		t.Fatal(err)
	}

	// Wait for request to expire.
	time.Sleep(expiresAfter)

	// Check that created statement diagnostics report is incomplete.
	report := db.QueryStr(t, `
SELECT completed
FROM system.statement_diagnostics_requests
WHERE statement_fingerprint = $1`, statementFingerprint)

	require.Equal(t, report[0][0], "false")

	// Check that expired report is not returned in API response.
	var respGet serverpb.StatementDiagnosticsReportsResponse
	if err := getStatusJSONProto(s, "stmtdiagreports", &respGet); err != nil {
		t.Fatal(err)
	}

	for _, report := range respGet.Reports {
		require.NotEqual(t, report.StatementFingerprint, statementFingerprint)
	}
}

func TestJobStatusResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ts := startServer(t)
	defer ts.Stopper().Stop(context.Background())

	rootConfig := testutils.NewTestBaseContext(security.RootUserName())
	rpcContext := newRPCTestContext(context.Background(), ts, rootConfig)

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
	jr := ts.JobRegistry().(*jobs.Registry)
	job, err := jr.CreateJobWithTxn(
		ctx,
		jobs.Record{
			Description: "testing",
			Statements:  []string{"SELECT 1"},
			Username:    security.RootUserName(),
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
		jr.MakeJobID(),
		nil)
	if err != nil {
		t.Fatal(err)
	}
	request.JobId = int64(job.ID())
	response, err = client.JobStatus(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, job.ID(), response.Job.Id)
	require.Equal(t, job.Payload(), *response.Job.Payload)
	require.Equal(t, job.Progress(), *response.Job.Progress)
}

func TestRegionsResponseFromNodesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	makeNodeResponseWithLocalities := func(tiers [][]roachpb.Tier) *serverpb.NodesResponse {
		ret := &serverpb.NodesResponse{}
		for _, l := range tiers {
			ret.Nodes = append(
				ret.Nodes,
				statuspb.NodeStatus{
					Desc: roachpb.NodeDescriptor{
						Locality: roachpb.Locality{Tiers: l},
					},
				},
			)
		}
		return ret
	}

	makeTiers := func(region, zone string) []roachpb.Tier {
		return []roachpb.Tier{
			{Key: "region", Value: region},
			{Key: "zone", Value: zone},
		}
	}

	testCases := []struct {
		desc     string
		resp     *serverpb.NodesResponse
		expected *serverpb.RegionsResponse
	}{
		{
			desc: "no nodes with regions",
			resp: makeNodeResponseWithLocalities([][]roachpb.Tier{
				{{Key: "a", Value: "a"}},
				{},
			}),
			expected: &serverpb.RegionsResponse{
				Regions: map[string]*serverpb.RegionsResponse_Region{},
			},
		},
		{
			desc: "nodes, some with AZs",
			resp: makeNodeResponseWithLocalities([][]roachpb.Tier{
				makeTiers("us-east1", "us-east1-a"),
				makeTiers("us-east1", "us-east1-a"),
				makeTiers("us-east1", "us-east1-a"),
				makeTiers("us-east1", "us-east1-b"),

				makeTiers("us-east2", "us-east2-a"),
				makeTiers("us-east2", "us-east2-a"),
				makeTiers("us-east2", "us-east2-a"),

				makeTiers("us-east3", "us-east3-a"),
				makeTiers("us-east3", "us-east3-b"),
				makeTiers("us-east3", "us-east3-b"),
				{{Key: "region", Value: "us-east3"}},

				{{Key: "region", Value: "us-east4"}},
			}),
			expected: &serverpb.RegionsResponse{
				Regions: map[string]*serverpb.RegionsResponse_Region{
					"us-east1": {
						Zones: []string{"us-east1-a", "us-east1-b"},
					},
					"us-east2": {
						Zones: []string{"us-east2-a"},
					},
					"us-east3": {
						Zones: []string{"us-east3-a", "us-east3-b"},
					},
					"us-east4": {
						Zones: []string{},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ret := regionsResponseFromNodesResponse(tc.resp)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestStatusServer_nodeStatusToResp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var nodeStatus = &statuspb.NodeStatus{
		StoreStatuses: []statuspb.StoreStatus{
			{Desc: roachpb.StoreDescriptor{
				Properties: roachpb.StoreProperties{
					Encrypted: true,
					FileStoreProperties: &roachpb.FileStoreProperties{
						Path:   "/secret",
						FsType: "ext4",
					},
				},
			}},
		},
		Desc: roachpb.NodeDescriptor{
			Address: util.UnresolvedAddr{
				NetworkField: "network",
				AddressField: "address",
			},
			Attrs: roachpb.Attributes{
				Attrs: []string{"attr"},
			},
			LocalityAddress: []roachpb.LocalityAddress{{Address: util.UnresolvedAddr{
				NetworkField: "network",
				AddressField: "address",
			}, LocalityTier: roachpb.Tier{Value: "v", Key: "k"}}},
			SQLAddress: util.UnresolvedAddr{
				NetworkField: "network",
				AddressField: "address",
			},
		},
		Args: []string{"args"},
		Env:  []string{"env"},
	}
	resp := nodeStatusToResp(nodeStatus, false)
	require.Empty(t, resp.Args)
	require.Empty(t, resp.Env)
	require.Empty(t, resp.Desc.Address)
	require.Empty(t, resp.Desc.Attrs.Attrs)
	require.Empty(t, resp.Desc.LocalityAddress)
	require.Empty(t, resp.Desc.SQLAddress)
	require.True(t, resp.StoreStatuses[0].Desc.Properties.Encrypted)
	require.NotEmpty(t, resp.StoreStatuses[0].Desc.Properties.FileStoreProperties.FsType)
	require.Empty(t, resp.StoreStatuses[0].Desc.Properties.FileStoreProperties.Path)

	// Now fetch all the node statuses as admin.
	resp = nodeStatusToResp(nodeStatus, true)
	require.NotEmpty(t, resp.Args)
	require.NotEmpty(t, resp.Env)
	require.NotEmpty(t, resp.Desc.Address)
	require.NotEmpty(t, resp.Desc.Attrs.Attrs)
	require.NotEmpty(t, resp.Desc.LocalityAddress)
	require.NotEmpty(t, resp.Desc.SQLAddress)
	require.True(t, resp.StoreStatuses[0].Desc.Properties.Encrypted)
	require.NotEmpty(t, resp.StoreStatuses[0].Desc.Properties.FileStoreProperties.FsType)
	require.NotEmpty(t, resp.StoreStatuses[0].Desc.Properties.FileStoreProperties.Path)
}

func TestStatusAPIContentionEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	ctx := context.Background()
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: params,
	})

	defer testCluster.Stopper().Stop(ctx)

	server1Conn := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))
	server2Conn := sqlutils.MakeSQLRunner(testCluster.ServerConn(1))

	sqlutils.CreateTable(
		t,
		testCluster.ServerConn(0),
		"test",
		"x INT PRIMARY KEY",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	testTableID, err :=
		strconv.Atoi(server1Conn.QueryStr(t, "SELECT 'test.test'::regclass::oid")[0][0])
	require.NoError(t, err)

	server1Conn.Exec(t, "USE test")
	server2Conn.Exec(t, "USE test")

	server1Conn.Exec(t, `
SET TRACING=on;
BEGIN;
UPDATE test SET x = 100 WHERE x = 1;
`)
	server2Conn.Exec(t, `
SET TRACING=on;
BEGIN PRIORITY HIGH;
UPDATE test SET x = 1000 WHERE x = 1;
COMMIT;
SET TRACING=off;
`)
	server1Conn.ExpectErr(
		t,
		"^pq: restart transaction.+",
		`
COMMIT;
SET TRACING=off;
`,
	)

	var resp serverpb.ListContentionEventsResponse
	require.NoError(t,
		getStatusJSONProtoWithAdminOption(
			testCluster.Server(2),
			"contention_events",
			&resp,
			true /* isAdmin */),
	)

	require.GreaterOrEqualf(t, len(resp.Events.IndexContentionEvents), 1,
		"expecting at least 1 contention event, but found none")

	found := false
	for _, event := range resp.Events.IndexContentionEvents {
		if event.TableID == descpb.ID(testTableID) && event.IndexID == descpb.IndexID(1) {
			found = true
			break
		}
	}

	require.True(t, found,
		"expect to find contention event for table %d, but found %+v", testTableID, resp)
}

func TestStatusCancelSessionGatewayMetadataPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(ctx)

	// Start a SQL session as admin on node 1.
	sql0 := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))
	results := sql0.QueryStr(t, "SELECT session_id FROM [SHOW SESSIONS] LIMIT 1")
	sessionID, err := hex.DecodeString(results[0][0])
	require.NoError(t, err)

	// Attempt to cancel that SQL session as non-admin over HTTP on node 2.
	req := &serverpb.CancelSessionRequest{
		SessionID: sessionID,
	}
	resp := &serverpb.CancelSessionResponse{}
	err = postStatusJSONProtoWithAdminOption(testCluster.Server(1), "cancel_session/1", req, resp, false)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "status: 403 Forbidden")
}

func TestStatusAPIListSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	ctx := context.Background()
	testCluster := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: params,
	})
	defer testCluster.Stopper().Stop(ctx)

	serverProto := testCluster.Server(0)
	serverSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	appName := "test_sessions_api"
	serverSQL.Exec(t, fmt.Sprintf(`SET application_name = "%s"`, appName))

	getSessionWithTestAppName := func(response *serverpb.ListSessionsResponse) *serverpb.Session {
		require.NotEmpty(t, response.Sessions)
		for _, s := range response.Sessions {
			if s.ApplicationName == appName {
				return &s
			}
		}
		t.Errorf("expected to find session with app name %s", appName)
		return nil
	}

	userNoAdmin := authenticatedUserNameNoAdmin()
	var resp serverpb.ListSessionsResponse
	// Non-admin without VIEWWACTIVITY or VIEWACTIVITYREDACTED should work and fetch user's own sessions.
	err := getStatusJSONProtoWithAdminOption(serverProto, "sessions", &resp, false)
	require.NoError(t, err)

	// Grant VIEWACTIVITYREDACTED.
	serverSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", userNoAdmin.Normalized()))
	serverSQL.Exec(t, "SELECT 1")
	err = getStatusJSONProtoWithAdminOption(serverProto, "sessions", &resp, false)
	require.NoError(t, err)
	session := getSessionWithTestAppName(&resp)
	require.Empty(t, session.LastActiveQuery)
	require.Equal(t, "SELECT _", session.LastActiveQueryNoConstants)

	// Grant VIEWACTIVITY, VIEWACTIVITYREDACTED should take precedence.
	serverSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", userNoAdmin.Normalized()))
	serverSQL.Exec(t, "SELECT 1, 1")
	err = getStatusJSONProtoWithAdminOption(serverProto, "sessions", &resp, false)
	require.NoError(t, err)
	session = getSessionWithTestAppName(&resp)
	require.Equal(t, appName, session.ApplicationName)
	require.Empty(t, session.LastActiveQuery)
	require.Equal(t, "SELECT _, _", session.LastActiveQueryNoConstants)

	// Remove VIEWACTIVITYREDCATED. User should now see full query.
	serverSQL.Exec(t, fmt.Sprintf("ALTER USER %s NOVIEWACTIVITYREDACTED", userNoAdmin.Normalized()))
	serverSQL.Exec(t, "SELECT 2")
	err = getStatusJSONProtoWithAdminOption(serverProto, "sessions", &resp, false)
	require.NoError(t, err)
	session = getSessionWithTestAppName(&resp)
	require.Equal(t, "SELECT _", session.LastActiveQueryNoConstants)
	require.Equal(t, "SELECT 2", session.LastActiveQuery)
}

func TestTransactionContentionEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, conn1, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlutils.CreateTable(
		t,
		conn1,
		"test",
		"x INT PRIMARY KEY",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)

	conn2 :=
		serverutils.OpenDBConn(t, s.ServingSQLAddr(), "", false /* insecure */, s.Stopper())
	defer func() {
		require.NoError(t, conn2.Close())
	}()

	sqlConn1 := sqlutils.MakeSQLRunner(conn1)
	sqlConn1.Exec(t, "SET CLUSTER SETTING sql.contention.txn_id_cache.max_size = '1GB'")
	sqlConn1.Exec(t, "USE test")
	sqlConn1.Exec(t, "SET application_name='conn1'")

	sqlConn2 := sqlutils.MakeSQLRunner(conn2)
	sqlConn2.Exec(t, "USE test")
	sqlConn2.Exec(t, "SET application_name='conn2'")

	// Start the first transaction.
	sqlConn1.Exec(t, `
	SET TRACING=on;
	BEGIN;
	`)

	txnID1 := sqlConn1.QueryStr(t, `
	SELECT txn_id
	FROM [SHOW TRANSACTIONS]
	WHERE application_name = 'conn1'`)[0][0]

	sqlConn1.Exec(t, "UPDATE test SET x = 100 WHERE x = 1")

	// Start the second transaction with higher priority. This will cause the
	// first transaction to be aborted.
	sqlConn2.Exec(t, `
	SET TRACING=on;
	BEGIN PRIORITY HIGH;
	`)

	txnID2 := sqlConn1.QueryStr(t, `
	SELECT txn_id
	FROM [SHOW TRANSACTIONS]
	WHERE application_name = 'conn2'`)[0][0]

	sqlConn2.Exec(t, `
	UPDATE test SET x = 1000 WHERE x = 1;
	COMMIT;`)

	// Ensure that the first transaction is aborted.
	sqlConn1.ExpectErr(
		t,
		"^pq: restart transaction.+",
		`
		COMMIT;
		SET TRACING=off;`,
	)

	// Sanity check to see the first transaction has been aborted.
	sqlConn1.CheckQueryResults(t, "SELECT * FROM test",
		[][]string{{"1000"}})

	txnIDCache := s.SQLServer().(*sql.Server).GetTxnIDCache()

	// Since contention event store's resolver only retries once in the case of
	// missing txn fingerprint ID for a given txnID, we ensure that the txnIDCache
	// write buffer is properly drained before we go on to test the contention
	// registry.
	testutils.SucceedsSoon(t, func() error {
		txnIDCache.DrainWriteBuffer()

		txnID, err := uuid.FromString(txnID1)
		require.NoError(t, err)

		if _, found := txnIDCache.Lookup(txnID); !found {
			return errors.Newf("expected the txn fingerprint ID for txn %s to be "+
				"stored in txnID cache, but it is not", txnID1)
		}

		txnID, err = uuid.FromString(txnID2)
		require.NoError(t, err)

		if _, found := txnIDCache.Lookup(txnID); !found {
			return errors.Newf("expected the txn fingerprint ID for txn %s to be "+
				"stored in txnID cache, but it is not", txnID2)
		}

		return nil
	})

	testutils.SucceedsWithin(t, func() error {
		err := s.ExecutorConfig().(sql.ExecutorConfig).ContentionRegistry.FlushEventsForTest(ctx)
		require.NoError(t, err)

		notEmpty := sqlConn1.QueryStr(t, `
		SELECT count(*) > 0
		FROM crdb_internal.transaction_contention_events
		WHERE
		  blocking_txn_id = $1::UUID AND
		  waiting_txn_id = $2::UUID AND
		  encode(blocking_txn_fingerprint_id, 'hex') != '0000000000000000' AND
		  encode(waiting_txn_fingerprint_id, 'hex') != '0000000000000000' AND
		  length(contending_key) > 0`, txnID1, txnID2)[0][0]

		if notEmpty != "true" {
			return errors.Newf("expected at least one contention events, but " +
				"none was found")
		}

		return nil
	}, 10*time.Second)

	nonAdminUser := authenticatedUserNameNoAdmin().Normalized()
	adminUser := authenticatedUserName().Normalized()

	// N.B. We need both test users to be created before establishing SQL
	//      connections with their usernames. We use
	//      getStatusJSONProtoWithAdminOption() to implicitly create those
	//      usernames instead of regular CREATE USER statements, since the helper
	//      getStatusJSONProtoWithAdminOption() couldn't handle the case where
	//      those two usernames already exist.
	//      This is the reason why we don't check for returning errors.
	_ = getStatusJSONProtoWithAdminOption(
		s,
		"transactioncontentionevents",
		&serverpb.TransactionContentionEventsResponse{},
		true, /* isAdmin */
	)
	_ = getStatusJSONProtoWithAdminOption(
		s,
		"transactioncontentionevents",
		&serverpb.TransactionContentionEventsResponse{},
		false, /* isAdmin */
	)

	type testCase struct {
		testName             string
		userName             string
		canViewContendingKey bool
		grantPerm            string
		revokePerm           string
		isAdmin              bool
	}

	tcs := []testCase{
		{
			testName:             "nopermission",
			userName:             nonAdminUser,
			canViewContendingKey: false,
		},
		{
			testName:             "viewactivityredacted",
			userName:             nonAdminUser,
			canViewContendingKey: false,
			grantPerm:            fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", nonAdminUser),
			revokePerm:           fmt.Sprintf("ALTER USER %s NOVIEWACTIVITYREDACTED", nonAdminUser),
		},
		{
			testName:             "viewactivity",
			userName:             nonAdminUser,
			canViewContendingKey: true,
			grantPerm:            fmt.Sprintf("ALTER USER %s VIEWACTIVITY", nonAdminUser),
			revokePerm:           fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", nonAdminUser),
		},
		{
			testName:             "viewactivity_and_viewactivtyredacted",
			userName:             nonAdminUser,
			canViewContendingKey: false,
			grantPerm: fmt.Sprintf(`ALTER USER %s VIEWACTIVITY;
																		 ALTER USER %s VIEWACTIVITYREDACTED;`,
				nonAdminUser, nonAdminUser),
			revokePerm: fmt.Sprintf(`ALTER USER %s NOVIEWACTIVITY;
																		 ALTER USER %s NOVIEWACTIVITYREDACTED;`,
				nonAdminUser, nonAdminUser),
		},
		{
			testName:             "adminuser",
			userName:             adminUser,
			canViewContendingKey: true,
			isAdmin:              true,
		},
	}

	expectationStringHelper := func(canViewContendingKey bool) string {
		if canViewContendingKey {
			return "able to view contending keys"
		}
		return "not able to view contending keys"
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			if tc.grantPerm != "" {
				sqlConn1.Exec(t, tc.grantPerm)
			}
			if tc.revokePerm != "" {
				defer sqlConn1.Exec(t, tc.revokePerm)
			}

			expectationStr := expectationStringHelper(tc.canViewContendingKey)
			t.Run("sql_cli", func(t *testing.T) {
				// Check we have proper permission control in SQL CLI. We use internal
				// executor here since we can easily override the username without opening
				// new SQL sessions.
				row, err := s.InternalExecutor().(*sql.InternalExecutor).QueryRowEx(
					ctx,
					"test-contending-key-redaction",
					nil, /* txn */
					sessiondata.InternalExecutorOverride{
						User: security.MakeSQLUsernameFromPreNormalizedString(tc.userName),
					},
					`
				SELECT count(*)
				FROM crdb_internal.transaction_contention_events
				WHERE length(contending_key) > 0`,
				)
				if tc.testName == "nopermission" {
					require.Contains(t, err.Error(), "requires VIEWACTIVITY")
				} else {
					require.NoError(t, err)
					visibleContendingKeysCount := tree.MustBeDInt(row[0])

					require.Equal(t, tc.canViewContendingKey, visibleContendingKeysCount > 0,
						"expected to %s, but %d keys have been retrieved",
						expectationStr, visibleContendingKeysCount)
				}
			})

			t.Run("http", func(t *testing.T) {
				// Check we have proper permission control in RPC/HTTP endpoint.
				resp := serverpb.TransactionContentionEventsResponse{}
				err := getStatusJSONProtoWithAdminOption(
					s,
					"transactioncontentionevents",
					&resp,
					tc.isAdmin,
				)

				if tc.testName == "nopermission" {
					require.Contains(t, err.Error(), "status: 403")
				} else {
					require.NoError(t, err)
				}

				for _, event := range resp.Events {
					require.Equal(t, tc.canViewContendingKey, len(event.BlockingEvent.Key) > 0,
						"expected to %s, but the contending key has length of %d",
						expectationStr,
						len(event.BlockingEvent.Key),
					)
				}
			})

		})
	}
}
