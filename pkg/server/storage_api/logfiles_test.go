// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_api_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.ApplicationLayer()

	logCtx := ts.AnnotateCtx(context.Background())

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
	log.Errorf(logCtx, "TestStatusLocalLogFile test message-Error")
	time.Sleep(sleepBuffer)
	timestampE := timeutil.Now().UnixNano()
	time.Sleep(sleepBuffer)
	log.Warningf(logCtx, "TestStatusLocalLogFile test message-Warning")
	time.Sleep(sleepBuffer)
	timestampEW := timeutil.Now().UnixNano()
	time.Sleep(sleepBuffer)
	log.Infof(logCtx, "TestStatusLocalLogFile test message-Info")
	time.Sleep(sleepBuffer)
	timestampEWI := timeutil.Now().UnixNano()

	var wrapper serverpb.LogFilesListResponse
	if err := srvtestutils.GetStatusJSONProto(ts, "logfiles/local", &wrapper); err != nil {
		t.Fatal(err)
	}
	if a, e := len(wrapper.Files), 1; a != e {
		t.Fatalf("expected %d log files; got %d", e, a)
	}

	// Check each individual log can be fetched and is non-empty.
	var foundInfo, foundWarning, foundError bool
	for _, file := range wrapper.Files {
		var wrapper serverpb.LogEntriesResponse
		if err := srvtestutils.GetStatusJSONProto(ts, "logfiles/local/"+file.Name, &wrapper); err != nil {
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
		if err := srvtestutils.GetStatusJSONProto(ts, path, &wrapper); err != nil {
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

// TestStatusLocalLogsTenantFilter checks to ensure that local/logfiles,
// local/logfiles/{filename} and local/log function correctly filter
// logs by tenant ID.
func TestStatusLocalLogsTenantFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if log.V(3) {
		skip.IgnoreLint(t, "Test only works with low verbosity levels")
	}

	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	// This test cares about the number of output files. Ensure
	// there's just one.
	defer sc.SetupSingleFileLogging()()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.ApplicationLayer()

	appTenantID := roachpb.MustMakeTenantID(uint64(2))
	ctxSysTenant, ctxAppTenant := server.TestingMakeLoggingContexts(appTenantID)

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
	log.Errorf(ctxSysTenant, "system tenant msg 1")
	time.Sleep(sleepBuffer)
	log.Errorf(ctxAppTenant, "app tenant msg 1")
	time.Sleep(sleepBuffer)
	log.Warningf(ctxSysTenant, "system tenant msg 2")
	time.Sleep(sleepBuffer)
	log.Warningf(ctxAppTenant, "app tenant msg 2")
	time.Sleep(sleepBuffer)
	log.Infof(ctxSysTenant, "system tenant msg 3")
	time.Sleep(sleepBuffer)
	log.Infof(ctxAppTenant, "app tenant msg 3")
	timestampEnd := timeutil.Now().UnixNano()

	var listFilesResp serverpb.LogFilesListResponse
	if err := srvtestutils.GetStatusJSONProto(ts, "logfiles/local", &listFilesResp); err != nil {
		t.Fatal(err)
	}
	require.Lenf(t, listFilesResp.Files, 1, "expected 1 log files; got %d", len(listFilesResp.Files))

	testCases := []struct {
		name     string
		tenantID roachpb.TenantID
	}{
		{
			name:     "logs for system tenant does not apply filter",
			tenantID: roachpb.SystemTenantID,
		},
		{
			name:     "logs for app tenant applies tenant ID filter",
			tenantID: appTenantID,
		},
	}

	for _, testCase := range testCases {
		// Non-system tenant servers filter to the tenant that they belong to.
		// Set the server tenant ID for this test case.
		ts.RPCContext().TenantID = testCase.tenantID

		var logfilesResp serverpb.LogEntriesResponse
		if err := srvtestutils.GetStatusJSONProto(ts, "logfiles/local/"+listFilesResp.Files[0].Name, &logfilesResp); err != nil {
			t.Fatal(err)
		}
		var logsResp serverpb.LogEntriesResponse
		if err := srvtestutils.GetStatusJSONProto(ts, fmt.Sprintf("logs/local?end_time=%d", timestampEnd), &logsResp); err != nil {
			t.Fatal(err)
		}

		// Run the same set of assertions against both responses, as they are both expected
		// to contain the log entries we're looking for.
		for _, response := range []serverpb.LogEntriesResponse{logfilesResp, logsResp} {
			sysTenantFound, appTenantFound := false, false
			for _, logEntry := range response.Entries {
				if !strings.HasSuffix(logEntry.File, "logfiles_test.go") {
					continue
				}

				if testCase.tenantID != roachpb.SystemTenantID {
					require.Equal(t, logEntry.TenantID, testCase.tenantID.String())
				} else {
					// Logs use the literal system tenant ID when tagging.
					if logEntry.TenantID == fmt.Sprintf("%d", roachpb.SystemTenantID.InternalValue) {
						sysTenantFound = true
					} else if logEntry.TenantID == appTenantID.String() {
						appTenantFound = true
					}
				}
			}
			if testCase.tenantID == roachpb.SystemTenantID {
				require.True(t, sysTenantFound)
				require.True(t, appTenantFound)
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

			srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
			defer srv.Stopper().Stop(context.Background())
			ts := srv.ApplicationLayer()

			// Log something.
			logCtx := ts.AnnotateCtx(context.Background())
			log.Infof(logCtx, "THISISSAFE %s", "THISISUNSAFE")

			// Determine the log file name.
			var wrapper serverpb.LogFilesListResponse
			if err := srvtestutils.GetStatusJSONProto(ts, "logfiles/local", &wrapper); err != nil {
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
								if !strings.HasSuffix(entry.File, "logfiles_test.go") {
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
						if err := srvtestutils.GetStatusJSONProto(ts, logFilesURL, &wrapper); err != nil {
							t.Fatal(err)
						}
						checkEntries(wrapper.Entries)

						// If the test specifies redact=false, check that a non-admin
						// user gets a privilege error.
						if !tc.redact {
							err := srvtestutils.GetStatusJSONProtoWithAdminOption(ts, logFilesURL, &wrapper, false /* isAdmin */)
							if !testutils.IsError(err, "status: 403") {
								t.Fatalf("expected privilege error, got %v", err)
							}
						}

						// Retrieve the log entries using the Logs() RPC.
						// Set a high `max` value to ensure we get the log line we're searching for.
						logsURL := fmt.Sprintf("logs/local?redact=%v&max=5000", tc.redact)
						var wrapper2 serverpb.LogEntriesResponse
						if err := srvtestutils.GetStatusJSONProto(ts, logsURL, &wrapper2); err != nil {
							t.Fatal(err)
						}
						checkEntries(wrapper2.Entries)

						// If the test specifies redact=false, check that a non-admin
						// user gets a privilege error.
						if !tc.redact {
							err := srvtestutils.GetStatusJSONProtoWithAdminOption(ts, logsURL, &wrapper2, false /* isAdmin */)
							if !testutils.IsError(err, "status: 403") {
								t.Fatalf("expected privilege error, got %v", err)
							}
						}
					})
			}
		})
}
