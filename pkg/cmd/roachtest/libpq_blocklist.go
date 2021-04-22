// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

var libPQBlocklists = blocklistsForVersion{
	{"v20.1", "libPQBlocklist20_1", libPQBlocklist20_1, "libPQIgnorelist20_1", libPQIgnorelist20_1},
	{"v20.2", "libPQBlocklist20_2", libPQBlocklist20_2, "libPQIgnorelist20_2", libPQIgnorelist20_2},
}

var libPQBlocklist20_2 = blocklist{
	"pq.ExampleConnectorWithNoticeHandler":           "unknown",
	"pq.TestBinaryByteSliceToInt":                    "41547",
	"pq.TestBinaryByteSlicetoUUID":                   "41547",
	"pq.TestByteaOutputFormats":                      "26947",
	"pq.TestConnListen":                              "41522",
	"pq.TestConnUnlisten":                            "41522",
	"pq.TestConnUnlistenAll":                         "41522",
	"pq.TestConnectorWithNoticeHandler_Simple":       "unknown",
	"pq.TestConnectorWithNotificationHandler_Simple": "unknown",
	"pq.TestContextCancelBegin":                      "41335",
	"pq.TestContextCancelExec":                       "41335",
	"pq.TestContextCancelQuery":                      "41335",
	"pq.TestCopyFromError":                           "5807",
	"pq.TestCopyInRaiseStmtTrigger":                  "5807",
	"pq.TestCopyInTypes":                             "5807",
	"pq.TestCopyRespLoopConnectionError":             "5807",
	"pq.TestEncodeAndParseTs":                        "41563",
	"pq.TestErrorDuringStartup":                      "41551",
	"pq.TestInfinityTimestamp":                       "41564",
	"pq.TestIssue186":                                "41558",
	"pq.TestIssue196":                                "41689",
	"pq.TestIssue282":                                "12137",
	"pq.TestListenerFailedQuery":                     "41522",
	"pq.TestListenerListen":                          "41522",
	"pq.TestListenerReconnect":                       "41522",
	"pq.TestListenerUnlisten":                        "41522",
	"pq.TestListenerUnlistenAll":                     "41522",
	"pq.TestNotifyExtra":                             "41522",
	"pq.TestPing":                                    "35897",
	"pq.TestQueryRowBugWorkaround":                   "5807",
	"pq.TestReconnect":                               "35897",
	"pq.TestRowsColumnTypes":                         "41688",
	"pq.TestRuntimeParameters":                       "12137",
	"pq.TestStringWithNul":                           "26366",
}

var libPQBlocklist20_1 = blocklist{
	"pq.ExampleConnectorWithNoticeHandler":                     "unknown",
	"pq.TestBinaryByteSliceToInt":                              "41547",
	"pq.TestBinaryByteSlicetoUUID":                             "41547",
	"pq.TestByteaOutputFormats":                                "26947",
	"pq.TestConnListen":                                        "41522",
	"pq.TestConnUnlisten":                                      "41522",
	"pq.TestConnUnlistenAll":                                   "41522",
	"pq.TestConnectorWithNoticeHandler_Simple":                 "unknown",
	"pq.TestConnectorWithNotificationHandler_Simple":           "unknown",
	"pq.TestContextCancelBegin":                                "41335",
	"pq.TestContextCancelExec":                                 "41335",
	"pq.TestContextCancelQuery":                                "41335",
	"pq.TestCopyFromError":                                     "5807",
	"pq.TestCopyInRaiseStmtTrigger":                            "5807",
	"pq.TestCopyInTypes":                                       "5807",
	"pq.TestCopyRespLoopConnectionError":                       "5807",
	"pq.TestEncodeAndParseTs":                                  "41563",
	"pq.TestErrorDuringStartup":                                "41551",
	"pq.TestInfinityTimestamp":                                 "41564",
	"pq.TestIssue186":                                          "41558",
	"pq.TestIssue196":                                          "41689",
	"pq.TestIssue282":                                          "12137",
	"pq.TestListenerFailedQuery":                               "41522",
	"pq.TestListenerListen":                                    "41522",
	"pq.TestListenerReconnect":                                 "41522",
	"pq.TestListenerUnlisten":                                  "41522",
	"pq.TestListenerUnlistenAll":                               "41522",
	"pq.TestNotifyExtra":                                       "41522",
	"pq.TestPing":                                              "35897",
	"pq.TestQueryRowBugWorkaround":                             "5807",
	"pq.TestReconnect":                                         "35897",
	"pq.TestRowsColumnTypes":                                   "41688",
	"pq.TestRuntimeParameters":                                 "12137",
	"pq.TestStringWithNul":                                     "26366",
	"pq.TestTimeWithTimezone":                                  "44548",
	"pq.TestTimeWithTimezone/24:00+00_=>_0000-01-02T00:00:00Z": "44548",
	"pq.TestTimeWithTimezone/24:00-04:00_=>_0000-01-02T00:00:00-04:00":   "44548",
	"pq.TestTimeWithTimezone/24:00:00+00_=>_0000-01-02T00:00:00Z":        "44548",
	"pq.TestTimeWithTimezone/24:00:00.0+00_=>_0000-01-02T00:00:00Z":      "44548",
	"pq.TestTimeWithTimezone/24:00:00.000000+00_=>_0000-01-02T00:00:00Z": "44548",
	"pq.TestTimeWithTimezone/24:00Z_=>_0000-01-02T00:00:00Z":             "44548",
	"pq.TestTimeWithoutTimezone":                                         "44548",
	"pq.TestTimeWithoutTimezone/24:00:00.000000_=>_0000-01-02T00:00:00Z": "44548",
	"pq.TestTimeWithoutTimezone/24:00:00.0_=>_0000-01-02T00:00:00Z":      "44548",
	"pq.TestTimeWithoutTimezone/24:00:00_=>_0000-01-02T00:00:00Z":        "44548",
	"pq.TestTimeWithoutTimezone/24:00_=>_0000-01-02T00:00:00Z":           "44548",
}

var libPQIgnorelist20_2 = libPQIgnorelist20_1

// The test names here do not include "pq." since `go test -list` returns
// the test name without "pq.". We use the name returned from `go test -list`
// to ignore the test.
var libPQIgnorelist20_1 = blocklist{
	// TestFormatTsBacked fails due to not returning an error for accepting a
	// timestamp format that postgres does not.
	"TestFormatTsBackend": "41690",
	// TestTxOptions fails because it attempts to change isolation levels.
	"TestTxOptions": "41690",
	// TestCopyInBinaryError is expected to error with:
	// pq: only text format supported for COPY, however no error is returned
	// for CRDB.
	"TestCopyInBinaryError": "63235",
}
