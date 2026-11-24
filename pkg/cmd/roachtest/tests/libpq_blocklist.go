// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

var libPQBlocklist = blocklist{
	"pq.TestBinaryByteSliceToInt":                    "41547",
	"pq.TestBinaryByteSlicetoUUID":                   "41547",
	"pq.TestConnListen":                              "41522",
	"pq.TestConnUnlisten":                            "41522",
	"pq.TestConnUnlistenAll":                         "41522",
	"pq.TestConnectorWithNotificationHandler_Simple": "unknown",
	"pq.TestCopyInRaiseStmtTrigger":                  "5807",
	"pq.TestCopyInTypes":                             "5807",
	"pq.TestCopyRespLoopConnectionError":             "5807",
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
	// The following tests fail because they weren't designed for the
	// autocommit_before_ddl behaviour in CRDB.
	"pq.TestCopyInWrongType":      "142038",
	"pq.TestCopyInMultipleValues": "142038",
	"pq.TestCopyFromError":        "142038",
}

// The test names here do not include "pq." since `go test -list` returns
// the test name without "pq.". We use the name returned from `go test -list`
// to ignore the test.
var libPQIgnorelist = blocklist{
	// TestFormatTsBacked fails due to not returning an error for accepting a
	// timestamp format that postgres does not.
	"TestFormatTsBackend": "41690",
	// TestTxOptions fails because it attempts to change isolation levels.
	"TestTxOptions": "41690",
	// TestCopyInBinaryError is expected to error with:
	// pq: only text format supported for COPY, however no error is returned
	// for CRDB.
	"TestCopyInBinaryError": "63235",
	// TestContextCancelExec has a race between context cancellation and query
	// execution.
	// https://github.com/lib/pq/blob/381d253611d666974d43dfa634d29fe16ea9e293/go18_test.go#L92
	"TestContextCancelExec": "102674",
}
