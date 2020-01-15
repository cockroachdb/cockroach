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

var libPQBlacklists = blacklistsForVersion{
	{"v19.2", "libPQBlacklist19_2", libPQBlacklist19_2, "libPQIgnorelist19_2", libPQIgnorelist19_2},
	{"v20.1", "libPQBlacklist20_1", libPQBlacklist20_1, "libPQIgnorelist20_1", libPQIgnorelist20_1},
}

var libPQBlacklist20_1 = blacklist{
	"pq.TestBinaryByteSliceToInt":        "41547",
	"pq.TestBinaryByteSlicetoUUID":       "41547",
	"pq.TestBindError":                   "5807",
	"pq.TestByteaOutputFormats":          "26947",
	"pq.TestCommit":                      "5807",
	"pq.TestConnListen":                  "41522",
	"pq.TestConnUnlisten":                "41522",
	"pq.TestConnUnlistenAll":             "41522",
	"pq.TestContextCancelBegin":          "41335",
	"pq.TestContextCancelExec":           "41335",
	"pq.TestContextCancelQuery":          "41335",
	"pq.TestCopyFromError":               "5807",
	"pq.TestCopyInBinaryError":           "5807",
	"pq.TestCopyInMultipleValues":        "5807",
	"pq.TestCopyInRaiseStmtTrigger":      "5807",
	"pq.TestCopyInStmtAffectedRows":      "5807",
	"pq.TestCopyInTypes":                 "5807",
	"pq.TestCopyInWrongType":             "5807",
	"pq.TestCopyRespLoopConnectionError": "5807",
	"pq.TestEncodeAndParseTs":            "41563",
	"pq.TestErrorDuringStartup":          "41551",
	"pq.TestErrorOnExec":                 "5807",
	"pq.TestErrorOnQuery":                "5807",
	"pq.TestErrorOnQueryRowSimpleQuery":  "5807",
	"pq.TestExec":                        "5807",
	"pq.TestInfinityTimestamp":           "41564",
	"pq.TestIssue186":                    "41558",
	"pq.TestIssue196":                    "41689",
	"pq.TestIssue282":                    "12137",
	"pq.TestIssue494":                    "5807",
	"pq.TestListenerFailedQuery":         "41522",
	"pq.TestListenerListen":              "41522",
	"pq.TestListenerReconnect":           "41522",
	"pq.TestListenerUnlisten":            "41522",
	"pq.TestListenerUnlistenAll":         "41522",
	"pq.TestNotifyExtra":                 "41522",
	"pq.TestPing":                        "35897",
	"pq.TestQueryRowBugWorkaround":       "5807",
	"pq.TestReconnect":                   "35897",
	"pq.TestReturning":                   "5807",
	"pq.TestRowsColumnTypes":             "41688",
	"pq.TestRowsResultTag":               "5807",
	"pq.TestRuntimeParameters":           "12137",
	"pq.TestStringWithNul":               "26366",
}

var libPQBlacklist19_2 = blacklist{
	"pq.TestBinaryByteSliceToInt":        "41547",
	"pq.TestBinaryByteSlicetoUUID":       "41547",
	"pq.TestBindError":                   "5807",
	"pq.TestByteaOutputFormats":          "26947",
	"pq.TestCommit":                      "5807",
	"pq.TestConnListen":                  "41522",
	"pq.TestConnUnlisten":                "41522",
	"pq.TestConnUnlistenAll":             "41522",
	"pq.TestContextCancelBegin":          "41335",
	"pq.TestContextCancelExec":           "41335",
	"pq.TestContextCancelQuery":          "41335",
	"pq.TestCopyFromError":               "5807",
	"pq.TestCopyInBinaryError":           "5807",
	"pq.TestCopyInMultipleValues":        "5807",
	"pq.TestCopyInRaiseStmtTrigger":      "5807",
	"pq.TestCopyInStmtAffectedRows":      "5807",
	"pq.TestCopyInTypes":                 "5807",
	"pq.TestCopyInWrongType":             "5807",
	"pq.TestCopyRespLoopConnectionError": "5807",
	"pq.TestEncodeAndParseTs":            "41563",
	"pq.TestErrorDuringStartup":          "41551",
	"pq.TestErrorOnExec":                 "5807",
	"pq.TestErrorOnQuery":                "5807",
	"pq.TestErrorOnQueryRowSimpleQuery":  "5807",
	"pq.TestExec":                        "5807",
	"pq.TestInfinityTimestamp":           "41564",
	"pq.TestIssue186":                    "41558",
	"pq.TestIssue196":                    "41689",
	"pq.TestIssue282":                    "12137",
	"pq.TestIssue494":                    "5807",
	"pq.TestListenerFailedQuery":         "41522",
	"pq.TestListenerListen":              "41522",
	"pq.TestListenerReconnect":           "41522",
	"pq.TestListenerUnlisten":            "41522",
	"pq.TestListenerUnlistenAll":         "41522",
	"pq.TestNotifyExtra":                 "41522",
	"pq.TestPing":                        "35897",
	"pq.TestQueryRowBugWorkaround":       "5807",
	"pq.TestReconnect":                   "35897",
	"pq.TestReturning":                   "5807",
	"pq.TestRowsColumnTypes":             "41688",
	"pq.TestRowsResultTag":               "5807",
	"pq.TestRuntimeParameters":           "12137",
	"pq.TestStringWithNul":               "26366",
	"pq.TestTimestampWithTimeZone":       "41565",
}

var libPQIgnorelist20_1 = libPQIgnorelist19_2

var libPQIgnorelist19_2 = blacklist{
	// TestFormatTsBacked fails due to not returning an error for accepting a
	// timestamp format that postgres does not.
	"pq.TestFormatTsBackend": "41690",
	// TestTxOptions fails because it attempts to change isolation levels.
	"pq.TestTxOptions": "41690",
}
