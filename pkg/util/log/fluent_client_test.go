// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
)

func TestFluentClientEncodeEntry(t *testing.T) {
	tm, err := time.Parse(MessageTimeFormat, "060102 15:04:05.654321")
	if err != nil {
		t.Fatal(err)
	}
	testCases := []struct {
		entry    logpb.Entry
		expected string
	}{
		{logpb.Entry{}, `
{"tag":"log_test.dev","c":0,"t":"0.000000000","s":0,"g":0,"f":"","l":0,"n":0,"r":0,"message":""}
`},
		{logpb.Entry{Time: tm.UnixNano(), Severity: severity.INFO, File: "hello", Goroutine: 876, Line: 432, Counter: 543, Redactable: true}, `
{"tag":"log_test.dev","c":0,"t":"1136214245.654321000","s":1,"sev":"I","g":876,"f":"hello","l":432,"n":543,"r":1,"message":""}
`},
		// Check escaping of newlines and other special characters.
		// However generally unicode characters can be preserved.
		{logpb.Entry{Tags: "hai", Message: "hello\nsnowman " + `"☃"`}, `
{"tag":"log_test.dev","c":0,"t":"0.000000000","s":0,"g":0,"f":"","l":0,"n":0,"r":0,"tags":"hai","message":"hello\nsnowman \"☃\""}
`},
	}

	for _, tc := range testCases {
		var f formatFluentJSONCompact
		buf := f.formatEntry(tc.entry, nil)
		exp := strings.TrimPrefix(tc.expected, "\n")
		actual := buf.String()
		putBuffer(buf)
		if exp != actual {
			t.Errorf("expected:\n%s\ngot:\n%s", exp, actual)
		}
	}
}
