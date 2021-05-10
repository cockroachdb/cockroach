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
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/logtags"
)

func TestFormatCrdbV2(t *testing.T) {
	tm, err := time.Parse(MessageTimeFormat, "060102 15:04:05.654321")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	ctx = logtags.AddTag(ctx, "noval", nil)
	ctx = logtags.AddTag(ctx, "s", "1")
	ctx = logtags.AddTag(ctx, "long", "2")

	defer func(prev int) { crdbV2LongLineLen.set(prev) }(int(crdbV2LongLineLen))
	crdbV2LongLineLen.set(1024)

	longLine := string(bytes.Repeat([]byte("a"), 1030))

	withStack := func(e logEntry) logEntry {
		e.stacks = []byte("this is a fake stack")
		return e
	}
	withBigStack := func(e logEntry) logEntry {
		e.stacks = []byte("this is " + longLine + " fake stack")
		return e
	}

	ev := &eventpb.RenameDatabase{
		CommonEventDetails: eventpb.CommonEventDetails{
			Timestamp: 123,
			EventType: "rename_database",
		},
		DatabaseName:    "hello",
		NewDatabaseName: "world",
	}

	testCases := []logEntry{
		// Header entry.
		func() logEntry {
			e := makeUnstructuredEntry(ctx, 0, 0, 0, true, "hello %s", "world")
			e.header = true
			return e
		}(),

		// Normal (non-header) entries.

		// Empty entry.
		{},
		// Structured entry.
		makeStructuredEntry(ctx, severity.INFO, channel.DEV, 0, ev),
		// Structured entry, with a stack trace.
		withStack(makeStructuredEntry(ctx, severity.INFO, channel.DEV, 0, ev)),

		// Single-line unstructured entries, with and without redaction markers.
		makeUnstructuredEntry(ctx, severity.WARNING, channel.OPS, 0, false, "hello %s", "world"),
		makeUnstructuredEntry(ctx, severity.ERROR, channel.HEALTH, 0, true, "hello %s", "world"),

		// Unstructured entry, with a counter.
		func() logEntry {
			e := makeUnstructuredEntry(ctx, severity.WARNING, channel.OPS, 0, false, "hello %s", "world")
			e.counter = 123
			return e
		}(),

		// Single-line unstructured, followed by a stack trace.
		withStack(makeUnstructuredEntry(ctx, severity.ERROR, channel.HEALTH, 0, true, "hello %s", "stack")),

		// Multi-line unstructured.
		makeUnstructuredEntry(ctx, severity.INFO, channel.DEV, 0, false, "maybe %s", "multi\nline"),
		makeUnstructuredEntry(ctx, severity.INFO, channel.DEV, 0, true, "maybe %s", "multi\nline"),
		// Multi-line unstructured, with a stack tace.
		withStack(makeUnstructuredEntry(ctx, severity.INFO, channel.DEV, 0, true, "maybe %s", "multi\nline with stack")),

		// Many-byte unstructured.
		makeUnstructuredEntry(ctx, severity.INFO, channel.DEV, 0, false, "%s", longLine),
		// Many-byte structured.
		makeStructuredEntry(ctx, severity.INFO, channel.DEV, 0, &eventpb.RenameDatabase{
			CommonEventDetails: eventpb.CommonEventDetails{
				Timestamp: 123,
				EventType: "rename_database",
			},
			DatabaseName: longLine,
		}),
		// Unstructured with long stack trace.
		withBigStack(makeUnstructuredEntry(ctx, severity.ERROR, channel.HEALTH, 0, true, "hello %s", "stack")),
	}

	// We only use the datadriven framework for the ability to rewrite the output.
	datadriven.RunTest(t, "testdata/crdb_v2", func(t *testing.T, _ *datadriven.TestData) string {
		var buf bytes.Buffer
		for _, tc := range testCases {
			// override non-deterministic fields to stabilize the expected output.
			tc.ts = tm.UnixNano()
			tc.line = 123
			tc.gid = 11

			buf.WriteString("#\n")
			f := formatCrdbV2{}
			b := f.formatEntry(tc)
			fmt.Fprintf(&buf, "%s", b.String())
			putBuffer(b)
		}
		return buf.String()
	})

}

func TestFormatCrdbV2LongLineBreaks(t *testing.T) {
	f := formatCrdbV2{}
	datadriven.RunTest(t, "testdata/crdb_v2_break_lines", func(t *testing.T, td *datadriven.TestData) string {
		if td.Cmd != "run" {
			t.Fatalf("unknown command: %s", td.Cmd)
		}
		var maxLen int
		var redactable bool
		td.ScanArgs(t, "maxlen", &maxLen)
		td.ScanArgs(t, "redactable", &redactable)

		defer func(prev int) { crdbV2LongLineLen.set(prev) }(int(crdbV2LongLineLen))
		crdbV2LongLineLen.set(maxLen)

		entry := logEntry{
			payload: entryPayload{
				redactable: redactable,
				message:    td.Input,
			},
		}
		b := f.formatEntry(entry)
		out := b.String()
		putBuffer(b)

		// Sanity check: verify that no payload is longer (in bytes) than the configured max length.
		const prefix1 = "I000101 00:00:00.000000 0 :0  [-]  "
		const prefix2 = "I000101 00:00:00.000000 0 :0 ⋮ [-]  "
		lines := strings.Split(out, "\n")
		for i, l := range lines {
			l = strings.TrimSuffix(l, "\n")
			if len(l) == 0 {
				continue
			}
			l = strings.TrimPrefix(l, prefix1)
			l = strings.TrimPrefix(l, prefix2)
			// Remove the start or continutation marker
			if l[0] != ' ' && l[0] != '|' {
				t.Fatalf("unexpected continuation marker on line %d: %q", i+1, l)
			}
			l = l[1:]
			if len(l) > maxLen {
				t.Fatalf("line too large: %d bytes, expected max %d - %q", len(l), maxLen, l)
			}
		}

		return out
	})
}
