// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/kr/pretty"
)

type testIDPayload struct {
	tenantID   string
	tenantName string
}

func (t testIDPayload) ServerIdentityString(key serverident.ServerIdentificationKey) string {
	switch key {
	case serverident.IdentifyTenantID:
		return t.tenantID
	case serverident.IdentifyTenantName:
		return t.tenantName
	default:
		return ""
	}
}

var _ serverident.ServerIdentificationPayload = (*testIDPayload)(nil)

func TestFormatCrdbV2(t *testing.T) {
	tm, err := time.Parse(MessageTimeFormat, "060102 15:04:05.654321")
	if err != nil {
		t.Fatal(err)
	}

	tm2, err := time.Parse(MessageTimeFormatWithTZ, "060102 17:04:05.654321+020000")
	if err != nil {
		t.Fatal(err)
	}

	if tm.UnixNano() != tm2.UnixNano() {
		t.Fatalf("expected same, got %q vs %q", tm.In(time.UTC), tm2.In(time.UTC))
	}

	emptyCtx := context.Background()

	sysCtx := context.Background()
	sysIDPayload := testIDPayload{tenantID: "1"}
	sysCtx = serverident.ContextWithServerIdentification(sysCtx, sysIDPayload)
	sysCtx = logtags.AddTag(sysCtx, "noval", nil)
	sysCtx = logtags.AddTag(sysCtx, "s", "1")
	sysCtx = logtags.AddTag(sysCtx, "long", "2")

	tenantIDPayload := testIDPayload{tenantID: "2"}
	tenantCtx := context.Background()
	tenantCtx = serverident.ContextWithServerIdentification(tenantCtx, tenantIDPayload)
	tenantCtx = logtags.AddTag(tenantCtx, "noval", nil)
	tenantCtx = logtags.AddTag(tenantCtx, "p", "3")
	tenantCtx = logtags.AddTag(tenantCtx, "longKey", "456")

	namedTenantIDPayload := tenantIDPayload
	namedTenantIDPayload.tenantName = "abc"
	namedTenantCtx := serverident.ContextWithServerIdentification(tenantCtx, namedTenantIDPayload)

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

	ev := &logpb.TestingStructuredLogEvent{
		CommonEventDetails: logpb.CommonEventDetails{
			Timestamp: 123,
			EventType: "rename_database",
		},
		Channel: logpb.Channel_SQL_SCHEMA,
		Event:   "rename from `hello` to `world`",
	}

	testCases := []logEntry{
		// Header entry.
		func() logEntry {
			e := makeUnstructuredEntry(sysCtx, 0, 0, 0, true, "hello %s", "world")
			e.header = true
			return e
		}(),

		// Normal (non-header) entries.

		// Empty entry.
		{},
		// Structured entry.
		makeStructuredEntry(sysCtx, severity.INFO, channel.DEV, 0, ev),
		// Structured entry, with a stack trace.
		withStack(makeStructuredEntry(sysCtx, severity.INFO, channel.DEV, 0, ev)),

		// Single-line unstructured entries, with and without redaction markers.
		makeUnstructuredEntry(sysCtx, severity.WARNING, channel.OPS, 0, false, "hello %s", "world"),
		makeUnstructuredEntry(sysCtx, severity.ERROR, channel.HEALTH, 0, true, "hello %s", "world"),

		// Unstructured entry, with a counter.
		func() logEntry {
			e := makeUnstructuredEntry(sysCtx, severity.WARNING, channel.OPS, 0, false, "hello %s", "world")
			e.counter = 123
			return e
		}(),

		// Single-line unstructured, followed by a stack trace.
		withStack(makeUnstructuredEntry(sysCtx, severity.ERROR, channel.HEALTH, 0, true, "hello %s", "stack")),

		// Multi-line unstructured.
		makeUnstructuredEntry(sysCtx, severity.INFO, channel.DEV, 0, false, "maybe %s", "multi\nline"),
		makeUnstructuredEntry(sysCtx, severity.INFO, channel.DEV, 0, true, "maybe %s", "multi\nline"),
		// Multi-line unstructured, with a stack tace.
		withStack(makeUnstructuredEntry(sysCtx, severity.INFO, channel.DEV, 0, true, "maybe %s", "multi\nline with stack")),

		// Many-byte unstructured.
		makeUnstructuredEntry(sysCtx, severity.INFO, channel.DEV, 0, false, "%s", longLine),
		// Many-byte structured.
		makeStructuredEntry(sysCtx, severity.INFO, channel.DEV, 0, &logpb.TestingStructuredLogEvent{
			CommonEventDetails: logpb.CommonEventDetails{
				Timestamp: 123,
				EventType: "rename_database",
			},
			Channel: logpb.Channel_SQL_SCHEMA,
			Event:   longLine,
		}),
		// Unstructured with long stack trace.
		withBigStack(makeUnstructuredEntry(sysCtx, severity.ERROR, channel.HEALTH, 0, true, "hello %s", "stack")),
		// Secondary tenant entries.
		makeStructuredEntry(tenantCtx, severity.INFO, channel.DEV, 0, ev),
		makeUnstructuredEntry(tenantCtx, severity.WARNING, channel.OPS, 0, false, "hello %s", "world"),
		makeStructuredEntry(namedTenantCtx, severity.INFO, channel.DEV, 0, ev),
		makeUnstructuredEntry(namedTenantCtx, severity.WARNING, channel.OPS, 0, false, "hello %s", "world"),
		// Entries with empty ctx
		makeStructuredEntry(emptyCtx, severity.INFO, channel.DEV, 0, ev),
		makeUnstructuredEntry(emptyCtx, severity.WARNING, channel.OPS, 0, false, "hello %s", "world"),
	}

	// We only use the datadriven framework for the ability to rewrite the output.
	datadriven.RunTest(t, "testdata/crdb_v2", func(t *testing.T, td *datadriven.TestData) string {
		var loc *time.Location
		if arg, ok := td.Arg("tz"); ok {
			var err error
			var tz string
			arg.Scan(t, 0, &tz)
			loc, err = timeutil.LoadLocation(tz)
			if err != nil {
				td.Fatalf(t, "invalid tz: %v", err)
			}
		}

		var buf bytes.Buffer
		for _, tc := range testCases {
			// override non-deterministic fields to stabilize the expected output.
			tc.ts = tm.UnixNano()
			tc.line = 123
			tc.gid = 11

			buf.WriteString("#\n")
			f := formatCrdbV2{loc: loc}
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
			IDPayload: serverident.IDPayload{
				TenantID: "1",
			},
			payload: entryPayload{
				redactable: redactable,
				message:    td.Input,
			},
		}
		b := f.formatEntry(entry)
		out := b.String()
		putBuffer(b)

		// Sanity check: verify that no payload is longer (in bytes) than the configured max length.
		const prefix1 = "I000101 00:00:00.000000 0 :0  [T1]  "
		const prefix2 = "I000101 00:00:00.000000 0 :0 ⋮ [T1]  "
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

func TestCrdbV2Decode(t *testing.T) {
	datadriven.RunTest(t, "testdata/parse",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "log":
				var out strings.Builder

				d, err := NewEntryDecoderWithFormat(strings.NewReader(td.Input), WithMarkedSensitiveData, "crdb-v2")
				if err != nil {
					td.Fatalf(t, "error while constructing decoder: %v", err)
				}

				for {
					var e logpb.Entry
					if err := d.Decode(&e); err != nil {
						if err == io.EOF {
							break
						}
						if errors.Is(err, ErrMalformedLogEntry) {
							fmt.Fprintf(&out, "malformed entry:%# v\n", pretty.Formatter(e))
							continue
						}
						td.Fatalf(t, "error while decoding: %v", err)
					}
					fmt.Fprintf(&out, "%# v\n", pretty.Formatter(e))
				}
				return out.String()
			default:
				t.Fatalf("unknown directive: %q", td.Cmd)
			}
			// unreachable
			return ""
		})
}
