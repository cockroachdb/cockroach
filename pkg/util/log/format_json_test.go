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
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/kr/pretty"
)

func TestJSONFormats(t *testing.T) {
	// CLI tests are sensitive to the server version, but test binaries don't have
	// a version injected. Pretend to be a very up-to-date version.
	defer build.TestingOverrideVersion("v999.0.0")()

	tm, err := time.Parse(MessageTimeFormat, "060102 15:04:05.654321")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	sysIDPayload := testIDPayload{tenantID: "1"}
	ctx = serverident.ContextWithServerIdentification(ctx, sysIDPayload)
	ctx = logtags.AddTag(ctx, "noval", nil)
	ctx = logtags.AddTag(ctx, "s", "1")
	ctx = logtags.AddTag(ctx, "long", "2")

	testCases := []logEntry{
		// Header entry.
		func() logEntry {
			e := makeUnstructuredEntry(ctx, 0, 0, 0, true, "hello %s", "world")
			e.header = true
			return e
		}(),
		// Normal (non-header) entries.
		{},
		{IDPayload: serverident.IDPayload{TenantID: "1", TenantName: "system", ClusterID: "abc", NodeID: "123"}},
		{IDPayload: serverident.IDPayload{TenantID: "456", TenantName: "vc42", SQLInstanceID: "123"}},
		makeStructuredEntry(ctx, severity.INFO, channel.DEV, 0, &logpb.TestingStructuredLogEvent{
			CommonEventDetails: logpb.CommonEventDetails{
				Timestamp: 123,
				EventType: "rename_database",
			},
			Channel: logpb.Channel_SQL_SCHEMA,
			Event:   "rename from `hello` to `world`",
		}),
		makeUnstructuredEntry(ctx, severity.WARNING, channel.OPS, 0, false, "hello %s", "world"),
		makeUnstructuredEntry(ctx, severity.ERROR, channel.HEALTH, 0, true, "hello %s", "world"),
	}

	l, err := timeutil.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	formats := []logFormatter{
		&formatJSONFull{fluentTag: true, tags: tagCompact},
		&formatJSONFull{fluentTag: true, tags: tagVerbose},
		&formatJSONFull{tags: tagCompact},
		&formatJSONFull{},
		&formatJSONFull{datetimeFormat: "2006-01-02 xx 15:04:05+07", loc: l},
	}

	// We only use the datadriven framework for the ability to rewrite the output.
	datadriven.RunTest(t, "testdata/json", func(t *testing.T, _ *datadriven.TestData) string {
		var buf bytes.Buffer
		for _, tc := range testCases {
			// override non-deterministic fields to stabilize the expected output.
			tc.ts = tm.UnixNano()
			tc.line = 123
			tc.gid = 11

			buf.WriteString("#\n")
			for _, f := range formats {
				b := f.formatEntry(tc)
				fmt.Fprintf(&buf, "%19s: %s", f.formatterName(), b.String())
				putBuffer(b)
			}
		}

		return buf.String()
	})

}

func TestJsonDecode(t *testing.T) {
	datadriven.RunTest(t, "testdata/parse_json",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "log":
				var out strings.Builder

				d, err := NewEntryDecoderWithFormat(strings.NewReader(td.Input), WithMarkedSensitiveData, td.CmdArgs[0].Vals[0])
				if err != nil {
					td.Fatalf(t, "error while constructing decoder: %v", err)
				}

				for {
					var e logpb.Entry
					if err := d.Decode(&e); err != nil {
						if err == io.EOF || errors.Is(err, ErrMalformedLogEntry) {
							break
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
