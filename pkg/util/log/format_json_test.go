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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/logtags"
)

func TestJSONFormats(t *testing.T) {
	tm, err := time.Parse(MessageTimeFormat, "060102 15:04:05.654321")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
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
		{idPayload: idPayload{clusterID: "abc", nodeID: 123}},
		{idPayload: idPayload{tenantID: "abc", sqlInstanceID: 123}},
		makeStructuredEntry(ctx, severity.INFO, channel.DEV, 0, &eventpb.RenameDatabase{
			CommonEventDetails: eventpb.CommonEventDetails{
				Timestamp: 123,
				EventType: "rename_database",
			},
			DatabaseName:    "hello",
			NewDatabaseName: "world",
		}),
		makeUnstructuredEntry(ctx, severity.WARNING, channel.OPS, 0, false, "hello %s", "world"),
		makeUnstructuredEntry(ctx, severity.ERROR, channel.HEALTH, 0, true, "hello %s", "world"),
	}

	formats := []logFormatter{
		formatFluentJSONCompact{},
		formatFluentJSONFull{},
		formatJSONCompact{},
		formatJSONFull{},
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
