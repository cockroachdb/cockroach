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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/logtags"
)

func TestJSONFormats(t *testing.T) {
	// CLI tests are sensitive to the server version, but test binaries don't have
	// a version injected. Pretend to be a very up-to-date version.
	defer build.TestingOverrideTag("v999.0.0")()

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
		{idPayload: idPayload{clusterID: "abc", nodeID: "123"}},
		{idPayload: idPayload{tenantID: "456", sqlInstanceID: "123"}},
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

func TestFormatJSONLongLineBreaks(t *testing.T) {
	formats := []logFormatter{
		formatJSONFull{},
	}
	datadriven.RunTest(t, "testdata/json_break_lines", func(t *testing.T, td *datadriven.TestData) string {
		if td.Cmd != "run" {
			t.Fatalf("unknown command: %s", td.Cmd)
		}
		var maxLen int
		var redactable bool
		var structured bool
		var withLongStack bool
		td.ScanArgs(t, "maxlen", &maxLen)
		td.ScanArgs(t, "redactable", &redactable)
		td.ScanArgs(t, "structured", &structured)
		td.ScanArgs(t, "withLongStack", &withLongStack)

		defer func(prev int) { longLineLenJSON.set(prev) }(int(longLineLenJSON))
		longLineLenJSON.set(maxLen)

		longLine := string(bytes.Repeat([]byte("a"), 50))

		withBigStack := func(e logEntry) logEntry {
			e.stacks = []byte("this is " + longLine + " fake stack")
			return e
		}

		entry := logEntry{
			payload: entryPayload{
				redactable: redactable,
				message:    td.Input,
			},
			structured: structured,
		}

		if withLongStack {
			entry = withBigStack(entry)
		}

		var buf bytes.Buffer
		for _, f := range formats {
			b := f.formatEntry(entry)
			out := b.String()
			putBuffer(b)

			lines := strings.Split(out, "\n")
			for _, l := range lines {
				l = strings.TrimSuffix(l, "\n")
				if len(l) == 0 {
					continue
				}
				fmt.Fprintf(&buf, "%s: %s\n", f.formatterName(), l)

				// Verify that the JSON log is valid JSON format.
				var raw map[string]interface{}
				if unMarshalErr := json.Unmarshal([]byte(l), &raw); unMarshalErr != nil {
					t.Fatalf("error unmarshaling log line %s\n, with error: %v", l, unMarshalErr)
				}

				var eventOrMsgBytes []byte
				var eventOrMsgInterface interface{}
				var err error

				if structured {
					eventOrMsgInterface = raw["event"]
				} else {
					eventOrMsgInterface = raw["message"]
				}

				if !withLongStack && eventOrMsgInterface == nil {
					t.Fatalf("couldn't parse event/message from log line")
				}

				// Verify that the event/message follows valid JSON format.
				eventOrMsgBytes, err = json.Marshal(eventOrMsgInterface)

				if err != nil {
					t.Fatalf("error marshalling eventPayload %v: %v", eventOrMsgInterface, err)
				}

				// If the log is structured.
				if structured {
					// Get the raw event.
					var rawEvent map[string]interface{}
					if unMarshalErr := json.Unmarshal(eventOrMsgBytes, &rawEvent); unMarshalErr != nil {
						t.Fatalf("error unmarshaling event %s\n, with error: %v", eventOrMsgBytes, unMarshalErr)
					}

					// Determine the length of the event by totalling the length of its
					// keys and string values. We do not account for the length of
					// non-string values as non-string values are not split. We also do
					// not account the length of a value if its key is an "unsplittable".
					var lenOfKeys int
					var lenOfValues int
					for k, v := range rawEvent {
						lenOfKeys += len(k)
						valString, isString := v.(string)
						_, unsplittable := unsplittables[`"`+k+`":`]
						if isString && !unsplittable {
							lenOfValues += len(valString)
						}
					}
					eventLength := lenOfKeys + lenOfValues
					if eventLength > maxLen {
						t.Fatalf("line too large: %d bytes, expected max %d - %q", eventLength, maxLen, l)
					}
				} else if len(eventOrMsgBytes) > maxLen {
					// Log is not structured, check if the message length exceeds the max
					// line length.
					t.Fatalf("line too large: %d bytes, expected max %d - %q", len(eventOrMsgBytes), maxLen, l)
				}
			}
		}
		return buf.String()
	})
}
