// Copyright 2021 The Cockroach Authors.
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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/kr/pretty"
)

func TestCrdbV1EncodeDecode(t *testing.T) {
	var templateEntry logpb.Entry
	// We only use the datadriven framework for the ability to rewrite the output.
	datadriven.RunTest(t, "testdata/crdb_v1", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "common":
			templateEntry = logpb.Entry{}
			if err := json.Unmarshal([]byte(td.Input), &templateEntry); err != nil {
				td.Fatalf(t, "invalid input syntax: %v", err)
			}

		case "entries":
			var inputEntries []logpb.Entry
			for _, line := range strings.Split(td.Input, "\n") {
				entry := templateEntry
				if err := json.Unmarshal([]byte(line), &entry); err != nil {
					td.Fatalf(t, "invalid input syntax: %v", err)
				}
				inputEntries = append(inputEntries, entry)
			}

			var buf bytes.Buffer
			// Encode.
			for _, entry := range inputEntries {
				_ = FormatLegacyEntry(entry, &buf)
			}
			// Decode.
			entryStr := buf.String()
			decoder := NewEntryDecoder(strings.NewReader(entryStr), WithMarkedSensitiveData)
			var outputEntries []logpb.Entry
			for {
				var entry logpb.Entry
				if err := decoder.Decode(&entry); err != nil {
					if err == io.EOF {
						break
					}
					td.Fatalf(t, "error while decoding: %v", err)
				}
				outputEntries = append(outputEntries, entry)
			}
			if len(outputEntries) != len(inputEntries) {
				td.Fatalf(t, "parse results in %d entries, expected %d. Input:\n%s", len(outputEntries), len(inputEntries), entryStr)
			}
			// Show what has been decoded.
			buf.WriteString("# after parse:\n")
			for _, entry := range outputEntries {
				fmt.Fprintf(&buf, "%# v\n", pretty.Formatter(entry))
				if entry.StructuredEnd != 0 {
					var payload interface{}
					if err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &payload); err != nil {
						td.Fatalf(t, "JSON entry not parsable: %v", err)
					}
					fmt.Fprintf(&buf, "JSON payload in previous entry: %+v\n", payload)
				}
			}
			return buf.String()

		default:
			t.Fatalf("unsupported command: %q", td.Cmd)
		}
		return ""
		// unreachable
	})
}

func TestCrdbV1EntryDecoderForVeryLargeEntries(t *testing.T) {
	entryIdx := 1
	formatEntry := func(s Severity, c Channel, now time.Time, gid int, file string, line int, tags, msg string) string {
		entry := logpb.Entry{
			Severity:  s,
			Channel:   c,
			Time:      now.UnixNano(),
			Goroutine: int64(gid),
			File:      file,
			Line:      int64(line),
			Tags:      tags,
			Message:   msg,
			Counter:   uint64(entryIdx),
		}
		entryIdx++
		var buf bytes.Buffer
		_ = FormatLegacyEntry(entry, &buf)
		return buf.String()
	}

	t1 := timeutil.Now().Round(time.Microsecond)
	t2 := t1.Add(time.Microsecond)

	// Verify the truncation logic for reading logs that are longer than the
	// default scanner can handle.
	preambleLength := len(formatEntry(severity.INFO, channel.DEV, t1, 0, "clog_test.go", 136, ``, ""))
	maxMessageLength := bufio.MaxScanTokenSize - preambleLength - 1
	reallyLongEntry := string(bytes.Repeat([]byte("a"), maxMessageLength))
	tooLongEntry := reallyLongEntry + "a"

	contents := formatEntry(severity.INFO, channel.DEV, t1, 2, "clog_test.go", 138, ``, reallyLongEntry)
	contents += formatEntry(severity.INFO, channel.DEV, t2, 3, "clog_test.go", 139, ``, tooLongEntry)

	readAllEntries := func(contents string) []logpb.Entry {
		decoder := NewEntryDecoder(strings.NewReader(contents), WithFlattenedSensitiveData)
		var entries []logpb.Entry
		var entry logpb.Entry
		for {
			if err := decoder.Decode(&entry); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatal(err)
			}
			entries = append(entries, entry)
		}
		return entries
	}

	entries := readAllEntries(contents)
	expected := []logpb.Entry{
		{
			Severity:  severity.INFO,
			Channel:   channel.DEV,
			Time:      t1.UnixNano(),
			Goroutine: 2,
			File:      `clog_test.go`,
			Line:      138,
			Message:   reallyLongEntry,
			Counter:   2,
		},
		{
			Severity:  severity.INFO,
			Channel:   channel.DEV,
			Time:      t2.UnixNano(),
			Goroutine: 3,
			File:      `clog_test.go`,
			Line:      139,
			Message:   tooLongEntry[:maxMessageLength],
			Counter:   3,
		},
	}
	if !reflect.DeepEqual(expected, entries) {
		t.Fatalf("%s\n", strings.Join(pretty.Diff(expected, entries), "\n"))
	}

	entries = readAllEntries("file header\n\n\n" + contents)
	if !reflect.DeepEqual(expected, entries) {
		t.Fatalf("%s\n", strings.Join(pretty.Diff(expected, entries), "\n"))
	}
}
