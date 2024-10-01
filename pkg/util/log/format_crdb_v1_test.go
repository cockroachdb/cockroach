// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
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
				lbuf := formatLogEntryInternalV1(entry, false /* header */, true /* showCounter */, nil /* colorProfile */, loc)
				buf.Write(lbuf.Bytes())
				putBuffer(lbuf)
			}
			// Decode.
			entryStr := buf.String()
			decoder, err := NewEntryDecoderWithFormat(strings.NewReader(entryStr), WithMarkedSensitiveData, "crdb-v1")
			if err != nil {
				td.Fatalf(t, "error while constructing decoder: %v", err)
			}
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
	preambleLength := len(formatEntry(severity.INFO, channel.DEV, t1, 0, "somefile.go", 136, ``, ""))
	maxMessageLength := bufio.MaxScanTokenSize - preambleLength - 1
	reallyLongEntry := string(bytes.Repeat([]byte("a"), maxMessageLength))
	tooLongEntry := reallyLongEntry + "a"

	contents := formatEntry(severity.INFO, channel.DEV, t1, 2, "somefile.go", 138, ``, reallyLongEntry)
	contents += formatEntry(severity.INFO, channel.DEV, t2, 3, "somefile.go", 139, ``, tooLongEntry)

	readAllEntries := func(contents string) []logpb.Entry {
		decoder, err := NewEntryDecoderWithFormat(strings.NewReader(contents), WithFlattenedSensitiveData, "crdb-v1")
		if err != nil {
			t.Fatal(err)
		}
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
			File:      `somefile.go`,
			Line:      138,
			Message:   reallyLongEntry,
			Counter:   2,
			TenantID:  serverident.SystemTenantID,
		},
		{
			Severity:  severity.INFO,
			Channel:   channel.DEV,
			Time:      t2.UnixNano(),
			Goroutine: 3,
			File:      `somefile.go`,
			Line:      139,
			Message:   tooLongEntry[:maxMessageLength],
			Counter:   3,
			TenantID:  serverident.SystemTenantID,
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

func TestReadTenantDetails(t *testing.T) {
	tc := []struct {
		in               string
		expRemainderTags string
		expTenantID      string
		expTenantName    string
	}{
		{"", "", serverident.SystemTenantID, ""},
		{"T123", "", "123", ""},
		{"T123,Vabc", "", "123", "abc"},
		{"unknown", "unknown", serverident.SystemTenantID, ""},
		{"T123,unknown", "unknown", "123", ""},
		{"T123,Vabc,unknown", "unknown", "123", "abc"},
		{"unknown,T123", "unknown,T123", serverident.SystemTenantID, ""},
		{"Vabc,unknown", "Vabc,unknown", serverident.SystemTenantID, ""},
		{"T123,unknown,Vabc", "unknown,Vabc", "123", ""},
	}

	for _, test := range tc {
		t.Run(test.in, func(t *testing.T) {
			tid, tname, remainderTags := maybeReadTenantDetails([]byte(test.in))
			assert.Equal(t, tid, test.expTenantID)
			assert.Equal(t, tname, test.expTenantName)
			assert.Equal(t, string(remainderTags), test.expRemainderTags)
		})
	}
}
