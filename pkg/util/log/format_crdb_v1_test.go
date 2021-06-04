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
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
)

func TestEntryDecoder(t *testing.T) {
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
	t3 := t2.Add(time.Microsecond)
	t4 := t3.Add(time.Microsecond)
	t5 := t4.Add(time.Microsecond)
	t6 := t5.Add(time.Microsecond)
	t7 := t6.Add(time.Microsecond)
	t8 := t7.Add(time.Microsecond)
	t9 := t8.Add(time.Microsecond)
	t10 := t9.Add(time.Microsecond)
	t11 := t10.Add(time.Microsecond)

	// Verify the truncation logic for reading logs that are longer than the
	// default scanner can handle.
	preambleLength := len(formatEntry(severity.INFO, channel.DEV, t1, 0, "clog_test.go", 136, ``, ""))
	maxMessageLength := bufio.MaxScanTokenSize - preambleLength - 1
	reallyLongEntry := string(bytes.Repeat([]byte("a"), maxMessageLength))
	tooLongEntry := reallyLongEntry + "a"

	contents := formatEntry(severity.INFO, channel.DEV, t1, 0, "clog_test.go", 136, ``, "info")
	contents += formatEntry(severity.INFO, channel.DEV, t2, 1, "clog_test.go", 137, ``, "multi-\nline")
	contents += formatEntry(severity.INFO, channel.DEV, t3, 2, "clog_test.go", 138, ``, reallyLongEntry)
	contents += formatEntry(severity.INFO, channel.DEV, t4, 3, "clog_test.go", 139, ``, tooLongEntry)
	contents += formatEntry(severity.WARNING, channel.DEV, t5, 4, "clog_test.go", 140, ``, "warning")
	contents += formatEntry(severity.ERROR, channel.DEV, t6, 5, "clog_test.go", 141, ``, "error")
	contents += formatEntry(severity.FATAL, channel.DEV, t7, 6, "clog_test.go", 142, ``, "fatal\nstack\ntrace")
	contents += formatEntry(severity.INFO, channel.DEV, t8, 7, "clog_test.go", 143, ``, tooLongEntry)

	// Regression test for #56873.
	contents += formatEntry(severity.INFO, channel.DEV, t8, 8, "clog_test.go", 144, `sometags`, "foo")
	contents += formatEntry(severity.INFO, channel.DEV, t8, 9, "clog_test.go", 145, ``, "bar" /* no tags */)
	// Different channel.
	contents += formatEntry(severity.INFO, channel.SESSIONS, t9, 10, "clog_test.go", 146, ``, "info")
	// Ensure that IPv6 addresses in tags get parsed properly.
	contents += formatEntry(severity.INFO, channel.DEV, t10, 11, "clog_test.go", 147, `client=[1::]:2`, "foo")
	// Ensure that empty messages don't wreak havoc.
	contents += formatEntry(severity.INFO, channel.DEV, t11, 12, "clog_test.go", 148, "", "")

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
			Goroutine: 0,
			File:      `clog_test.go`,
			Line:      136,
			Message:   `info`,
			Counter:   2,
		},
		{
			Severity:  severity.INFO,
			Channel:   channel.DEV,
			Time:      t2.UnixNano(),
			Goroutine: 1,
			File:      `clog_test.go`,
			Line:      137,
			Message: `multi-
line`,
			Counter: 3,
		},
		{
			Severity:  severity.INFO,
			Channel:   channel.DEV,
			Time:      t3.UnixNano(),
			Goroutine: 2,
			File:      `clog_test.go`,
			Line:      138,
			Message:   reallyLongEntry,
			Counter:   4,
		},
		{
			Severity:  severity.INFO,
			Channel:   channel.DEV,
			Time:      t4.UnixNano(),
			Goroutine: 3,
			File:      `clog_test.go`,
			Line:      139,
			Message:   tooLongEntry[:maxMessageLength],
			Counter:   5,
		},
		{
			Severity:  severity.WARNING,
			Channel:   channel.DEV,
			Time:      t5.UnixNano(),
			Goroutine: 4,
			File:      `clog_test.go`,
			Line:      140,
			Message:   `warning`,
			Counter:   6,
		},
		{
			Severity:  severity.ERROR,
			Channel:   channel.DEV,
			Time:      t6.UnixNano(),
			Goroutine: 5,
			File:      `clog_test.go`,
			Line:      141,
			Message:   `error`,
			Counter:   7,
		},
		{
			Severity:  severity.FATAL,
			Channel:   channel.DEV,
			Time:      t7.UnixNano(),
			Goroutine: 6,
			File:      `clog_test.go`,
			Line:      142,
			Message: `fatal
stack
trace`,
			Counter: 8,
		},
		{
			Severity:  severity.INFO,
			Channel:   channel.DEV,
			Time:      t8.UnixNano(),
			Goroutine: 7,
			File:      `clog_test.go`,
			Line:      143,
			Message:   tooLongEntry[:maxMessageLength],
			Counter:   9,
		},
		{
			Severity:  severity.INFO,
			Channel:   channel.DEV,
			Time:      t8.UnixNano(),
			Goroutine: 8,
			File:      `clog_test.go`,
			Line:      144,
			Tags:      `sometags`,
			Message:   `foo`,
			Counter:   10,
		},
		{
			Severity:  severity.INFO,
			Channel:   channel.DEV,
			Time:      t8.UnixNano(),
			Goroutine: 9,
			File:      `clog_test.go`,
			Line:      145,
			Message:   `bar`,
			Counter:   11,
		},
		{
			Severity:  severity.INFO,
			Channel:   channel.SESSIONS,
			Time:      t9.UnixNano(),
			Goroutine: 10,
			File:      `clog_test.go`,
			Line:      146,
			Message:   `info`,
			Counter:   12,
		},
		{
			Severity:  severity.INFO,
			Channel:   channel.DEV,
			Time:      t10.UnixNano(),
			Goroutine: 11,
			File:      `clog_test.go`,
			Line:      147,
			Tags:      `client=[1::]:2`,
			Message:   `foo`,
			Counter:   13,
		},
		{
			Severity:  severity.INFO,
			Channel:   channel.DEV,
			Time:      t11.UnixNano(),
			Goroutine: 12,
			File:      `clog_test.go`,
			Line:      148,
			Tags:      ``,
			Message:   ``,
			Counter:   14,
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
