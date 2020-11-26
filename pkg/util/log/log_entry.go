// Copyright 2019 The Cockroach Authors.
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
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
	"github.com/petermattis/goid"
)

const severityChar = "IWEF"

// makeStartLine creates a formatted log entry suitable for the start
// of a logging output using the canonical logging format.
func makeStartLine(formatter logFormatter, format string, args ...interface{}) *buffer {
	entry := MakeEntry(
		context.Background(),
		severity.INFO,
		channel.DEV, /* DEV ensures the channel number is omitted in headers. */
		2,           /* depth */
		true,        /* redactable */
		format,
		args...)
	entry.Tags = "config"
	return formatter.formatEntry(entry, nil)
}

// getStartLines retrieves the log entries for the start
// of a new log file output.
func (l *sinkInfo) getStartLines(now time.Time) []*buffer {
	f := l.formatter
	messages := make([]*buffer, 0, 6)
	messages = append(messages,
		makeStartLine(f, "file created at: %s", Safe(now.Format("2006/01/02 15:04:05"))),
		makeStartLine(f, "running on machine: %s", host),
		makeStartLine(f, "binary: %s", Safe(build.GetInfo().Short())),
		makeStartLine(f, "arguments: %s", os.Args),
	)

	logging.mu.Lock()
	if logging.mu.clusterID != "" {
		messages = append(messages, makeStartLine(f, "clusterID: %s", logging.mu.clusterID))
	}
	logging.mu.Unlock()

	// Including a non-ascii character in the first 1024 bytes of the log helps
	// viewers that attempt to guess the character encoding.
	messages = append(messages,
		makeStartLine(f, "line format: [IWEF]yymmdd hh:mm:ss.uuuuuu goid file:line msg utf8=\u2713"))
	return messages
}

// MakeEntry creates an logpb.Entry.
func MakeEntry(
	ctx context.Context,
	s Severity,
	c Channel,
	depth int,
	redactable bool,
	format string,
	args ...interface{},
) (res logpb.Entry) {
	res = logpb.Entry{
		Severity:   s,
		Channel:    c,
		Time:       timeutil.Now().UnixNano(),
		Goroutine:  goid.Get(),
		Redactable: redactable,
	}

	// Populate file/lineno.
	file, line, _ := caller.Lookup(depth + 1)
	res.File = file
	res.Line = int64(line)

	// Populate the tags.
	var buf strings.Builder
	if redactable {
		renderTagsAsRedactable(ctx, &buf)
	} else {
		formatTags(ctx, false /* brackets */, &buf)
	}
	res.Tags = buf.String()

	// Populate the message.
	buf.Reset()
	if redactable {
		renderArgsAsRedactable(&buf, format, args...)
	} else {
		formatArgs(&buf, format, args...)
	}
	res.Message = buf.String()

	return
}

func renderArgsAsRedactable(buf *strings.Builder, format string, args ...interface{}) {
	if len(args) == 0 {
		buf.WriteString(format)
	} else if len(format) == 0 {
		redact.Fprint(buf, args...)
	} else {
		redact.Fprintf(buf, format, args...)
	}
}

func formatArgs(buf *strings.Builder, format string, args ...interface{}) {
	if len(args) == 0 {
		buf.WriteString(format)
	} else if len(format) == 0 {
		fmt.Fprint(buf, args...)
	} else {
		fmt.Fprintf(buf, format, args...)
	}
}
