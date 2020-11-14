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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/ttycolor"
	"github.com/petermattis/goid"
)

// formatLogEntry formats an logpb.Entry into a newly allocated *buffer.
// The caller is responsible for calling putBuffer() afterwards.
func (l *loggingT) formatLogEntry(entry logpb.Entry, stacks []byte, cp ttycolor.Profile) *buffer {
	buf := l.formatLogEntryInternal(entry, cp)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		_ = buf.WriteByte('\n')
	}
	if len(stacks) > 0 {
		buf.Write(stacks)
	}
	return buf
}

const severityChar = "IWEF"

// processForStderr formats a log entry for output to standard error.
func (l *loggingT) processForStderr(entry logpb.Entry, stacks []byte) *buffer {
	return l.formatLogEntry(entry, stacks, ttycolor.StderrProfile)
}

// processForFile formats a log entry for output to a file.
func (l *loggingT) processForFile(entry logpb.Entry, stacks []byte) *buffer {
	return l.formatLogEntry(entry, stacks, nil)
}

// makeStartLine creates a log entry suitable for the start of a logging
// output using the canonical logging format.
func (l *loggerT) makeStartLine(format string, args ...interface{}) logpb.Entry {
	entry := MakeEntry(
		context.Background(),
		severity.INFO,
		nil, /* logCounter */
		2,   /* depth */
		l.redactableLogs.Get(),
		format,
		args...)
	entry.Tags = "config"
	return entry
}

// getStartLines retrieves the log entries for the start
// of a new logging output.
func (l *loggerT) getStartLines(now time.Time) []logpb.Entry {
	messages := make([]logpb.Entry, 0, 6)
	messages = append(messages,
		l.makeStartLine("file created at: %s", Safe(now.Format("2006/01/02 15:04:05"))),
		l.makeStartLine("running on machine: %s", host),
		l.makeStartLine("binary: %s", Safe(build.GetInfo().Short())),
		l.makeStartLine("arguments: %s", os.Args),
	)

	logging.mu.Lock()
	if logging.mu.clusterID != "" {
		messages = append(messages, l.makeStartLine("clusterID: %s", logging.mu.clusterID))
	}
	logging.mu.Unlock()

	// Including a non-ascii character in the first 1024 bytes of the log helps
	// viewers that attempt to guess the character encoding.
	messages = append(messages,
		l.makeStartLine("line format: [IWEF]yymmdd hh:mm:ss.uuuuuu goid file:line msg utf8=\u2713"))
	return messages
}

// MakeEntry creates an logpb.Entry.
func MakeEntry(
	ctx context.Context,
	s Severity,
	lc *EntryCounter,
	depth int,
	redactable bool,
	format string,
	args ...interface{},
) (res logpb.Entry) {
	res = logpb.Entry{
		Severity:   s,
		Time:       timeutil.Now().UnixNano(),
		Goroutine:  goid.Get(),
		Redactable: redactable,
	}

	// Populate file/lineno.
	file, line, _ := caller.Lookup(depth + 1)
	res.File = file
	res.Line = int64(line)

	// Optionally populate the counter.
	if lc != nil && lc.EnableMsgCount {
		// Add a counter. This is important for e.g. the SQL audit logs.
		res.Counter = atomic.AddUint64(&lc.msgCount, 1)
	}

	// Populate the tags.
	var buf strings.Builder
	if redactable {
		redactTags(ctx, &buf)
	} else {
		formatTags(ctx, false /* brackets */, &buf)
	}
	res.Tags = buf.String()

	// Populate the message.
	buf.Reset()
	renderArgs(redactable, &buf, format, args...)
	res.Message = buf.String()

	return
}

func renderArgs(redactable bool, buf *strings.Builder, format string, args ...interface{}) {
	if len(args) == 0 {
		buf.WriteString(format)
	} else if len(format) == 0 {
		if redactable {
			redact.Fprint(buf, args...)
		} else {
			fmt.Fprint(buf, args...)
		}
	} else {
		if redactable {
			redact.Fprintf(buf, format, args...)
		} else {
			fmt.Fprintf(buf, format, args...)
		}
	}
}
