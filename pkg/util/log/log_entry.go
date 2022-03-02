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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
	"github.com/petermattis/goid"
)

// logEntry represents a logging event flowing through this package.
//
// It is different from logpb.Entry in that it is able to preserve
// more information about the structure of the source event, so that
// more details about this structure can be preserved by output
// formatters. logpb.Entry, in comparison, was tailored specifically
// to the legacy crdb-v1 formatter, and is a lossy representation.
type logEntry struct {
	idPayload

	// The entry timestamp.
	ts int64

	// If header is true, the entry is for a sink header and is emitted
	// no matter the filter.
	//
	// Header entries currently bypass the filter because they are emitted
	// deep in the file handling logic of file sinks, and not in the outer
	// coordination logic that ventilates entries across multiple sinks.
	// See the functions makeStartLine() / getStartLines() and how they
	// are used.
	//
	// This behavior is desirable because we want sinks to get an
	// identifying header that explains the cluster, node ID, etc,
	// regardless of the filtering parameters.
	header bool

	// The severity of the event. This is not reported by formatters
	// when the header boolean is set.
	sev Severity
	// The channel on which the entry was sent. This is not reported by
	// formatters when the header boolean is set.
	ch Channel
	// The binary version with which the event was generated.
	version string

	// The goroutine where the event was generated.
	gid int64
	// The file/line where the event was generated.
	file string
	line int

	// The entry counter. Populated by outputLogEntry().
	counter uint64

	// The stack trace(s), when processing e.g. a fatal event.
	stacks []byte

	// Whether the entry is structured or not.
	structured bool

	// The entry payload.
	payload entryPayload
}

var _ redact.SafeFormatter = (*logEntry)(nil)
var _ fmt.Stringer = (*logEntry)(nil)

func (e *logEntry) SafeFormat(w interfaces.SafePrinter, _ rune) {
	if len(e.file) != 0 {
		// TODO(knz): The "canonical" way to represent a file/line prefix
		// is: <file>:<line>: msg
		// with a colon between the line number and the message.
		// However, some location filter deep inside SQL doesn't
		// understand a colon after the line number.
		w.SafeString(redact.SafeString(e.file))
		w.SafeRune(':')
		w.SafeInt(redact.SafeInt(e.line))
		w.SafeRune(' ')
	}
	if e.payload.tags != nil {
		w.SafeRune('[')
		e.payload.tags.formatToSafeWriter(w, e.payload.redactable)
		w.SafeString("] ")
	}

	if !e.payload.redactable {
		w.Print(e.payload.message)
	} else {
		w.Print(redact.RedactableString(e.payload.message))
	}
}

// String is a faster implementation than `SafeFormat` which is why we
// don't follow the usual convention of implementing `String` via a call
// to `redact.StringWithoutMarkers()`. This implementation is still
// around because it sits in the hot path of verbose tracing.
func (e *logEntry) String() string {
	entry := e.convertToLegacy()
	if len(entry.Tags) == 0 && len(entry.File) == 0 && !entry.Redactable {
		// Shortcut.
		return entry.Message
	}

	var buf strings.Builder
	if len(entry.File) != 0 {
		buf.WriteString(entry.File)
		buf.WriteByte(':')
		buf.WriteString(strconv.FormatInt(entry.Line, 10))
		buf.WriteByte(' ')
	}
	if len(entry.Tags) > 0 {
		buf.WriteByte('[')
		buf.WriteString(entry.Tags)
		buf.WriteString("] ")
	}
	buf.WriteString(entry.Message)
	msg := buf.String()

	if entry.Redactable {
		// This is true when eventInternal is called from logfDepth(),
		// ie. a regular log call. In this case, the tags and message may contain
		// redaction markers. We remove them here.
		msg = redact.RedactableString(msg).StripMarkers()
	}
	return msg
}

type entryPayload struct {
	// The actual payload string.
	// For structured entries, this is the JSON
	// representation of the payload fields, without the
	// outer '{}'.
	// For unstructured entries, this is the (flat) message.
	//
	// If redactable is true, message is a RedactableString
	// in disguise. If it is false, message is a flat string with
	// no guarantees about content.
	message string

	// The tags, in a formattable representation.
	//
	// If redactable below is true, the value part of the
	// formattableTags is encoded as a RedactableString. If redactable
	// is false, the value part is raw and can contain redaction
	// markers. (Same as message above.)
	tags formattableTags

	// Whether the payload message is redactable or not.
	redactable bool
}

func makeRedactablePayload(ctx context.Context, m redact.RedactableString) entryPayload {
	return entryPayload{
		message:    string(m),
		tags:       makeFormattableTags(ctx, true /* redactable */),
		redactable: true,
	}
}

func makeUnsafePayload(ctx context.Context, m string) entryPayload {
	return entryPayload{
		message:    m,
		tags:       makeFormattableTags(ctx, false /* redactable */),
		redactable: false,
	}
}

// makeEntry creates a logEntry.
func makeEntry(ctx context.Context, s Severity, c Channel, depth int) (res logEntry) {
	ids := getIdentificationPayload(ctx)

	res = logEntry{
		idPayload: ids,
		ts:        timeutil.Now().UnixNano(),
		sev:       s,
		ch:        c,
		version:   build.BinaryVersion(),
		gid:       goid.Get(),
	}

	// Populate file/lineno.
	res.file, res.line, _ = caller.Lookup(depth + 1)

	return res
}

// makeStructuredEntry creates a logEntry using a structured payload.
func makeStructuredEntry(
	ctx context.Context, s Severity, c Channel, depth int, payload eventpb.EventPayload,
) (res logEntry) {
	res = makeEntry(ctx, s, c, depth+1)

	res.structured = true
	_, b := payload.AppendJSONFields(false, nil)
	res.payload = makeRedactablePayload(ctx, b.ToString())
	return res
}

// makeUnstructuredEntry creates a logEntry using an unstructured message.
func makeUnstructuredEntry(
	ctx context.Context,
	s Severity,
	c Channel,
	depth int,
	redactable bool,
	format string,
	args ...interface{},
) (res logEntry) {
	res = makeEntry(ctx, s, c, depth+1)

	res.structured = false

	if redactable {
		var buf redact.StringBuilder
		if len(args) == 0 {
			// TODO(knz): Remove this legacy case.
			buf.Print(redact.Safe(format))
		} else if len(format) == 0 {
			buf.Print(args...)
		} else {
			buf.Printf(format, args...)
		}
		res.payload = makeRedactablePayload(ctx, buf.RedactableString())
	} else {
		var buf strings.Builder
		formatArgs(&buf, format, args...)
		res.payload = makeUnsafePayload(ctx, buf.String())
	}

	return res
}

var configTagsCtx = logtags.AddTag(context.Background(), "config", nil)

// makeStartLine creates a formatted log entry suitable for the start
// of a logging output using the canonical logging format.
func makeStartLine(formatter logFormatter, format string, args ...interface{}) *buffer {
	entry := makeUnstructuredEntry(
		configTagsCtx,
		severity.UNKNOWN, /* header - ignored */
		0,                /* header - ignored */
		2,                /* depth */
		true,             /* redactable */
		format,
		args...)
	entry.header = true
	return formatter.formatEntry(entry)
}

// getStartLines retrieves the log entries for the start
// of a new log file output.
func (l *sinkInfo) getStartLines(now time.Time) []*buffer {
	f := l.formatter
	messages := make([]*buffer, 0, 6)
	messages = append(messages,
		makeStartLine(f, "file created at: %s", redact.Safe(now.Format("2006/01/02 15:04:05"))),
		makeStartLine(f, "running on machine: %s", fullHostName),
		makeStartLine(f, "binary: %s", redact.Safe(build.GetInfo().Short())),
		makeStartLine(f, "arguments: %s", os.Args),
	)

	// Including a non-ascii character in the first 1024 bytes of the log helps
	// viewers that attempt to guess the character encoding.
	messages = append(messages, makeStartLine(f, "log format (utf8=\u2713): %s", redact.Safe(f.formatterName())))

	if strings.HasPrefix(f.formatterName(), "crdb-") {
		// For the crdb file formats, suggest the structure of each log line.
		messages = append(messages,
			makeStartLine(f, `line format: [IWEF]yymmdd hh:mm:ss.uuuuuu goid [chan@]file:line redactionmark \[tags\] [counter] msg`))
	}
	return messages
}

// convertToLegacy turns the entry into a logpb.Entry.
func (e logEntry) convertToLegacy() (res logpb.Entry) {
	res = logpb.Entry{
		Severity:   e.sev,
		Channel:    e.ch,
		Time:       e.ts,
		File:       e.file,
		Line:       int64(e.line),
		Goroutine:  e.gid,
		Counter:    e.counter,
		Redactable: e.payload.redactable,
		Message:    e.payload.message,
	}

	if e.payload.tags != nil {
		var buf buffer
		e.payload.tags.formatToBuffer(&buf)
		res.Tags = buf.String()
	}

	if e.structured {
		// At this point, the message only contains the JSON fields of the
		// payload. Add the decoration suitable for our legacy file
		// format.
		res.Message = structuredEntryPrefix + "{" + res.Message + "}"
		res.StructuredStart = uint32(len(structuredEntryPrefix))
		res.StructuredEnd = uint32(len(res.Message))
	}

	if e.stacks != nil {
		res.StackTraceStart = uint32(len(res.Message)) + 1
		res.Message += "\n" + string(e.stacks)
	}

	return res
}

const structuredEntryPrefix = "Structured entry: "

// MakeLegacyEntry creates an logpb.Entry.
func MakeLegacyEntry(
	ctx context.Context,
	s Severity,
	c Channel,
	depth int,
	redactable bool,
	format string,
	args ...interface{},
) (res logpb.Entry) {
	return makeUnstructuredEntry(ctx, s, c, depth+1, redactable, format, args...).convertToLegacy()
}
