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
	"bufio"
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/ttycolor"
	"github.com/petermattis/goid"
)

// formatLogEntry formats an Entry into a newly allocated *buffer.
// The caller is responsible for calling putBuffer() afterwards.
func (l *loggingT) formatLogEntry(entry Entry, stacks []byte, cp ttycolor.Profile) *buffer {
	buf := l.formatLogEntryInternal(entry, cp)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		_ = buf.WriteByte('\n')
	}
	if len(stacks) > 0 {
		buf.Write(stacks)
	}
	return buf
}

// formatEntryInternal renders a log entry.
// Log lines are colorized depending on severity.
// It uses a newly allocated *buffer. The caller is responsible
// for calling putBuffer() afterwards.
//
// Log lines have this form:
// 	Lyymmdd hh:mm:ss.uuuuuu goid file:line <redactable> [tags] counter msg...
// where the fields are defined as follows:
// 	L                A single character, representing the log level (eg 'I' for INFO)
// 	yy               The year (zero padded; ie 2016 is '16')
// 	mm               The month (zero padded; ie May is '05')
// 	dd               The day (zero padded)
// 	hh:mm:ss.uuuuuu  Time in hours, minutes and fractional seconds
// 	goid             The goroutine id (omitted if zero for use by tests)
// 	file             The file name
// 	line             The line number
// 	tags             The context tags
// 	counter          The log entry counter, if non-zero
// 	msg              The user-supplied message
func (l *loggingT) formatLogEntryInternal(entry Entry, cp ttycolor.Profile) *buffer {
	if l.noColor {
		cp = nil
	}

	buf := getBuffer()
	if entry.Line < 0 {
		entry.Line = 0 // not a real line number, but acceptable to someDigits
	}
	if entry.Severity > Severity_FATAL || entry.Severity <= Severity_UNKNOWN {
		entry.Severity = Severity_INFO // for safety.
	}

	tmp := buf.tmp[:len(buf.tmp)]
	var n int
	var prefix []byte
	switch entry.Severity {
	case Severity_INFO:
		prefix = cp[ttycolor.Cyan]
	case Severity_WARNING:
		prefix = cp[ttycolor.Yellow]
	case Severity_ERROR, Severity_FATAL:
		prefix = cp[ttycolor.Red]
	}
	n += copy(tmp, prefix)
	// Avoid Fprintf, for speed. The format is so simple that we can do it quickly by hand.
	// It's worth about 3X. Fprintf is hard.
	now := timeutil.Unix(0, entry.Time)
	year, month, day := now.Date()
	hour, minute, second := now.Clock()
	// Lyymmdd hh:mm:ss.uuuuuu file:line
	tmp[n] = severityChar[entry.Severity-1]
	n++
	if year < 2000 {
		year = 2000
	}
	n += buf.twoDigits(n, year-2000)
	n += buf.twoDigits(n, int(month))
	n += buf.twoDigits(n, day)
	n += copy(tmp[n:], cp[ttycolor.Gray]) // gray for time, file & line
	tmp[n] = ' '
	n++
	n += buf.twoDigits(n, hour)
	tmp[n] = ':'
	n++
	n += buf.twoDigits(n, minute)
	tmp[n] = ':'
	n++
	n += buf.twoDigits(n, second)
	tmp[n] = '.'
	n++
	n += buf.nDigits(6, n, now.Nanosecond()/1000, '0')
	tmp[n] = ' '
	n++
	if entry.Goroutine > 0 {
		n += buf.someDigits(n, int(entry.Goroutine))
		tmp[n] = ' '
		n++
	}
	buf.Write(tmp[:n])
	buf.WriteString(entry.File)
	tmp[0] = ':'
	n = buf.someDigits(1, int(entry.Line))
	n++
	// Reset the color to default.
	n += copy(tmp[n:], cp[ttycolor.Reset])
	tmp[n] = ' '
	n++
	// If redaction is enabled, indicate that the current entry has
	// markers. This indicator is used in the log parser to determine
	// which redaction strategy to adopt.
	if entry.Redactable {
		copy(tmp[n:], redactableIndicatorBytes)
		n += len(redactableIndicatorBytes)
	}
	// Note: when the redactable indicator is not introduced
	// there are two spaces next to each other. This is intended
	// and should be preserved for backward-compatibility with
	// 3rd party log parsers.
	tmp[n] = ' '
	n++
	buf.Write(tmp[:n])

	// The remainder is variable-length and could exceed
	// the static size of tmp. But we do have an upper bound.
	buf.Grow(len(entry.Tags) + 14 + len(entry.Message))

	// Display the tags if set.
	if len(entry.Tags) != 0 {
		buf.Write(cp[ttycolor.Blue])
		buf.WriteByte('[')
		buf.WriteString(entry.Tags)
		buf.WriteString("] ")
		buf.Write(cp[ttycolor.Reset])
	}

	// Display the counter if set.
	if entry.Counter > 0 {
		n = buf.someDigits(0, int(entry.Counter))
		tmp[n] = ' '
		n++
		buf.Write(tmp[:n])
	}

	// Display the message.
	buf.WriteString(entry.Message)
	return buf
}

// processForStderr formats a log entry for output to standard error.
func (l *loggingT) processForStderr(entry Entry, stacks []byte) *buffer {
	return l.formatLogEntry(entry, stacks, ttycolor.StderrProfile)
}

// processForFile formats a log entry for output to a file.
func (l *loggingT) processForFile(entry Entry, stacks []byte) *buffer {
	return l.formatLogEntry(entry, stacks, nil)
}

// MakeEntry creates an Entry.
func MakeEntry(
	ctx context.Context,
	s Severity,
	lc *EntryCounter,
	depth int,
	redactable bool,
	format string,
	args ...interface{},
) (res Entry) {
	res = Entry{
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

// Format writes the log entry to the specified writer.
func (e Entry) Format(w io.Writer) error {
	buf := logging.formatLogEntry(e, nil, nil)
	defer putBuffer(buf)
	_, err := w.Write(buf.Bytes())
	return err
}

// We don't include a capture group for the log message here, just for the
// preamble, because a capture group that handles multiline messages is very
// slow when running on the large buffers passed to EntryDecoder.split.
var entryRE = regexp.MustCompile(
	`(?m)^` +
		/* Severity         */ `([IWEF])` +
		/* Date and time    */ `(\d{6} \d{2}:\d{2}:\d{2}.\d{6}) ` +
		/* Goroutine ID     */ `(?:(\d+) )?` +
		/* File/Line        */ `([^:]+):(\d+) ` +
		/* Redactable flag  */ `((?:` + redactableIndicator + `)?) ` +
		/* Context tags     */ `(?:\[([^]]+)\] )?`,
)

// EntryDecoder reads successive encoded log entries from the input
// buffer. Each entry is preceded by a single big-ending uint32
// describing the next entry's length.
type EntryDecoder struct {
	re                 *regexp.Regexp
	scanner            *bufio.Scanner
	sensitiveEditor    redactEditor
	truncatedLastEntry bool
}

// NewEntryDecoder creates a new instance of EntryDecoder.
func NewEntryDecoder(in io.Reader, editMode EditSensitiveData) *EntryDecoder {
	d := &EntryDecoder{
		re:              entryRE,
		scanner:         bufio.NewScanner(in),
		sensitiveEditor: getEditor(editMode),
	}
	d.scanner.Split(d.split)
	return d
}

// MessageTimeFormat is the format of the timestamp in log message headers as
// used in time.Parse and time.Format.
const MessageTimeFormat = "060102 15:04:05.999999"

// Decode decodes the next log entry into the provided protobuf message.
func (d *EntryDecoder) Decode(entry *Entry) error {
	for {
		if !d.scanner.Scan() {
			if err := d.scanner.Err(); err != nil {
				return err
			}
			return io.EOF
		}
		b := d.scanner.Bytes()
		m := d.re.FindSubmatch(b)
		if m == nil {
			continue
		}

		// Process the severity.
		entry.Severity = Severity(strings.IndexByte(severityChar, m[1][0]) + 1)

		// Process the timestamp.
		t, err := time.Parse(MessageTimeFormat, string(m[2]))
		if err != nil {
			return err
		}
		entry.Time = t.UnixNano()

		// Process the goroutine ID.
		if len(m[3]) > 0 {
			goroutine, err := strconv.Atoi(string(m[3]))
			if err != nil {
				return err
			}
			entry.Goroutine = int64(goroutine)
		}

		// Process the file/line details.
		entry.File = string(m[4])
		line, err := strconv.Atoi(string(m[5]))
		if err != nil {
			return err
		}
		entry.Line = int64(line)

		// Process the context tags.
		redactable := len(m[6]) != 0
		if len(m[7]) != 0 {
			r := redactablePackage{
				msg:        m[7],
				redactable: redactable,
			}
			r = d.sensitiveEditor(r)
			entry.Tags = string(r.msg)
		}

		// Process the log message itself
		r := redactablePackage{
			msg:        trimFinalNewLines(b[len(m[0]):]),
			redactable: redactable,
		}
		r = d.sensitiveEditor(r)
		entry.Message = string(r.msg)
		entry.Redactable = r.redactable
		return nil
	}
}

func trimFinalNewLines(s []byte) []byte {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '\n' {
			s = s[:i]
		} else {
			break
		}
	}
	return s
}

func (d *EntryDecoder) split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if d.truncatedLastEntry {
		i := d.re.FindIndex(data)
		if i == nil {
			// If there's no entry that starts in this chunk, advance past it, since
			// we've truncated the entry it was originally part of.
			return len(data), nil, nil
		}
		d.truncatedLastEntry = false
		if i[0] > 0 {
			// If an entry starts anywhere other than the first index, advance to it
			// to maintain the invariant that entries start at the beginning of data.
			// This isn't necessary, but simplifies the code below.
			return i[0], nil, nil
		}
		// If i[0] == 0, then a new entry starts at the beginning of data, so fall
		// through to the normal logic.
	}
	// From this point on, we assume we're currently positioned at a log entry.
	// We want to find the next one so we start our search at data[1].
	i := d.re.FindIndex(data[1:])
	if i == nil {
		if atEOF {
			return len(data), data, nil
		}
		if len(data) >= bufio.MaxScanTokenSize {
			// If there's no room left in the buffer, return the current truncated
			// entry.
			d.truncatedLastEntry = true
			return len(data), data, nil
		}
		// If there is still room to read more, ask for more before deciding whether
		// to truncate the entry.
		return 0, nil, nil
	}
	// i[0] is the start of the next log entry, but we need to adjust the value
	// to account for using data[1:] above.
	i[0]++
	return i[0], data[:i[0]], nil
}
