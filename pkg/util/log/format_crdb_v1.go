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
	"bufio"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/ttycolor"
)

// FormatEntry writes the log entry to the specified writer.
func FormatEntry(e logpb.Entry, w io.Writer) error {
	var f formatCrdbV1WithCounter
	buf := f.formatEntry(e, nil)
	defer putBuffer(buf)
	_, err := w.Write(buf.Bytes())
	return err
}

// formatCrdbV1 is the canonical log format, without
// a counter column.
type formatCrdbV1 struct{}

func (formatCrdbV1) formatEntry(entry logpb.Entry, stacks []byte) *buffer {
	return formatLogEntryInternal(entry, false /*showCounter*/, nil, stacks)
}

// formatCrdbV1WithCounter is the canonical log format including a
// counter column.
type formatCrdbV1WithCounter struct{}

func (formatCrdbV1WithCounter) formatEntry(entry logpb.Entry, stacks []byte) *buffer {
	return formatLogEntryInternal(entry, true /*showCounter*/, nil, stacks)
}

// formatCrdbV1TTY is like formatCrdbV1 and includes VT color codes if
// the stderr output is a TTY and -nocolor is not passed on the
// command line.
type formatCrdbV1TTY struct{}

func (formatCrdbV1TTY) formatEntry(entry logpb.Entry, stacks []byte) *buffer {
	cp := ttycolor.StderrProfile
	if logging.stderrSink.noColor {
		cp = nil
	}
	return formatLogEntryInternal(entry, false /*showCounter*/, cp, stacks)
}

// formatCrdbV1ColorWithCounter is like formatCrdbV1WithCounter and
// includes VT color codes if the stderr output is a TTY and -nocolor
// is not passed on the command line.
type formatCrdbV1TTYWithCounter struct{}

func (formatCrdbV1TTYWithCounter) formatEntry(entry logpb.Entry, stacks []byte) *buffer {
	cp := ttycolor.StderrProfile
	if logging.stderrSink.noColor {
		cp = nil
	}
	return formatLogEntryInternal(entry, true /*showCounter*/, cp, stacks)
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
// 	counter          The log entry counter, if enabled and non-zero
// 	msg              The user-supplied message
func formatLogEntryInternal(
	entry logpb.Entry, showCounter bool, cp ttycolor.Profile, stacks []byte,
) *buffer {
	buf := getBuffer()
	if entry.Line < 0 {
		entry.Line = 0 // not a real line number, but acceptable to someDigits
	}
	if entry.Severity > severity.FATAL || entry.Severity <= severity.UNKNOWN {
		entry.Severity = severity.INFO // for safety.
	}

	tmp := buf.tmp[:len(buf.tmp)]
	var n int
	var prefix []byte
	switch entry.Severity {
	case severity.INFO:
		prefix = cp[ttycolor.Cyan]
	case severity.WARNING:
		prefix = cp[ttycolor.Yellow]
	case severity.ERROR, severity.FATAL:
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

	// Display the counter if set and enabled.
	if showCounter && entry.Counter > 0 {
		n = buf.someDigits(0, int(entry.Counter))
		tmp[n] = ' '
		n++
		buf.Write(tmp[:n])
	}

	// Display the message.
	buf.WriteString(entry.Message)

	// Ensure there is a final newline.
	if buf.Bytes()[buf.Len()-1] != '\n' {
		_ = buf.WriteByte('\n')
	}

	// If there are stack traces, print them.
	// Note that stacks are assumed to always contain
	// a final newline already.
	if len(stacks) > 0 {
		buf.Write(stacks)
	}

	return buf
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
func (d *EntryDecoder) Decode(entry *logpb.Entry) error {
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
