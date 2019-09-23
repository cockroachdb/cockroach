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
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/ttycolor"
	"github.com/petermattis/goid"
)

// the --no-color flag.
var noColor bool

// formatLogEntry formats an Entry into a newly allocated *buffer.
// The caller is responsible for calling putBuffer() afterwards.
func (l *loggingT) formatLogEntry(entry Entry, stacks []byte, cp ttycolor.Profile) *buffer {
	buf := l.formatHeader(entry.Severity, timeutil.Unix(0, entry.Time),
		int(entry.Goroutine), entry.File, int(entry.Line), cp)
	_, _ = buf.WriteString(entry.Message)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		_ = buf.WriteByte('\n')
	}
	if len(stacks) > 0 {
		buf.Write(stacks)
	}
	return buf
}

// formatHeader formats a log header using the provided file name and
// line number. Log lines are colorized depending on severity.
// It uses a newly allocated *buffer. The caller is responsible
// for calling putBuffer() afterwards.
//
// Log lines have this form:
// 	Lyymmdd hh:mm:ss.uuuuuu goid file:line msg...
// where the fields are defined as follows:
// 	L                A single character, representing the log level (eg 'I' for INFO)
// 	yy               The year (zero padded; ie 2016 is '16')
// 	mm               The month (zero padded; ie May is '05')
// 	dd               The day (zero padded)
// 	hh:mm:ss.uuuuuu  Time in hours, minutes and fractional seconds
// 	goid             The goroutine id (omitted if zero for use by tests)
// 	file             The file name
// 	line             The line number
// 	msg              The user-supplied message
func (l *loggingT) formatHeader(
	s Severity, now time.Time, gid int, file string, line int, cp ttycolor.Profile,
) *buffer {
	if noColor {
		cp = nil
	}

	buf := getBuffer()
	if line < 0 {
		line = 0 // not a real line number, but acceptable to someDigits
	}
	if s > Severity_FATAL || s <= Severity_UNKNOWN {
		s = Severity_INFO // for safety.
	}

	tmp := buf.tmp[:len(buf.tmp)]
	var n int
	var prefix []byte
	switch s {
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
	year, month, day := now.Date()
	hour, minute, second := now.Clock()
	// Lyymmdd hh:mm:ss.uuuuuu file:line
	tmp[n] = severityChar[s-1]
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
	if gid > 0 {
		n += buf.someDigits(n, gid)
		tmp[n] = ' '
		n++
	}
	buf.Write(tmp[:n])
	buf.WriteString(file)
	tmp[0] = ':'
	n = buf.someDigits(1, line)
	n++
	// Extra space between the header and the actual message for scannability.
	tmp[n] = ' '
	n++
	n += copy(tmp[n:], cp[ttycolor.Reset])
	tmp[n] = ' '
	n++
	buf.Write(tmp[:n])
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
func MakeEntry(s Severity, t int64, file string, line int, msg string) Entry {
	return Entry{
		Severity:  s,
		Time:      t,
		Goroutine: goid.Get(),
		File:      file,
		Line:      int64(line),
		Message:   msg,
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
	`(?m)^([IWEF])(\d{6} \d{2}:\d{2}:\d{2}.\d{6}) (?:(\d+) )?([^:]+):(\d+)`)

// EntryDecoder reads successive encoded log entries from the input
// buffer. Each entry is preceded by a single big-ending uint32
// describing the next entry's length.
type EntryDecoder struct {
	re                 *regexp.Regexp
	scanner            *bufio.Scanner
	truncatedLastEntry bool
}

// NewEntryDecoder creates a new instance of EntryDecoder.
func NewEntryDecoder(in io.Reader) *EntryDecoder {
	d := &EntryDecoder{scanner: bufio.NewScanner(in), re: entryRE}
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
		entry.Severity = Severity(strings.IndexByte(severityChar, m[1][0]) + 1)
		t, err := time.Parse(MessageTimeFormat, string(m[2]))
		if err != nil {
			return err
		}
		entry.Time = t.UnixNano()
		if len(m[3]) > 0 {
			goroutine, err := strconv.Atoi(string(m[3]))
			if err != nil {
				return err
			}
			entry.Goroutine = int64(goroutine)
		}
		entry.File = string(m[4])
		line, err := strconv.Atoi(string(m[5]))
		if err != nil {
			return err
		}
		entry.Line = int64(line)
		entry.Message = strings.TrimSpace(string(b[len(m[0]):]))
		return nil
	}
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
