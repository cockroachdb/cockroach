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
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/ttycolor"
)

// formatCrdbV1 is the pre-v21.1 canonical log format, without a
// counter column.
type formatCrdbV1 struct{}

func (formatCrdbV1) formatterName() string { return "crdb-v1" }

func (formatCrdbV1) formatEntry(entry logEntry) *buffer {
	return formatLogEntryInternalV1(entry.convertToLegacy(), entry.header, false /*showCounter*/, nil)
}

func (formatCrdbV1) doc() string { return formatCrdbV1CommonDoc(false /* withCounter */) }

func (formatCrdbV1) contentType() string { return "text/plain" }

func formatCrdbV1CommonDoc(withCounter bool) string {
	var buf strings.Builder

	if !withCounter {
		buf.WriteString(`This is the legacy file format used from CockroachDB v1.0.`)
	} else {
		buf.WriteString(`This is an alternative, backward-compatible legacy file format used from CockroachDB v2.0.`)
	}

	buf.WriteString(`

Each log entry is emitted using a common prefix, described below,`)

	if withCounter {
		buf.WriteString(`
followed by the text of the log entry.`)
	} else {
		buf.WriteString(`
followed by:

- The logging context tags enclosed between ` + "`[`" + ` and ` + "`]`" + `, if any. It is possible
  for this to be omitted if there were no context tags.
- the text of the log entry.`)
	}

	buf.WriteString(`

Beware that the text of the log entry can span multiple lines. The following caveats apply:

`)

	if !withCounter {
		// If there is no counter, the format is ambiguous. Explain that.
		buf.WriteString(`
- The text of the log entry can start with text enclosed between ` + "`[`" + ` and ` + "`]`" + `.
  If there were no logging tags to start with, it is not possible to distinguish between
  logging context tag information and a ` + "`[...]`" + ` string in the main text of the
  log entry. This means that this format is ambiguous. For an unambiguous alternative,
  consider ` + "`" + formatCrdbV1WithCounter{}.formatterName() + "`" + `.
`)
	}

	// General disclaimer about the lack of boundaries.
	buf.WriteString(`
- The text of the log entry can embed arbitrary application-level strings,
  including strings that represent log entries. In particular, an accident
  of implementation can cause the common entry prefix (described below)
  to also appear on a line of its own, as part of the payload of a previous
  log entry. There is no automated way to recognize when this occurs.
  Care must be taken by a human observer to recognize these situations.

- The log entry parser provided by CockroachDB to read log files is faulty
  and is unable to recognize the aforementioned pitfall; nor can it read
  entries larger than 64KiB successfully. Generally, use of this internal
  log entry parser is discouraged for entries written with this format.

See the newer format ` + "`crdb-v2`" + ` for an alternative
without these limitations.

### Header lines

At the beginning of each file, a header is printed using a similar format as
regular log entries. This header reports when the file was created,
which parameters were used to start the server, the server identifiers
if known, and other metadata about the running process.

- This header appears to be logged at severity ` + "`INFO`" + ` (with an ` + "`I`" + ` prefix
  at the start of the line) even though it does not really have a severity.
- The header is printed unconditionally even when a filter is configured to
  omit entries at the ` + "`INFO`" + ` level.

### Common log entry prefix

Each line of output starts with the following prefix:

     Lyymmdd hh:mm:ss.uuuuuu goid [chan@]file:line marker`)

	if withCounter {
		buf.WriteString(` tags counter`)
	}

	buf.WriteString(`

| Field           | Description                                                                                                                          |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------|
| L               | A single character, representing the [log level](logging.html#logging-levels-severities) (e.g., ` + "`I`" + ` for ` + "`INFO`" + `). |
| yy              | The year (zero padded; i.e., 2016 is ` + "`16`" + `).                                                                                |
| mm              | The month (zero padded; i.e., May is ` + "`05`" + `).                                                                                |
| dd              | The day (zero padded).                                                                                                               |
| hh:mm:ss.uuuuuu | Time in hours, minutes and fractional seconds. Timezone is UTC.                                                                      |
| goid            | The goroutine id (omitted if zero for use by tests).                                                                                 |
| chan            | The channel number (omitted if zero for backward compatibility).                                                                     |
| file            | The file name where the entry originated.                                                                                            |
| line            | The line number where the entry originated.                                                                                          |
| marker          | Redactability marker ` + "` + redactableIndicator + `" + ` (see below for details).                                                  |`)

	if withCounter {
		buf.WriteString(`
| tags    | The logging tags, enclosed between ` + "`[`" + ` and ` + "`]`" + `. May be absent. |
| counter | The entry counter. Always present.                                                 |`)
	}

	buf.WriteString(`

The redactability marker can be empty; in this case, its position in the common prefix is
a double ASCII space character which can be used to reliably identify this situation.

If the marker ` + "` + redactableIndicator + `" + ` is present, the remainder of the log entry
contains delimiters (` + string(redact.StartMarker()) + "..." + string(redact.EndMarker()) + `) around
fields that are considered sensitive. These markers are automatically recognized
by ` + "[`cockroach debug zip`](cockroach-debug-zip.html)" + ` and ` +
		"[`cockroach debug merge-logs`](cockroach-debug-merge-logs.html)" +
		` when log redaction is requested.
`)

	return buf.String()
}

// formatCrdbV1WithCounter is the canonical log format including a
// counter column.
type formatCrdbV1WithCounter struct{}

func (formatCrdbV1WithCounter) formatterName() string { return "crdb-v1-count" }

func (formatCrdbV1WithCounter) formatEntry(entry logEntry) *buffer {
	return formatLogEntryInternalV1(entry.convertToLegacy(), entry.header, true /*showCounter*/, nil)
}

func (formatCrdbV1WithCounter) doc() string { return formatCrdbV1CommonDoc(true /* withCounter */) }

func (formatCrdbV1WithCounter) contentType() string { return "text/plain" }

// formatCrdbV1TTY is like formatCrdbV1 and includes VT color codes if
// the stderr output is a TTY and -nocolor is not passed on the
// command line.
type formatCrdbV1TTY struct{}

func (formatCrdbV1TTY) formatterName() string { return "crdb-v1-tty" }

func (formatCrdbV1TTY) formatEntry(entry logEntry) *buffer {
	cp := ttycolor.StderrProfile
	if logging.stderrSink.noColor.Get() {
		cp = nil
	}
	return formatLogEntryInternalV1(entry.convertToLegacy(), entry.header, false /*showCounter*/, cp)
}

const ttyFormatDoc = `

In addition, if the output stream happens to be a VT-compatible terminal,
and the flag ` + "`no-color`" + ` was *not* set in the configuration, the entries
are decorated using ANSI color codes.`

func (formatCrdbV1TTY) doc() string {
	return "Same textual format as `" + formatCrdbV1{}.formatterName() + "`." + ttyFormatDoc
}

func (formatCrdbV1TTY) contentType() string { return "text/plain" }

// formatCrdbV1ColorWithCounter is like formatCrdbV1WithCounter and
// includes VT color codes if the stderr output is a TTY and -nocolor
// is not passed on the command line.
type formatCrdbV1TTYWithCounter struct{}

func (formatCrdbV1TTYWithCounter) formatterName() string { return "crdb-v1-tty-count" }

func (formatCrdbV1TTYWithCounter) formatEntry(entry logEntry) *buffer {
	cp := ttycolor.StderrProfile
	if logging.stderrSink.noColor.Get() {
		cp = nil
	}
	return formatLogEntryInternalV1(entry.convertToLegacy(), entry.header, true /*showCounter*/, cp)
}

func (formatCrdbV1TTYWithCounter) doc() string {
	return "Same textual format as `" + formatCrdbV1WithCounter{}.formatterName() + "`." + ttyFormatDoc
}

func (formatCrdbV1TTYWithCounter) contentType() string { return "text/plain" }

// formatEntryInternalV1 renders a log entry.
// Log lines are colorized depending on severity.
// It uses a newly allocated *buffer. The caller is responsible
// for calling putBuffer() afterwards.
//
func formatLogEntryInternalV1(
	entry logpb.Entry, isHeader, showCounter bool, cp ttycolor.Profile,
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
	if !isHeader && entry.Channel != 0 {
		// Prefix the filename with the channel number.
		n += buf.someDigits(n, int(entry.Channel))
		tmp[n] = '@'
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

	return buf
}

// We don't include a capture group for the log message here, just for the
// preamble, because a capture group that handles multiline messages is very
// slow when running on the large buffers passed to EntryDecoder.split.
var entryREV1 = regexp.MustCompile(
	`(?m)^` +
		/* Severity         */ `([` + severityChar + `])` +
		/* Date and time    */ `(\d{6} \d{2}:\d{2}:\d{2}.\d{6}) ` +
		/* Goroutine ID     */ `(?:(\d+) )?` +
		/* Channel/File/Line*/ `([^:]+):(\d+) ` +
		/* Redactable flag  */ `((?:` + redactableIndicator + `)?) ` +
		/* Context tags     */ `(?:\[((?:[^]]|\][^ ])+)\] )?`,
)

type entryDecoderV1 struct {
	scanner            *bufio.Scanner
	sensitiveEditor    redactEditor
	truncatedLastEntry bool
}

// Decode decodes the next log entry into the provided protobuf message.
func (d *entryDecoderV1) Decode(entry *logpb.Entry) error {
	for {
		if !d.scanner.Scan() {
			if err := d.scanner.Err(); err != nil {
				return err
			}
			return io.EOF
		}
		b := d.scanner.Bytes()
		m := entryREV1.FindSubmatch(b)
		if m == nil {
			continue
		}

		// Erase all the fields, to be sure.
		*entry = logpb.Entry{}

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

		// Process the channel/file/line details.
		entry.File = string(m[4])
		if idx := strings.IndexByte(entry.File, '@'); idx != -1 {
			ch, err := strconv.Atoi(entry.File[:idx])
			if err != nil {
				return err
			}
			entry.Channel = Channel(ch)
			entry.File = entry.File[idx+1:]
		}

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

		// If there's an entry counter at the start of the message, process it.
		msg := b[len(m[0]):]
		i := 0
		for ; i < len(msg) && msg[i] >= '0' && msg[i] <= '9'; i++ {
			entry.Counter = entry.Counter*10 + uint64(msg[i]-'0')
		}
		if i > 0 && i < len(msg) && msg[i] == ' ' {
			// Only accept the entry counter if followed by a space. In all
			// other cases, the number was part of the message string.
			msg = msg[i+1:]
		} else {
			// This was not truly an entry counter. Ignore the work done previously.
			entry.Counter = 0
		}

		// Process the remainder of the log message.
		r := redactablePackage{
			msg:        trimFinalNewLines(msg),
			redactable: redactable,
		}
		r = d.sensitiveEditor(r)
		entry.Message = string(r.msg)
		entry.Redactable = r.redactable

		if strings.HasPrefix(entry.Message, structuredEntryPrefix+"{") /* crdb-v1 prefix */ {
			// Note: we do not recognize the v2 marker here (" ={") because
			// v2 entries can be split across multiple lines.
			entry.StructuredStart = uint32(len(structuredEntryPrefix))

			if nl := strings.IndexByte(entry.Message, '\n'); nl != -1 {
				entry.StructuredEnd = uint32(nl)
				entry.StackTraceStart = uint32(nl + 1)
			} else {
				entry.StructuredEnd = uint32(len(entry.Message))
			}
		}
		// Note: we only know how to populate entry.StackTraceStart upon
		// parse if the entry was structured (see above). If it is not
		// structured, we cannot distinguish where the message ends and
		// where the stack trace starts. This is another reason why the
		// crdb-v1 format is lossy.

		return nil
	}
}

// split function for the crdb-v1 entry decoder scanner.
func (d *entryDecoderV1) split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if d.truncatedLastEntry {
		i := entryREV1.FindIndex(data)
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
	i := entryREV1.FindIndex(data[1:])
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
