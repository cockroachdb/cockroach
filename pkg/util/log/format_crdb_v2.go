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
	"bytes"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/ttycolor"
)

// formatCrdbV2 is the canonical log format.
type formatCrdbV2 struct{}

func (formatCrdbV2) formatterName() string { return "crdb-v2" }

func (formatCrdbV2) formatEntry(entry logEntry) *buffer {
	return formatLogEntryInternalV2(entry, nil)
}

func (formatCrdbV2) doc() string { return formatCrdbV2CommonDoc() }

func (formatCrdbV2) contentType() string { return "text/plain" }

func formatCrdbV2CommonDoc() string {
	var buf strings.Builder

	buf.WriteString(`This is the main file format used from CockroachDB v21.1.

Each log entry is emitted using a common prefix, described below,
followed by the text of the log entry.

### Entry format

Each line of output starts with the following prefix:

     Lyymmdd hh:mm:ss.uuuuuu goid [chan@]file:line marker [tags...] counter cont

| Field           | Description                                                                                                                          |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------|
| L               | A single character, representing the [log level](logging.html#logging-levels-severities) (e.g., ` + "`I`" + ` for ` + "`INFO`" + `). |
| yy              | The year (zero padded; i.e., 2016 is ` + "`16`" + `).                                                                                |
| mm              | The month (zero padded; i.e., May is ` + "`05`" + `).                                                                                |
| dd              | The day (zero padded).                                                                                                               |
| hh:mm:ss.uuuuuu | Time in hours, minutes and fractional seconds. Timezone is UTC.                                                                      |
| goid            | The goroutine id (zero when cannot be determined).                                                                                   |
| chan            | The channel number (omitted if zero for backward compatibility).                                                                     |
| file            | The file name where the entry originated. Also see below.                                                                            |
| line            | The line number where the entry originated.                                                                                          |
| marker          | Redactability marker "` + redactableIndicator + `" (see below for details).                                                          |
| tags            | The logging tags, enclosed between ` + "`[`" + ` and ` + "`]`" + `. See below.                                                       |
| counter         | The optional entry counter (see below for details).                                                                                  |
| cont            | Continuation mark for structured and multi-line entries. See below.                                                                  |

The ` + "`chan@`" + ` prefix before the file name indicates the logging channel,
and is omitted if the channel is ` + "`DEV`" + `.

The file name may be prefixed by the string ` + "`(gostd) `" + ` to indicate
that the log entry was produced inside the Go standard library, instead
of a CockroachDB component. Entry parsers must be configured to ignore this prefix
when present.

` + "`marker`" + ` can be empty; in this case, its position in the common prefix is
a double ASCII space character which can be used to reliably identify this situation.
If the marker "` + redactableIndicator + `" is present, the remainder of the log entry
contains delimiters (` + string(redact.StartMarker()) + "..." + string(redact.EndMarker()) + `)
around fields that are considered sensitive. These markers are automatically recognized
by ` + "[`cockroach debug zip`](cockroach-debug-zip.html)" + ` and ` +
		"[`cockroach debug merge-logs`](cockroach-debug-merge-logs.html)" + `
when log redaction is requested.

The logging ` + "`tags`" + ` are enclosed between square brackets ` + "`[...]`" + `,
and the syntax ` + "`[-]`" + ` is used when there are no logging tags
associated with the log entry.

` + "`counter`" + ` is numeric, and is incremented for every
log entry emitted to this sink. (There is thus one counter sequence per
sink.) For entries that do not have a counter value
associated (e.g., header entries in file sinks), the counter position
in the common prefix is empty: ` + "`tags`" + ` is then
followed by two ASCII space characters, instead of one space; the ` + "`counter`" + `,
and another space. The presence of the two ASCII spaces indicates
reliably that no counter was present.

` + "`cont`" + ` is a format/continuation indicator:

| Continuation indicator | ASCII | Description |
|------------------------|-------|--|
| space                  | 0x32  | Start of an unstructured entry. |
| equal sign, "="        | 0x3d  | Start of a structured entry. |
| exclamation mark, "!"  | 0x21  | Start of an embedded stack trace. |
| plus sign, "+"         | 0x2b  | Continuation of a multi-line entry. The payload contains a newline character at this position. |
| vertical bar           | 0x7c  | Continuation of a large entry. |

### Examples

Example single-line unstructured entry:

~~~
I210116 21:49:17.073282 14 server/node.go:464 ⋮ [-] 23  started with engine type ‹2›
~~~

Example multi-line unstructured entry:

~~~
I210116 21:49:17.083093 14 1@cli/start.go:690 ⋮ [-] 40  node startup completed:
I210116 21:49:17.083093 14 1@cli/start.go:690 ⋮ [-] 40 +CockroachDB node starting at 2021-01-16 21:49 (took 0.0s)
~~~

Example structured entry:

~~~
I210116 21:49:17.080713 14 1@util/log/event_log.go:32 ⋮ [-] 32 ={"Timestamp":1610833757080706620,"EventType":"node_restart"}
~~~

Example long entries broken up into multiple lines:

~~~
I210116 21:49:17.073282 14 server/node.go:464 ⋮ [-] 23  aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa....
I210116 21:49:17.073282 14 server/node.go:464 ⋮ [-] 23 |aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
~~~

~~~
I210116 21:49:17.080713 14 1@util/log/event_log.go:32 ⋮ [-] 32 ={"Timestamp":1610833757080706620,"EventTy...
I210116 21:49:17.080713 14 1@util/log/event_log.go:32 ⋮ [-] 32 |pe":"node_restart"}
~~~

### Backward-compatibility notes

Entries in this format can be read by most ` + "`crdb-v1`" + ` log parsers,
in particular the one included in the DB console and
also the [` + "`cockroach debug merge-logs`" + `](cockroach-debug-merge-logs.html)
facility.

However, implementers of previous version parsers must
understand that the logging tags field is now always
included, and the lack of logging tags is included
by a tag string set to ` + "`[-]`" + `.

Likewise, the entry counter is now also always included,
and there is a special character after ` + "`counter`" + `
to indicate whether the remainder of the line is a
structured entry, or a continuation of a previous entry.

Finally, in the previous format, structured entries
were prefixed with the string ` + "`" + structuredEntryPrefix + "`" + `. In
the new format, they are prefixed by the ` + "`=`" + ` continuation
indicator.
`)

	return buf.String()
}

// formatCrdbV2TTY is like formatCrdbV2 and includes VT color codes if
// the stderr output is a TTY and -nocolor is not passed on the
// command line.
type formatCrdbV2TTY struct{}

func (formatCrdbV2TTY) formatterName() string { return "crdb-v2-tty" }

func (formatCrdbV2TTY) formatEntry(entry logEntry) *buffer {
	cp := ttycolor.StderrProfile
	if logging.stderrSink.noColor.Get() {
		cp = nil
	}
	return formatLogEntryInternalV2(entry, cp)
}

func (formatCrdbV2TTY) doc() string {
	return "Same textual format as `" + formatCrdbV2{}.formatterName() + "`." + ttyFormatDoc
}

func (formatCrdbV2TTY) contentType() string { return "text/plain" }

// formatEntryInternalV2 renders a log entry.
// Log lines are colorized depending on severity.
// It uses a newly allocated *buffer. The caller is responsible
// for calling putBuffer() afterwards.
//
// Note: the prefix up to and including the logging tags
// needs to remain the same as in crdb-v1, so as to
// preserve cross-version compatibility with at least
// one version backwards.
func formatLogEntryInternalV2(entry logEntry, cp ttycolor.Profile) *buffer {
	buf := getBuffer()
	if entry.line < 0 {
		entry.line = 0 // not a real line number, but acceptable to someDigits
	}
	if entry.sev > severity.FATAL || entry.sev <= severity.UNKNOWN {
		entry.sev = severity.INFO // for safety.
	}

	tmp := buf.tmp[:len(buf.tmp)]
	var n int
	var prefix []byte
	switch entry.sev {
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
	now := timeutil.Unix(0, entry.ts)
	year, month, day := now.Date()
	hour, minute, second := now.Clock()
	// Lyymmdd hh:mm:ss.uuuuuu file:line
	tmp[n] = severityChar[entry.sev-1]
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
	n += buf.someDigits(n, int(entry.gid))
	tmp[n] = ' '
	n++
	if entry.ch != channel.DEV {
		// Prefix the filename with the channel number.
		n += buf.someDigits(n, int(entry.ch))
		tmp[n] = '@'
		n++
	}
	buf.Write(tmp[:n])
	buf.WriteString(entry.file)
	tmp[0] = ':'
	n = buf.someDigits(1, entry.line)
	n++
	// Reset the color to default.
	n += copy(tmp[n:], cp[ttycolor.Reset])
	tmp[n] = ' '
	n++
	// If redaction is enabled, indicate that the current entry has
	// markers. This indicator is used in the log parser to determine
	// which redaction strategy to adopt.
	if entry.payload.redactable {
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
	// the static size of tmp. But we do have a best-case upper bound.
	buf.Grow(20 + len(entry.payload.message))

	// Display the tags if set.
	buf.Write(cp[ttycolor.Blue])
	// We must always tag with tenant ID.
	buf.WriteByte('[')
	writeTagToBuffer(buf, tenantIDLogTagBytePrefix, []byte(entry.TenantID()))
	if entry.payload.tags != nil {
		buf.WriteByte(',')
		entry.payload.tags.formatToBuffer(buf)
	}
	buf.WriteByte(']')
	buf.Write(cp[ttycolor.Reset])
	buf.WriteByte(' ')

	// Display the counter if set and enabled.
	if entry.counter > 0 {
		n = buf.someDigits(0, int(entry.counter))
		buf.Write(cp[ttycolor.Cyan])
		buf.Write(tmp[:n])
		buf.Write(cp[ttycolor.Reset])
	}
	buf.WriteByte(' ')

	commonPrefixLen := buf.Len()

	// Display the message. We have three cases:
	// - structured entries, introduced with a dash.
	// - unstructured entries on a single line. Empty continuation marker,
	//   to simplify the common case.
	// - unstructured entries on multiple lines; every line after
	//   the first gets a '+' to mark it's a continuation.
	if entry.structured {
		buf.Write(cp[ttycolor.Green])
		buf.WriteByte('=')
		buf.Write(cp[ttycolor.Reset])
		// Structured entries are guaranteed to fit on a single line already.
		buf.WriteByte('{')
		buf.maybeMultiLine(commonPrefixLen, '+', entry.payload.redactable, entry.payload.message, cp)
		buf.WriteByte('}')
	} else {
		buf.WriteByte(' ')
		buf.maybeMultiLine(commonPrefixLen, '+', entry.payload.redactable, entry.payload.message, cp)
	}
	if entry.stacks != nil {
		buf.WriteByte('\n')
		buf.Write(buf.Bytes()[0:commonPrefixLen])
		buf.Write(cp[ttycolor.Green])
		buf.WriteByte('!')
		buf.Write(cp[ttycolor.Reset])
		buf.maybeMultiLine(commonPrefixLen, '!', false /* redactable */, string(entry.stacks), cp)
	}

	// Ensure there is a final newline.
	buf.WriteByte('\n')

	return buf
}

// crdbV2LongLineLen is the max length of a log entry, in bytes, before
// it gets broken up into multiple lines.
// This maximum is applied to the size of the entry without considering
// the prefix (timestamp, location etc).
// The value is approximate: lines can be effectively shorter than
// this maximum. This margin exists so as to accommodate lines that
// end with a multi-byte UTF-8 sequence, as these cannot be broken up.
//
// This is implemented as a variable so it can be modified
// in unit tests.
// TODO(knz): This could also be made configurable by the user.
//
// NB: the value of this variable might be mentioned in the format's
// documentation above. Keep them in sync if necessary.
var crdbV2LongLineLen longLineLen

func init() {
	crdbV2LongLineLen.set(16 * 1000)
}

type longLineLen int

func (l *longLineLen) set(v int) {
	// We refuse to break a long entry in the middle of a UTF-8
	// sequence, so the effective max length needs to be reduced by the
	// maximum size of an UTF-8 sequence.
	suffixLen := utf8.UTFMax
	// We also refuse to break a long entry in the middle of a redaction
	// marker. Additionally, if we observe a start redaction marker,
	// we are going to insert a closing redaction marker after it
	// before we break up the line.
	if len(startRedactionMarker)+len(endRedactionMarker) > suffixLen {
		suffixLen = len(startRedactionMarker) + len(endRedactionMarker)
	}
	newMax := v - suffixLen
	if newMax < 1 {
		panic("max line length cannot be zero or negative")
	}
	*l = longLineLen(newMax)
}

func (l longLineLen) shouldBreak(lastLen int) bool {
	return lastLen >= int(l)
}

func (buf *buffer) maybeMultiLine(
	prefixLen int, contMark byte, redactable bool, msg string, cp ttycolor.Profile,
) {
	var i int
	for i = len(msg) - 1; i > 0 && msg[i] == '\n'; i-- {
		msg = msg[:i]
	}
	// k is the index in the message up to (and excluding) the byte
	// which we've already copied into buf.
	k := 0
	lastLen := 0
	betweenRedactionMarkers := false
	for i := 0; i < len(msg); i++ {
		if msg[i] == '\n' {
			buf.WriteString(msg[k : i+1])
			buf.Write(buf.Bytes()[0:prefixLen])
			buf.Write(cp[ttycolor.Green])
			buf.WriteByte(contMark)
			buf.Write(cp[ttycolor.Reset])
			k = i + 1
			lastLen = 0
			continue
		}
		if crdbV2LongLineLen.shouldBreak(lastLen) {
			buf.WriteString(msg[k:i])
			if betweenRedactionMarkers {
				// We are breaking a long line in-between redaction
				// markers. Ensures that the opening and closing markers do
				// not straddle log entries.
				buf.WriteString(endRedactionMarker)
			}
			buf.WriteByte('\n')
			buf.Write(buf.Bytes()[0:prefixLen])
			buf.Write(cp[ttycolor.Green])
			buf.WriteByte('|')
			buf.Write(cp[ttycolor.Reset])
			k = i
			lastLen = 0
			if betweenRedactionMarkers {
				// See above: if we are splitting in-between redaction
				// markers, continue the sensitive item on the new line.
				buf.WriteString(startRedactionMarker)
				lastLen += len(startRedactionMarker)
			}
		}
		// Common case: single-byte runes and redaction marker known to
		// start with a multi-byte sequence. Take a shortcut.
		if markersStartWithMultiByteRune && msg[i] < utf8.RuneSelf {
			lastLen++
			continue
		}
		if redactable {
			// If we see an opening redaction marker, remember this fact
			// so that we close/open it properly.
			if strings.HasPrefix(msg[i:], startRedactionMarker) {
				betweenRedactionMarkers = true
				lm := len(startRedactionMarker)
				i += lm - 1
				lastLen += lm
				continue
			} else if strings.HasPrefix(msg[i:], endRedactionMarker) {
				betweenRedactionMarkers = false
				le := len(endRedactionMarker)
				i += le - 1
				lastLen += le
				continue
			}
		}

		// Avoid breaking in the middle of UTF-8 sequences.
		_, width := utf8.DecodeRuneInString(msg[i:])
		i += width - 1
		lastLen += width
	}
	buf.WriteString(msg[k:])
}

var startRedactionMarker = string(redact.StartMarker())
var endRedactionMarker = string(redact.EndMarker())
var markersStartWithMultiByteRune = startRedactionMarker[0] >= utf8.RuneSelf && endRedactionMarker[0] >= utf8.RuneSelf

var (
	entryREV2 = regexp.MustCompile(
		`(?m)^` +
			/* Severity                 */ `(?P<severity>[` + severityChar + `])` +
			/* Date and time            */ `(?P<datetime>\d{6} \d{2}:\d{2}:\d{2}.\d{6}) ` +
			/* Goroutine ID             */ `(?:(?P<goroutine>\d+) )` +
			/* Go standard library flag */ `(\(gostd\) )?` +
			/* Channel                  */ `(?:(?P<channel>\d+)@)?` +
			/* File                     */ `(?P<file>[^:]+):` +
			/* Line                     */ `(?:(?P<line>\d+) )` +
			/* Redactable flag          */ `(?P<redactable>(?:` + redactableIndicator + `)?) ` +
			/* Context tags             */ `\[(?P<tags>(?:[^]]|\][^ ])+)\] ` +
			/* Counter                  */ `(?P<counter>(?:\d+)?) ` +
			/* Continuation marker      */ `(?P<continuation>[ =!+|])` +
			/* Message                  */ `(?P<msg>.*)$`,
	)
	v2SeverityIdx              = entryREV2.SubexpIndex("severity")
	v2DateTimeIdx              = entryREV2.SubexpIndex("datetime")
	v2GoroutineIdx             = entryREV2.SubexpIndex("goroutine")
	v2ChannelIdx               = entryREV2.SubexpIndex("channel")
	v2FileIdx                  = entryREV2.SubexpIndex("file")
	v2LineIdx                  = entryREV2.SubexpIndex("line")
	v2RedactableIdx            = entryREV2.SubexpIndex("redactable")
	v2TagsIdx                  = entryREV2.SubexpIndex("tags")
	v2CounterIdx               = entryREV2.SubexpIndex("counter")
	v2ContinuationIdx          = entryREV2.SubexpIndex("continuation")
	v2MsgIdx                   = entryREV2.SubexpIndex("msg")
	tenantIDLogTagStringPrefix = string(TenantIDLogTagKey)
	tenantIDLogTagBytePrefix   = []byte{TenantIDLogTagKey}
)

type entryDecoderV2 struct {
	lines           int // number of lines read from reader
	reader          *bufio.Reader
	nextFragment    entryDecoderV2Fragment
	sensitiveEditor redactEditor
}

// Decode decodes the next log entry into the provided protobuf message.
func (d *entryDecoderV2) Decode(entry *logpb.Entry) (err error) {
	defer func() {
		switch r := recover().(type) {
		case nil: // do nothing
		case error:
			err = errors.Wrapf(r, "decoding on line %d", d.lines)
		default:
			panic(r)
		}
	}()
	frag, atEOF := d.peekNextFragment()
	if atEOF {
		return io.EOF
	}
	d.popFragment()
	if err := d.initEntryFromFirstLine(entry, frag); err != nil {
		return err
	}

	// Process the message.
	var entryMsg bytes.Buffer
	entryMsg.Write(frag.getMsg())

	// While the entry has additional lines, collect the full message.
	for {
		frag, atEOF := d.peekNextFragment()
		if atEOF || !frag.isContinuation() {
			break
		}
		d.popFragment()
		d.addContinuationFragmentToEntry(entry, &entryMsg, frag)
	}

	r := redactablePackage{
		msg:        entryMsg.Bytes(),
		redactable: entry.Redactable,
	}
	r = d.sensitiveEditor(r)
	entry.Message = string(r.msg)
	entry.Redactable = r.redactable

	return nil
}

func (d *entryDecoderV2) addContinuationFragmentToEntry(
	entry *logpb.Entry, entryMsg *bytes.Buffer, frag entryDecoderV2Fragment,
) {
	switch frag.getContinuation() {
	case '+':
		entryMsg.WriteByte('\n')
		entryMsg.Write(frag.getMsg())
	case '|':
		entryMsg.Write(frag.getMsg())
		if entry.StructuredEnd != 0 {
			entry.StructuredEnd = uint32(entryMsg.Len())
		}
	case '!':
		if entry.StackTraceStart == 0 {
			entry.StackTraceStart = uint32(entryMsg.Len()) + 1
			entryMsg.WriteString("\nstack trace:\n")
			entryMsg.Write(frag.getMsg())
		} else {
			entryMsg.WriteString("\n")
			entryMsg.Write(frag.getMsg())
		}
	default:
		panic(errors.Errorf("unexpected continuation character %c", frag.getContinuation()))
	}
}

// peekNextFragment populates the nextFragment buffer by reading from the
// underlying reader a line at a time until a valid line is reached.
// It will panic if a malformed log line is discovered. It permits the first
// line in the decoder to be malformed and it will skip that line. Upon EOF,
// if there is no text left to consume, the atEOF return value will be true.
func (d *entryDecoderV2) peekNextFragment() (_ entryDecoderV2Fragment, atEOF bool) {
	for d.nextFragment == nil {
		d.lines++
		nextLine, err := d.reader.ReadBytes('\n')
		if isEOF := errors.Is(err, io.EOF); isEOF {
			if len(nextLine) == 0 {
				return nil, true
			}
		} else if err != nil {
			panic(err)
		}
		nextLine = bytes.TrimSuffix(nextLine, []byte{'\n'})
		m := entryREV2.FindSubmatch(nextLine)
		if m == nil {
			if d.lines == 1 { // allow non-matching lines if we've never seen a line
				continue
			}
			panic(errors.New("malformed log entry"))
		}
		d.nextFragment = m
	}
	return d.nextFragment, false
}

func (d *entryDecoderV2) popFragment() {
	if d.nextFragment == nil {
		panic(errors.AssertionFailedf("cannot pop unpopulated fragment"))
	}
	d.nextFragment = nil
}

func (d *entryDecoderV2) initEntryFromFirstLine(
	entry *logpb.Entry, m entryDecoderV2Fragment,
) (err error) {
	// Erase all the fields, to be sure.
	*entry = logpb.Entry{
		Severity:   m.getSeverity(),
		Time:       m.getTimestamp(),
		Goroutine:  m.getGoroutine(),
		Channel:    m.getChannel(),
		File:       m.getFile(),
		Line:       m.getLine(),
		Redactable: m.isRedactable(),
		Tags:       m.getTags(d.sensitiveEditor),
		TenantID:   m.getTenantID(),
		Counter:    m.getCounter(),
	}
	if m.isStructured() {
		entry.StructuredStart = 0
		entry.StructuredEnd = uint32(len(m.getMsg()))
	}
	return nil
}

// entryDecoderV2Fragment is a line which is part of a v2 log entry.
// It is the output of entryV2RE.FindSubmatch.
type entryDecoderV2Fragment [][]byte

func (f entryDecoderV2Fragment) getSeverity() logpb.Severity {
	return Severity(strings.IndexByte(severityChar, f[v2SeverityIdx][0]) + 1)
}

func (f entryDecoderV2Fragment) getMsg() []byte {
	return f[v2MsgIdx]
}

func (f entryDecoderV2Fragment) getContinuation() byte {
	return f[v2ContinuationIdx][0]
}

func (f entryDecoderV2Fragment) isContinuation() bool {
	switch f.getContinuation() {
	case '|', '+', '!':
		return true
	default:
		return false
	}
}

func (f entryDecoderV2Fragment) getGoroutine() int64 {
	return parseInt(f[v2GoroutineIdx], "goroutine")
}

func (f entryDecoderV2Fragment) getTimestamp() (unixNano int64) {
	t, err := time.Parse(MessageTimeFormat, string(f[v2DateTimeIdx]))
	if err != nil {
		panic(err)
	}
	return t.UnixNano()
}

func (f entryDecoderV2Fragment) getChannel() logpb.Channel {
	if len(f[v2ChannelIdx]) == 0 {
		return Channel(0)
	}
	return Channel(parseInt(f[v2ChannelIdx], "channel"))
}

func (f entryDecoderV2Fragment) getFile() string {
	return string(f[v2FileIdx])
}

func (f entryDecoderV2Fragment) getLine() int64 {
	return parseInt(f[v2LineIdx], "line")
}

func (f entryDecoderV2Fragment) isRedactable() bool {
	return len(f[v2RedactableIdx]) > 0
}

func (f entryDecoderV2Fragment) getTags(editor redactEditor) string {
	tagsStr := string(f[v2TagsIdx])
	if strings.HasPrefix(tagsStr, tenantIDLogTagStringPrefix) {
		firstCommaIndex := strings.IndexByte(tagsStr, ',')
		if firstCommaIndex >= 0 {
			tagsStr = tagsStr[firstCommaIndex+1:]
		} else {
			tagsStr = tagsStr[len(tagsStr):]
		}
	}
	switch tagsStr {
	case "":
		fallthrough
	case "-":
		return ""
	default:
		r := editor(redactablePackage{
			msg:        []byte(tagsStr),
			redactable: f.isRedactable(),
		})
		return string(r.msg)
	}
}

func (f entryDecoderV2Fragment) getTenantID() string {
	out := serverident.SystemTenantID
	switch tagsStr := string(f[v2TagsIdx]); tagsStr {
	case "-":
	default:
		tags := string(f[v2TagsIdx])
		if strings.HasPrefix(tags, tenantIDLogTagStringPrefix) {
			out = strings.Split(tags, ",")[0][1:]
		}
	}
	return out
}

func (f entryDecoderV2Fragment) getCounter() uint64 {
	if len(f[v2CounterIdx]) == 0 {
		return 0
	}
	return uint64(parseInt(f[v2CounterIdx], "counter"))
}

func (f entryDecoderV2Fragment) isStructured() bool {
	return f.getContinuation() == '='
}

func parseInt(data []byte, name string) int64 {
	i, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		panic(errors.Wrapf(err, "parsing %s", name))
	}
	return i
}
