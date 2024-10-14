// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/ttycolor"
)

// formatCrdbV1 is the pre-v21.1 canonical log format, without a
// counter column.
type formatCrdbV1 struct {
	// showCounter determines whether the counter column is printed.
	showCounter bool
	// colorProfile is used to colorize the output.
	colorProfile ttycolor.Profile
	// colorProfileName is the name of the color profile, used for
	// documentation purposes.
	colorProfileName string
	// loc is the time zone to use. When non-nil, it forces the
	// presentation of a time zone specification after the time stamp.
	// The corresponding code path is much slower.
	loc *time.Location
}

func (f *formatCrdbV1) setOption(k string, v string) error {
	switch k {
	case "show-counter":
		switch v {
		case "true":
			f.showCounter = true
		case "false":
			f.showCounter = false
		default:
			return errors.WithHint(
				errors.Newf("unknown show-counter value: %q", redact.Safe(v)),
				"Possible values: true, false.")
		}
		return nil

	case "colors":
		switch v {
		case "none":
			f.colorProfile = nil
		case "auto":
			f.colorProfile = ttycolor.StderrProfile
		case "ansi":
			f.colorProfile = ttycolor.Profile8
		case "256color":
			f.colorProfile = ttycolor.Profile256
		default:
			return errors.WithHint(
				errors.Newf("unknown colors value: %q", redact.Safe(v)),
				"Possible values: none, auto, ansi, 256color.")
		}
		f.colorProfileName = v
		return nil

	case "timezone":
		l, err := timeutil.LoadLocation(v)
		if err != nil {
			return errors.Wrapf(err, "invalid timezone: %q", v)
		}
		if l == time.UTC {
			// Avoid triggering the slow path in the entry formatter.
			l = nil
		}
		f.loc = l
		return nil

	default:
		return errors.Newf("unknown format option: %q", redact.Safe(k))
	}
}

func (f formatCrdbV1) formatterName() string {
	var buf strings.Builder
	buf.WriteString("crdb-v1")
	if f.colorProfileName != "none" {
		buf.WriteString("-tty")
	}
	if f.showCounter {
		buf.WriteString("-count")
	}
	return buf.String()
}

func (formatCrdbV1) contentType() string { return "text/plain" }

func (f formatCrdbV1) doc() string {
	if f.formatterName() != "crdb-v1" {
		var buf strings.Builder
		fmt.Fprintf(&buf, `This format name is an alias for 'crdb-v1' with
the following format option defaults:

- `+"`show-counter: %v`"+`
- `+"`colors: %v`"+`
`, f.showCounter, f.colorProfileName)
		return buf.String()
	}

	var buf strings.Builder

	buf.WriteString(`
This is a legacy file format used from CockroachDB v1.0.

Each log entry is emitted using a common prefix, described below, followed by:

- The logging context tags enclosed between ` + "`[`" + ` and ` + "`]`" + `, if any. It is possible
  for this to be omitted if there were no context tags.
- Optionally, a counter column, if the option 'show-counter' is enabled. See below for details.
- the text of the log entry.

Beware that the text of the log entry can span multiple lines.
The following caveats apply:

- The text of the log entry can start with text enclosed between ` + "`[`" + ` and ` + "`]`" + `.
  If there were no logging tags to start with, it is not possible to distinguish between
  logging context tag information and a ` + "`[...]`" + ` string in the main text of the
  log entry. This means that this format is ambiguous.

  To remove this ambiguity, you can use the option 'show-counter'.

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

     Lyymmdd hh:mm:ss.uuuuuu goid [chan@]file:line marker tags counter

Reminder, the tags may be omitted; and the counter is only printed if the option
'show-counter' is specified.

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
| marker          | Redactability marker ` + "` + redactableIndicator + `" + ` (see below for details).                                                  |
| tags            | The logging tags, enclosed between ` + "`[`" + ` and ` + "`]`" + `. May be absent.                                                   |
| counter         | The entry counter. Only included if 'show-counter' is enabled.                                                                       |

The redactability marker can be empty; in this case, its position in the common prefix is
a double ASCII space character which can be used to reliably identify this situation.

If the marker ` + "` + redactableIndicator + `" + ` is present, the remainder of the log entry
contains delimiters (` + string(redact.StartMarker()) + "..." + string(redact.EndMarker()) + `) around
fields that are considered sensitive. These markers are automatically recognized
by ` + "[`cockroach debug zip`](cockroach-debug-zip.html)" + ` and ` +
		"[`cockroach debug merge-logs`](cockroach-debug-merge-logs.html)" +
		` when log redaction is requested.


Additional options recognized via ` + "`format-options`" + `:

| Option | Description |
|--------|-------------|
| ` + "`show-counter`" + ` | Whether to include the counter column in the line header. Without it, the format may be ambiguous due to the optionality of tags. |
| ` + "`colors`" + ` | The color profile to use. Possible values: none, auto, ansi, 256color. Default is auto. |
| ` + "`timezone`" + ` | The timezone to use for the timestamp column. The value can be any timezone name recognized by the Go standard library. Default is ` + "`UTC`" + ` |

`)

	return buf.String()
}

func (f formatCrdbV1) formatEntry(entry logEntry) *buffer {
	return formatLogEntryInternalV1(entry.convertToLegacy(),
		entry.header, f.showCounter, f.colorProfile, f.loc)
}

func writeCrdbHeader(
	buf *buffer,
	cp ttycolor.Profile,
	sev logpb.Severity,
	ch logpb.Channel,
	file string,
	line int,
	ts int64,
	loc *time.Location,
	gid int,
	redactable bool,
) {
	if line < 0 {
		line = 0 // not a real line number, but acceptable to someDigits
	}
	if sev > severity.FATAL || sev <= severity.UNKNOWN {
		sev = severity.INFO // for safety.
	}

	tmp := buf.tmp[:len(buf.tmp)]
	var n int
	var prefix []byte
	switch sev {
	case severity.INFO:
		prefix = cp[ttycolor.Cyan]
	case severity.WARNING:
		prefix = cp[ttycolor.Yellow]
	case severity.ERROR, severity.FATAL:
		prefix = cp[ttycolor.Red]
	}
	n += copy(tmp, prefix)
	// Lyymmdd hh:mm:ss.uuuuuu file:line
	tmp[n] = severityChar[sev-1]
	n++

	if loc == nil {
		// Default time zone (UTC).
		// Avoid Fprintf, for speed. The format is so simple that we can do it quickly by hand.
		// It's worth about 3X. Fprintf is hard.
		now := timeutil.Unix(0, ts)
		year, month, day := now.Date()
		hour, minute, second := now.Clock()
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
	} else {
		// Time zone was specified.
		// Slooooow path.
		buf.Write(tmp[:n])
		n = 0
		now := timeutil.Unix(0, ts).In(loc)
		buf.WriteString(now.Format("060102"))
		buf.Write(cp[ttycolor.Gray])
		buf.WriteByte(' ')
		buf.WriteString(now.Format("15:04:05.000000-070000"))
	}

	tmp[n] = ' '
	n++
	if gid >= 0 {
		n += buf.someDigits(n, gid)
		tmp[n] = ' '
		n++
	}

	if ch != 0 {
		// Prefix the filename with the channel number.
		n += buf.someDigits(n, int(ch))
		tmp[n] = '@'
		n++
	}

	buf.Write(tmp[:n])
	buf.WriteString(file)
	tmp[0] = ':'
	n = buf.someDigits(1, line)
	n++
	// Reset the color to default.
	n += copy(tmp[n:], cp[ttycolor.Reset])
	tmp[n] = ' '
	n++
	// If redaction is enabled, indicate that the current entry has
	// markers. This indicator is used in the log parser to determine
	// which redaction strategy to adopt.
	if redactable {
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
}

// checkTenantLabel returns true if the entry has a tenant label
// and a size estimate for the printout of the label.
func checkTenantLabel(tenantID string, tenantName string) (hasLabel bool, length int) {
	const tenantDetailsTagPrefixLen = len(string(tenantIDLogTagKey) + string(tenantNameLogTagKey) + ",,")
	return tenantID != "", len(tenantID) + tenantDetailsTagPrefixLen + len(tenantName)
}

// writeTenantLabel is a representation of the tenant (ID,name) pair
// specific to the CRDB logging format, and that can also be parsed
// back.
func writeTenantLabel(buf *buffer, tenantID string, tenantName string) {
	if tenantID != "" {
		buf.WriteByte(tenantIDLogTagKey)
		buf.WriteString(tenantID)
		if tenantName != "" {
			buf.WriteByte(',')
		}
	}
	if tenantName != "" {
		buf.WriteByte(tenantNameLogTagKey)
		buf.WriteString(tenantName)
	}
}

// formatEntryInternalV1 renders a log entry.
// Log lines are colorized depending on severity.
// It uses a newly allocated *buffer. The caller is responsible
// for calling putBuffer() afterwards.
func formatLogEntryInternalV1(
	entry logpb.Entry, isHeader, showCounter bool, cp ttycolor.Profile, loc *time.Location,
) *buffer {
	buf := getBuffer()
	ch := entry.Channel
	if isHeader {
		ch = 0
	}
	gid := int(entry.Goroutine)
	if gid == 0 {
		// In the -v1 format, a goroutine ID of 0 is hidden.
		gid = -1
	}
	writeCrdbHeader(buf, cp, entry.Severity, ch, entry.File, int(entry.Line), entry.Time, loc, gid, entry.Redactable)

	hasTenantLabel, tenantLabelLength := checkTenantLabel(entry.TenantID, entry.TenantName)
	hasTags := len(entry.Tags) > 0

	// The remainder is variable-length and could exceed
	// the static size of tmp. But we do have an optimistic upper bound.
	//
	// We optimistically count 3 times the size of entry.Tags to have
	// one character for the key, one character for the value and one
	// for the comma.
	buf.Grow(len(entry.Tags)*3 + tenantLabelLength + 14 + len(entry.Message))

	// We must always tag with tenant ID if present.
	buf.Write(cp[ttycolor.Blue])
	if hasTenantLabel || hasTags {
		buf.WriteByte('[')
		if hasTenantLabel {
			writeTenantLabel(buf, entry.TenantID, entry.TenantName)
			if hasTags {
				buf.WriteByte(',')
			}
		}
		// Display the tags if set.
		if hasTags {
			buf.WriteString(entry.Tags)
		}
		buf.WriteString("] ")
	}
	buf.Write(cp[ttycolor.Reset])

	// Display the counter if set and enabled.
	if showCounter && entry.Counter > 0 {
		tmp := buf.tmp[:len(buf.tmp)]
		n := buf.someDigits(0, int(entry.Counter))
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
		/* Date and time    */ `(\d{6} \d{2}:\d{2}:\d{2}.\d{6}(?:[---+]\d{6})?) ` +
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

func decodeTimestamp(fragment []byte) (unixNano int64, err error) {
	timeFormat := MessageTimeFormat
	if len(fragment) > 7 && (fragment[len(fragment)-7] == '+' || fragment[len(fragment)-7] == '-') {
		// The timestamp has a timezone offset.
		timeFormat = MessageTimeFormatWithTZ
	}
	t, err := time.Parse(timeFormat, string(fragment))
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
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
		var err error
		entry.Time, err = decodeTimestamp(m[2])
		if err != nil {
			return err
		}

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
		// Look for a tenant ID tag. Default to system otherwise.
		entry.TenantID = serverident.SystemTenantID
		tagsToProcess := m[7]
		entry.TenantID, entry.TenantName, tagsToProcess = maybeReadTenantDetails(tagsToProcess)

		// Process any remaining tags.
		if len(tagsToProcess) != 0 {
			r := redactablePackage{
				msg:        tagsToProcess,
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

// maybeReadTenantDetails reads the tenant ID and name. If neither the
// ID nor the name are present, the system tenant ID is returned.
func maybeReadTenantDetails(
	tagsToProcess []byte,
) (tenantID string, tenantName string, remainingTagsToProcess []byte) {
	var hasTenantID bool
	hasTenantID, tenantID, tagsToProcess = maybeReadOneTag(tagsToProcess, tenantIDLogTagBytePrefix)
	if !hasTenantID {
		// If we do not see a tenant ID, avoid reading a name. This is because (currently)
		// our log filtering is only able to use the tenant ID.
		// It's better to fold entries with an unknown tenant ID into the stream of
		// events for the system tenant, than mistakenly insert the system tenant ID
		// into the event stream of a secondary tenant.
		//
		// TODO(obs-inf): We will want a better story there when the name
		// will be used for filtering.
		return serverident.SystemTenantID, "", tagsToProcess
	}
	_, tenantName, tagsToProcess = maybeReadOneTag(tagsToProcess, tenantNameLogTagBytePrefix)
	return tenantID, tenantName, tagsToProcess
}

// maybeReadOneTag reads a single tag from the byte array if it
// starts with the given prefix.
func maybeReadOneTag(
	tagsToProcess []byte, prefix []byte,
) (foundValue bool, tagValue string, remainingTagsToProcess []byte) {
	if !bytes.HasPrefix(tagsToProcess, prefix) {
		return false, "", tagsToProcess
	}
	commaIndex := bytes.IndexByte(tagsToProcess, ',')
	if commaIndex < 0 {
		commaIndex = len(tagsToProcess)
	}
	retVal := string(tagsToProcess[1:commaIndex])
	tagsToProcess = tagsToProcess[commaIndex:]
	if len(tagsToProcess) > 0 && tagsToProcess[0] == ',' {
		tagsToProcess = tagsToProcess[1:]
	}
	return true, retVal, tagsToProcess
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
