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
	"bytes"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/util/jsonbytes"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/redact"
)

type formatFluentJSONCompact struct{}

func (formatFluentJSONCompact) formatterName() string { return "json-fluent-compact" }

func (formatFluentJSONCompact) doc() string { return formatJSONDoc(true /* fluent */, tagCompact) }

func (f formatFluentJSONCompact) formatEntry(entry logEntry) *buffer {
	return formatJSON(entry, true /* fluent */, tagCompact)
}

func (formatFluentJSONCompact) contentType() string { return "application/json" }

type formatFluentJSONFull struct{}

func (formatFluentJSONFull) formatterName() string { return "json-fluent" }

func (f formatFluentJSONFull) formatEntry(entry logEntry) *buffer {
	return formatJSON(entry, true /* fluent */, tagVerbose)
}

func (formatFluentJSONFull) doc() string { return formatJSONDoc(true /* fluent */, tagVerbose) }

func (formatFluentJSONFull) contentType() string { return "application/json" }

type formatJSONCompact struct{}

func (formatJSONCompact) formatterName() string { return "json-compact" }

func (f formatJSONCompact) formatEntry(entry logEntry) *buffer {
	return formatJSON(entry, false /* fluent */, tagCompact)
}

func (formatJSONCompact) doc() string { return formatJSONDoc(false /* fluent */, tagCompact) }

func (formatJSONCompact) contentType() string { return "application/json" }

type formatJSONFull struct{}

func (formatJSONFull) formatterName() string { return "json" }

func (f formatJSONFull) formatEntry(entry logEntry) *buffer {
	return formatJSON(entry, false /* fluent */, tagVerbose)
}

func (formatJSONFull) doc() string { return formatJSONDoc(false /* fluent */, tagVerbose) }

func (formatJSONFull) contentType() string { return "application/json" }

func formatJSONDoc(forFluent bool, tags tagChoice) string {
	var buf strings.Builder
	buf.WriteString(`This format emits log entries as a JSON payload.

The JSON object is guaranteed to not contain unescaped newlines
or other special characters, and the entry as a whole is followed
by a newline character. This makes the format suitable for
processing over a stream unambiguously.

Each entry contains at least the following fields:

| Field | Description |
|-------|-------------|
`)
	if forFluent {
		buf.WriteString("| `tag` | A Fluent tag for the event, formed by the process name and the logging channel. |\n")
	}

	keys := make([]string, 0, len(jsonTags))
	for c := range jsonTags {
		if strings.IndexByte(serverIdentifierFields, c) != -1 {
			continue
		}
		keys = append(keys, string(c))
	}
	sort.Strings(keys)
	for _, k := range keys {
		c := k[0]
		if !jsonTags[c].includedInHeader {
			continue
		}
		fmt.Fprintf(&buf, "| `%s` | %s |\n", jsonTags[c].tags[tags], jsonTags[c].description)
	}

	buf.WriteString(`

After a couple of *header* entries written at the beginning of each log sink,
all subsequent log entries also contain the following fields:

| Field               | Description |
|---------------------|-------------|
`)
	for _, k := range keys {
		c := k[0]
		if jsonTags[c].includedInHeader {
			continue
		}
		fmt.Fprintf(&buf, "| `%s` | %s |\n", jsonTags[c].tags[tags], jsonTags[c].description)
	}

	buf.WriteString(`

Additionally, the following fields are conditionally present:

| Field               | Description |
|---------------------|-------------|
`)
	for _, k := range serverIdentifierFields {
		b := byte(k)
		fmt.Fprintf(&buf, "| `%s` | %s |\n", jsonTags[b].tags[tags], jsonTags[b].description)
	}

	buf.WriteString(`| ` + "`tags`" + `    | The logging context tags for the entry, if there were context tags. |
| ` + "`message`" + ` | For unstructured events, the flat text payload. |
| ` + "`event`" + `   | The logging event, if structured (see below for details). |
| ` + "`stacks`" + `  | Goroutine stacks, for fatal events. |

When an entry is structured, the ` + "`event`" + ` field maps to a dictionary
whose structure is one of the documented structured events. See the [reference documentation](eventlog.html)
for structured events for a list of possible payloads.

When the entry is marked as ` + "`redactable`" + `, the ` + "`tags`, `message`, and/or `event`" + ` payloads
contain delimiters (` + string(redact.StartMarker()) + "..." + string(redact.EndMarker()) + `) around
fields that are considered sensitive. These markers are automatically recognized
by ` + "[`cockroach debug zip`](cockroach-debug-zip.html)" + ` and ` +
		"[`cockroach debug merge-logs`](cockroach-debug-merge-logs.html)" + ` when log redaction is requested.


`)

	return buf.String()
}

var jsonTags = map[byte]struct {
	tags             [2]string
	description      string
	includedInHeader bool
}{
	'c': {[2]string{"c", "channel_numeric"},
		"The numeric identifier for the logging channel where the event was sent.", false},
	'C': {[2]string{"C", "channel"},
		"The name of the logging channel where the event was sent.", false},
	't': {[2]string{"t", "timestamp"},
		"The timestamp at which the event was emitted on the logging channel.", true},
	's': {[2]string{"s", "severity_numeric"},
		"The numeric value of the severity of the event.", false},
	'S': {[2]string{"sev", "severity"},
		"The severity of the event.", false},
	'g': {[2]string{"g", "goroutine"},
		"The identifier of the goroutine where the event was emitted.", true},
	'f': {[2]string{"f", "file"},
		"The name of the source file where the event was emitted.", true},
	'l': {[2]string{"l", "line"},
		"The line number where the event was emitted in the source.", true},
	'n': {[2]string{"n", "entry_counter"},
		"The entry number on this logging sink, relative to the last process restart.", false},
	'r': {[2]string{"r", "redactable"},
		"Whether the payload is redactable (see below for details).", true},
	'N': {[2]string{"N", "node_id"},
		"The node ID where the event was generated, once known. Only reported for single-tenant or KV servers.", true},
	'x': {[2]string{"x", "cluster_id"},
		"The cluster ID where the event was generated, once known. Only reported for single-tenant of KV servers.", true},
	'v': {[2]string{"v", "version"},
		"The binary version with which the event was generated.", true},
	// SQL servers in multi-tenant deployments.
	'q': {[2]string{"q", "instance_id"},
		"The SQL instance ID where the event was generated, once known. Only reported for multi-tenant SQL servers.", true},
	'T': {[2]string{"T", "tenant_id"},
		"The SQL tenant ID where the event was generated, once known. Only reported for multi-tenant SQL servers.", true},
}

const serverIdentifierFields = "NxqT"

type tagChoice int

const (
	tagCompact tagChoice = 0
	tagVerbose tagChoice = 1
)

var channelNamesLowercase = func() map[Channel]string {
	lnames := make(map[Channel]string, len(logpb.Channel_name))
	for ch, s := range logpb.Channel_name {
		lnames[Channel(ch)] = strings.ToLower(s)
	}
	return lnames
}()

func formatJSON(entry logEntry, forFluent bool, tags tagChoice) *buffer {
	jtags := jsonTags
	buf := getBuffer()
	buf.WriteByte('{')
	if forFluent {
		// Tag: this is the main category for Fluentd events.
		buf.WriteString(`"tag":"`)
		// Note: fluent prefers if there is no period in the tag other
		// than the one splitting the application and category.
		// We rely on program having been processed by replacePeriods()
		// already.
		// Also use escapeString() in case program contains double
		// quotes or other special JSON characters.
		escapeString(buf, fileNameConstants.program)
		buf.WriteByte('.')
		if !entry.header {
			buf.WriteString(channelNamesLowercase[entry.ch])
		} else {
			// Sink headers have no channel.
			// Note: this string should never occur in practice, when the sink
			// is connected to Fluentd over the network. Header entries
			// only occur when emitting to file sinks, and when using file
			// output it's likelier for the user to use format 'json' instead
			// of 'json-fluent'.
			buf.WriteString("unknown")
		}
		// Also include the channel number in numeric form to facilitate
		// automatic processing.
		buf.WriteString(`",`)
	}
	if !entry.header {
		buf.WriteByte('"')
		buf.WriteString(jtags['c'].tags[tags])
		buf.WriteString(`":`)
		n := buf.someDigits(0, int(entry.ch))
		buf.Write(buf.tmp[:n])
		if tags != tagCompact {
			buf.WriteString(`,"`)
			buf.WriteString(jtags['C'].tags[tags])
			buf.WriteString(`":"`)
			escapeString(buf, entry.ch.String())
			buf.WriteByte('"')
		}
		buf.WriteByte(',')
	} else {
		buf.WriteString(`"header":1,`)
	}
	// Timestamp.
	// Note: fluentd is particular about the time format; although this
	// looks like a float with a fractional number of seconds, fluentd
	// interprets the number after the period as a number of
	// nanoseconds. So for example "1.2" is interpreted as "2
	// nanoseconds after the second". So we really need to emit all 9
	// digits.
	// Also, we enclose the timestamp in double quotes because the
	// precision of the resulting number exceeds json's native float
	// precision. Fluentd doesn't care and still parses the value properly.
	buf.WriteByte('"')
	buf.WriteString(jtags['t'].tags[tags])
	buf.WriteString(`":"`)
	n := buf.someDigits(0, int(entry.ts/1000000000))
	buf.tmp[n] = '.'
	n++
	n += buf.nDigits(9, n, int(entry.ts%1000000000), '0')
	buf.Write(buf.tmp[:n])
	buf.WriteByte('"')

	// Server identifiers.
	if entry.clusterID != "" {
		buf.WriteString(`,"`)
		buf.WriteString(jtags['x'].tags[tags])
		buf.WriteString(`":"`)
		escapeString(buf, entry.clusterID)
		buf.WriteByte('"')
	}
	if entry.nodeID != "" {
		buf.WriteString(`,"`)
		buf.WriteString(jtags['N'].tags[tags])
		buf.WriteString(`":`)
		buf.WriteString(entry.nodeID)
	}
	if entry.tenantID != "" {
		buf.WriteString(`,"`)
		buf.WriteString(jtags['T'].tags[tags])
		buf.WriteString(`":`)
		buf.WriteString(entry.tenantID)
	}
	if entry.sqlInstanceID != "" {
		buf.WriteString(`,"`)
		buf.WriteString(jtags['q'].tags[tags])
		buf.WriteString(`":`)
		buf.WriteString(entry.sqlInstanceID)
	}

	// The binary version.
	if entry.version != "" {
		buf.WriteString(`,"`)
		buf.WriteString(jtags['v'].tags[tags])
		buf.WriteString(`":"`)
		escapeString(buf, entry.version)
		buf.WriteByte('"')
	}

	if !entry.header {
		// Severity, both in numeric form (for ease of processing) and
		// string form (to facilitate human comprehension).
		buf.WriteString(`,"`)
		buf.WriteString(jtags['s'].tags[tags])
		buf.WriteString(`":`)
		n = buf.someDigits(0, int(entry.sev))
		buf.Write(buf.tmp[:n])

		if tags == tagCompact {
			if entry.sev > 0 && int(entry.sev) <= len(severityChar) {
				buf.WriteString(`,"`)
				buf.WriteString(jtags['S'].tags[tags])
				buf.WriteString(`":"`)
				buf.WriteByte(severityChar[int(entry.sev)-1])
				buf.WriteByte('"')
			}
		} else {
			buf.WriteString(`,"`)
			buf.WriteString(jtags['S'].tags[tags])
			buf.WriteString(`":"`)
			escapeString(buf, entry.sev.String())
			buf.WriteByte('"')
		}
	}

	// Goroutine number.
	buf.WriteString(`,"`)
	buf.WriteString(jtags['g'].tags[tags])
	buf.WriteString(`":`)
	n = buf.someDigits(0, int(entry.gid))
	buf.Write(buf.tmp[:n])

	// Source location.
	buf.WriteString(`,"`)
	buf.WriteString(jtags['f'].tags[tags])
	buf.WriteString(`":"`)
	escapeString(buf, entry.file)
	buf.WriteString(`","`)
	buf.WriteString(jtags['l'].tags[tags])
	buf.WriteString(`":`)
	n = buf.someDigits(0, entry.line)
	buf.Write(buf.tmp[:n])

	if !entry.header {
		// Entry counter.
		buf.WriteString(`,"`)
		buf.WriteString(jtags['n'].tags[tags])
		buf.WriteString(`":`)
		n = buf.someDigits(0, int(entry.counter))
		buf.Write(buf.tmp[:n])
	}

	// Whether the tags/message are redactable.
	// We use 0/1 instead of true/false, because
	// it's likely there will be more redaction formats
	// in the future.
	buf.WriteString(`,"`)
	buf.WriteString(jtags['r'].tags[tags])
	buf.WriteString(`":`)
	if entry.payload.redactable {
		buf.WriteByte('1')
	} else {
		buf.WriteByte('0')
	}

	// Tags.
	if entry.payload.tags != nil {
		buf.WriteString(`,"tags":{`)
		entry.payload.tags.formatJSONToBuffer(buf)
		buf.WriteByte('}')
	}

	// Stacks.
	if len(entry.stacks) > 0 {
		buf.WriteString(`,"stacks":"`)
		escapeString(buf, string(entry.stacks))
		buf.WriteByte('"')
	}

	if entry.structured {
		buf.WriteString(`,"event":{`)
		commonPrefixLen := buf.Len()
		buf.maybeJSONMultiLine(commonPrefixLen, entry.payload.redactable, entry.payload.message)
		buf.WriteByte('}')
	} else {
		// Message.
		buf.WriteString(`,"message":"`)
		escapeString(buf, entry.payload.message)
		buf.WriteByte('"')
	}

	buf.WriteByte('}')
	buf.WriteByte('\n')
	return buf
}

func escapeString(buf *buffer, s string) {
	b := buf.Bytes()
	b = jsonbytes.EncodeString(b, s)
	buf.Buffer = *bytes.NewBuffer(b)
}

type JSONLongLineLen int

// jsonLongLineLen is the max length of a log entry with JSON formatting,
// before it gets broken up into multiple lines. This maximum is applied
// to the size of the entry without considering the prefix (timestamp, location
// etc) or suffix (stack traces etc.).
var jsonLongLineLen JSONLongLineLen

// minValueSpaceBuffer is the minimum amount of space required for a JSON value
// to be inserted into a log. If there is not enough space in the current log
// for the JSON value, we end the current log and insert it into the next one.
const minValueSpaceBuffer = 8

func init() {
	jsonLongLineLen.set(16 * 10)
}

func (l *JSONLongLineLen) set(v int) {
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
	// We would like to include an ending '|' marker to indicate if the current payload
	// has been split into multiple entries.
	suffixLen += 1
	// We also cannot break a long entry without a closing quotation mark
	// and two closing JSON brackets (one for the message, one for the entire
	// JSON log).
	suffixLen += 3
	newMax := v - suffixLen
	if newMax < 1 {
		panic("max line length cannot be zero or negative")
	}
	*l = JSONLongLineLen(newMax)
}

func (l JSONLongLineLen) shouldBreakValue(lastLen int) bool {
	return lastLen >= int(l)
}

func (buf *buffer) maybeJSONMultiLine(
	prefixLen int, redactable bool, msg string,
) {
	var i int
	for i = len(msg) - 1; i > 0 && msg[i] == '\n'; i-- {
		msg = msg[:i]
	}

	lastLen := 0
	beforeColon := true
	betweenRedactionMarkers := false
	betweenQuotesInValue := false
	inValueNoQuotes := false
	var keyBuffer []byte
	var valueBuffer []byte

	// Note: This implementation is under the assumption the payload is
	// well-formed JSON, and that the payload message does not contain inner
	// JSON objects.
	for i, w := 0, 0; i < len(msg); i += w {
		runeValue, width := utf8.DecodeRuneInString(msg[i:])
		w = width
		if beforeColon {
			keyBuffer = append(keyBuffer, string(runeValue)...)
			if runeValue == ':' {
				// Check if the key space exceeds or matches the remaining amount of
				// space in log (plus additional buffer for some space for the value).
				// If so, split the log early.
				// TODO(thomas): extract logic to function
				if lastLen+len(keyBuffer) >= int(jsonLongLineLen)-minValueSpaceBuffer {
					// If the last byte in the buffer is a comma, replace with closing bracket.
					if buf.Bytes()[len(buf.Bytes())-1] == ',' {
						buf.Bytes()[len(buf.Bytes())-1] = '}'
					}
					// Close the entire log.
					buf.WriteByte('}')
					// Move to next log.
					buf.WriteByte('\n')
					// Write the prefix.
					buf.Write(buf.Bytes()[0:prefixLen])
					// Reset the length of the log.
					lastLen = 0
				}
				// Update that we have past the JSON field's colon (i.e. entering JSON
				// value).
				beforeColon = !beforeColon
				// Move to the next character.
				continue
			}
		}

		// If we are after the colon (i.e. in the JSON value).
		if !beforeColon {
			// If the value buffer is empty, this is the first character in the new
			// value.
			if len(valueBuffer) == 0 {
				if runeValue == '"' {
					valueBuffer = append(valueBuffer, string(runeValue)...)
					betweenQuotesInValue = true
					continue
				} else {
					inValueNoQuotes = true
				}
			}

			// Check if the log should be split.
			// TODO(thomas): extract logic to function
			if lastLen+len(keyBuffer)+len(valueBuffer) >= int(jsonLongLineLen) {
				buf.Write(keyBuffer)
				buf.Write(valueBuffer)
				// Write the marker to signify the end split of the payload.
				buf.WriteByte('|')
				if betweenRedactionMarkers {
					// We are breaking a long line in-between redaction
					// markers. Ensures that the opening and closing markers do
					// not straddle log entries.
					buf.WriteString(endRedactionMarker)
				}
				if betweenQuotesInValue {
					// Add a closing quote.
					buf.WriteByte('"')
				}
				// Add a closing bracket.
				buf.WriteByte('}')
				// Close the entire JSON payload.
				buf.WriteByte('}')
				// Move to next log.
				buf.WriteByte('\n')
				// Reset the length of the log.
				lastLen = 0
				// Clear the value buffer.
				valueBuffer = valueBuffer[:0]
				// Write the prefix (i.e. "... event:{").
				buf.Write(buf.Bytes()[0:prefixLen])

				if betweenQuotesInValue {
					// Write an opening quote to the value buffer.
					valueBuffer = append(valueBuffer, '"')
				}

				if betweenRedactionMarkers {
					// See above: if we are splitting in-between redaction
					// markers, continue the sensitive item on the new line.
					valueBuffer = append(valueBuffer, startRedactionMarker...)
				}
				// Write the marker to mark the beginning split of the payload.
				valueBuffer = append(valueBuffer, '|')
			}

			if redactable {
				// If we see an opening redaction marker, remember this fact
				// so that we close/open it properly.
				if strings.HasPrefix(msg[i:], startRedactionMarker) {
					betweenRedactionMarkers = true
					valueBuffer = append(valueBuffer, startRedactionMarker...)
					w = len(startRedactionMarker)
					continue
				} else if strings.HasPrefix(msg[i:], endRedactionMarker) {
					betweenRedactionMarkers = false
					valueBuffer = append(valueBuffer, endRedactionMarker...)
					w = len(endRedactionMarker)
					continue
				}
			}

			// After checking for a log split and redaction markers, append the
			// current rune to the value buffer.
			valueBuffer = append(valueBuffer, string(runeValue)...)

			if betweenQuotesInValue {
				// If we are in a string value and the current rune is a backslash.
				if runeValue == '\\' {
					// Append the escaped character.
					nextRuneValue, nextWidth := utf8.DecodeRuneInString(msg[i+w:])
					valueBuffer = append(valueBuffer, string(nextRuneValue)...)
					w += nextWidth
				} else if runeValue == '"' {
					// If we are in a string value and the current rune is a double
					// quote, we have reached the end of the string.

					// If there exists another rune after the closing quote, another JSON
					// field exists, append a comma after the current value.
					if i+w < len(msg)-1 {
						nextRuneValue, nextWidth := utf8.DecodeRuneInString(msg[i+w:])
						// Expect the next character to be a comma if we encounter the
						// closing quotation mark, and we are not at the end of msg.
						if nextRuneValue == ',' {
							valueBuffer = append(valueBuffer, string(nextRuneValue)...)
							w += nextWidth
						} else {
							panic("expected comma after closing quotation mark in value got " + string(nextRuneValue))
						}
					}

					// We have parsed the entire value. Write the key-value pair.
					buf.Write(keyBuffer)
					buf.Write(valueBuffer)
					lastLen += len(keyBuffer) + len(valueBuffer)
					// Clear the buffers.
					keyBuffer = keyBuffer[:0]
					valueBuffer = valueBuffer[:0]
					// Update that we are out of the value.
					beforeColon = !beforeColon
					betweenQuotesInValue = false
				}
			}

			if inValueNoQuotes {
				// We write the current key-value pairing to the buffer when:
				//	- we are in a non-string value and encounter a comma, we have
				// 	finished parsing the value
				// 	- there is no comma, but we have finished parsing the message
				//	(there does not exist another rune after the current rune)
				if runeValue == ',' || i+w >= len(msg) {
					// We have parsed the entire value. Write the key-value pair.
					buf.Write(keyBuffer)
					buf.Write(valueBuffer)
					lastLen += len(keyBuffer) + len(valueBuffer)
					// Clear the buffers.
					keyBuffer = keyBuffer[:0]
					valueBuffer = valueBuffer[:0]
					// Update that we are out of the value.
					beforeColon = !beforeColon
					inValueNoQuotes = false
				}
			}
		}
	}
}
