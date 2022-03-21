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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/util/jsonbytes"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
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

	commonPrefixLen := buf.Len()

	if entry.structured {
		buf.WriteString(`,"event":{`)
		if entry.ch == channel.TELEMETRY {
			eventPrefixLen := buf.Len()
			buf.maybeJSONMultiLine(eventPrefixLen, entry.payload.redactable, entry.payload.message, true, entry.payload.splittable, 0)
		} else {
			buf.WriteString(entry.payload.message) // Already JSON.
		}
		buf.WriteByte('}')
	} else {
		// Message.
		if entry.ch == channel.TELEMETRY {
			buf.WriteString(`,`)
			msgBuffer := getBuffer()
			escapeString(msgBuffer, entry.payload.message)
			msgString := `"message":"` + msgBuffer.String() + `"`
			// Message prefix length increments by 1 to include the comma written
			// above.
			messagePrefixLen := commonPrefixLen + 1
			buf.maybeJSONMultiLine(messagePrefixLen, entry.payload.redactable, msgString, false, entry.payload.splittable, 0)
		} else {
			buf.WriteString(`,"message":"`)
			escapeString(buf, entry.payload.message)
			buf.WriteByte('"')
		}
	}

	// Stacks.
	if len(entry.stacks) > 0 {
		if entry.ch == channel.TELEMETRY {
			buf.WriteString(`,`)
			stacksBuffer := getBuffer()
			escapeString(stacksBuffer, string(entry.stacks))
			stacksString := `"stacks":"` + stacksBuffer.String() + `"`
			// Stacks prefix length increments by 1 to include the comma written
			// above.
			stacksPrefixLen := commonPrefixLen + 1
			lines := strings.Split(buf.String(), "\n")
			currentLog := lines[len(lines)-1] // Can we do this in a better way without splitting? (how can we get the length of the current log without splitting?)
			payloadSpaceConsumedByCurrentLog := len(currentLog) - stacksPrefixLen
			buf.maybeJSONMultiLine(stacksPrefixLen, false, stacksString, false, entry.payload.splittable, payloadSpaceConsumedByCurrentLog)
		} else {
			buf.WriteString(`,"stacks":"`)
			escapeString(buf, string(entry.stacks))
			buf.WriteByte('"')
		}
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

type jsonLongLineLen int

// truncateLineLenJSON is the max length of a message/event/stack with JSON
// formatting, before it gets truncated.
var truncateLineLenJSON jsonLongLineLen

// splitLineLenJSON is the max length of a message/event/stack with JSON
// formatting, before it gets broken into multiple lines.
var splitLineLenJSON jsonLongLineLen

// longLineLenJSON is the line length being used during formatting, it is set
// to either truncateLineLenJSON or splitLineLenJSON depending on whether we
// are truncating or splitting the log.
var longLineLenJSON jsonLongLineLen

func init() {
	truncateLineLenJSON.set(2048)
	splitLineLenJSON.set(10240)
}

func (l *jsonLongLineLen) set(v int) {
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
	// We need to add opening and closing splittable markers when we split.
	suffixLen += len(startSplittableMarker) + len(endSplittableMarker)
	// If we are breaking in the middle of a string, we need to include a closing
	// quotation mark.
	suffixLen++
	// Need to add a closing bracket to close the payload message.
	suffixLen++
	// Need to add a closing bracket to close the log entry.
	suffixLen++
	newMax := v - suffixLen
	if newMax < 1 {
		panic("max line length cannot be zero or negative")
	}
	*l = jsonLongLineLen(newMax)
}

func resetLongLineLenJSON(splittable bool) {
	longLineLenJSON = truncateLineLenJSON
	if splittable {
		longLineLenJSON = splitLineLenJSON
	}
}

var startSplittableMarker = string(eventpb.StartSplittableMarker())
var endSplittableMarker = string(eventpb.EndSplittableMarker())

// shouldSplitOnKey checks if the key space exceeds or matches the remaining
// amount of space in log. If so, split the log early (move key to next log).
// Note: this implementation assumes that the key buffer length will not exceed
// the entire longLineLenJSON.
func shouldSplitOnKey(prefixLen int, lastLen int, keyBufferLen int) bool {
	// We only split on a JSON key when there is not enough space for the key,
	// and the smallest part of its value. longLineLenJSON already accounts for
	// a value's suffix length (max utf8 character, redaction markers, and
	// closing string quote), here we include the opening string quote as a
	// prefix.
	const valueOpeningQuoteLen = 1
	return prefixLen+lastLen+keyBufferLen >= int(longLineLenJSON)+valueOpeningQuoteLen
}

func shouldSplitOnValue(prefixLen, totalLen int, isStringValue bool, isSplittable bool, isEvent bool) bool {
	// If the payload is structured, but we are either:
	// - not in a string value
	// - or the value is not designated as splittable
	// we do not split the value.
	// Otherwise, we split if the long line length has
	// been exceeded.
	if isEvent && (!isStringValue || !isSplittable) {
		return false
	}
	if prefixLen+totalLen >= int(longLineLenJSON) {
		return true
	}
	return false
}

func (buf *buffer) maybeJSONMultiLine(prefixLen int, redactable bool, msg string, isEvent bool, splittable bool, payloadSpaceConsumedInCurrentLog int) {
	var i int
	for i = len(msg) - 1; i > 0 && msg[i] == '\n'; i-- {
		msg = msg[:i]
	}

	resetLongLineLenJSON(splittable)
	longLineLenJSON -= jsonLongLineLen(payloadSpaceConsumedInCurrentLog)
	lastLen := 0
	beforeColon := true
	betweenRedactionMarkers := false
	betweenSplittableMarkers := false
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
				if shouldSplitOnKey(prefixLen, lastLen, len(keyBuffer)) {
					// If the last byte in the buffer is a comma.
					if buf.Bytes()[len(buf.Bytes())-1] == ',' {
						// If the message is a structured event, replace with closing
						// bracket.
						if isEvent {
							buf.Bytes()[len(buf.Bytes())-1] = '}'
						} else {
							// Otherwise, truncate the buffer to remove the comma byte.
							buf.Truncate(len(buf.Bytes()) - 1)
						}
					}
					// Close the entire log.
					buf.WriteByte('}')
					// If we are not splitting (we are truncating), end early.
					if !splittable {
						return
					}
					// Move to next log.
					buf.WriteByte('\n')
					// Previous payload space has been freed, reset long line length.
					resetLongLineLenJSON(splittable)
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
			if shouldSplitOnValue(prefixLen, lastLen+len(keyBuffer)+len(valueBuffer), betweenQuotesInValue, betweenSplittableMarkers, isEvent) {
				buf.Write(keyBuffer)
				buf.Write(valueBuffer)
				if betweenRedactionMarkers {
					// We are breaking a long line in-between redaction
					// markers. Ensures that the opening and closing markers do
					// not straddle log entries.
					buf.WriteString(endRedactionMarker)
				}
				if betweenSplittableMarkers {
					// We are breaking a long line in-between splittable
					// markers. Ensures that the opening and closing markers do
					// not straddle log entries.
					buf.WriteString(endSplittableMarker)
				}
				if betweenQuotesInValue {
					// Add a closing quote.
					buf.WriteByte('"')
				}
				// If we have reached the end of the log, finish instead of creating
				// another (empty) log. Closing event & log bracket will be added by
				// caller.
				if i+w >= len(msg) {
					return
				}
				if isEvent {
					// Add a closing bracket.
					buf.WriteByte('}')
				}
				// Close the entire JSON payload.
				buf.WriteByte('}')
				// If we are not splitting (we are truncating), end early.
				if !splittable {
					return
				}
				// Move to next log.
				buf.WriteByte('\n')
				// Previous payload space has been freed, reset long line length.
				resetLongLineLenJSON(splittable)
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
				if betweenSplittableMarkers {
					// See above: if we are splitting in-between splittable
					// markers, continue the split item on the new line.
					valueBuffer = append(valueBuffer, startSplittableMarker...)
				}
				if betweenRedactionMarkers {
					// See above: if we are splitting in-between redaction
					// markers, continue the sensitive item on the new line.
					valueBuffer = append(valueBuffer, startRedactionMarker...)
				}
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

			// If we see an opening splittable marker, remember this fact
			// so that we close/open it properly.
			if strings.HasPrefix(msg[i:], startSplittableMarker) {
				betweenSplittableMarkers = true
				valueBuffer = append(valueBuffer, startSplittableMarker...)
				w = len(startSplittableMarker)
				continue
			} else if strings.HasPrefix(msg[i:], endSplittableMarker) {
				betweenSplittableMarkers = false
				valueBuffer = append(valueBuffer, endSplittableMarker...)
				w = len(endSplittableMarker)
				continue
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

type entryDecoderJSON struct {
	decoder         *json.Decoder
	sensitiveEditor redactEditor
	compact         bool
}

type jsonCommon struct {
	Header  int                    `json:"header,omitempty"`
	Message string                 `json:"message"`
	Stacks  string                 `json:"stacks"`
	Tags    map[string]interface{} `json:"tags"`
	Event   map[string]interface{} `json:"event"`
}

// JSONEntry represents a JSON log entry.
type JSONEntry struct {
	jsonCommon

	//Channel         Channel  `json:"channel,omitempty"`
	ChannelNumeric int64  `json:"channel_numeric,omitempty"`
	Timestamp      string `json:"timestamp,omitempty"`
	//Severity        Severity `json:"severity,omitempty"`
	SeverityNumeric int64  `json:"severity_numeric,omitempty"`
	Goroutine       int64  `json:"goroutine,omitempty"`
	File            string `json:"file,omitempty"`
	Line            int64  `json:"line,omitempty"`
	EntryCounter    uint64 `json:"entry_counter,omitempty"`
	Redactable      int    `json:"redactable,omitempty"`
	NodeID          int64  `json:"node_id,omitempty"`
	ClusterID       string `json:"cluster_id,omitempty"`
	Version         string `json:"version,omitempty"`
	InstanceID      int64  `json:"instance_id,omitempty"`
	TenantID        int64  `json:"tenant_id,omitempty"`
}

// JSONCompactEntry represents a JSON log entry in the compact format.
type JSONCompactEntry struct {
	jsonCommon

	//Channel         Channel  `json:"C,omitempty"`
	ChannelNumeric int64  `json:"c,omitempty"`
	Timestamp      string `json:"t,omitempty"`
	//Severity        Severity `json:"sev,omitempty"`
	SeverityNumeric int64  `json:"s,omitempty"`
	Goroutine       int64  `json:"g,omitempty"`
	File            string `json:"f,omitempty"`
	Line            int64  `json:"l,omitempty"`
	EntryCounter    uint64 `json:"n,omitempty"`
	Redactable      int    `json:"r,omitempty"`
	NodeID          int64  `json:"N,omitempty"`
	ClusterID       string `json:"x,omitempty"`
	Version         string `json:"v,omitempty"`
	InstanceID      int64  `json:"q,omitempty"`
	TenantID        int64  `json:"T,omitempty"`
}

// populate is a method that populates fields from the source JSONEntry
// into the `logpb.Entry`. Redactability is applied to the tags,
// message, stacks, and event fields if it's missing.
func (e *JSONEntry) populate(entry *logpb.Entry, d *entryDecoderJSON) (*redactablePackage, error) {
	ts, err := fromFluent(e.Timestamp)
	if err != nil {
		return nil, err
	}
	entry.Time = ts

	entry.Goroutine = e.Goroutine
	entry.File = e.File
	entry.Line = e.Line
	entry.Redactable = e.Redactable == 1

	if e.Header == 0 {
		entry.Severity = Severity(e.SeverityNumeric)
		entry.Channel = Channel(e.ChannelNumeric)
		entry.Counter = e.EntryCounter
	}

	var entryMsg bytes.Buffer
	if e.Event != nil {
		by, err := json.Marshal(e.Event)
		if err != nil {
			return nil, err
		}
		entryMsg.Write(by)
		entry.StructuredStart = 0
		entry.StructuredEnd = uint32(entryMsg.Len())
	} else {
		entryMsg.Write([]byte(e.Message))
	}

	if e.Tags != nil {
		var t *logtags.Buffer
		for k, v := range e.Tags {
			t = t.Add(k, v)
		}
		s := &strings.Builder{}
		t.FormatToString(s)
		tagStrings := strings.Split(s.String(), ",")
		sort.Strings(tagStrings)
		r := redactablePackage{
			msg:        []byte(strings.Join(tagStrings, ",")),
			redactable: entry.Redactable,
		}
		r = d.sensitiveEditor(r)
		entry.Tags = string(r.msg)
	}

	if e.Stacks != "" {
		entry.StackTraceStart = uint32(entryMsg.Len()) + 1
		entryMsg.Write([]byte("\nstack trace:\n"))
		entryMsg.Write([]byte(e.Stacks))
	}

	return &redactablePackage{
		msg:        entryMsg.Bytes(),
		redactable: entry.Redactable,
	}, nil
}

func (e *JSONCompactEntry) toEntry(entry *JSONEntry) {
	entry.jsonCommon = e.jsonCommon
	entry.ChannelNumeric = e.ChannelNumeric
	entry.Timestamp = e.Timestamp
	entry.SeverityNumeric = e.SeverityNumeric
	entry.Goroutine = e.Goroutine
	entry.File = e.File
	entry.Line = e.Line
	entry.EntryCounter = e.EntryCounter
	entry.Redactable = e.Redactable
	entry.NodeID = e.NodeID
	entry.ClusterID = e.ClusterID
	entry.Version = e.Version
	entry.InstanceID = e.InstanceID
	entry.TenantID = e.TenantID
}

// Decode decodes the next log entry into the provided protobuf message.
func (d *entryDecoderJSON) Decode(entry *logpb.Entry) error {
	var rp *redactablePackage
	var e JSONEntry
	if d.compact {
		var compact JSONCompactEntry
		err := d.decoder.Decode(&compact)
		if err != nil {
			return err
		}
		compact.toEntry(&e)
	} else {
		err := d.decoder.Decode(&e)
		if err != nil {
			return err
		}
	}
	rp, err := e.populate(entry, d)
	if err != nil {
		return err
	}

	r := d.sensitiveEditor(*rp)
	entry.Message = string(r.msg)
	entry.Redactable = r.redactable

	return nil
}

// fromFluent parses a fluentbit timestamp format into nanoseconds since
// the epoch. The fluentbit format is a string consisting of two
// concatenanted integers joined by a `.`. The left-hand side is the
// number of seconds since the epich, the right hand side is the
// additional number of nanoseconds of precision.
//
// For example: `"1136214245.654321000"` parses into `1136214245654321000`.
func fromFluent(timestamp string) (int64, error) {
	parts := strings.Split(timestamp, ".")
	if len(parts) != 2 {
		return 0, errors.New("bad timestamp format")
	}
	left, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}
	right, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return left*1000000000 + right, nil
}
