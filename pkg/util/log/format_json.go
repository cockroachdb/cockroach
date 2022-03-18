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

	"github.com/cockroachdb/cockroach/pkg/util/jsonbytes"
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

	if entry.structured {
		buf.WriteString(`,"event":{`)
		buf.WriteString(entry.payload.message) // Already JSON.
		buf.WriteByte('}')
	} else {
		// Message.
		buf.WriteString(`,"message":"`)
		escapeString(buf, entry.payload.message)
		buf.WriteByte('"')
	}

	// Stacks.
	if len(entry.stacks) > 0 {
		buf.WriteString(`,"stacks":"`)
		escapeString(buf, string(entry.stacks))
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

type entryDecoderJSON struct {
	decoder         *json.Decoder
	sensitiveEditor redactEditor
}

func getFirstMatchString(obj map[string]interface{}, fields [2]string) (string, bool) {
	for _, f := range fields {
		if v, ok := obj[f]; ok {
			vString, ok := v.(string)
			if ok {
				return vString, true
			}
		}
	}
	return "", false
}

func getFirstMatchInt64(obj map[string]interface{}, fields [2]string) (int64, bool) {
	for _, f := range fields {
		if v, ok := obj[f]; ok {
			vFloat, ok := v.(float64)
			if ok {
				return int64(vFloat), true
			}
		}
	}
	return 0, false
}

// Decode decodes the next log entry into the provided protobuf message.
func (d *entryDecoderJSON) Decode(entry *logpb.Entry) error {
	var e map[string]interface{}

	err := d.decoder.Decode(&e)
	if err != nil {
		return err
	}

	if jsonTs, ok := getFirstMatchString(e, jsonTags['t'].tags); ok {
		ts, err := fromFluent(jsonTs)
		if err != nil {
			return err
		}
		entry.Time = ts
	}

	if goroutine, ok := getFirstMatchInt64(e, jsonTags['g'].tags); ok {
		entry.Goroutine = goroutine
	}

	if file, ok := getFirstMatchString(e, jsonTags['f'].tags); ok {
		entry.File = file
	}

	if line, ok := getFirstMatchInt64(e, jsonTags['l'].tags); ok {
		entry.Line = line
	}

	if redactable, ok := getFirstMatchInt64(e, jsonTags['r'].tags); ok {
		entry.Redactable = redactable == 1
	}

	// If not a header entry, process the fields that belong to such entries.
	if header, ok := getFirstMatchInt64(e, [2]string{"header", ""}); !ok || header == 0 {
		if severityNumeric, ok := getFirstMatchInt64(e, jsonTags['s'].tags); ok {
			entry.Severity = Severity(severityNumeric)
		}
		if channelNumeric, ok := getFirstMatchInt64(e, jsonTags['c'].tags); ok {
			entry.Channel = Channel(channelNumeric)
		}
		if counter, ok := getFirstMatchInt64(e, jsonTags['n'].tags); ok {
			entry.Counter = uint64(counter)
		}
	}

	var entryMsg bytes.Buffer
	// Process the message.
	if ev, ok := e["event"]; ok {
		if event, ok := ev.(map[string]interface{}); ok {
			if len(event) > 0 {
				// TODO(davidh): HANDLE ERROR!!!!
				by, _ := json.Marshal(event)
				entryMsg.Write(by)
				entry.StructuredStart = 0
				entry.StructuredEnd = uint32(entryMsg.Len())
			}
		}
	} else {
		if msg, ok := e["message"]; ok {
			if message, ok := msg.(string); ok {
				entryMsg.Write([]byte(message))
			}
		}

	}

	// Process conditional fields.
	if tgs, ok := e["tags"]; ok {
		if tags, ok := tgs.(map[string]interface{}); ok {
			var t *logtags.Buffer
			for k, v := range tags {
				if vs, ok := v.(string); ok {
					t = t.Add(k, vs)
				}
			}
			// TODO(davidh): does redaction need to be handled here???
			// TODO(davidh): in the test cases, tag values are not getting redacted...
			s := &strings.Builder{}
			t.FormatToString(s)
			tagStrings := strings.Split(s.String(), ",")
			sort.Strings(tagStrings)
			entry.Tags = strings.Join(tagStrings, ",")
		}
	}

	if stacks, ok := e["stacks"]; ok {
		if stacksString, ok := stacks.(string); ok {
			entry.StackTraceStart = uint32(entryMsg.Len()) + 1
			entryMsg.Write([]byte("\nstack trace:\n"))
			entryMsg.Write([]byte(stacksString))
		}
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
