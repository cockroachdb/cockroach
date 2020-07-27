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
	"fmt"
	"net"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

type fluentLogSink struct {
	// The severity level beyond which events are sent to this sink.
	fluentThreshold Severity

	// The entry formatter.
	formatter logFormatter

	// The network address of the fluentd collector.
	network string
	addr    string

	// good indicates that the connection can be used.
	good bool
	conn net.Conn
}

var fluent = newFluentLogSink("tcp", "localhost:5170")

func newFluentLogSink(network, addr string) *fluentLogSink {
	f := &fluentLogSink{
		formatter: formatFluentJSON{},
		addr:      addr,
		network:   network,
	}
	return f
}

func (f *fluentLogSink) Close() {
	f.good = false
	if f.conn != nil {
		if err := f.conn.Close(); err != nil {
			fmt.Fprintf(OrigStderr, "error closing network logger: %v\n", err)
		}
		f.conn = nil
	}
}

func (f *fluentLogSink) ensureConn(b []byte) {
	if f.good {
		return
	}
	f.Close()
	var err error
	f.conn, err = net.Dial(f.network, f.addr)
	if err != nil {
		fmt.Fprintf(OrigStderr, "error dialing network logger: %v\n%s", err, b)
		return
	} else {
		fmt.Fprintf(OrigStderr, "connection to network logger resumed\n")
	}
	f.good = true
}

func (f *fluentLogSink) Write(buf *buffer) {
	b := buf.Bytes()
	f.tryWrite(b)
	if !f.good {
		f.ensureConn(b)
		f.tryWrite(b)
	}
}

func (f *fluentLogSink) tryWrite(b []byte) {
	if !f.good {
		return
	}
	n, err := f.conn.Write(b)
	if err != nil || n < len(b) {
		fmt.Fprintf(OrigStderr, "logging error: %v or short write (%d/%d)\n%s",
			err, n, len(b), b)
		f.good = false
	}
}

var channelNamesLowercase = func() map[logpb.Channel]string {
	r := make(map[logpb.Channel]string)
	for v, n := range logpb.Channel_name {
		r[logpb.Channel(v)] = strings.ToLower(n)
	}
	return r
}()

type formatFluentJSON struct{}

func (f formatFluentJSON) formatEntry(entry logpb.Entry, stacks []byte) *buffer {
	buf := getBuffer()
	// Tag: this is the main category for Fluentd events.
	buf.WriteString(`{"tag":"`)
	// FIXME(knz): fluent prefers if there is no period in the tag other
	// than the one splitting the application and category. So we need
	// to escape characters here (the same way as done in file.go).
	buf.WriteString(program)
	buf.WriteByte('.')
	buf.WriteString(channelNamesLowercase[entry.Channel])
	// Also include the channel number in numeric form to facilitate
	// automatic processing.
	buf.WriteString(`","c":`)
	n := buf.someDigits(0, int(entry.Channel))
	buf.Write(buf.tmp[:n])

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
	buf.WriteString(`,"t":"`)
	n = buf.someDigits(0, int(entry.Time/1000000000))
	buf.tmp[n] = '.'
	n++
	n += buf.nDigits(9, n, int(entry.Time%1000000000), '0')
	buf.Write(buf.tmp[:n])

	// Severity, both in numeric form (for ease of processing) and
	// string form (to facilitate human comprehension).
	buf.WriteString(`","s":`)
	n = buf.someDigits(0, int(entry.Severity))
	buf.Write(buf.tmp[:n])
	if entry.Severity > 0 && int(entry.Severity) <= len(severityChar) {
		buf.WriteString(`,"sev":"`)
		buf.WriteByte(severityChar[int(entry.Severity)-1])
		buf.WriteByte('"')
	}

	// Goroutine number.
	buf.WriteString(`,"g":`)
	n = buf.someDigits(0, int(entry.Goroutine))
	buf.Write(buf.tmp[:n])

	// Source location.
	buf.WriteString(`,"f":"`)
	escapeString(buf, entry.File)
	buf.WriteString(`","l":`)
	n = buf.someDigits(0, int(entry.Line))
	buf.Write(buf.tmp[:n])

	// Entry counter.
	buf.WriteString(`,"n":`)
	n = buf.someDigits(0, int(entry.Counter))
	buf.Write(buf.tmp[:n])

	// Whether the tags/message are redactable.
	// We use 0/1 instead of true/false, because
	// it's likely there will be more redaction formats
	// in the future.
	buf.WriteString(`,"r":`)
	if entry.Redactable {
		buf.WriteByte('1')
	} else {
		buf.WriteByte('0')
	}

	// Tags.
	if entry.Tags != "" {
		buf.WriteString(`,"tags":"`)
		escapeString(buf, entry.Tags)
		buf.WriteByte('"')
	}

	// Message and stacks.
	buf.WriteString(`,"message":"`)
	escapeString(buf, entry.Message)
	if len(stacks) > 0 {
		buf.WriteString(`","stacks":"`)
		escapeString(buf, string(stacks))
	}
	buf.WriteString(`"}` + "\n")
	return buf
}

// escapeString is a variant of Go's json (*encodeState).stringBytes()
// which writes to buf directly.
func escapeString(buf *buffer, s string) {
	const hex = "0123456789abcdef"
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if b >= 0x20 && b < 0x80 && b != '"' && b != '\\' {
				i++
				continue
			}
			if start < i {
				buf.WriteString(s[start:i])
			}
			buf.WriteByte('\\')
			switch b {
			case '\\', '"':
				buf.WriteByte(b)
			case '\n':
				buf.WriteByte('n')
			case '\r':
				buf.WriteByte('r')
			case '\t':
				buf.WriteByte('t')
			default:
				// This encodes bytes < 0x20 except for \t, \n and \r.
				buf.WriteString(`u00`)
				buf.WriteByte(hex[b>>4])
				buf.WriteByte(hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				buf.WriteString(s[start:i])
			}
			buf.WriteString(`\ufffd`)
			i += size
			start = i
			continue
		}
		// U+2028 is LINE SEPARATOR.
		// U+2029 is PARAGRAPH SEPARATOR.
		// They are both technically valid characters in JSON strings,
		// but don't work in JSONP, which has to be evaluated as JavaScript,
		// and can lead to security holes there. It is valid JSON to
		// escape them, so we do so unconditionally.
		// See http://timelessrepo.com/json-isnt-a-javascript-subset for discussion.
		if c == '\u2028' || c == '\u2029' {
			if start < i {
				buf.WriteString(s[start:i])
			}
			buf.WriteString(`\u202`)
			buf.WriteByte(hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		buf.WriteString(s[start:])
	}
}
