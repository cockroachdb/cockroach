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

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

const severityChar = "IWEF"

// MessageTimeFormat is the format of the timestamp in log message headers of crdb formatted logs.
// as used in time.Parse and time.Format.
const MessageTimeFormat = "060102 15:04:05.999999"

// FormatLegacyEntry writes the contents of the legacy log entry struct to the specified writer.
func FormatLegacyEntry(e logpb.Entry, w io.Writer) error {
	buf := formatLogEntryInternalV1(e, false /* isHeader */, true /* showCounter */, nil)
	defer putBuffer(buf)
	_, err := w.Write(buf.Bytes())
	return err
}

type logFormatter interface {
	formatterName() string
	// doc is used to generate the formatter documentation.
	doc() string
	// formatEntry formats a logEntry into a newly allocated *buffer.
	// The caller is responsible for calling putBuffer() afterwards.
	formatEntry(entry logEntry) *buffer
}

var formatters = func() map[string]logFormatter {
	m := make(map[string]logFormatter)
	r := func(f logFormatter) {
		m[f.formatterName()] = f
	}
	r(formatCrdbV1{})
	r(formatCrdbV1WithCounter{})
	r(formatCrdbV1TTY{})
	r(formatCrdbV1TTYWithCounter{})
	r(formatCrdbV2{})
	r(formatCrdbV2TTY{})
	r(formatFluentJSONCompact{})
	r(formatFluentJSONFull{})
	r(formatJSONCompact{})
	r(formatJSONFull{})
	return m
}()

// GetFormatterDocs returns the embedded documentation for all the
// supported formats.
func GetFormatterDocs() map[string]string {
	m := make(map[string]string)
	for fmtName, f := range formatters {
		m[fmtName] = f.doc()
	}
	return m
}

// EntryDecoder reads successive encoded log entries from the data.
type EntryDecoder struct {
	re                 *regexp.Regexp
	format             string
	data               []byte
	scanner            *bufio.Scanner
	sensitiveEditor    redactEditor
	truncatedLastEntry bool
}

// NewEntryDecoder creates a new instance of EntryDecoder.
// The format of the log file determines how the decoder is constructed.
func NewEntryDecoder(
	data []byte, in io.Reader, editMode EditSensitiveData, logFormat string,
) *EntryDecoder {
	var d *EntryDecoder
	var format string

	formats := map[string]string{
		"crdb-v1":             "v1",
		"crdb-v1-count":       "v1",
		"crdb-v1-tty":         "v1",
		"crdb-v1-tty-count":   "v1",
		"crdb-v2":             "v2",
		"crdb-v2-tty":         "v2",
		"json":                "json",
		"json-compact":        "json-compact",
		"json-fluent":         "json-fluent",
		"json-fluent-compact": "json-fluent-compact",
		"v1":                  "v1",
		"v2":                  "v2",
	}

	// If the log format has been specified, use the corresponding parser
	if logFormat != "" {
		f, ok := formats[logFormat]
		if !ok {
			panic("unknown log file format")
		}
		format = f
	} else { // otherwise, get the log format from the top of the log
		lf := getLogFormat(data)
		if lf == "" {
			panic("failed to extract log file format from the log")
		}
		format = formats[lf]
	}

	switch format {
	case "v2":
		d = &EntryDecoder{
			re:     entryREV2,
			format: format,
			data:   data,
		}
	case "v1":
		d = &EntryDecoder{
			re:              entryREV1,
			format:          format,
			scanner:         bufio.NewScanner(in),
			sensitiveEditor: getEditor(editMode),
		}
		d.scanner.Split(d.split)
	default:
		panic("decoder for this log file format does not exist yet")
	}
	return d
}

// Decode decodes the next log entry into the provided protobuf message.
// It decodes according to the format of the log file.
func (d *EntryDecoder) Decode(entry *logpb.Entry) error {
	switch d.format {
	case "v2":
		return d.DecodeV2(entry)
	case "v1":
		return d.DecodeV1(entry)
	}
	return nil
}

// getLogFormat retrieves the log format recorded at the top of a log
func getLogFormat(data []byte) string {
	var re = regexp.MustCompile(
		`(?m)^` +
			/* Prefix */ `(?:.* log format \(utf8=.+\): )` +
			/* Format */ `(.*)$`,
	)

	m := re.FindSubmatch(data)
	if m == nil {
		return ""
	}

	return string(m[1])
}

// split function for the crdb-v1 entry decoder scanner.
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
