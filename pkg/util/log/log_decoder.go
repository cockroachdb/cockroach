// Copyright 2021 The Cockroach Authors.
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
	"errors"
	"math"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

// numHeaderChars represents the number of characters in the first few log lines relevant for searching for
// the log format.
const numHeaderChars = 1000

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
func NewEntryDecoder(data []byte, editMode EditSensitiveData) (*EntryDecoder, error) {
	return NewEntryDecoderf(data, editMode, ""/*format*/)
}
func NewEntryDecoderf(data []byte, editMode EditSensitiveData, logFormat string) (*EntryDecoder, error) {
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
		"json-compact":        "json",
		"json-fluent":         "json",
		"json-fluent-compact": "json",
		"v1":                  "v1",
		"v2":                  "v2",
	}

	// If the log format has been specified, use the corresponding parser.
	if logFormat != "" {
		f, ok := formats[logFormat]
		if !ok {
			return nil, errors.New("unknown log file format")
		}
		format = f
	} else { // otherwise, get the log format from the top of the log
		// extract the format from the first few log lines
		numChars := int(math.Min(float64(numHeaderChars), float64(len(data))))
		logFormat, err := getLogFormat(data[:numChars])
		if err != nil  {
			return nil, err
		}
		format, _ = formats[logFormat]
	}

	switch format {
	case "v2":
		d = &EntryDecoder{
			re:              entryREV2,
			format:          format,
			data:            data,
			sensitiveEditor: getEditor(editMode),
		}
	case "v1":
		d = &EntryDecoder{
			re:              entryREV1,
			format:          format,
			scanner:         bufio.NewScanner(bytes.NewReader(data)),
			sensitiveEditor: getEditor(editMode),
		}
		d.scanner.Split(d.split)
	default:
		return nil, unimplemented.NewWithIssue(66684, "unable to decode this log file format")
	}
	return d, nil
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
	// unreachable
	return nil
}

// getLogFormat retrieves the log format recorded at the top of a log.
func getLogFormat(data []byte) (string, error) {
	var re = regexp.MustCompile(
		`(?m)^` +
			/* Prefix */ `(?:.*\[config\]   log format \(utf8=.+\): )` +
			/* Format */ `(.*)$`,
	)

	m := re.FindSubmatch(data)
	if m == nil {
		return "", errors.New("failed to extract log file format from the log")
	}

	return string(m[1]), nil
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
