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
	"errors"
	"io"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

type EntryDecoder interface {
	Decode(entry *logpb.Entry) error
}

// NewEntryDecoder creates a new instance of EntryDecoder.
// The format of the log file determines how the decoder is constructed.
func NewEntryDecoder(in io.Reader, editMode EditSensitiveData) (EntryDecoder, error) {
	return NewEntryDecoderf(in, editMode, "" /*format*/)
}
func NewEntryDecoderf(
	in io.Reader, editMode EditSensitiveData, logFormat string,
) (EntryDecoder, error) {
	var d EntryDecoder
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
		/*TODO*/
		return nil, errors.New("failed to get log format")
	}

	switch format {
	case "v2":
		d = &entryDecoderV2{
			reader:          bufio.NewReader(io.MultiReader(in, strings.NewReader("\n"))),
			sensitiveEditor: getEditor(editMode),
		}
	case "v1":
		decoder := &entryDecoderV1{
			scanner:         bufio.NewScanner(in),
			sensitiveEditor: getEditor(editMode),
		}
		decoder.scanner.Split(decoder.split)
		d = decoder
	default:
		return nil, unimplemented.NewWithIssue(66684, "unable to decode this log file format")
	}
	return d, nil
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
