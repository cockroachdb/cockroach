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
	"io"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
)

// EntryDecoder is used to decode log entries.
type EntryDecoder interface {
	Decode(entry *logpb.Entry) error
}

// NewEntryDecoder creates a new instance of EntryDecoder.
// The format of the log file determines how the decoder is constructed.
func NewEntryDecoder(in io.Reader, editMode EditSensitiveData) (EntryDecoder, error) {
	return NewEntryDecoderWithFormat(in, editMode, "" /*format*/)
}

// NewEntryDecoderWithFormat is like NewEntryDecoder but the caller can specify the format of the log file.
// The header lines do not need to be searched for the log entry format when 'logFormat' is non-empty.
func NewEntryDecoderWithFormat(
	in io.Reader, editMode EditSensitiveData, logFormat string,
) (EntryDecoder, error) {
	var d EntryDecoder
	var format string

	// If the log format has not been specified, get the format from the first few header lines of the log file.
	if logFormat == "" {
		var buf bytes.Buffer
		rest := bufio.NewReader(in)
		r := io.TeeReader(rest, &buf)
		{
			const headerBytes = 8096
			header := make([]byte, headerBytes)
			n, err := r.Read(header)
			if err != nil {
				return nil, err
			}
			header = header[:n]
			logFormat, err = getLogFormat(header)
			if err != nil {
				return nil, errors.Wrap(err, "decoding format")
			}
		}
		in = io.MultiReader(&buf, rest)
	}
	f, ok := formatParsers[logFormat]
	if !ok {
		return nil, errors.Newf("unknown log file format: %s", logFormat)
	}
	format = f

	switch format {
	case "v2":
		d = &entryDecoderV2{
			reader:          bufio.NewReader(in),
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
		// The unimplemented.WithIssue function is not used here because it results in circular dependency issues.
		return nil, errors.WithTelemetry(
			errors.UnimplementedError(
				errors.IssueLink{IssueURL: build.MakeIssueURL(66684)},
				"unable to decode this log file format",
			),
			"#66684",
		)
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
