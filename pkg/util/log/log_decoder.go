// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
)

var (
	formatRE = regexp.MustCompile(
		`(?m)^` +
			/* Prefix */ `(?:.*\[config\][ ]+log format \(utf8=.+\): )` +
			/* Format */ `(.*)$`,
	)
	v2IndicatorRE = regexp.MustCompile(
		`(?m)^` +
			/* crdb-v2 indicator */ `(?:.*line format: \[IWEF\]yymmdd hh:mm:ss.uuuuuu goid \[chan@\]file:line.*)$`,
	)
	v1IndicatorRE = regexp.MustCompile(
		`(?m)^` +
			/* crdb-v1 indicator */ `(?:.*line format: \[IWEF\]yymmdd hh:mm:ss.uuuuuu goid file:line.*)$`,
	)
	jsonIndicatorRE = regexp.MustCompile(
		`(?m)^` + `(?:.*\"config\".+log format \(utf8=.+\): )json\".+$`)
	jsonCompactIndicatorRE = regexp.MustCompile(
		`(?m)^` + `(?:.*\"config\".+log format \(utf8=.+\): )json-compact\".+$`)
	jsonFluentIndicatorRE = regexp.MustCompile(
		`(?m)^` + `(?:.*\"config\".+log format \(utf8=.+\): )json-fluent\".+$`)
	jsonFluentCompactIndicatorRE = regexp.MustCompile(
		`(?m)^` + `(?:.*\"config\".+log format \(utf8=.+\): )json-fluent-compact\".+$`)
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
	in io.Reader, editMode EditSensitiveData, format string,
) (EntryDecoder, error) {
	var d EntryDecoder

	// If the log format has not been specified, get the format from the first few header lines of the log file.
	if format == "" {
		var read io.Reader
		var err error
		read, format, err = ReadFormatFromLogFile(in)
		if err != nil {
			if err == io.EOF {
				return nil, errors.Wrap(err,
					"cannot read format from empty log file")
			}
			return nil, err
		}
		in = io.MultiReader(read, in)
	}
	f, ok := FormatParsers[format]
	if !ok {
		return nil, errors.Newf("unknown log file format: %s", format)
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
	case "json":
		d = &entryDecoderJSON{
			decoder:         json.NewDecoder(in),
			sensitiveEditor: getEditor(editMode),
		}
	case "json-compact":
		d = &entryDecoderJSON{
			decoder:         json.NewDecoder(in),
			sensitiveEditor: getEditor(editMode),
			compact:         true,
		}
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

// ReadFormatFromLogFile attempts to read the format from the header data of
// in. It returns the data consumed from input in the read return value.
func ReadFormatFromLogFile(in io.Reader) (read io.Reader, format string, err error) {
	var buf bytes.Buffer
	rest := bufio.NewReader(in)
	r := io.TeeReader(rest, &buf)
	const headerBytes = 8096
	header := make([]byte, headerBytes)
	n, err := r.Read(header)
	if err != nil {
		return nil, "", err
	}
	header = header[:n]
	format, err = getLogFormat(header)
	if err != nil {
		return nil, "", errors.Wrap(err, "decoding format")
	}
	return &buf, format, nil
}

// getLogFormat retrieves the log format recorded at the top of a log.
func getLogFormat(data []byte) (string, error) {
	if m := formatRE.FindSubmatch(data); m != nil {
		return string(m[1]), nil
	}

	// If the log format is not specified in the log, determine the format based on the line format entry.
	if v1IndicatorRE.Match(data) {
		return "crdb-v1", nil
	}

	if v2IndicatorRE.Match(data) {
		return "crdb-v2", nil
	}

	if jsonIndicatorRE.Match(data) {
		return "json", nil
	}
	if jsonCompactIndicatorRE.Match(data) {
		return "json-compact", nil
	}
	if jsonFluentIndicatorRE.Match(data) {
		return "json-fluent", nil
	}
	if jsonFluentCompactIndicatorRE.Match(data) {
		return "json-fluent-compact", nil
	}
	return "", errors.New("failed to extract log file format from the log")
}

var ErrMalformedLogEntry = errors.New("malformed log entry")
