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
	"hash/adler32"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/ttycolor"
)

const severityChar = "IWEF"

// MessageTimeFormat is the format of the timestamp in log message headers of crdb formatted logs.
// as used in time.Parse and time.Format.
const MessageTimeFormat = "060102 15:04:05.999999"

// DefaultFormat is the main log format.
const DefaultFormat = "crdb-v2"

// FormatLegacyEntry writes the contents of the legacy log entry struct to the specified writer.
func FormatLegacyEntry(e logpb.Entry, w io.Writer, format string) error {
	return FormatLegacyEntryWithOptionalColors(e, w, nil /* cp */, format)
}

// FormatLegacyEntryWithOptionalColors is like FormatLegacyEntry but the caller can specify
// a color profile.
func FormatLegacyEntryWithOptionalColors(
	legacyEntry logpb.Entry, w io.Writer, cp ttycolor.Profile, format string,
) error {
	var buf *buffer
	switch format {
	case "crdb-v2":
		buf = formatLogEntryInternalV2(convertToCanon(legacyEntry), cp)
	case "crdb-v1":
		buf = formatLogEntryInternalV1(legacyEntry, false /* isHeader */, true /* showCounter */, cp)
	default:
		// The unimplemented.WithIssue function is not used here because it results in circular dependency issues.
		return errors.WithTelemetry(
			errors.UnimplementedError(
				errors.IssueLink{IssueURL: build.MakeIssueURL(66684)},
				"unable to encode this log file format",
			),
			"#66684",
		)
	}
	defer putBuffer(buf)
	_, err := w.Write(buf.Bytes())
	return err
}

// FormatLegacyEntryPrefix writes a color-decorated prefix to the specified
// writer. The color is rendered in the background of the prefix and is chosen
// from an arbitrary but deterministic mapping from the prefix bytes to the
// color profile entries.
func FormatLegacyEntryPrefix(prefix []byte, w io.Writer, cp ttycolor.Profile) (err error) {
	if prefix == nil {
		return nil
	}

	if cp != nil {
		code := ttycolor.PickArbitraryColor(adler32.Checksum(prefix))
		if _, err = w.Write(cp.BackgroundColorSequence(code)); err != nil {
			return err
		}
		defer func() {
			_, errReset := w.Write(cp[ttycolor.Reset])
			err = errors.CombineErrors(err, errReset)
		}()
	}

	_, err = w.Write(prefix)
	return err
}

// convertToCanon turns the legacy log entry struct into the canonical struct.
func convertToCanon(e logpb.Entry) (res logEntry) {
	res = logEntry{
		sev:     e.Severity,
		ch:      e.Channel,
		ts:      e.Time,
		file:    e.File,
		line:    int(e.Line),
		gid:     e.Goroutine,
		counter: e.Counter,
		payload: entryPayload{
			message:    e.Message,
			tags:       nil,
			redactable: e.Redactable,
		},
	}

	if len(e.Tags) > 0 {
		tags := strings.Split(e.Tags, ",")
		for _, t := range tags {
			kv := strings.Split(t, "=")

			res.payload.tags = append(res.payload.tags, kv[0]...)
			res.payload.tags = append(res.payload.tags, 0)

			if len(kv) > 1 {
				res.payload.tags = escapeNulBytes(res.payload.tags, kv[1])
			}
			res.payload.tags = append(res.payload.tags, 0)
		}
	}

	if e.StructuredEnd != 0 {
		res.structured = true
		res.payload.message = res.payload.message[e.StructuredStart:e.StructuredEnd]
	}

	if e.StackTraceStart != 0 {
		res.stacks = []byte(res.payload.message[e.StackTraceStart:])
		res.payload.message = res.payload.message[:e.StackTraceStart]
	}

	return res
}
