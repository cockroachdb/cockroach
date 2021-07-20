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

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/ttycolor"
)

const severityChar = "IWEF"

// MessageTimeFormat is the format of the timestamp in log message headers of crdb formatted logs.
// as used in time.Parse and time.Format.
const MessageTimeFormat = "060102 15:04:05.999999"

// FormatLegacyEntry writes the contents of the legacy log entry struct to the specified writer.
func FormatLegacyEntry(e logpb.Entry, w io.Writer) error {
	return formatLegacyEntry(e, w, nil /* cp */)
}

// FormatLegacyEntryTTY writes the legacy log entry to the specified writer,
// using colors if possible.
func FormatLegacyEntryTTY(e logpb.Entry, w io.Writer) error {
	cp := ttycolor.StderrProfile
	if logging.stderrSink.noColor.Get() {
		cp = nil
	}
	return formatLegacyEntry(e, w, cp)
}

func formatLegacyEntry(e logpb.Entry, w io.Writer, cp ttycolor.Profile) error {
	buf := formatLogEntryInternalV1(e, false /* isHeader */, true /* showCounter */, cp)
	defer putBuffer(buf)
	_, err := w.Write(buf.Bytes())
	return err
}

// FormatLegacyEntryPrefixTTY writes a color-decorated prefix to the specified
// writer. The color is rendered in the background of the prefix and is chosen
// from an arbitrary but deterministic mapping from the prefix bytes to the
// color profile entries.
func FormatLegacyEntryPrefixTTY(prefix []byte, w io.Writer) (err error) {
	if prefix == nil {
		return nil
	}
	cp := ttycolor.StderrProfile
	if logging.stderrSink.noColor.Get() {
		cp = nil
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
