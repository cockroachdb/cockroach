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
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestReadLogFormat(t *testing.T) {
	datadriven.RunTest(t, "testdata/read_header",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "log":
				_, format, err := ReadFormatFromLogFile(strings.NewReader(td.Input))
				if err != nil {
					td.Fatalf(t, "error while reading format from the log file: %v", err)
				}
				return format
			default:
				t.Fatalf("unknown directive: %q", td.Cmd)
			}
			// unreachable
			return ""
		})
}

func TestNewEntryDecoder_EmptyFile(t *testing.T) {
	in := strings.NewReader("")
	decoder, err := NewEntryDecoder(in, SelectEditMode(true /* redact */, true /*keepRedactable*/))
	require.Nil(t, decoder)
	require.ErrorContains(t, err, "cannot read format from empty log file")
}
