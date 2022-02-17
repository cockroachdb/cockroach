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
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

func TestDecodeWithRedactedMessage(t *testing.T) {
	defer Scope(t).Close(t)
	ctx := context.Background()
	start := timeutil.Now()

	// Emit an arbitrary structured log entry.
	StructuredEvent(ctx, &eventpb.CertsReload{
		CommonEventDetails: eventpb.CommonEventDetails{
			Timestamp: start.UnixNano(),
		},
		ErrorMessage: "error",
	})

	Flush()

	for _, format := range []string{"crdb-v1", "crdb-v2"} {
		// Fetch the entry.
		entries, err := FetchEntriesFromFilesWithFormat(start.UnixNano(), timeutil.Now().UnixNano(), 1,
			regexp.MustCompile("certs_reload"), WithFlattenedSensitiveData, format)
		require.NoError(t, err)
		require.Len(t, entries, 1)

		// Check that the entry field `StructuredEnd` correctly indicates
		// the end of the message, given that the message has been redacted.
		entry := entries[0]
		require.LessOrEqual(t, int(entry.StructuredEnd), len(entry.Message))
	}
}
