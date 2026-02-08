// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestJSONDecoderRecoversFromCorruption verifies that the JSON log decoder
// can recover after encountering a corrupted line. The decoder reads
// line-by-line using bufio.Scanner + json.Unmarshal, so a single bad line
// is skipped and subsequent valid entries are decoded normally.
//
// This is important because the LogFile RPC handler (status.go) continues
// decoding on ErrMalformedLogEntry. If the decoder could not recover
// (as was the case with the previous json.Decoder-based implementation),
// a single corrupted line would produce an infinite stream of errors,
// leading to unbounded memory growth and OOM.
func TestJSONDecoderRecoversFromCorruption(t *testing.T) {
	validEntry := func(i int) string {
		return fmt.Sprintf(
			`{"channel_numeric":%d,"timestamp":"1136214245.654321000","severity_numeric":1,"goroutine":1,"file":"test.go","line":1,"redactable":1,"message":"entry %d"}`,
			i, i,
		)
	}

	testCases := []struct {
		name          string
		corruptedLine string
	}{
		{
			name:          "corruption outside string literal",
			corruptedLine: `{"channel_numeric":99, CORRUPTED`,
		},
		{
			name:          "truncated string literal",
			corruptedLine: `{"channel_numeric":99,"timestamp":"113621`,
		},
		{
			name:          "plain text (non-JSON)",
			corruptedLine: `this is not json at all`,
		},
		{
			name:          "empty JSON object",
			corruptedLine: `{}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Build a JSON log stream: 5 valid entries, 1 corrupted line, 5 more valid entries.
			var lines []string
			for i := 0; i < 5; i++ {
				lines = append(lines, validEntry(i))
			}
			lines = append(lines, tc.corruptedLine)
			for i := 5; i < 10; i++ {
				lines = append(lines, validEntry(i))
			}
			input := strings.Join(lines, "\n") + "\n"

			decoder, err := NewEntryDecoderWithFormat(strings.NewReader(input), WithMarkedSensitiveData, "json")
			require.NoError(t, err)

			var (
				decoded   int
				malformed int
			)

			// Mimic the LogFile handler's decode loop: continue on ErrMalformedLogEntry.
			for {
				var entry logpb.Entry
				if err := decoder.Decode(&entry); err != nil {
					if err == io.EOF {
						break
					}
					if errors.Is(err, ErrMalformedLogEntry) {
						malformed++
						continue
					}
					t.Fatalf("unexpected error: %v", err)
				}
				decoded++
			}

			t.Logf("decoded: %d, malformed: %d", decoded, malformed)

			// The corrupted line should produce exactly 1 malformed entry.
			require.Equal(t, 1, malformed,
				"expected exactly 1 malformed entry for the corrupted line")

			// All 10 valid entries should be decoded successfully — the decoder
			// recovers after the corrupted line because it reads line-by-line.
			require.Equal(t, 10, decoded,
				"expected all 10 valid entries to be decoded (decoder should recover from corruption)")
		})
	}
}
