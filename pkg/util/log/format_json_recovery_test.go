// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestJSONDecoderCannotRecoverFromCorruption demonstrates that the JSON log
// entry decoder cannot recover after encountering a corrupted line. Once
// json.Decoder hits a syntax error, its stream position becomes indeterminate
// and all subsequent Decode() calls fail — either by returning errors or by
// hanging indefinitely as the decoder tries to parse the corrupted stream.
//
// This is problematic because the LogFile RPC handler (status.go) continues
// decoding on ErrMalformedLogEntry, accumulating every subsequent failed entry
// and its error string in memory — leading to unbounded memory growth.
//
// Compare with the crdb-v2 decoder which reads line-by-line via bufio.Reader
// and CAN recover from malformed lines.
func TestJSONDecoderCannotRecoverFromCorruption(t *testing.T) {
	// Build a JSON log stream: 5 valid entries, 1 corrupted line, 5 more valid entries.
	validEntry := func(i int) string {
		return fmt.Sprintf(
			`{"channel_numeric":%d,"timestamp":"1136214245.654321000","severity_numeric":1,"goroutine":1,"file":"test.go","line":1,"redactable":1,"message":"entry %d"}`,
			i, i,
		)
	}

	var lines []string
	for i := 0; i < 5; i++ {
		lines = append(lines, validEntry(i))
	}
	// Insert a corrupted line — truncated JSON that ends cleanly (not mid-string).
	// A truncation inside a JSON string literal causes json.Decoder to consume
	// subsequent lines as part of the string, which can hang the decoder entirely.
	// This corruption simulates a partial JSON object that is syntactically broken
	// but does not leave the scanner inside a string literal.
	lines = append(lines, `{"channel_numeric":99, CORRUPTED`)
	for i := 5; i < 10; i++ {
		lines = append(lines, validEntry(i))
	}
	input := strings.Join(lines, "\n") + "\n"

	decoder, err := NewEntryDecoderWithFormat(strings.NewReader(input), WithMarkedSensitiveData, "json")
	require.NoError(t, err)

	type result struct {
		decoded   int
		malformed int
	}

	// Run the decode loop with a timeout. The decoder enters an infinite loop
	// after corruption because json.Decoder's internal scanp is never advanced
	// past the error position — every Decode() call fails at the same offset,
	// producing an infinite stream of ErrMalformedLogEntry errors. In the
	// LogFile handler, this means unbounded growth of resp.ParseErrors and
	// resp.Entries until OOM.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resCh := make(chan result, 1)
	go func() {
		var res result
		// Mimic the LogFile handler's decode loop: continue on ErrMalformedLogEntry.
		for {
			var entry logpb.Entry
			if err := decoder.Decode(&entry); err != nil {
				if err == io.EOF {
					break
				}
				if errors.Is(err, ErrMalformedLogEntry) {
					res.malformed++
					// Stop after accumulating enough to prove the point,
					// otherwise this loop runs forever.
					if res.malformed > 1000 {
						break
					}
					continue
				}
				// Unexpected error type — treat as fatal.
				break
			}
			res.decoded++
		}
		resCh <- res
	}()

	var res result
	select {
	case res = <-resCh:
		// Decode loop finished (hit our safety limit).
	case <-ctx.Done():
		t.Fatal("decode loop timed out unexpectedly (should have hit malformed limit)")
	}

	t.Logf("decoded: %d, malformed: %d", res.decoded, res.malformed)

	// We expect 5 entries decoded successfully (before the corruption).
	require.Equal(t, 5, res.decoded, "should decode the 5 entries before the corrupted line")

	// The key assertion: the decoder produces FAR more than 1 malformed entry
	// from a single corrupted line. json.Decoder's scanp never advances past the
	// error, so every subsequent Decode() call fails at the same position.
	// In production, this loop runs until OOM.
	require.Greater(t, res.malformed, 1,
		"expected many malformed entries because json.Decoder enters an infinite "+
			"error loop — scanp is never advanced past the corrupted position")

	// This proves the bug: a single corrupted line generates an unbounded number
	// of malformed entries, each with an error string appended to ParseErrors.
	// With 25MiB log files, this quickly exhausts memory.
	t.Logf("BUG CONFIRMED: single corrupted line generated %d malformed entries "+
		"(in production this loop runs until OOM)", res.malformed)
}

// TestJSONDecoderHangsOnTruncatedString demonstrates an even worse failure
// mode: when corruption occurs inside a JSON string literal (e.g., a truncated
// timestamp value), json.Decoder consumes all subsequent data as part of the
// string, effectively hanging until EOF. In production with large log files,
// this means the decoder processes the entire remaining file content as a
// single malformed entry, wasting CPU and accumulating memory.
func TestJSONDecoderHangsOnTruncatedString(t *testing.T) {
	validEntry := func(i int) string {
		return fmt.Sprintf(
			`{"channel_numeric":%d,"timestamp":"1136214245.654321000","severity_numeric":1,"goroutine":1,"file":"test.go","line":1,"redactable":1,"message":"entry %d"}`,
			i, i,
		)
	}

	var lines []string
	for i := 0; i < 5; i++ {
		lines = append(lines, validEntry(i))
	}
	// Truncation inside a string literal — the worst case.
	// json.Decoder will try to read until it finds the closing quote,
	// consuming all subsequent valid entries as part of the string.
	lines = append(lines, `{"channel_numeric":99,"timestamp":"113621`)
	for i := 5; i < 10; i++ {
		lines = append(lines, validEntry(i))
	}
	input := strings.Join(lines, "\n") + "\n"

	decoder, err := NewEntryDecoderWithFormat(strings.NewReader(input), WithMarkedSensitiveData, "json")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type result struct {
		decoded   int
		malformed int
	}
	resCh := make(chan result, 1)
	go func() {
		var res result
		for {
			var entry logpb.Entry
			if err := decoder.Decode(&entry); err != nil {
				if err == io.EOF {
					break
				}
				if errors.Is(err, ErrMalformedLogEntry) {
					res.malformed++
					continue
				}
				break
			}
			res.decoded++
		}
		resCh <- res
	}()

	select {
	case res := <-resCh:
		// If the decoder finishes, all entries after corruption should be lost.
		t.Logf("decoded: %d, malformed: %d", res.decoded, res.malformed)
		require.Equal(t, 5, res.decoded, "only entries before corruption should decode")
		// The remaining entries are consumed as part of the truncated string —
		// they may appear as 0 or 1 malformed entries (the entire rest of stream
		// being treated as one big broken token).
		t.Logf("entries after corruption were consumed by the truncated string scan")
	case <-ctx.Done():
		// This is the expected outcome for large inputs — the decoder gets stuck
		// scanning the broken string. With a finite input it may eventually hit
		// EOF, but with large files this manifests as a hang.
		t.Logf("CONFIRMED: json.Decoder hung for >5s on truncated string literal " +
			"(in production with large files this causes unbounded CPU/memory usage)")
	}

	// Both outcomes (hang or data loss) demonstrate the bug:
	// the JSON decoder cannot safely continue after corruption.
}
