// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/assert"
)

const startRedactable = "‹"
const endRedactable = "›"
const escapeMark = "?"

// TestRedactedLogOutput ensures that the logging output emits markers
// when redactable logs are enabled, and no mark indicator when they
// are not.
func TestRedactedLogOutput(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()
	defer mainLog.swap(mainLog.newBuffers())

	defer TestingSetRedactable(false)()

	Errorf(context.Background(), "test1 %v end", "hello")
	if contains(redactableIndicator, t) {
		t.Errorf("expected no marker indicator, got %q", contents())
	}
	if !contains("test1 hello end", t) {
		t.Errorf("expected no markers, got %q", contents())
	}
	// Also verify that raw markers are preserved, when redactable
	// markers are disabled.
	mainLog.newBuffers()
	Errorf(context.Background(), "test2 %v end", startRedactable+"hello"+endRedactable)
	if !contains("test2 "+startRedactable+"hello"+endRedactable+" end", t) {
		t.Errorf("expected unquoted markers, got %q", contents())
	}

	mainLog.newBuffers()
	mainLog.redactableLogs.Set(true)
	Errorf(context.Background(), "test3 %v end", "hello")
	if !contains(redactableIndicator+" test3", t) {
		t.Errorf("expected marker indicator, got %q", contents())
	}
	if !contains("test3 "+startRedactable+"hello"+endRedactable+" end", t) {
		t.Errorf("expected marked data, got %q", contents())
	}
	// When redactable logs are enabled, the markers are always quoted.
	mainLog.newBuffers()
	const specialString = "x" + startRedactable + "hello" + endRedactable + "y"
	Errorf(context.Background(), "test4 %v end", specialString)
	if contains(specialString, t) {
		t.Errorf("expected markers to be removed, got %q", contents())
	}
	if !contains("test4 "+startRedactable+"x"+escapeMark+"hello"+escapeMark+"y"+endRedactable+" end", t) {
		t.Errorf("expected escape mark, got %q", contents())
	}
}

func quote(s string) string {
	return startRedactable + s + endRedactable
}

// TestRedactTags ensure that context tags can be redacted.
func TestRedactTags(t *testing.T) {
	baseCtx := context.Background()

	testData := []struct {
		ctx      context.Context
		expected string
	}{
		{baseCtx, ""},
		{logtags.AddTag(baseCtx, "k", nil), "k"},
		{logtags.AddTag(baseCtx, "k", redact.Unsafe(123)), "k" + quote("123") + ""},
		{logtags.AddTag(baseCtx, "k", 123), "k123"},
		{logtags.AddTag(baseCtx, "k", redact.Safe(123)), "k123"},
		{logtags.AddTag(baseCtx, "k", startRedactable), "k" + quote(escapeMark) + ""},
		{logtags.AddTag(baseCtx, "kg", redact.Unsafe(123)), "kg=" + quote("123") + ""},
		{logtags.AddTag(baseCtx, "kg", 123), "kg=123"},
		{logtags.AddTag(baseCtx, "kg", redact.Safe(123)), "kg=123"},
		{logtags.AddTag(logtags.AddTag(baseCtx, "k", nil), "n", redact.Unsafe(55)), "k,n" + quote("55") + ""},
		{logtags.AddTag(logtags.AddTag(baseCtx, "k", nil), "n", 55), "k,n55"},
		{logtags.AddTag(logtags.AddTag(baseCtx, "k", nil), "n", redact.Safe(55)), "k,n55"},
	}

	for _, tc := range testData {
		var buf strings.Builder
		redactTags(tc.ctx, &buf)
		assert.Equal(t, tc.expected, buf.String())
	}
}

func TestRedactedDecodeFile(t *testing.T) {
	testData := []struct {
		redactableLogs bool
		redactMode     EditSensitiveData
		expRedactable  bool
		expMessage     string
	}{
		{false, WithMarkedSensitiveData, true, "‹marker: this is safe, stray marks ??, this is not safe›"},
		{false, WithFlattenedSensitiveData, false, "marker: this is safe, stray marks ‹›, this is not safe"},
		{false, WithoutSensitiveData, true, "‹×›"},
		{true, WithMarkedSensitiveData, true, "marker: this is safe, stray marks ‹›, ‹this is not safe›"},
		{true, WithFlattenedSensitiveData, false, "marker: this is safe, stray marks , this is not safe"},
		{true, WithoutSensitiveData, true, "marker: this is safe, stray marks ‹×›, ‹×›"},
	}

	for _, tc := range testData {
		// Use a closure to force scope boundaries.
		t.Run(fmt.Sprintf("%v/%v", tc.redactableLogs, tc.redactMode), func(t *testing.T) {
			// Initialize the logging system for this test.
			// The log file go to a different directory in each sub-test.
			s := ScopeWithoutShowLogs(t)
			defer s.Close(t)
			setFlags()
			defer TestingSetRedactable(tc.redactableLogs)()

			// Force file re-initialization.
			s.Rotate(t)

			// Emit the message of interest for this test.
			Infof(context.Background(), "marker: this is safe, stray marks ‹›, %s", "this is not safe")

			// Retrieve the log writer and log location for this test.
			info, ok := mainLog.mu.file.(*syncBuffer)
			if !ok {
				t.Fatalf("buffer wasn't created")
			}
			// Ensure our log message above made it to the file.
			if err := info.Flush(); err != nil {
				t.Fatal(err)
			}

			// Prepare reading the entries from the file.
			infoName := filepath.Base(info.file.Name())
			reader, err := GetLogReader(infoName, true /* restricted */)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			decoder := NewEntryDecoder(reader, tc.redactMode)

			// Now verify we have what we want in the file.
			foundMessage := false
			var entry Entry
			for {
				if err := decoder.Decode(&entry); err != nil {
					if err == io.EOF {
						break
					}
					t.Fatal(err)
				}
				if strings.HasSuffix(entry.File, "redact_test.go") {
					assert.Equal(t, tc.expRedactable, entry.Redactable)
					assert.Equal(t, tc.expMessage, entry.Message)
					foundMessage = true
				}
			}
			if !foundMessage {
				t.Error("expected marked message in log, found none")
			}
		})
	}
}
