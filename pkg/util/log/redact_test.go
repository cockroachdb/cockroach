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

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
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
	defer capture()()

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
	resetCaptured()

	Errorf(context.Background(), "test2 %v end", startRedactable+"hello"+endRedactable)
	if !contains("test2 ?hello? end", t) {
		t.Errorf("expected escaped markers, got %q", contents())
	}

	resetCaptured()
	_ = TestingSetRedactable(true)
	Errorf(context.Background(), "test3 %v end", "hello")
	if !contains(redactableIndicator+" test3", t) {
		t.Errorf("expected marker indicator, got %q", contents())
	}
	if !contains("test3 "+startRedactable+"hello"+endRedactable+" end", t) {
		t.Errorf("expected marked data, got %q", contents())
	}

	// Verify that safe parts of errors don't get enclosed in redaction markers
	resetCaptured()
	Errorf(context.Background(), "test3e %v end",
		errors.AssertionFailedf("hello %v",
			errors.Newf("error-in-error %s", "world")))
	if !contains(redactableIndicator+" test3e", t) {
		t.Errorf("expected marker indicator, got %q", contents())
	}
	if !contains("test3e hello error-in-error "+startRedactable+"world"+endRedactable+" end", t) {
		t.Errorf("expected marked data, got %q", contents())
	}

	// When redactable logs are enabled, the markers are always quoted.
	resetCaptured()

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
		renderTagsAsRedactable(tc.ctx, &buf)
		assert.Equal(t, tc.expected, buf.String())
	}
}

func TestRedactedDecodeFile(t *testing.T) {
	testData := []struct {
		redactMode    EditSensitiveData
		expRedactable bool
		expMessage    string
	}{
		{WithMarkedSensitiveData, true, "marker: this is safe, stray marks ??, ‹this is not safe›"},
		{WithFlattenedSensitiveData, false, "marker: this is safe, stray marks ??, this is not safe"},
		{WithoutSensitiveData, true, "marker: this is safe, stray marks ??, ‹×›"},
		{WithoutSensitiveDataNorMarkers, false, "marker: this is safe, stray marks ??, ×"},
	}

	for _, tc := range testData {
		// Use a closure to force scope boundaries.
		t.Run(fmt.Sprintf("%v", tc.redactMode), func(t *testing.T) {
			// Initialize the logging system for this test.
			// The log file go to a different directory in each sub-test.
			s := ScopeWithoutShowLogs(t)
			defer s.Close(t)
			setFlags()
			defer TestingSetRedactable(true)()

			// Force file re-initialization.
			s.Rotate(t)

			// Emit the message of interest for this test.
			Infof(context.Background(), "marker: this is safe, stray marks ‹›, %s", "this is not safe")

			// Retrieve the log writer and log location for this test.
			info, ok := debugLog.getFileSink().mu.file.(*syncBuffer)
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
			var entry logpb.Entry
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

// TestRedactableFlag checks that --redactable-logs does its job.
// TODO(knz): Remove this with
// https://github.com/cockroachdb/cockroach/pull/51987.
func TestRedactableFlag(t *testing.T) {
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	setFlags()

	TestingResetActive()

	// Request redaction markers in generated files.
	defer func(p bool) { logging.redactableLogsRequested = p }(logging.redactableLogsRequested)
	logging.redactableLogsRequested = true
	// Propagate the flag.
	cleanup, err := SetupRedactionAndStderrRedirects()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	// Now we check that they are present.
	defer capture()()
	Infof(context.Background(), "safe %s", "unsafe")

	if !contains("safe "+startRedactable+"unsafe"+endRedactable, t) {
		t.Errorf("expected marked data, got %q", contents())
	}
}
