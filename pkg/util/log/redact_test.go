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

	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const startRedactable = "‹"
const endRedactable = "›"
const escapeMark = "?"

// TestRedactedLogOutput ensures that the logging output emits markers
// when redactable logs are enabled, and no mark indicator when they
// are not.
func TestRedactedLogOutput(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	defer capture()()

	defer TestingSetRedactable(false)()

	ctx := context.Background()
	sysIDPayload := testIDPayload{tenantID: "1"}
	ctx = context.WithValue(ctx, serverident.ServerIdentificationContextKey{}, sysIDPayload)

	Errorf(ctx, "test1 %v end", "hello")
	if contains(redactableIndicator, t) {
		t.Errorf("expected no marker indicator, got %q", contents())
	}
	if !contains("test1 hello end", t) {
		t.Errorf("expected no markers, got %q", contents())
	}
	// Also verify that raw markers are preserved, when redactable
	// markers are disabled.
	resetCaptured()

	Errorf(ctx, "test2 %v end", startRedactable+"hello"+endRedactable)
	if !contains("test2 ?hello? end", t) {
		t.Errorf("expected escaped markers, got %q", contents())
	}

	resetCaptured()
	_ = TestingSetRedactable(true)
	Errorf(ctx, "test3 %v end", "hello")
	if !contains(redactableIndicator+" [T1] 3  test3", t) {
		t.Errorf("expected marker indicator, got %q", contents())
	}
	if !contains("test3 "+startRedactable+"hello"+endRedactable+" end", t) {
		t.Errorf("expected marked data, got %q", contents())
	}

	// Verify that safe parts of errors don't get enclosed in redaction markers
	resetCaptured()
	Errorf(ctx, "test3e %v end",
		errors.AssertionFailedf("hello %v",
			errors.Newf("error-in-error %s", "world"))) // nolint:errwrap
	if !contains(redactableIndicator+" [T1] 4  test3e", t) {
		t.Errorf("expected marker indicator, got %q", contents())
	}
	if !contains("test3e hello error-in-error "+startRedactable+"world"+endRedactable+" end", t) {
		t.Errorf("expected marked data, got %q", contents())
	}

	// When redactable logs are enabled, the markers are always quoted.
	resetCaptured()

	const specialString = "x" + startRedactable + "hello" + endRedactable + "y"
	Errorf(ctx, "test4 %v end", specialString)
	if contains(specialString, t) {
		t.Errorf("expected markers to be removed, got %q", contents())
	}
	if !contains("test4 "+startRedactable+"x"+escapeMark+"hello"+escapeMark+"y"+endRedactable+" end", t) {
		t.Errorf("expected escape mark, got %q", contents())
	}
}

func TestSafeManaged(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := ScopeWithoutShowLogs(t)
	defer s.Close(t)
	tests := []struct {
		name                          string
		arg                           interface{}
		expected                      redact.RedactableString
		redactionPolicyManagedEnabled bool
	}{
		{
			name:                          "redacts when not in redaction policy managed mode",
			arg:                           "some value",
			expected:                      redact.Sprint("some value"),
			redactionPolicyManagedEnabled: false,
		},
		{
			name:                          "marks safe when in redaction policy managed mode",
			arg:                           "some value",
			expected:                      redact.Sprint(redact.Safe("some value")),
			redactionPolicyManagedEnabled: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				envutil.ClearEnvCache()
			})

			t.Setenv(redactionPolicyManagedEnvVar, fmt.Sprint(tc.redactionPolicyManagedEnabled))

			TestingResetActive()
			cfg := logconfig.DefaultConfig()
			if err := cfg.Validate(&s.logDir); err != nil {
				t.Fatal(err)
			}
			cleanupFn, err := ApplyConfig(cfg, FileSinkMetrics{})
			if err != nil {
				t.Fatal(err)
			}
			defer cleanupFn()

			require.Equal(t, logging.hasManagedRedactionPolicy(), tc.redactionPolicyManagedEnabled)
			require.Equal(t, tc.expected, redact.Sprint(SafeManaged(tc.arg)))
		})
	}
}

func TestRedactedDecodeFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

			// Force file re-initialization.
			s.Rotate(t)

			// Emit the message of interest for this test.
			Infof(context.Background(), "marker: this is safe, stray marks ‹›, %s", "this is not safe")

			// Retrieve the log writer and log location for this test.
			debugSink := debugLog.getFileSink()
			fileName := debugSink.getFileName(t)

			// Ensure our log message above made it to the file.
			debugSink.lockAndFlushAndMaybeSync(false)

			// Prepare reading the entries from the file.
			infoName := filepath.Base(fileName)
			reader, err := GetLogReader(infoName)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			decoder, err := NewEntryDecoder(reader, tc.redactMode)
			if err != nil {
				t.Fatal(err)
			}

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
					msg := strings.TrimPrefix(strings.TrimSpace(entry.Message), "1  ")
					assert.Equal(t, tc.expMessage, msg)
					foundMessage = true
				}
			}
			if !foundMessage {
				t.Error("expected marked message in log, found none")
			}
		})
	}
}

// TestDefaultRedactable checks that redaction markers are enabled by
// default.
func TestDefaultRedactable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)

	// Check redaction markers in the output.
	defer capture()()
	Infof(context.Background(), "safe %s", "unsafe")

	if !contains("safe "+startRedactable+"unsafe"+endRedactable, t) {
		t.Errorf("expected marked data, got %q", contents())
	}
}
