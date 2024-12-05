// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
	ctx = serverident.ContextWithServerIdentification(ctx, sysIDPayload)

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
			// Reassign `RedactionPolicyManaged` var manually because it's not possible to use
			// testutils.TestingHook due to circular dependency.
			initRedactionPolicyManaged := RedactionPolicyManaged
			RedactionPolicyManaged = tc.redactionPolicyManagedEnabled
			defer func() {
				RedactionPolicyManaged = initRedactionPolicyManaged
			}()

			TestingResetActive()
			cfg := logconfig.DefaultConfig()
			if err := cfg.Validate(&s.logDir); err != nil {
				t.Fatal(err)
			}
			cleanupFn, err := ApplyConfig(cfg, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */)
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

	t.Run("with defaut safe redaction", func(t *testing.T) {
		defer capture()()
		defer setShouldSanitizeArgs(true)()

		// when "default safe" redaction is enabled, we need to explicitly
		// mark the unsafe arg as unsafe.
		Infof(context.Background(), "safe %s", encoding.Unsafe("unsafe"))
		if !contains("safe "+startRedactable+"unsafe"+endRedactable, t) {
			t.Errorf("expected marked data, got %q", contents())
		}
	})
}

func setShouldSanitizeArgs(val bool) func() {
	prev := envDefaultSafeRedactionEnabled
	envDefaultSafeRedactionEnabled = val
	return func() { envDefaultSafeRedactionEnabled = prev }
}

// does not implement SafeFormatter
type sampleSafe struct {
	a string
}

func (s sampleSafe) String() string {
	return s.a
}

// implements SafeFormatter
type sampleWithUnsafe struct {
	a, b string
}

func (s sampleWithUnsafe) String() string {
	return fmt.Sprintf("a: %s, b: %s", s.a, s.b)
}

func (s sampleWithUnsafe) SafeFormat(w redact.SafePrinter, _ rune) {
	// explicitly marks s.a as safe
	w.Printf("a: %s, b: %s", redact.Safe(s.a), s.b)
}

type errWithHint struct {
	err error
}

func (e errWithHint) Error() string {
	return e.err.Error()
}

func (e errWithHint) ErrorHint() string {
	return "hint"
}

func TestDefaultSafeRedaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ScopeWithoutShowLogs(t).Close(t)
	defer setShouldSanitizeArgs(true)()

	tt := []struct {
		name     string
		args     any
		expected string
	}{
		{
			name:     "does not implement SafeFormatter",
			args:     sampleSafe{a: "safe"},
			expected: "safe",
		},
		{
			name:     "implements SafeFormatter",
			args:     sampleWithUnsafe{a: "safe", b: "unsafe"},
			expected: "a: safe, b: ‹unsafe›",
		},
		{
			name:     "uses encoding.Unsafe with struct",
			args:     encoding.Unsafe(sampleSafe{a: "unsafe"}),
			expected: "‹unsafe›",
		},
		{
			name:     "uses primitive type", // should always be safe
			args:     "safe",
			expected: "safe",
		},
		{
			name:     "uses encoding.Unsafe with primitive type",
			args:     encoding.Unsafe(100),
			expected: "‹100›",
		},
		{
			name:     "has error hints",
			args:     errWithHint{err: errors.New("error")},
			expected: "‹error›\n+HINT: ‹hint›",
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			defer capture()()
			Infof(context.Background(), "%v", tc.args)

			messages := []string{} // there might be more than one log line. Collect all the messages
			for _, line := range strings.Split(strings.TrimSpace(contents()), "\n") {
				messages = append(messages, strings.TrimSpace(parseLogEntry(t, line).Message))
			}

			require.Equal(t, tc.expected, strings.Join(messages, "\n"))
		})
	}
}

func parseLogEntry(t *testing.T, line string) logpb.Entry {
	t.Helper()

	reader := strings.NewReader(line)
	decoder, err := NewEntryDecoderWithFormat(reader, WithMarkedSensitiveData, "crdb-v1")
	require.NoError(t, err)

	var entry logpb.Entry
	require.NoError(t, decoder.Decode(&entry))
	return entry
}

func BenchmarkDefaultSafeRedaction(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer ScopeWithoutShowLogs(b).Close(b)
	defer capture()()

	printLogLines := func() {
		Info(context.Background(), "safe")
		Infof(context.Background(), "%s", sampleWithUnsafe{a: "safe", b: "unsafe"})
		Infof(context.Background(), "%s", sampleSafe{a: "safe"})
		Infof(
			context.Background(), "%s %s",
			sampleWithUnsafe{a: "safe", b: "unsafe"}, sampleSafe{a: "safe"},
		)
	}

	b.Run("with default unsafe (current behaviour)", func(b *testing.B) {
		defer setShouldSanitizeArgs(false)()
		for i := 0; i < b.N; i++ {
			printLogLines()
		}
	})

	b.Run("with default safe", func(b *testing.B) {
		defer setShouldSanitizeArgs(true)()
		for i := 0; i < b.N; i++ {
			printLogLines()
		}
	})
}
