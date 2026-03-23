// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/redact"
	"github.com/pmezard/go-difflib/difflib"
)

func TestAppliedStandaloneConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const expected = `sinks:
  stderr:
    channels: {INFO: all}
    format: crdb-v2-tty
    redact: false
    redactable: false
    exit-on-error: true
`
	actual := DescribeAppliedConfig()
	if expected != actual {
		t.Errorf("expected:\n%s\ngot:\n%s\ndiff:\n%s",
			expected, actual, getDiff(expected, actual))
	}
}

func getDiff(expected, actual string) string {
	diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(expected),
		B:        difflib.SplitLines(actual),
		FromFile: "Expected",
		FromDate: "",
		ToFile:   "Actual",
		ToDate:   "",
		Context:  1,
	})
	return diff
}

func TestAppliedConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	datadriven.RunTest(t, "testdata/config",
		func(t *testing.T, d *datadriven.TestData) string {
			// Load the default config and apply the test's input.
			h := logconfig.Holder{Config: logconfig.DefaultConfig()}
			if err := h.Set(d.Input); err != nil {
				t.Fatal(err)
			}
			if err := h.Config.Validate(&sc.logDir); err != nil {
				t.Fatal(err)
			}

			cleanup, err := ApplyConfigForReconfig(h.Config, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()

			actual := DescribeAppliedConfig()
			// Make the test output deterministic.
			actual = strings.ReplaceAll(actual, sc.logDir, "TMPDIR")
			return actual
		})
}

func TestApplyConfigRaceCondition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test demonstrates the race condition that exists when using
	// TestingResetActive() followed by ApplyConfig(). If any logging
	// happens between these two calls, ApplyConfig() will panic.
	t.Run("demonstrate panic with old pattern", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic from ApplyConfig when logging is active, but got none")
			} else {
				t.Logf("got expected panic: %v", r)
			}
		}()

		sc := ScopeWithoutShowLogs(t)
		defer sc.Close(t)

		cfg := logconfig.DefaultConfig()
		if err := cfg.Validate(&sc.logDir); err != nil {
			t.Fatal(err)
		}
		TestingResetActive()
		setActive()

		// Applying the configuration after setting active should panic.
		_, _ = ApplyConfig(cfg, nil, nil)
	})

	// This test demonstrates that ApplyConfigForReconfig() can be called
	// even when logging is already active, without panicking.
	t.Run("demonstrate reconfiguring multiple times without panic", func(t *testing.T) {
		sc := ScopeWithoutShowLogs(t)
		defer sc.Close(t)

		cfg := logconfig.DefaultConfig()
		if err := cfg.Validate(&sc.logDir); err != nil {
			t.Fatal(err)
		}
		setActive()
		// Using ApplyConfigForReconfig should succeed, despite logging being active.
		cleanup, err := ApplyConfigForReconfig(cfg, nil, nil)
		if err != nil {
			t.Fatalf("ApplyConfigForReconfig failed: %v", err)
		}
		cleanup()
	})
}

func TestApplyConfigEnablesHashing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Start from a known global redact state for this process.
	redact.DisableHashing()
	t.Cleanup(redact.DisableHashing)

	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	hashAlice := func() string {
		return string(redact.Sprintf("user=%s", redact.HashString("alice")).Redact())
	}

	// Apply a config with hashing enabled and a salt.
	cfg := logconfig.DefaultConfig()
	cfg.Redaction.Hashing.Enabled = true
	cfg.Redaction.Hashing.Salt = "inline-salt"
	if err := cfg.Validate(&sc.logDir); err != nil {
		t.Fatal(err)
	}

	cleanup, err := ApplyConfigForReconfig(cfg, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */)
	if err != nil {
		t.Fatal(err)
	}

	const expectSalted = "user=‹3bdacf48›"
	if got := hashAlice(); got != expectSalted {
		t.Fatalf("with salt: expected %q, got %q", expectSalted, got)
	}

	// Tear down the first config before applying the second. This is
	// required because ApplyConfigForReconfig sets up fd2 capture, which
	// cannot be set up twice.
	cleanup()

	// Now reconfigure with hashing disabled and verify it reverts to
	// full redaction.
	cfg2 := logconfig.DefaultConfig()
	cfg2.Redaction.Hashing.Enabled = false
	if err := cfg2.Validate(&sc.logDir); err != nil {
		t.Fatal(err)
	}

	cleanup2, err := ApplyConfigForReconfig(cfg2, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup2()

	const expectDisabled = "user=‹×›"
	if got := hashAlice(); got != expectDisabled {
		t.Fatalf("after disable: expected %q, got %q", expectDisabled, got)
	}
}
