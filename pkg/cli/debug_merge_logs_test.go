// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/spf13/pflag"
)

type testCase struct {
	name      string
	dataPaths []string
	flags     []string
	format    string
}

func getCases(format string) []testCase {
	cases := []testCase{
		{
			name:      "1.all",
			dataPaths: []string{"/1/*/*"},
			flags:     []string{"--redact=false", "--redactable-output=false"},
		},
		{
			name:      "1.filter-program",
			dataPaths: []string{"/1/*/*"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--program-filter", "not-cockroach"},
		},
		{
			name:      "1.seek-past-end-of-file",
			dataPaths: []string{"/1/*/*"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--from", "181130 22:15:07.525317"},
		},
		{
			name:      "1.filter-message",
			dataPaths: []string{"/1/*/*"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--filter", "gossip"},
		},
		{
			name:      "2.multiple-files-from-node",
			dataPaths: []string{"/2/*/*"},
			flags:     []string{"--redact=false", "--redactable-output=false"},
		},
		{
			// NB: the output here matches 2.multiple-files-from-node.
			name:      "2.walk-directory",
			dataPaths: []string{"/2"},
			flags:     []string{"--redact=false", "--redactable-output=false"},
		},
		{
			name:      "2.skip-file",
			dataPaths: []string{"/2/*/*"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--from", "181130 22:15:07.525316"},
		},
		{
			name: "2.remove-duplicates",
			dataPaths: []string{
				"/2/1.logs/cockroach.test-0001.ubuntu.2018-11-30T22_06_47Z.004130.log",
				"/2/1.logs/cockroach.test-0001.ubuntu.2018-11-30T22_14_47Z.004130.log",
				"/2/2.logs/cockroach.stderr",
				"/2/2.logs/cockroach.test-0002.ubuntu.2018-11-30T22_06_47Z.003959.log",
				"/2/2.logs/cockroach.test-0002.ubuntu.2018-11-30T22_06_47Z.003959.log",
				"/2/2.logs/roachprod.log",
			},
			flags: []string{"--redact=false", "--redactable-output=false", "--from", "181130 22:15:07.525316"},
		},
		{
			name:      "3.non-standard",
			dataPaths: []string{"/3/*/*"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--prefix", ""},
		},
		{
			// Prints only lines that match the filter (if no submatches).
			name:      "4.filter",
			dataPaths: []string{"/4/*"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--filter", "3:0"},
		},
		{
			// Prints only the submatch.
			name:      "4.filter-submatch",
			dataPaths: []string{"/4/*"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--filter", "(3:)0"},
		},
		{
			// Prints only the submatches.
			name:      "4.filter-submatch-double",
			dataPaths: []string{"/4/*"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--filter", "(3):(0)"},
		},
		{
			// Simple grep for a panic line only.
			name:      "4.filter-npe",
			dataPaths: []string{"/4/npe.log"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--filter", `(panic: .*)`},
		},
		{
			// Grep for a panic and a few lines more. This is often not so useful
			// because there's lots of recover() and re-panic happening; the real
			// source of the panic is harder to find.
			name:      "4.filter-npe-with-context",
			dataPaths: []string{"/4/npe.log"},
			flags: []string{
				"--redact=false",
				"--redactable-output=false",
				"--file-pattern",
				".*",
				"--filter",
				`(?m)(panic:.(?:.*\n){0,5})`,
			},
		},
		{
			// This regexp attempts to find the source of the panic, essentially by
			// matching on:
			//
			// panic(...
			//     stack
			// anything
			//     stack
			// not-a-panic
			//     stack
			//
			// This will skip past the recover-panic extra stacks at the top which
			// usually alternate with panic().
			name:      "4.filter-npe-origin-stack-only",
			dataPaths: []string{"/4/npe-repanic.log"}, // (?:panic\(.*)*
			flags: []string{
				"--redact=false",
				"--redactable-output=false",
				"--file-pattern",
				".*",
				"--filter",
				`(?m)^(panic\(.*\n.*\n.*\n.*\n[^p].*)`,
			},
		},
		{
			name:      "5.redact-off-redactable-off",
			dataPaths: []string{"/5/redactable.log"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--file-pattern", ".*"},
		},
		{
			name:      "5.redact-off-redactable-on",
			dataPaths: []string{"/5/redactable.log"},
			flags:     []string{"--redact=false", "--redactable-output=true", "--file-pattern", ".*"},
		},
		{
			name:      "5.redact-on-redactable-off",
			dataPaths: []string{"/5/redactable.log"},
			flags:     []string{"--redact=true", "--redactable-output=false", "--file-pattern", ".*"},
		},
		{
			name:      "5.redact-on-redactable-on",
			dataPaths: []string{"/5/redactable.log"},
			flags:     []string{"--redact=true", "--redactable-output=true", "--file-pattern", ".*"},
		},
		{
			name:      "6.no-tenant-filter",
			dataPaths: []string{"/6/*/*"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--file-pattern", ".*"},
		},
		{
			name:      "6.app-tenants-filter",
			dataPaths: []string{"/6/*/*"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--tenant-ids=2,3"},
		},
		{
			name:      "6.system-tenant-filter",
			dataPaths: []string{"/6/*/*"},
			flags:     []string{"--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--tenant-ids=1"},
		},
	}
	for i := range cases {
		cases[i].format = format
	}
	return cases
}

func resetDebugMergeLogFlags(errorFn func(s string)) {
	debugMergeLogsCmd.Flags().Visit(func(f *pflag.Flag) {
		if err := f.Value.Set(f.DefValue); err != nil {
			errorFn(fmt.Sprintf("Failed to set flag to default: %v", err))
		}
	})
	// (Value).Set() for Slice flags has weird behavior where it appends to the existing
	// slice instead of truly clearing the value. Manually reset it instead.
	debugMergeLogsOpts.tenantIDsFilter = []string{}
}

func (c testCase) run(t *testing.T) {
	resetDebugMergeLogFlags(func(s string) { t.Fatal(s) })
	outBuf := bytes.Buffer{}
	debugMergeLogsCmd.SetOut(&outBuf)
	// Ensure that the original writer is restored when the test
	// completes. Otherwise, subsequent tests may not see their output.
	defer debugMergeLogsCmd.SetOut(nil)

	// Specify the log format in the flags.
	c.flags = append(c.flags, "--format="+c.format)

	// Set the flags.
	if err := debugMergeLogsCmd.ParseFlags(c.flags); err != nil {
		t.Fatalf("Failed to set flags: %v", err)
	}

	// Note: there is a different base test data path for each log format.
	base := "testdata/merge_logs_" + c.format
	var args []string
	for _, dataPath := range c.dataPaths {
		args = append(args, base+dataPath)
	}

	// Run the command.
	if err := debugMergeLogsCmd.RunE(debugMergeLogsCmd, args); err != nil {
		t.Fatalf("Failed to run command: %v", err)
	}

	// Note: the expected output lives in filepath.Join(testCase.base, "results", testCase.name)
	resultFile := filepath.Join(base, "results", c.name)

	expected, err := os.ReadFile(resultFile)
	if err != nil {
		t.Errorf("Failed to read expected result from %v: %v", resultFile, err)
	}
	if !bytes.Equal(expected, outBuf.Bytes()) {
		t.Fatalf("Result does not match expected. Got %d bytes, expected %d. got:\n%v\nexpected:\n%v",
			len(outBuf.String()), len(expected), outBuf.String(), string(expected))
	}
}

func TestDebugMergeLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Run the test for each possible log format.
	format := []string{"crdb-v2", "crdb-v1"}
	testutils.RunValues(t, "format", format, func(t *testing.T, format string) {
		cases := getCases(format)
		for _, c := range cases {
			t.Run(c.name, c.run)
		}
	})
}

func Example_format_error() {
	c := NewCLITest(TestCLIParams{NoServer: true})
	defer c.Cleanup()

	resetDebugMergeLogFlags(func(s string) { fmt.Fprintf(stderr, "ERROR: %v", s) })

	c.RunWithArgs([]string{"debug", "merge-logs", "testdata/merge_logs_crdb-v1/missing_format/*"})

	// Output:
	// debug merge-logs testdata/merge_logs_crdb-v1/missing_format/*
	// ERROR: decoding format: failed to extract log file format from the log
}
