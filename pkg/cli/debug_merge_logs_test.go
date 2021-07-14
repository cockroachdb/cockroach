// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/spf13/pflag"
)

const crdbV2testdataPath = "testdata/merge_logs_v2"
const crdbV1testdataPath = "testdata/merge_logs_v1"

type testCase struct {
	name   string
	format string
	flags  []string
	args   []string
}

func getCases(format string) []testCase {
	var testdataPath string
	switch format {
	case "crdb-v2":
		testdataPath = crdbV2testdataPath
	case "crdb-v1":
		testdataPath = crdbV1testdataPath
	}
	return []testCase{
		{
			name:   "1.all",
			format: format,
			args:   []string{testdataPath + "/1/*/*"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false"},
		},
		{
			name:   "1.filter-program",
			format: format,
			args:   []string{testdataPath + "/1/*/*"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--program-filter", "not-cockroach"},
		},
		{
			name:   "1.seek-past-end-of-file",
			format: format,
			args:   []string{testdataPath + "/1/*/*"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--from", "181130 22:15:07.525317"},
		},
		{
			name:   "1.filter-message",
			format: format,
			args:   []string{testdataPath + "/1/*/*"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--filter", "gossip"},
		},
		{
			name:   "2.multiple-files-from-node",
			format: format,
			args:   []string{testdataPath + "/2/*/*"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false"},
		},
		{
			// NB: the output here matches 2.multiple-files-from-node.
			name:   "2.walk-directory",
			format: format,
			args:   []string{testdataPath + "/2"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false"},
		},
		{
			name:   "2.skip-file",
			format: format,
			args:   []string{testdataPath + "/2/*/*"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--from", "181130 22:15:07.525316"},
		},
		{
			name:   "2.remove-duplicates",
			format: format,
			args: []string{
				testdataPath + "/2/1.logs/cockroach.test-0001.ubuntu.2018-11-30T22_06_47Z.004130.log",
				testdataPath + "/2/1.logs/cockroach.test-0001.ubuntu.2018-11-30T22_14_47Z.004130.log",
				testdataPath + "/2/2.logs/cockroach.stderr",
				testdataPath + "/2/2.logs/cockroach.test-0002.ubuntu.2018-11-30T22_06_47Z.003959.log",
				testdataPath + "/2/2.logs/cockroach.test-0002.ubuntu.2018-11-30T22_06_47Z.003959.log",
				testdataPath + "/2/2.logs/roachprod.log",
			},
			flags: []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--from", "181130 22:15:07.525316"},
		},
		{
			name:   "3.non-standard",
			format: format,
			args:   []string{testdataPath + "/3/*/*"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--prefix", ""},
		},
		{
			// Prints only lines that match the filter (if no submatches).
			name:   "4.filter",
			format: format,
			args:   []string{testdataPath + "/4/*"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--filter", "3:0"},
		},
		{
			// Prints only the submatch.
			name:   "4.filter-submatch",
			format: format,
			args:   []string{testdataPath + "/4/*"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--filter", "(3:)0"},
		},
		{
			// Prints only the submatches.
			name:   "4.filter-submatch-double",
			format: format,
			args:   []string{testdataPath + "/4/*"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--filter", "(3):(0)"},
		},
		{
			// Simple grep for a panic line only.
			name:   "4.filter-npe",
			format: format,
			args:   []string{testdataPath + "/4/npe.log"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--filter", `(panic: .*)`},
		},
		{
			// Grep for a panic and a few lines more. This is often not so useful
			// because there's lots of recover() and re-panic happening; the real
			// source of the panic is harder to find.
			name:   "4.filter-npe-with-context",
			format: format,
			args:   []string{testdataPath + "/4/npe.log"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--filter", `(?m)(panic:.(?:.*\n){0,5})`},
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
			name:   "4.filter-npe-origin-stack-only",
			format: format,
			args:   []string{testdataPath + "/4/npe-repanic.log"}, // (?:panic\(.*)*
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--file-pattern", ".*", "--filter", `(?m)^(panic\(.*\n.*\n.*\n.*\n[^p].*)`},
		},
		{
			name:   "5.redact-off-redactable-off",
			format: format,
			args:   []string{testdataPath + "/5/redactable.log"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=false", "--file-pattern", ".*"},
		},
		{
			name:   "5.redact-off-redactable-on",
			format: format,
			args:   []string{testdataPath + "/5/redactable.log"},
			flags:  []string{"--format=" + format, "--redact=false", "--redactable-output=true", "--file-pattern", ".*"},
		},
		{
			name:   "5.redact-on-redactable-off",
			format: format,
			args:   []string{testdataPath + "/5/redactable.log"},
			flags:  []string{"--format=" + format, "--redact=true", "--redactable-output=false", "--file-pattern", ".*"},
		},
		{
			name:   "5.redact-on-redactable-on",
			format: format,
			args:   []string{testdataPath + "/5/redactable.log"},
			flags:  []string{"--format=" + format, "--redact=true", "--redactable-output=true", "--file-pattern", ".*"},
		},
	}
}

func (c testCase) run(t *testing.T) {
	outBuf := bytes.Buffer{}
	debugMergeLogsCmd.Flags().Visit(func(f *pflag.Flag) {
		if err := f.Value.Set(f.DefValue); err != nil {
			t.Fatalf("Failed to set flag to default: %v", err)
		}
	})
	debugMergeLogsCmd.SetOut(&outBuf)
	if err := debugMergeLogsCmd.ParseFlags(c.flags); err != nil {
		t.Fatalf("Failed to set flags: %v", err)
	}
	if err := debugMergeLogsCmd.RunE(debugMergeLogsCmd, c.args); err != nil {
		t.Fatalf("Failed to run command: %v", err)
	}
	// The expected output lives in filepath.Join(testdataPath, "results", testCase.name)
	var resultFile string
	switch c.format {
	case "crdb-v2":
		resultFile = filepath.Join(crdbV2testdataPath, "results", c.name)
	case "crdb-v1":
		resultFile = filepath.Join(crdbV1testdataPath, "results", c.name)
	}
	expected, err := ioutil.ReadFile(resultFile)
	if err != nil {
		t.Errorf("Failed to read expected result from %v: %v", resultFile, err)
	}
	if !bytes.Equal(expected, outBuf.Bytes()) {
		t.Fatalf("Result does not match expected. Got %d bytes, expected %d. got:\n%v\nexpected:\n%v",
			len(outBuf.String()), len(expected), outBuf.String(), string(expected))
	}
}

func TestCrdbV2DebugMergeLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var cases = getCases("crdb-v2")
	for _, c := range cases {
		t.Run(c.name, c.run)
	}
}

func TestCrdbV1DebugMergeLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var cases = getCases("crdb-v1")
	for _, c := range cases {
		t.Run(c.name, c.run)
	}
}
