// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// testfilter is a utility to manipulate JSON streams in [test2json] format.
// Standard input is read and each line starting with `{` and ending with `}`
// parsed (and expected to parse successfully). Lines not matching this pattern
// are classified as output not related to the test and, depending on the args
// passed to `testfilter`, are passed through or removed. The arguments available
// are `--mode=(strip|omit|convert)`, where:
//
// strip: omit output for non-failing tests, pass everything else through. In
//   particular, non-test output and tests that never terminate are passed through.
// omit: print only failing tests. Note that test2json does not close scopes for
//   tests that are running in parallel (in the same package) with a "foreground"
//   test that panics, so it will pass through *only* the one foreground test.
//   Note also that package scopes are omitted; test2json does not reliably close
//   them on panic/Exit anyway.
// convert:
//   no filtering is performed, but any test2json input is translated back into
//   its pure Go test framework text representation. This is useful for output
//   intended for human eyes.
//
// [test2json]: https://golang.org/cmd/test2json/
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

const modeUsage = `strip:
  omit output for non-failing tests, but print run/pass/skip events for all tests
omit:
  only emit failing tests
convert:
  don't perform any filtering, simply convert the json back to original test format'
`

type modeT byte

const (
	modeStrip modeT = iota
	modeOmit
	modeConvert
)

func (m *modeT) Set(s string) error {
	switch s {
	case "strip":
		*m = modeStrip
	case "omit":
		*m = modeOmit
	case "convert":
		*m = modeConvert
	default:
		return errors.New("unsupported mode")
	}
	return nil
}

func (m *modeT) String() string {
	switch *m {
	case modeStrip:
		return "strip"
	case modeOmit:
		return "omit"
	case modeConvert:
		return "convert"
	default:
		return "unknown"
	}
}

var modeVar = modeStrip

func init() {
	flag.Var(&modeVar, "mode", modeUsage)
}

type testEvent struct {
	Time    time.Time // encodes as an RFC3339-format string
	Action  string
	Package string
	Test    string
	Elapsed float64 // seconds
	Output  string
}

func (t *testEvent) json() string {
	j, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return string(j)
}

func main() {
	flag.Parse()
	if err := filter(os.Stdin, os.Stdout, modeVar); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

const packageLevelTestName = "PackageLevel"

// tup identifies a single test.
type tup struct {
	pkg  string
	test string
}

type ent struct {
	first, last     string // RUN and (SKIP|PASS|FAIL)
	strings.Builder        // output

	// The following fields are set for package-level entries.
	numActiveTests int // number of tests currently running in package.
	numTestsFailed int // number of tests failed so far in package.
}

func filter(in io.Reader, out io.Writer, mode modeT) error {
	scanner := bufio.NewScanner(in)
	m := map[tup]*ent{}
	ev := &testEvent{}
	var n int               // number of JSON lines parsed
	var passFailLine string // catch common error of piping non-json test output in
	for scanner.Scan() {
		line := scanner.Text() // has no EOL marker
		if len(line) <= 2 || line[0] != '{' || line[len(line)-1] != '}' {
			// Not test2json output, pass it through except in `omit` mode.
			// It's important that we still see build errors etc when running
			// in -mode=strip.
			if passFailLine == "" && (strings.Contains(line, "PASS") || strings.Contains(line, "FAIL")) {
				passFailLine = line
			}
			if mode != modeOmit {
				fmt.Fprintln(out, line)
			}
			continue
		}
		*ev = testEvent{}
		if err := json.Unmarshal([]byte(line), ev); err != nil {
			return err
		}
		n++

		if mode == modeConvert {
			if ev.Action == "output" {
				fmt.Fprint(out, ev.Output)
			}
			continue
		}

		if ev.Test == "" {
			// This is a package-level message.

			// Populate a fake test name. We need this because TC is blind
			// to anything without a "Test" field.
			ev.Test = packageLevelTestName
			pkey := tup{ev.Package, ev.Test}

			switch ev.Action {
			case "fail":
				buf := m[pkey]

				// Is the package failing with some non-terminated tests?
				hasOpenSubTests := buf != nil && buf.numActiveTests > 0

				// Did the package contain any failing tests?
				hadSomeFailedTests := buf != nil && buf.numTestsFailed > 0

				// If the package is failing without non-terminated tests,
				// but it contained some failing tests, then we rely on these
				// tests' output to explain what happened to the user. In that
				// case, we are happy to ignore the package-level output.
				//
				// (If the package did not have any failing tests at all, we
				// still want some package-level output: in that case, if it
				// fails we want some details about that below. If there was
				// any non-terminating test, we also mandate a package-level
				// result, which will contain the list of non-terminating
				// tests.)
				if !hasOpenSubTests && hadSomeFailedTests {
					delete(m, pkey)
					continue
				}
			}

			// At this point, either:
			// - we are starting to see output for a package which
			//   has not yet terminated processing; or
			// - we are seeing the last pass/fail entry for a package,
			//   and there is something "interesting" to report
			//   for this package.
			//
			// In both cases, we are going to emit a test result for the
			// package itself in the common output processing case below.
			// For this to be valid/possible, we first need to ensure
			// the map contains a package-level entry.
			if err := ensurePackageEntry(m, out, ev, pkey, mode); err != nil {
				return err
			}

			// At this point, either we are still processing a package's
			// output before it completes, or the last package event.

			const helpMessage = `
Check full_output.txt in artifacts for stray panics or other errors that broke
the test process. Note that you might be looking at this message from a parent
CI job. To reliably get to the "correct" job, click the drop-down next to
"PackageLevel" above, then "Show in build log", and then navigate to the
artifacts tab. See:

https://user-images.githubusercontent.com/5076964/110923167-e2ab4780-8320-11eb-8fba-99da632aa814.png
https://user-images.githubusercontent.com/5076964/110923299-08d0e780-8321-11eb-91af-f4eedcf8bacb.png

for details.
`
			if ev.Action != "output" {
				// This is the final event for the package.
				//
				// Dump all the test scopes so far in this package, then
				// forget about them. This ensures that the test scopes are
				// closed before the package scope is closed the final output.
				var testReport strings.Builder
				for key := range m {
					if key.pkg == ev.Package && key.test != ev.Test {
						// We only mention the test scopes without their sub-tests;
						// otherwise we could get tens of thousands of output lines
						// for a failed logic test run due to a panic.
						if strings.Contains(key.test, "/") {
							// Sub-test. Just forget all about it.
							delete(m, key)
						} else {
							// Not a sub-test.

							// Remember the test's name to report in the
							// package-level output.
							testReport.WriteString("\n" + key.test)

							// Synthetize a "skip" message. We want "something" (and
							// not nothing) so that we get some timing information
							// in strip mode.
							//
							// We use "skip" and not "fail" to ensure that no issue
							// gets filed for the open-ended tests by the github
							// auto-poster: we don't have confidence for any of them
							// that they are the particular cause of the failure.
							syntheticSkipEv := testEvent{
								Time:    ev.Time,
								Action:  "skip",
								Package: ev.Package,
								Test:    key.test,
								Elapsed: 0,
								Output:  "unfinished due to package-level failure" + helpMessage,
							}
							// Translate the synthetic message back into an output line.
							syntheticLine := syntheticSkipEv.json()
							if err := processTestEvent(m, out, &syntheticSkipEv, syntheticLine, mode); err != nil {
								return err
							}
						}
					}
				}

				// If the package is failing, tell the user that something was amiss.
				if ev.Action == "fail" {
					ev.Output += helpMessage
					if testReport.Len() > 0 {
						ev.Output += "\nThe following tests have not completed and could be the cause of the failure:" + testReport.String()
					}
				}
			}

			// Re-populate the line from the JSON payload for the
			// PackageLevel pseudo-test.
			line = ev.json()
		}

		// Common output processing.
		if err := processTestEvent(m, out, ev, line, mode); err != nil {
			return err
		}
	}
	// Some scopes might still be open. To the best of my knowledge,
	// this is due to a panic/premature exit of a single-package test
	// binary. In that case, it seems that neither is the package scope
	// closed, nor the scopes for any tests that were running in
	// parallel, so we pass that through if stripping, but not when
	// omitting.
	if mode == modeStrip {
		for key := range m {
			buf := m[key]
			// Skip over the package-level pseudo-entries. Since we're
			// single-package, the remainder of the output is sufficient
			// here.
			if key.test == packageLevelTestName {
				continue
			}
			fmt.Fprintln(out, buf.String())
		}
	}
	// TODO(tbg): would like to return an error here for sanity, but the
	// JSON just isn't well-formed all the time. For example, at the time
	// of writing, here's a repro:
	// make benchshort PKG=./pkg/bench BENCHES=BenchmarkIndexJoin 2>&1 | \
	// testfilter -mode=strip
	// Interestingly it works once we remove the `log.Scope(b).Close` in
	// that test. Adding TESTFLAGS=-v doesn't matter apparently.
	// if len(m) != 0 {
	// 	return fmt.Errorf("%d tests did not terminate (a package likely exited prematurely)", len(m))
	// }
	if mode != modeConvert && n == 0 && passFailLine != "" {
		// Without this, if the input to this command wasn't even JSON, we would
		// pass. That's a mistake we should avoid at all costs. Note that even
		// `go test -run - ./some/pkg` produces n>0 due to the start/pass events
		// for the package, so if we're here then 100% something weird is going
		// on.
		return fmt.Errorf("not a single test was parsed, but detected test output: %s", passFailLine)
	}
	return nil
}

// ensurePackageEntry ensures there is a package-level entry in the
// map for each package.
//
// This is necessary because we want to consider package-level
// results as regular test results. To achieve this
// successfully, we need to understand how TC processes tests.
//
// TC is a bit peculiar and requires all tests to *start* with
// an event with action "run", then zero or more "output"
// actions, then one of either "pass", "skip" or "fail".
//
// Unfortunately, `go test` does not emit initial "run"
// entries for package-level outputs. This is arguably a
// bug. Instead it starts directly with either "output" or
// "pass"/"fail" at the end. This prevents TC from treating
// the package as a test. To fix that, We insert a synthetic
// "run" entry for the package in the map here.
func ensurePackageEntry(m map[tup]*ent, out io.Writer, ev *testEvent, pkey tup, mode modeT) error {
	if buf := m[pkey]; buf != nil {
		return nil
	}
	// Package not known yet. Synthetize an entry.
	packageEvent := *ev
	packageEvent.Test = packageLevelTestName
	packageEvent.Action = "run"
	packageEvent.Output = ""
	packageLine := packageEvent.json()
	return processTestEvent(m, out, &packageEvent, packageLine, mode)
}

func processTestEvent(m map[tup]*ent, out io.Writer, ev *testEvent, line string, mode modeT) error {
	// The package key.
	pkey := tup{ev.Package, packageLevelTestName}
	// The test's key.
	key := tup{ev.Package, ev.Test}

	// Is this a regular test? In that case, ensure there is a
	// package-level entry for this test.
	if ev.Test != packageLevelTestName {
		if err := ensurePackageEntry(m, out, ev, pkey, mode); err != nil {
			return err
		}
	}

	// Now process the test itself.
	buf := m[key]
	if buf == nil {
		buf = &ent{first: line}
		m[key] = buf
		if key != pkey {
			// Remember how many tests we're seeing.
			m[pkey].numActiveTests++
		}
	}
	if _, err := fmt.Fprintln(buf, line); err != nil {
		return err
	}
	switch ev.Action {
	case "pass", "skip", "fail":
		buf.last = line
		if ev.Action == "fail" {
			fmt.Fprint(out, buf.String())
		} else if mode == modeStrip {
			// Output only the start and end of test so that we preserve the
			// timing information. However, the output is omitted.
			fmt.Fprintln(out, buf.first)
			fmt.Fprintln(out, buf.last)
		}

		// Forget the test.
		delete(m, key)
		if key != pkey {
			m[pkey].numActiveTests--
			if ev.Action == "fail" {
				m[pkey].numTestsFailed++
			}
		}

	case "run", "pause", "cont", "bench", "output":
	default:
		// We must have parsed some JSON that wasn't a testData.
		return fmt.Errorf("unknown input: %s", line)
	}
	return nil
}
