// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package datadriven

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/pmezard/go-difflib/difflib"
)

var (
	rewriteTestFiles = flag.Bool(
		"rewrite", false,
		"ignore the expected results and rewrite the test files with the actual results from this "+
			"run. Used to update tests when a change affects many cases; please verify the testfile "+
			"diffs carefully!",
	)

	quietLog = flag.Bool(
		"datadriven-quiet", false,
		"avoid echoing the directives and responses from test files.",
	)
)

// Verbose returns true iff -datadriven-quiet was not passed.
func Verbose() bool {
	return testing.Verbose() && !*quietLog
}

// In CockroachDB we want to quiesce all the logs across all packages.
// If we had only a flag to work with, we'd get command line parsing
// errors on all packages that do not use datadriven. So
// we make do by also making a command line parameter available.
func init() {
	const quietEnvVar = "DATADRIVEN_QUIET_LOG"
	if str, ok := os.LookupEnv(quietEnvVar); ok {
		v, err := strconv.ParseBool(str)
		if err != nil {
			panic(fmt.Sprintf("error parsing %s: %s", quietEnvVar, err))
		}
		*quietLog = v
	}
}

// RunTest invokes a data-driven test. The test cases are contained in a
// separate test file and are dynamically loaded, parsed, and executed by this
// testing framework. By convention, test files are typically located in a
// sub-directory called "testdata". Each test file has the following format:
//
//   <command>[,<command>...] [arg | arg=val | arg=(val1, val2, ...)]...
//   <input to the command>
//   ----
//   <expected results>
//
// The command input can contain blank lines. However, by default, the expected
// results cannot contain blank lines. This alternate syntax allows the use of
// blank lines:
//
//   <command>[,<command>...] [arg | arg=val | arg=(val1, val2, ...)]...
//   <input to the command>
//   ----
//   ----
//   <expected results>
//
//   <more expected results>
//   ----
//   ----
//
// To execute data-driven tests, pass the path of the test file as well as a
// function which can interpret and execute whatever commands are present in
// the test file. The framework invokes the function, passing it information
// about the test case in a TestData struct.
//
// The function must returns the actual results of the case, which
// RunTest() compares with the expected results. If the two are not
// equal, the test is marked to fail.
//
// Note that RunTest() creates a sub-instance of testing.T for each
// directive in the input file. It is thus unsafe/invalid to call
// e.g. Fatal() or Skip() on the parent testing.T from inside the
// callback function. Use the provided testing.T instance instead.
//
// It is possible for a test to test for an "expected error" as follows:
// - run the code to test
// - if an error occurs, report the detail of the error as actual
//   output.
// - place the expected error details in the expected results
//   in the input file.
//
// It is also possible for a test to report an _unexpected_ test
// error by calling t.Error().
func RunTest(t *testing.T, path string, f func(t *testing.T, d *TestData) string) {
	t.Helper()
	mode := os.O_RDONLY
	if *rewriteTestFiles {
		// We only open read-write if rewriting, so as to enable running
		// tests on read-only copies of the source tree.
		mode = os.O_RDWR
	}
	file, err := os.OpenFile(path, mode, 0644 /* irrelevant */)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = file.Close()
	}()
	finfo, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	} else if finfo.IsDir() {
		t.Fatalf("%s is a directory, not a file; consider using datadriven.Walk", path)
	}

	rewriteData := runTestInternal(t, path, file, f, *rewriteTestFiles)
	if *rewriteTestFiles {
		if _, err := file.WriteAt(rewriteData, 0); err != nil {
			t.Fatal(err)
		}
		if err := file.Truncate(int64(len(rewriteData))); err != nil {
			t.Fatal(err)
		}
		if err := file.Sync(); err != nil {
			t.Fatal(err)
		}
	}
}

// RunTestFromString is a version of RunTest which takes the contents of a test
// directly.
func RunTestFromString(t *testing.T, input string, f func(t *testing.T, d *TestData) string) {
	t.Helper()
	runTestInternal(t, "<string>" /* sourceName */, strings.NewReader(input), f, *rewriteTestFiles)
}

func runTestInternal(
	t *testing.T,
	sourceName string,
	reader io.Reader,
	f func(t *testing.T, d *TestData) string,
	rewrite bool,
) (rewriteOutput []byte) {
	t.Helper()

	r := newTestDataReader(t, sourceName, reader, rewrite)
	for r.Next(t) {
		runDirectiveOrSubTest(t, r, "" /*mandatorySubTestPrefix*/, f)
	}

	if r.rewrite != nil {
		data := r.rewrite.Bytes()
		// Remove any trailing blank line.
		if l := len(data); l > 2 && data[l-1] == '\n' && data[l-2] == '\n' {
			data = data[:l-1]
		}
		return data
	}
	return nil
}

// runDirectiveOrSubTest runs either a "subtest" directive or an
// actual test directive. The "mandatorySubTestPrefix" argument indicates
// a mandatory prefix required from all sub-test names at this point.
func runDirectiveOrSubTest(
	t *testing.T,
	r *testDataReader,
	mandatorySubTestPrefix string,
	f func(*testing.T, *TestData) string,
) {
	t.Helper()
	if subTestName, ok := isSubTestStart(t, r, mandatorySubTestPrefix); ok {
		runSubTest(subTestName, t, r, f)
	} else {
		runDirective(t, r, f)
	}
	if t.Failed() {
		// If a test has failed with .Error(), we can't expect any
		// subsequent test to be even able to start. Stop processing the
		// file in that case.
		t.FailNow()
	}
}

// runSubTest runs a subtest up to and including the final `subtest
// end`. The opening `subtest` directive has been consumed already.
// The first parameter `subTestName` is the full path to the subtest,
// including the parent subtest names as prefix. This is used to
// validate the nesting and thus prevent mistakes.
func runSubTest(
	subTestName string, t *testing.T, r *testDataReader, f func(*testing.T, *TestData) string,
) {
	// Remember the current reader position in case we need to spell out
	// an error message below.
	subTestStartPos := r.data.Pos
	// seenSubTestEnd is used below to verify that a "subtest end" directive
	// has been detected (as opposed to EOF).
	seenSubTestEnd := false
	// seenSkip is used below to verify that "Skip" has not been used
	// inside a subtest. See below for details.
	seenSkip := false

	// The name passed to t.Run is the last component in the subtest
	// name, because all components before that are already prefixed by
	// t.Run from the names of the parent sub-tests.
	testingSubTestName := subTestName[strings.LastIndex(subTestName, "/")+1:]

	// Begin the sub-test.
	t.Run(testingSubTestName, func(t *testing.T) {
		defer func() {
			// Skips are signalled using Goexit() so we must catch it /
			// remember it here.
			if t.Skipped() {
				seenSkip = true
			}
		}()

		for r.Next(t) {
			if isSubTestEnd(t, r) {
				seenSubTestEnd = true
				return
			}
			runDirectiveOrSubTest(t, r, subTestName+"/" /*mandatorySubTestPrefix*/, f)
		}
	})

	if seenSkip {
		// t.Skip() is not yet supported inside a subtest. To add
		// this functionality the following extra complexity is needed:
		// - the test reader must continue to read after the skip
		//   until the end of the subtest, and ignore all the directives in-between.
		// - the rewrite logic must be careful to keep the input as-is
		//   for the skipped sub-test, while proceeding to rewrite for
		//   non-skipped tests.
		r.data.Fatalf(t,
			"cannot use t.Skip inside subtest\n%s: subtest started here", subTestStartPos)
	}

	if seenSubTestEnd && len(r.data.CmdArgs) == 2 && r.data.CmdArgs[1].Key != subTestName {
		// If a subtest name was provided after "subtest end", ensure that it matches.
		r.data.Fatalf(t,
			"mismatched subtest end directive: expected %q, got %q", r.data.CmdArgs[1].Key, subTestName)
	}

	if !seenSubTestEnd && !t.Failed() {
		// We only report missing "subtest end" if there was no error otherwise;
		// for if there was an error, the reading would have stopped.
		r.data.Fatalf(t,
			"EOF encountered without subtest end directive\n%s: subtest started here", subTestStartPos)
	}

}

func isSubTestStart(t *testing.T, r *testDataReader, mandatorySubTestPrefix string) (string, bool) {
	if r.data.Cmd != "subtest" {
		return "", false
	}
	if len(r.data.CmdArgs) != 1 {
		r.data.Fatalf(t, "invalid syntax for subtest")
	}
	subTestName := r.data.CmdArgs[0].Key
	if subTestName == "end" {
		r.data.Fatalf(t, "subtest end without corresponding start")
	}
	if !strings.HasPrefix(subTestName, mandatorySubTestPrefix) {
		r.data.Fatalf(t, "name of nested subtest must begin with %q", mandatorySubTestPrefix)
	}
	return subTestName, true
}

func isSubTestEnd(t *testing.T, r *testDataReader) bool {
	if r.data.Cmd != "subtest" {
		return false
	}
	if len(r.data.CmdArgs) == 0 || r.data.CmdArgs[0].Key != "end" {
		return false
	}
	if len(r.data.CmdArgs) > 2 {
		r.data.Fatalf(t, "invalid syntax for subtest end")
	}
	return true
}

// runDirective runs just one directive in the input.
//
// The stopNow and subTestSkipped booleans are modified by-reference
// instead of returned because the testing module implements t.Skip
// and t.Fatal using panics, and we're not guaranteed to get back to
// the caller via a return in those cases.
func runDirective(t *testing.T, r *testDataReader, f func(*testing.T, *TestData) string) {
	t.Helper()

	d := &r.data
	actual := func() string {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("\npanic during %s:\n%s\n", d.Pos, d.Input)
				panic(r)
			}
		}()
		actual := f(t, d)
		if actual != "" && !strings.HasSuffix(actual, "\n") {
			actual += "\n"
		}
		return actual
	}()

	if t.Failed() {
		// If the test has failed with .Error(), then we can't hope it
		// will have produced a useful actual output. Trying to do
		// something with it here would risk corrupting the expected
		// output.
		//
		// Moreover, we can't expect any subsequent test to be even
		// able to start. Stop processing the file in that case.
		t.FailNow()
	}

	// The test has not failed, we can analyze the expected
	// output.
	if r.rewrite != nil {
		r.emit("----")
		if hasBlankLine(actual) {
			r.emit("----")
			r.rewrite.WriteString(actual)
			r.emit("----")
			r.emit("----")
			r.emit("")
		} else {
			// Here actual already ends in \n so emit adds a blank line.
			r.emit(actual)
		}
	} else if d.Expected != actual {
		expectedLines := difflib.SplitLines(d.Expected)
		actualLines := difflib.SplitLines(actual)
		if len(expectedLines) > 5 {
			// Print a unified diff if there is a lot of output to compare.
			diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
				Context: 5,
				A:       expectedLines,
				B:       actualLines,
			})
			if err == nil {
				t.Fatalf("\n%s: %s\noutput didn't match expected:\n%s", d.Pos, d.Input, diff)
				return
			}
			t.Logf("Failed to produce diff %v", err)
		}
		t.Fatalf("\n%s: %s\nexpected:\n%s\nfound:\n%s", d.Pos, d.Input, d.Expected, actual)
	} else if Verbose() {
		input := d.Input
		if input == "" {
			input = "<no input to command>"
		}
		// TODO(tbg): it's awkward to reproduce the args, but it would be helpful.
		t.Logf("\n%s:\n%s [%d args]\n%s\n----\n%s", d.Pos, d.Cmd, len(d.CmdArgs), input, actual)
	}
	return
}

// Walk goes through all the files in a subdirectory, creating subtests to match
// the file hierarchy; for each "leaf" file, the given function is called.
//
// This can be used in conjunction with RunTest. For example:
//
//    datadriven.Walk(t, path, func (t *testing.T, path string) {
//      // initialize per-test state
//      datadriven.RunTest(t, path, func (t *testing.T, d *datadriven.TestData) string {
//       // ...
//      }
//    }
//
//   Files:
//     testdata/typing
//     testdata/logprops/scan
//     testdata/logprops/select
//
//   If path is "testdata/typing", the function is called once and no subtests
//   are created.
//
//   If path is "testdata/logprops", the function is called two times, in
//   separate subtests /scan, /select.
//
//   If path is "testdata", the function is called three times, in subtest
//   hierarchy /typing, /logprops/scan, /logprops/select.
//
func Walk(t *testing.T, path string, f func(t *testing.T, path string)) {
	finfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !finfo.IsDir() {
		f(t, path)
		return
	}
	files, err := ioutil.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if tempFileRe.MatchString(file.Name()) {
			// Temp or hidden file, don't even try processing.
			continue
		}
		t.Run(file.Name(), func(t *testing.T) {
			Walk(t, filepath.Join(path, file.Name()), f)
		})
	}
}

func ClearResults(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0644 /* irrelevant */)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	finfo, err := file.Stat()
	if err != nil {
		return err
	}

	if finfo.IsDir() {
		return errors.Newf("%s is a directory, not a file", path)
	}

	runTestInternal(
		&testing.T{}, path, file,
		func(t *testing.T, d *TestData) string { return "" },
		true, /* rewrite */
	)

	return nil
}

// Ignore files named .XXXX, XXX~ or #XXX#.
var tempFileRe = regexp.MustCompile(`(^\..*)|(.*~$)|(^#.*#$)`)

// TestData contains information about one data-driven test case that was
// parsed from the test file.
type TestData struct {
	// Pos is a file:line prefix for the input test file, suitable for
	// inclusion in logs and error messages.
	Pos string

	// Cmd is the first string on the directive line (up to the first whitespace).
	Cmd string

	// CmdArgs contains the k/v arguments to the command.
	CmdArgs []CmdArg

	// Input is the text between the first directive line and the ---- separator.
	Input string

	// Expected is the value below the ---- separator. In most cases,
	// tests need not check this, and instead return their own actual
	// output.
	// This field is provided so that a test can perform an early return
	// with "return d.Expected" to signal that nothing has changed.
	Expected string

	// Rewrite is set if the test is being run with the -rewrite flag.
	Rewrite bool
}

// HasArg checks whether the CmdArgs array contains an entry for the given key.
func (td *TestData) HasArg(key string) bool {
	for i := range td.CmdArgs {
		if td.CmdArgs[i].Key == key {
			return true
		}
	}
	return false
}

// ScanArgs looks up the first CmdArg matching the given key and scans it into
// the given destinations in order. If the arg does not exist, the number of
// destinations does not match that of the arguments, or a destination can not
// be populated from its matching value, a fatal error results.
// If the arg exists multiple times, the first occurrence is parsed.
//
// For example, for a TestData originating from
//
// cmd arg1=50 arg2=yoruba arg3=(50, 50, 50)
//
// the following would be valid:
//
// var i1, i2, i3, i4 int
// var s string
// td.ScanArgs(t, "arg1", &i1)
// td.ScanArgs(t, "arg2", &s)
// td.ScanArgs(t, "arg3", &i2, &i3, &i4)
func (td *TestData) ScanArgs(t *testing.T, key string, dests ...interface{}) {
	t.Helper()
	var arg CmdArg
	for i := range td.CmdArgs {
		if td.CmdArgs[i].Key == key {
			arg = td.CmdArgs[i]
			break
		}
	}
	if arg.Key == "" {
		td.Fatalf(t, "missing argument: %s", key)
	}
	if len(dests) != len(arg.Vals) {
		td.Fatalf(t, "%s: got %d destinations, but %d values", arg.Key, len(dests), len(arg.Vals))
	}

	for i := range dests {
		if err := arg.scanErr(i, dests[i]); err != nil {
			td.Fatalf(t, "%s: failed to scan argument %d: %v", arg.Key, i, err)
		}
	}
}

// CmdArg contains information about an argument on the directive line. An
// argument is specified in one of the following forms:
//  - argument
//  - argument=value
//  - argument=(values, ...)
type CmdArg struct {
	Key  string
	Vals []string
}

func (arg CmdArg) String() string {
	switch len(arg.Vals) {
	case 0:
		return arg.Key

	case 1:
		return fmt.Sprintf("%s=%s", arg.Key, arg.Vals[0])

	default:
		return fmt.Sprintf("%s=(%s)", arg.Key, strings.Join(arg.Vals, ", "))
	}
}

// Scan attempts to parse the value at index i into the dest.
func (arg CmdArg) Scan(t *testing.T, i int, dest interface{}) {
	if err := arg.scanErr(i, dest); err != nil {
		t.Fatal(err)
	}
}

// scanErr is like Scan but returns an error rather than taking a testing.T to fatal.
func (arg CmdArg) scanErr(i int, dest interface{}) error {
	if i < 0 || i >= len(arg.Vals) {
		return errors.Errorf("cannot scan index %d of key %s", i, arg.Key)
	}
	val := arg.Vals[i]
	switch dest := dest.(type) {
	case *string:
		*dest = val
	case *int:
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return err
		}
		*dest = int(n) // assume 64bit ints
	case *uint64:
		n, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return err
		}
		*dest = n
	case *bool:
		b, err := strconv.ParseBool(val)
		if err != nil {
			return err
		}
		*dest = b
	default:
		return errors.Errorf("unsupported type %T for destination #%d (might be easy to add it)", dest, i+1)
	}
	return nil
}

// Fatalf wraps a fatal testing error with test file position information, so
// that it's easy to locate the source of the error.
func (td TestData) Fatalf(tb testing.TB, format string, args ...interface{}) {
	tb.Helper()
	tb.Fatalf("%s: %s", td.Pos, fmt.Sprintf(format, args...))
}

// hasBlankLine returns true iff `s` contains at least one line that's
// empty or contains only whitespace.
func hasBlankLine(s string) bool {
	return blankLineRe.MatchString(s)
}

// blankLineRe matches lines that contain only whitespaces (or
// entirely empty/blank lines).  We use the "m" flag for "multiline"
// mode so that "^" can match the beginning of individual lines inside
// the input, not just the beginning of the input.  In multiline mode,
// "$" also matches the end of lines. However, note how the regexp
// uses "\n" to match the end of lines instead of "$". This is
// because of an oddity in the Go regexp engine: at the very end of
// the input, *after the final \n in the input*, Go estimates there is
// still one more line containing no characters but that matches the
// "^.*$" regexp. The result of this oddity is that an input text like
// "foo\n" will match as "foo\n" (no match) + "" (yes match). We don't
// want that final match to be included, so we force the end-of-line
// match using "\n" specifically.
var blankLineRe = regexp.MustCompile(`(?m)^[\t ]*\n`)
