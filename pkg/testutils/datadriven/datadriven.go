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
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var (
	rewriteTestFiles = flag.Bool(
		"rewrite", false,
		"ignore the expected results and rewrite the test files with the actual results from this "+
			"run. Used to update tests when a change affects many cases; please verify the testfile "+
			"diffs carefully!",
	)
)

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
// about the test case in a TestData struct. The function then returns the
// actual results of the case, which this function compares with the expected
// results, and either succeeds or fails the test.
func RunTest(t *testing.T, path string, f func(d *TestData) string) {
	r := newTestDataReader(t, path)
	for r.Next(t) {
		d := &r.data
		actual := func() string {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("\npanic during %s:\n%s\n", d.Pos, d.Input)
					panic(r)
				}
			}()
			return f(d)
		}()

		if r.rewrite != nil {
			r.emit("----")
			if hasBlankLine(actual) {
				r.emit("----")
				r.rewrite.WriteString(actual)
				r.emit("----")
				r.emit("----")
			} else {
				r.emit(actual)
			}
		} else if d.Expected != actual {
			t.Fatalf("%s: %s\nexpected:\n%s\nfound:\n%s", d.Pos, d.Input, d.Expected, actual)
		} else if testing.Verbose() {
			fmt.Printf("%s:\n%s\n----\n%s", d.Pos, d.Input, actual)
		}
	}

	if r.rewrite != nil {
		data := r.rewrite.Bytes()
		if l := len(data); l > 2 && data[l-1] == '\n' && data[l-2] == '\n' {
			data = data[:l-1]
		}
		err := ioutil.WriteFile(path, data, 0644)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Walk goes through all the files in a subdirectory, creating subtests to match
// the file hierarchy; for each "leaf" file, the given function is called.
//
// This can be used in conjunction with RunTest. For example:
//
//    datadriven.Walk(t, path, func (t *testing.T, path string) {
//      // initialize per-test state
//      datadriven.RunTest(t, path, func (d *datadriven.TestData) {
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
//   care created.
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
		t.Run(file.Name(), func(t *testing.T) {
			Walk(t, filepath.Join(path, file.Name()), f)
		})
	}
}

// TestData contains information about one data-driven test case that was
// parsed from the test file.
type TestData struct {
	Pos string // file and line number

	// Cmd is the first string on the directive line (up to the first whitespace).
	Cmd string

	CmdArgs []CmdArg

	Input    string
	Expected string
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

func (arg *CmdArg) String() string {
	switch len(arg.Vals) {
	case 0:
		return arg.Key

	case 1:
		return fmt.Sprintf("%s=%s", arg.Key, arg.Vals[0])

	default:
		return fmt.Sprintf("%s=(%s)", arg.Key, strings.Join(arg.Vals, ", "))
	}
}

// Fatalf wraps a fatal testing error with test file position information, so
// that it's easy to locate the source of the error.
func (td TestData) Fatalf(tb testing.TB, format string, args ...interface{}) {
	tb.Helper()
	tb.Fatalf("%s: %s", td.Pos, fmt.Sprintf(format, args...))
}

func hasBlankLine(s string) bool {
	scanner := bufio.NewScanner(strings.NewReader(s))
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "" {
			return true
		}
	}
	return false
}
