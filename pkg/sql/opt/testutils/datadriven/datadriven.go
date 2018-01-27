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
	"io/ioutil"
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
		actual := f(d)
		if r.rewrite != nil {
			r.emit(actual)
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

// TestData contains information about one data-driven test case that was
// parsed from the test file.
type TestData struct {
	Pos      string // file and line number
	Cmd      string
	CmdArgs  []string
	Input    string
	Expected string
}

// Fatalf wraps a fatal testing error with test file position information, so
// that it's easy to locate the source of the error.
func (td TestData) Fatalf(t *testing.T, format string, args ...interface{}) {
	t.Helper()
	t.Fatalf("%s: %s", td.Pos, fmt.Sprintf(format, args...))
}
