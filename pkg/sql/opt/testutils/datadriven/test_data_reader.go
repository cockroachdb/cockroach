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
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

type testDataReader struct {
	path    string
	file    *os.File
	scanner *lineScanner
	data    TestData
	rewrite *bytes.Buffer
}

func newTestDataReader(t *testing.T, path string) *testDataReader {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var rewrite *bytes.Buffer
	if *rewriteTestFiles {
		rewrite = &bytes.Buffer{}
	}
	return &testDataReader{
		path:    path,
		file:    file,
		scanner: newLineScanner(file),
		rewrite: rewrite,
	}
}

func (r *testDataReader) Close() error {
	return r.file.Close()
}

func (r *testDataReader) Next(t *testing.T) bool {
	t.Helper()

	r.data = TestData{}
	for r.scanner.Scan() {
		line := r.scanner.Text()
		r.emit(line)

		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			// Skip comment lines.
			continue
		}
		// Support wrapping directive lines using \, for example:
		//   build-scalar \
		//   vars(int)
		for strings.HasSuffix(line, `\`) && r.scanner.Scan() {
			nextLine := r.scanner.Text()
			r.emit(nextLine)
			line = strings.TrimSuffix(line, `\`) + " " + strings.TrimSpace(nextLine)
		}

		fields := splitDirectives(t, line)
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		r.data.Pos = fmt.Sprintf("%s:%d", r.path, r.scanner.line)
		r.data.Cmd = cmd
		r.data.CmdArgs = fields[1:]

		var buf bytes.Buffer
		var separator bool
		for r.scanner.Scan() {
			line := r.scanner.Text()
			if strings.TrimSpace(line) == "" {
				break
			}

			r.emit(line)
			if line == "----" {
				separator = true
				break
			}
			fmt.Fprintln(&buf, line)
		}

		r.data.Input = strings.TrimSpace(buf.String())

		if separator {
			buf.Reset()
			for r.scanner.Scan() {
				line := r.scanner.Text()
				if strings.TrimSpace(line) == "" {
					break
				}
				fmt.Fprintln(&buf, line)
			}
			r.data.Expected = buf.String()
		}
		return true
	}
	return false
}

func (r *testDataReader) emit(s string) {
	if r.rewrite != nil {
		r.rewrite.WriteString(s)
		r.rewrite.WriteString("\n")
	}
}

var splitDirectivesRE = regexp.MustCompile(`^ *[a-zA-Z0-9_,-]+(|=[a-zA-Z0-9_@]+|=\([^)]*\))( |$)`)

// splits a directive line into tokens, where each token is
// either:
//  - a,list,of,things
//  - argument
//  - argument=value
//  - argument=(values, ...)
func splitDirectives(t *testing.T, line string) []string {
	var res []string

	for line != "" {
		str := splitDirectivesRE.FindString(line)
		if len(str) == 0 {
			t.Fatalf("cannot parse directive %s\n", line)
		}
		res = append(res, strings.TrimSpace(line[0:len(str)]))
		line = line[len(str):]
	}
	return res
}
