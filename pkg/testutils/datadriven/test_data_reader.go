// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package datadriven

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"
)

type testDataReader struct {
	sourceName string
	reader     io.Reader
	scanner    *lineScanner
	data       TestData
	rewrite    *bytes.Buffer
}

func newTestDataReader(
	t *testing.T, sourceName string, file io.Reader, record bool,
) *testDataReader {
	t.Helper()

	var rewrite *bytes.Buffer
	if record {
		rewrite = &bytes.Buffer{}
	}
	return &testDataReader{
		sourceName: sourceName,
		reader:     file,
		scanner:    newLineScanner(file),
		rewrite:    rewrite,
	}
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
		r.data.Pos = fmt.Sprintf("%s:%d", r.sourceName, r.scanner.line)
		r.data.Cmd = cmd

		for _, arg := range fields[1:] {
			key := arg
			var vals []string
			if pos := strings.IndexByte(key, '='); pos >= 0 {
				key = arg[:pos]
				val := arg[pos+1:]

				if len(val) > 2 && val[0] == '(' && val[len(val)-1] == ')' {
					vals = strings.Split(val[1:len(val)-1], ",")
					for i := range vals {
						vals[i] = strings.TrimSpace(vals[i])
					}
				} else {
					vals = []string{val}
				}
			}
			r.data.CmdArgs = append(r.data.CmdArgs, CmdArg{Key: key, Vals: vals})
		}

		var buf bytes.Buffer
		var separator bool
		for r.scanner.Scan() {
			line := r.scanner.Text()
			if line == "----" {
				separator = true
				break
			}

			r.emit(line)
			fmt.Fprintln(&buf, line)
		}

		r.data.Input = strings.TrimSpace(buf.String())

		if separator {
			r.readExpected()
		}
		return true
	}
	return false
}

func (r *testDataReader) readExpected() {
	var buf bytes.Buffer
	var line string
	var allowBlankLines bool

	if r.scanner.Scan() {
		line = r.scanner.Text()
		if line == "----" {
			allowBlankLines = true
		}
	}

	if allowBlankLines {
		// Look for two successive lines of "----" before terminating.
		for r.scanner.Scan() {
			line = r.scanner.Text()

			if line == "----" {
				if r.scanner.Scan() {
					line2 := r.scanner.Text()
					if line2 == "----" {
						break
					}

					fmt.Fprintln(&buf, line)
					fmt.Fprintln(&buf, line2)
					continue
				}
			}

			fmt.Fprintln(&buf, line)
		}
	} else {
		// Terminate on first blank line.
		for {
			if strings.TrimSpace(line) == "" {
				break
			}

			fmt.Fprintln(&buf, line)

			if !r.scanner.Scan() {
				break
			}

			line = r.scanner.Text()
		}
	}

	r.data.Expected = buf.String()
}

func (r *testDataReader) emit(s string) {
	if r.rewrite != nil {
		r.rewrite.WriteString(s)
		r.rewrite.WriteString("\n")
	}
}

var splitDirectivesRE = regexp.MustCompile(`^ *[a-zA-Z0-9_,-\.]+(|=[-a-zA-Z0-9_@]+|=\([^)]*\))( |$)`)

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
