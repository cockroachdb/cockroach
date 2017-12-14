// Copyright 2017 The Cockroach Authors.
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

package opt

// This file is home to TestOpt, which is similar to the logic tests, except it
// is used for optimizer-specific testcases.
//
// Each testfile contains testcases of the form
//   <command>[,<command>...] [<index-var-types> ...]
//   <SQL statement or expression>
//   ----
//   <expected results>
//
// The supported commands are:
//
//  - build-scalar
//
//    Builds an expression tree from a scalar SQL expression and outputs a
//    representation of the tree. The expression can refer to external variables
//    using @1, @2, etc. in which case the types of the variables must be passed
//    on the command line.
//
//  - legacy-normalize
//
//    Runs the TypedExpr normalization code and rebuilds the scalar expression.
//    If present, must follow build-scalar.
//
//  - normalize
//
//    Normalizes the expression. If present, must follow build-scalar or
//    legacy-normalize.
//
//  - index-constraints
//
//    Creates index constraints on the assumption that the index is formed by
//    the index var columns (as specified by <index-var-types>).
//    If present, build-scalar must have been an earlier command.

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

var (
	logicTestData    = flag.String("d", "testdata/[^.]*", "test data glob")
	rewriteTestFiles = flag.Bool(
		"rewrite", false,
		"ignore the expected results and rewrite the test files with the actual results from this "+
			"run. Used to update tests when a change affects many cases; please verify the testfile "+
			"diffs carefully!",
	)
)

type lineScanner struct {
	*bufio.Scanner
	line int
}

func newLineScanner(r io.Reader) *lineScanner {
	return &lineScanner{
		Scanner: bufio.NewScanner(r),
		line:    0,
	}
}

func (l *lineScanner) Scan() bool {
	ok := l.Scanner.Scan()
	if ok {
		l.line++
	}
	return ok
}

type testdata struct {
	pos      string // file and line number
	cmd      string
	cmdArgs  []string
	sql      string
	expected string
}

func (td testdata) fatalf(t *testing.T, format string, args ...interface{}) {
	t.Helper()
	t.Fatalf("%s: %s", td.pos, fmt.Sprintf(format, args...))
}

type testdataReader struct {
	path    string
	file    *os.File
	scanner *lineScanner
	data    testdata
	rewrite *bytes.Buffer
}

func newTestdataReader(t *testing.T, path string) *testdataReader {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var rewrite *bytes.Buffer
	if *rewriteTestFiles {
		rewrite = &bytes.Buffer{}
	}
	return &testdataReader{
		path:    path,
		file:    file,
		scanner: newLineScanner(file),
		rewrite: rewrite,
	}
}

func (r *testdataReader) Close() error {
	return r.file.Close()
}

func (r *testdataReader) Next(t *testing.T) bool {
	t.Helper()

	r.data = testdata{}
	for r.scanner.Scan() {
		line := r.scanner.Text()
		r.emit(line)

		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if strings.HasPrefix(cmd, "#") {
			// Skip comment lines.
			continue
		}
		r.data.pos = fmt.Sprintf("%s:%d", r.path, r.scanner.line)
		r.data.cmd = cmd
		r.data.cmdArgs = fields[1:]

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

		r.data.sql = strings.TrimSpace(buf.String())

		if separator {
			buf.Reset()
			for r.scanner.Scan() {
				line := r.scanner.Text()
				if strings.TrimSpace(line) == "" {
					break
				}
				fmt.Fprintln(&buf, line)
			}
			r.data.expected = buf.String()
		}
		return true
	}
	return false
}

func (r *testdataReader) emit(s string) {
	if r.rewrite != nil {
		r.rewrite.WriteString(s)
		r.rewrite.WriteString("\n")
	}
}

// runTest reads through a file; for every testcase, it breaks it up into
// testdata.cmd, testdata.sql, testdata.expected, calls f, then compares the
// results.
func runTest(t *testing.T, path string, f func(d *testdata) string) {
	r := newTestdataReader(t, path)
	for r.Next(t) {
		d := &r.data
		str := f(d)
		if r.rewrite != nil {
			r.emit(str)
		} else if d.expected != str {
			t.Fatalf("%s: %s\nexpected:\n%s\nfound:\n%s", d.pos, d.sql, d.expected, str)
		} else if testing.Verbose() {
			fmt.Printf("%s:\n%s\n----\n%s", d.pos, d.sql, str)
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

func TestOpt(t *testing.T) {
	paths, err := filepath.Glob(*logicTestData)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no testfiles found matching: %s", *logicTestData)
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			runTest(t, path, func(d *testdata) string {
				var e *expr
				var types []types.T
				var typedExpr tree.TypedExpr

				buildScalarFn := func() {
					defer func() {
						if r := recover(); r != nil {
							d.fatalf(t, "buildScalar: %v", r)
						}
					}()
					e = buildScalar(&buildContext{}, typedExpr)
				}

				evalCtx := tree.MakeTestingEvalContext()
				for _, cmd := range strings.Split(d.cmd, ",") {
					switch cmd {
					case "build-scalar":
						var err error
						types, err = parseTypes(d.cmdArgs)
						if err != nil {
							d.fatalf(t, "%v", err)
						}
						typedExpr, err = parseScalarExpr(d.sql, types)
						if err != nil {
							d.fatalf(t, "%v", err)
						}

						buildScalarFn()
					case "legacy-normalize":
						// Apply the TypedExpr normalization and rebuild the expression.
						typedExpr, err = evalCtx.NormalizeExpr(typedExpr)
						if err != nil {
							d.fatalf(t, "%v", err)
						}
						buildScalarFn()
					case "normalize":
						normalizeExpr(e)
					case "index-constraints":
						if e == nil {
							d.fatalf(t, "no expression for index-constraints")
						}

						spans := MakeIndexConstraints(e, types, &evalCtx)
						var buf bytes.Buffer
						for _, sp := range spans {
							fmt.Fprintf(&buf, "%s\n", sp)
						}
						return buf.String()
					default:
						d.fatalf(t, "unsupported command: %s", cmd)
						return ""
					}
				}
				return e.String()
			})
		})
	}
}

func parseType(typeStr string) (types.T, error) {
	colType, err := parser.ParseType(typeStr)
	if err != nil {
		return nil, err
	}
	return coltypes.CastTargetToDatumType(colType), nil
}

func parseTypes(typeStrs []string) ([]types.T, error) {
	res := make([]types.T, len(typeStrs))
	for i, typeStr := range typeStrs {
		var err error
		res[i], err = parseType(typeStr)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

type indexedVars struct {
	types []types.T
}

var _ tree.IndexedVarContainer = &indexedVars{}

func (*indexedVars) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("unimplemented")
}

func (iv *indexedVars) IndexedVarResolvedType(idx int) types.T {
	if idx >= len(iv.types) {
		panic("out of bounds IndexedVar; not enough types provided")
	}
	return iv.types[idx]
}

func (*indexedVars) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	panic("unimplemented")
}

func parseScalarExpr(sql string, indexVarTypes []types.T) (tree.TypedExpr, error) {
	expr, err := parser.ParseExpr(sql)
	if err != nil {
		return nil, err
	}

	// Set up an indexed var helper so we can type-check the expression.
	iv := &indexedVars{types: indexVarTypes}

	sema := tree.MakeSemaContext(false /* privileged */)
	iVarHelper := tree.MakeIndexedVarHelper(iv, len(iv.types))
	sema.IVarHelper = &iVarHelper

	return expr.TypeCheck(&sema, types.Any)
}
