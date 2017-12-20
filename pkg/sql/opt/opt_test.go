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
//   <command>[,<command>...] [arg | arg=val | arg=(val1, val2, ...)]...
//   <SQL statement or expression>
//   ----
//   <expected results>
//
// The supported commands are:
//
//  - legacy-normalize
//
//    Builds an expression tree from a scalar SQL expression and runs the
//    TypedExpr normalization code. It must be followed by build-scalar.
//
//  - build-scalar
//
//    Builds an expression tree from a scalar SQL expression and outputs a
//    representation of the tree. The expression can refer to external variables
//    using @1, @2, etc. in which case the types of the variables must be passed
//    via a "columns" argument.
//
//  - normalize
//
//    Normalizes the expression. If present, must follow build-scalar.
//
//  - index-constraints
//
//    Creates index constraints on the assumption that the index is formed by
//    the index var columns (as specified by "columns").
//    If present, build-scalar must have been an earlier command.
//
// The supported arguments are:
//  - columns=(<type> [ascending|asc|descending|desc], ...)
//
//    Sets the types of index var columns, and optionally direction.

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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

var splitDirectivesRE = regexp.MustCompile(`^ *[a-zA-Z0-9_,-]+(|=[a-zA-Z0-9_]+|=\([^)]*\))( |$)`)

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

func (r *testdataReader) Next(t *testing.T) bool {
	t.Helper()

	r.data = testdata{}
	for r.scanner.Scan() {
		line := r.scanner.Text()
		r.emit(line)

		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			// Skip comment lines.
			continue
		}

		fields := splitDirectives(t, line)
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
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
				var colInfos []IndexColumnInfo
				var typedExpr tree.TypedExpr

				for _, arg := range d.cmdArgs {
					key := arg
					val := ""
					if pos := strings.Index(key, "="); pos >= 0 {
						key = arg[:pos]
						val = arg[pos+1:]
					}
					if len(val) > 2 && val[0] == '(' && val[len(val)-1] == ')' {
						val = val[1 : len(val)-1]
					}
					switch key {
					case "columns":
						var err error
						colInfos, err = parseColumns(strings.Split(val, ","))
						if err != nil {
							d.fatalf(t, "%v", err)
						}
					default:
						d.fatalf(t, "unknown argument: %s", key)
					}
				}

				buildScalarFn := func() {
					defer func() {
						if r := recover(); r != nil {
							d.fatalf(t, "buildScalar: %v", r)
						}
					}()
					e = buildScalar(&buildContext{}, typedExpr)
				}

				evalCtx := tree.MakeTestingEvalContext()
				var err error
				typedExpr, err = parseScalarExpr(d.sql, colInfos)
				if err != nil {
					d.fatalf(t, "%v", err)
				}
				for _, cmd := range strings.Split(d.cmd, ",") {
					switch cmd {
					case "legacy-normalize":
						// Apply the TypedExpr normalization and rebuild the expression.
						typedExpr, err = evalCtx.NormalizeExpr(typedExpr)
						if err != nil {
							d.fatalf(t, "%v", err)
						}
					case "build-scalar":
						buildScalarFn()

					case "normalize":
						normalizeExpr(e)

					case "index-constraints":
						if e == nil {
							d.fatalf(t, "no expression for index-constraints")
						}

						spans := MakeIndexConstraints(e, colInfos, &evalCtx)
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

// parseType parses a string describing a type.
func parseType(typeStr string) (types.T, error) {
	colType, err := parser.ParseType(typeStr)
	if err != nil {
		return nil, err
	}
	return coltypes.CastTargetToDatumType(colType), nil
}

// parseColumns parses descriptions of index columns; each
// string corresponds to an index column and is of the form:
//   <type> [ascending|descending]
func parseColumns(colStrs []string) ([]IndexColumnInfo, error) {
	res := make([]IndexColumnInfo, len(colStrs))
	for i := range colStrs {
		fields := strings.Fields(colStrs[i])
		var err error
		res[i].typ, err = parseType(fields[0])
		if err != nil {
			return nil, err
		}
		res[i].direction = encoding.Ascending
		for _, f := range fields[1:] {
			switch strings.ToLower(f) {
			case "ascending", "asc":
				// ascending is the default.
			case "descending", "desc":
				res[i].direction = encoding.Descending
			default:
				return nil, fmt.Errorf("unknown column attribute %s", f)
			}
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

func parseScalarExpr(sql string, indexVarCols []IndexColumnInfo) (tree.TypedExpr, error) {
	expr, err := parser.ParseExpr(sql)
	if err != nil {
		return nil, err
	}

	// Set up an indexed var helper so we can type-check the expression.
	iv := &indexedVars{types: make([]types.T, len(indexVarCols))}
	for i, colInfo := range indexVarCols {
		iv.types[i] = colInfo.typ
	}

	sema := tree.MakeSemaContext(false /* privileged */)
	iVarHelper := tree.MakeIndexedVarHelper(iv, len(iv.types))
	sema.IVarHelper = &iVarHelper

	return expr.TypeCheck(&sema, types.Any)
}
