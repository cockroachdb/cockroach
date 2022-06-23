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

package render

import (
	"bytes"
	"fmt"
	"go/ast"
	"io"
	"sort"

	"github.com/cockroachdb/crlfmt/internal/parser"
)

// An ImportGroup is a collection of related imports. It implements
// sort.Interface to sort imports by path.
type ImportGroup []parser.ImportSpec

func (ig ImportGroup) Len() int { return len(ig) }
func (ig ImportGroup) Less(i, j int) bool { return ig[i].Path() < ig[j].Path() }
func (ig ImportGroup) Swap(i, j int) { ig[i], ig[j] = ig[j], ig[i] }

// An ImportBlock is a collectino of ImportGroups.
type ImportBlock []ImportGroup

// Size returns the number of specs within the import block.
func (b ImportBlock) Size() int {
	var n int
	for _, g := range b {
		n += len(g)
	}
	return n
}

func renderImportGroup(w io.Writer, f *parser.File, group ImportGroup) {
	group = append(ImportGroup(nil), group...)
	sort.Sort(group)
	for i, imp := range group {
		if i > 0 {
			fmt.Fprintln(w)
		}
		if imp.Doc != nil {
			fmt.Fprintf(w, "\t%s\n", f.Slice(imp.Doc.Pos(), imp.Doc.End()))
		}
		fmt.Fprint(w, "\t")
		if imp.Name != nil {
			fmt.Fprintf(w, "%s ", imp.Name)
		}
		fmt.Fprint(w, imp.ImportSpec.Path.Value)
		if imp.Comment != nil {
			fmt.Fprintf(w, " %s", f.Slice(imp.Comment.Pos(), imp.Comment.End()))
		}
	}
}

// Imports renders imports into w. Imports are separated into groups; groups
// are output in the order that they appear and separated by a blank line.
func Imports(w io.Writer, f *parser.File, block ImportBlock) {
	var n int
	for _, group := range block {
		n += len(group)
	}
	if n == 1 {
		// When there is only one import, its doc comment is associated with the
		// import decl, not the import spec.
		for _, group := range block {
			if len(group) > 0 {
				imp := group[0]
				if imp.Doc != nil {
					fmt.Fprintf(w, "%s\n", f.Slice(imp.Doc.Pos(), imp.Doc.End()))
				}
				fmt.Fprint(w, "import ")
				imp.Doc = nil
				renderImportGroup(w, f, []parser.ImportSpec{imp})
				return
			}
		}
	}
	fmt.Fprint(w, "import")
	if n > 1 {
		fmt.Fprint(w, "(\n")
	}
	var needsBlankLine bool
	for _, group := range block {
		if len(group) > 0 {
			if needsBlankLine {
				fmt.Fprintf(w, "\n\n")
			}
			renderImportGroup(w, f, group)
			needsBlankLine = true
		}
	}
	if n > 1 {
		fmt.Fprint(w, "\n)")
	}
}

func renderLineFuncField(w io.Writer, f *parser.File, param *ast.Field) {
	if param.Doc != nil {
		fmt.Fprintf(w, "\t%s\n", f.Slice(param.Doc.Pos(), param.Doc.End()))
	}
	fmt.Fprint(w, "\t")
	for i, n := range param.Names {
		if i > 0 {
			fmt.Fprint(w, ", ")
		}
		fmt.Fprint(w, n.Name)
	}
	if len(param.Names) > 0 {
		fmt.Fprint(w, " ")
	}
	fmt.Fprintf(w, "%s,", f.Slice(param.Type.Pos(), param.Type.End()))
	if param.Comment != nil {
		fmt.Fprintf(w, " %s", f.Slice(param.Comment.Pos(), param.Comment.End()))
	}
	fmt.Fprintln(w)
}

// Func renders the function fn into w. The function is wrapped so that no line
// exceeds past the wrap column wrapCol when tabs are rendered with specified
// tab size.
func Func(w io.Writer, f *parser.File, fn *parser.FuncDecl, tabSize, wrapCol int) {
	params := fn.Type.Params
	results := fn.Type.Results

	opening := params.Pos() + 1
	closing := fn.BodyEnd()

	var paramsBuf bytes.Buffer
	if params != nil {
		paramsPrefix := ""
		for _, p := range params.List {
			paramsBuf.WriteString(paramsPrefix)
			paramsBuf.Write(f.Slice(p.Pos(), p.End()))
			paramsPrefix = ", "
		}
	}
	paramsJoined := paramsBuf.Bytes()

	// Final comma needed if params are written out onto their own single line.
	const paramsLineEndComma = `,`

	var resultsBuf bytes.Buffer
	var exactlyOneResult bool
	if results != nil {
		resultsPrefix := ""
		for _, r := range results.List {
			resultsBuf.WriteString(resultsPrefix)
			resultsBuf.Write(f.Slice(r.Pos(), r.End()))
			resultsPrefix = ", "
		}
		exactlyOneResult = len(results.List) == 1 && len(results.List[0].Names) == 0
	}
	resultsJoined := resultsBuf.Bytes()

	funcMid := `) (`
	funcEnd := `)`
	if results == nil || len(results.List) == 0 {
		funcMid = `)`
		funcEnd = ``
	} else if exactlyOneResult {
		funcMid = `) `
		funcEnd = ``
	}

	brace := 0
	if fn.Body != nil {
		brace = len(` {`)
	}

	var paramsHaveComments, resultsHaveComments bool
	for _, p := range params.List {
		if p.Doc != nil || p.Comment != nil {
			paramsHaveComments = true
			break
		}
	}
	if results != nil {
		for _, r := range results.List {
			if r.Doc != nil || r.Comment != nil {
				resultsHaveComments = true
				break
			}
		}
	}

	w.Write(f.Slice(fn.Pos(), opening))
	// colOffset - 1 accounts for `func (r *foo) bar(`
	colOffset := f.Position(opening).Column - 1
	singleLineLen := colOffset + len(paramsJoined) + len(funcMid) + len(resultsJoined) + len(funcEnd) + brace
	if singleLineLen <= wrapCol && !paramsHaveComments && !resultsHaveComments {
		w.Write(paramsJoined)
		fmt.Fprint(w, funcMid)
		w.Write(resultsJoined)
		fmt.Fprint(w, funcEnd)
	} else {
		// we're into wrapping, so the return types block usually starts on own
		// line intended by `tab`.
		resTypeStartingCol := tabSize
		if len(params.List) == 0 {
			// special case: if we have no params, the res type starts on the same
			// line rather than on its own.
			resTypeStartingCol = colOffset
		} else if tabSize+len(paramsJoined)+len(paramsLineEndComma) <= wrapCol && !paramsHaveComments {
			fmt.Fprintf(w, "\n\t%s,\n", paramsJoined)
		} else {
			fmt.Fprintln(w)
			for _, param := range params.List {
				renderLineFuncField(w, f, param)
			}
		}
		fmt.Fprint(w, funcMid)
		singleLineResultsLen := resTypeStartingCol + len(funcMid) + len(resultsJoined) + len(funcEnd) + brace
		if (singleLineResultsLen <= wrapCol || exactlyOneResult) && !resultsHaveComments {
			w.Write(resultsJoined)
			fmt.Fprint(w, funcEnd)
		} else {
			fmt.Fprintln(w)
			for _, result := range results.List {
				renderLineFuncField(w, f, result)
			}
			fmt.Fprint(w, funcEnd)
		}
	}
	w.Write(f.Slice(fn.Type.End(), closing))
}
