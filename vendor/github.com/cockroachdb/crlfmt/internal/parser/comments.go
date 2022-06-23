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
//
// Portions of this file are additionally subject to the license below.
//
// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE-GO file.

package parser

import (
	"go/ast"
	"go/token"
)

// A commentListReader helps iterating through a list of comment groups.
type commentListReader struct {
	fset     *token.FileSet
	comments []*ast.CommentGroup
	index    int
}

func (r *commentListReader) comment() *ast.CommentGroup {
	return r.comments[r.index]
}

func (r *commentListReader) pos() token.Position {
	return r.fset.Position(r.comment().Pos())
}

func (r *commentListReader) end() token.Position {
	return r.fset.Position(r.comment().End())
}

func (r *commentListReader) valid() bool {
	return r.index < len(r.comments)
}

func (r *commentListReader) next() {
	r.index++
}

// assocFieldListComments associates comments interspersed with a field list to
// the AST nodes in that field list. go/parser performs this mapping
// automatically for struct fields, but not for function parameters or results.
//
// For a given commentListReader, the fieldLists presented must be presented in
// source code order across multiple calls. After the call, the
// commentListReader may be advanced, but not beyond the first comment following
// fieldList.
func assocFieldListComments(cr *commentListReader, fieldList *ast.FieldList) {
	openPos := cr.fset.Position(fieldList.Opening)
	closePos := cr.fset.Position(fieldList.Closing)
	for i, f := range fieldList.List {
		fp := cr.fset.Position(f.Pos())
		np := closePos
		if i < len(fieldList.List)-1 {
			np = cr.fset.Position(fieldList.List[i+1].Pos())
		}
		for cr.valid() && cr.pos().Line <= fp.Line && cr.end().Offset <= np.Offset {
			// Consider any comment that starts on a line earlier than this
			// field and ends before the next field (or the closing parenthesis,
			// in the case of the last field.)
			if cr.end().Offset < openPos.Offset {
				// The comment starts too early.
			} else if cr.end().Line == fp.Line-1 || (cr.pos().Line == fp.Line && cr.end().Offset < fp.Offset) {
				// The comment immediately precedes the import. Doc comment.
				f.Doc = cr.comment()
			} else if cr.pos().Line == fp.Line {
				// The comment immediately follows the import. Line comment.
				f.Comment = cr.comment()
			}
			cr.next()
		}
	}
}
