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

// Package pretty prints documents based on a target line width.
//
// See: https://homepages.inf.ed.ac.uk/wadler/papers/prettier/prettier.pdf
//
// The paper describes a simple algorithm for printing a document tree with a
// single layout defined by text, newlines, and indentation. This concept is
// then expanded for documents that have more than one possible layout using
// a union type of two documents where both documents reduce to the same
// document, but one document has been flattened to a single line. A method
// ("best") is described that chooses the best of these two layouts (i.e.,
// it removes all union types from a document tree). It works by tracking
// the desired and current line length and choosing either the flattened
// side of a union if it fits on the current line, or else the non-flattened
// side. The paper then describes various performance improvements that reduce
// the search space of the best function such that it can complete in O(n)
// instead of O(n^2) time, where n is the number of nodes.
//
// For example code with SQL to experiment further, refer to
// https://github.com/knz/prettier/
//
package pretty

import (
	"fmt"
)

// Doc represents a document as described by the type "DOC" in the
// referenced paper. This is the abstract representation constructed
// by the pretty-printing code.
type Doc interface {
	isDoc()
}

func (text) isDoc()     {}
func (line) isDoc()     {}
func (nilDoc) isDoc()   {}
func (concat) isDoc()   {}
func (nest) isDoc()     {}
func (union) isDoc()    {}
func (*column) isDoc()  {}
func (*nesting) isDoc() {}

//
// Implementations of Doc ("DOC" in paper).
//

// nilDoc represents NIL :: DOC -- the empty doc.
type nilDoc struct{}

// Nil is the NIL constructor.
var Nil nilDoc

// text represents (TEXT s) :: DOC -- a simple text string.
type text string

// Text is the TEXT constructor.
func Text(s string) Doc {
	return text(s)
}

// line represents LINE :: DOC -- a "soft line" that can be flattened to a space.
type line struct{}

// Line is the LINE constructor.
var Line line

// concat represents (DOC <> DOC) :: DOC -- the concatenation of two docs.
type concat struct {
	a, b Doc
}

// Concat is the <> constructor.
// This uses simplifyNil to avoid actually inserting NIL docs
// in the abstract tree.
func Concat(a, b Doc) Doc {
	return simplifyNil(a, b, func(a, b Doc) Doc { return concat{a, b} })
}

// nest represents (NEST Int DOC) :: DOC -- nesting a doc "under" another.
// NEST indents d at effective length n.
type nest struct {
	n int
	d Doc
}

// Nest is the NEST constructor.
func Nest(n int, d Doc) Doc {
	return nest{n, d}
}

// union represents (DOC <|> DOC) :: DOC -- the union of two docs.
// <|> is really the union of two sets of layouts. x and y must flatten to the
// same layout. Additionally, no first line of a document in x is shorter
// than some first line of a document in y; or, equivalently, every first
// line in x is at least as long as every first line in y.
//
// The main use of the union is via the Group operator defined below.
//
// We do not provide a public constructor as this type is not
// exported.
type union struct {
	x, y Doc
}

// Group will format d on one line if possible.
func Group(d Doc) Doc {
	return union{flatten(d), d}
}

var textSpace = Text(" ")

func flatten(d Doc) Doc {
	switch t := d.(type) {
	case nilDoc:
		return Nil
	case concat:
		return Concat(flatten(t.a), flatten(t.b))
	case nest:
		return Nest(t.n, flatten(t.d))
	case text:
		return d
	case line:
		return textSpace
	case union:
		return flatten(t.x)
	case *column:
		return &column{f: func(c int) Doc { return flatten(t.f(c)) }}
	case *nesting:
		return &nesting{f: func(i int) Doc { return flatten(t.f(i)) }}
	default:
		panic(fmt.Errorf("unknown type: %T", d))
	}
}

// column is a special document which is replaced during rendering by
// another document depending on the current column on the rendering
// line.
//
// It is an extension to the Wadler printer commonly found in
// derivative code.  See e.g. use by Daniel Mendler in
// https://github.com/minad/wl-pprint-annotated/blob/master/src/Text/PrettyPrint/Annotated/WL.hs
//
// This type is not exposed, see the Align() operator below instead.
type column struct {
	f func(int) Doc
}

// nesting is a special document which is replaced during rendering by
// another document depending on the current nesting level.
//
// It is an extension to the Wadler printer commonly found in
// derivative code.  See e.g. use by Daniel Mendler in
// https://github.com/minad/wl-pprint-annotated/blob/master/src/Text/PrettyPrint/Annotated/WL.hs
//
// This type is not exposed, see the Align() operator below instead.
type nesting struct {
	f func(int) Doc
}

// Align renders document d with the nesting level set to the current
// column.
func Align(d Doc) Doc {
	return &column{
		f: func(k int) Doc {
			return &nesting{
				f: func(i int) Doc {
					return nest{k - i, d}
				},
			}
		},
	}
}
