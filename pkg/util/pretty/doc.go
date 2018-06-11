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
package pretty

import (
	"fmt"
)

// Doc represents a document as described by the referenced paper.
type Doc interface {
	isDoc()
	// All Docs can uniquely convert themselves into a string so they can be
	// memoized during better calculation.
	String() string
}

func (concat) isDoc() {}
func (union) isDoc()  {}
func (nest) isDoc()   {}
func (text) isDoc()   {}
func (nilDoc) isDoc() {}
func (line) isDoc()   {}
func (textX) isDoc()  {}
func (lineX) isDoc()  {}

func (d text) String() string   { return fmt.Sprintf("(%q)", string(d)) }
func (line) String() string     { return "LINE" }
func (nilDoc) String() string   { return "NIL" }
func (d concat) String() string { return fmt.Sprintf("(%s :<> %s)", d.a, d.b) }
func (d nest) String() string   { return fmt.Sprintf("(NEST %d %s)", d.i, d.d) }
func (d union) String() string  { return fmt.Sprintf("(%s :<|> %s)", d.x, d.y) }
func (d textX) String() string  { return fmt.Sprintf("(%q TEXTX %s)", d.s, d.d) }
func (d lineX) String() string  { return fmt.Sprintf("(%q LINEX %s)", d.s, d.d) }

type nilDoc struct{}

// Nil is the empty Doc.
var Nil nilDoc

type text string

// Text creates a string Doc.
func Text(s string) Doc {
	return text(s)
}

type line struct{}

// Line is either a newline or a space if flattened onto a single line.
var Line line

type concat struct {
	a, b Doc
}

// Concat concatenates two Docs.
func Concat(a, b Doc) Doc {
	if a == nil {
		a = Nil
	}
	if b == nil {
		b = Nil
	}
	if a == Nil {
		return b
	}
	if b == Nil {
		return a
	}
	return concat{a, b}
}

type group Doc

// Group will format d on one line if possible.
func Group(d Doc) Doc {
	return union{flatten(d), d}
}

func flatten(d Doc) Doc {
	if d == Nil {
		return Nil
	}
	if t, ok := d.(concat); ok {
		return Concat(flatten(t.a), flatten(t.b))
	}
	if t, ok := d.(nest); ok {
		return Nest(t.i, t.s, flatten(t.d))
	}
	if _, ok := d.(text); ok {
		return d
	}
	if d == Line {
		return Text(" ")
	}
	if t, ok := d.(union); ok {
		return flatten(t.x)
	}
	panic(fmt.Errorf("unknown type: %T", d))
}

type nest struct {
	i int
	s string
	d Doc
}

// Nest indents d by s at effective length n. len(s) does not have to be
// n. This allows s to be a tab character and n can be a tab width.
func Nest(n int, s string, d Doc) Doc {
	return nest{n, s, d}
}

// Doc types below are not directly accessible to users.

// union is the union of two sets of layouts. x and y must flatten to the
// same layout. Additionally, no first line of a document in x is shorter
// than some first line of a document in y; or, equivalently, every first
// line in x is at least as long as every first line in y.
type union struct {
	x, y Doc
}

type textX struct {
	s string
	d Doc
}

type lineX struct {
	s string
	d Doc
}
