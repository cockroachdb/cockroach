// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

import "fmt"

// Doc represents a document as described by the type "DOC" in the
// referenced paper. This is the abstract representation constructed
// by the pretty-printing code.
type Doc interface {
	isDoc()
}

func (text) isDoc()      {}
func (line) isDoc()      {}
func (softbreak) isDoc() {}
func (hardline) isDoc()  {}
func (nilDoc) isDoc()    {}
func (*concat) isDoc()   {}
func (nestt) isDoc()     {}
func (nests) isDoc()     {}
func (*union) isDoc()    {}
func (*scolumn) isDoc()  {}
func (*snesting) isDoc() {}
func (pad) isDoc()       {}
func (keyword) isDoc()   {}

//
// Implementations of Doc ("DOC" in paper).
//

// nilDoc represents NIL :: DOC -- the empty doc.
type nilDoc struct{}

// Nil is the NIL constructor.
var Nil Doc = nilDoc{}

// text represents (TEXT s) :: DOC -- a simple text string.
type text string

// Text is the TEXT constructor.
func Text(s string) Doc {
	return text(s)
}

// line represents LINE :: DOC -- a "soft line" that can be flattened to a space.
type line struct{}

// Line is a newline and is flattened to a space.
var Line Doc = line{}

// softbreak represents SOFTBREAK :: DOC -- an invisible space between
// words that tries to break the text across lines.
//
// For example, text "hello" <> softbreak <> text "world"
// flattens to "helloworld" (one word) but splits across lines as:
//     hello
//     world
//
// This is a common extension to Wadler's printer.
//
// Idea borrowed from Daniel Mendler's printer at
// https://github.com/minad/wl-pprint-annotated/blob/master/src/Text/PrettyPrint/Annotated/WL.hs
type softbreak struct{}

// SoftBreak is a newline and is flattened to an empty string.
var SoftBreak Doc = softbreak{}

type hardline struct{}

// HardLine is a newline and cannot be flattened.
var HardLine Doc = hardline{}

// concat represents (DOC <> DOC) :: DOC -- the concatenation of two docs.
type concat struct {
	a, b Doc
}

// Concat is the <> constructor.
// This uses simplifyNil to avoid actually inserting NIL docs
// in the abstract tree.
func Concat(a, b Doc) Doc {
	return simplifyNil(a, b, func(a, b Doc) Doc { return &concat{a, b} })
}

// nests represents (NESTS Int DOC) :: DOC -- nesting a doc "under" another.
// NESTS indents d with n spaces.
// This is more or less exactly the NEST operator in Wadler's printer.
type nests struct {
	n int16
	d Doc
}

// NestS is the NESTS constructor.
func NestS(n int16, d Doc) Doc {
	return nests{n, d}
}

// nestt represents (NESTT DOC) :: DOC -- nesting a doc "under" another
// NESTT indents d with a tab character.
// This is a variant of the NEST operator in Wadler's printer.
type nestt struct {
	d Doc
}

// NestT is the NESTT constructor.
func NestT(d Doc) Doc {
	return nestt{d}
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
	return &union{flatten(d), d}
}

var textSpace = Text(" ")

func flatten(d Doc) Doc {
	switch t := d.(type) {
	case nilDoc:
		return Nil
	case *concat:
		return Concat(flatten(t.a), flatten(t.b))
	case nestt:
		return NestT(flatten(t.d))
	case nests:
		return NestS(t.n, flatten(t.d))
	case text, keyword, hardline:
		return d
	case line:
		return textSpace
	case softbreak:
		return Nil
	case *union:
		return flatten(t.x)
	case *scolumn:
		return &scolumn{f: func(c int16) Doc { return flatten(t.f(c)) }}
	case *snesting:
		return &snesting{f: func(i int16) Doc { return flatten(t.f(i)) }}
	case pad:
		return Nil
	default:
		panic(fmt.Errorf("unknown type: %T", d))
	}
}

// scolumn is a special document which is replaced during rendering by
// another document depending on the current relative column on the
// rendering line (tab prefix excluded).
//
// It is an extension to the Wadler printer commonly found in
// derivative code. See e.g. use by Daniel Mendler in
// https://github.com/minad/wl-pprint-annotated/blob/master/src/Text/PrettyPrint/Annotated/WL.hs
//
// This type is not exposed, see the Align() operator below instead.
type scolumn struct {
	f func(int16) Doc
}

// snesting is a special document which is replaced during rendering
// by another document depending on the current space-based nesting
// level (the one added by NestS).
//
// It is an extension to the Wadler printer commonly found in
// derivative code.  See e.g. use by Daniel Mendler in
// https://github.com/minad/wl-pprint-annotated/blob/master/src/Text/PrettyPrint/Annotated/WL.hs
//
// This type is not exposed, see the Align() operator below instead.
type snesting struct {
	f func(int16) Doc
}

// Align renders document d with the space-based nesting level set to
// the current column.
func Align(d Doc) Doc {
	return &scolumn{
		f: func(k int16) Doc {
			return &snesting{
				f: func(i int16) Doc {
					return nests{k - i, d}
				},
			}
		},
	}
}

// pad is a special document which is replaced during rendering by
// the specified amount of whitespace. However it is flattened
// to an empty document during grouping.
//
// This is an extension to Wadler's printer first prototyped in
// https://github.com/knz/prettier.
//
// Note that this special document must be handled especially
// carefully with anything that produces a union (<|>) (e.g. Group),
// so as to preserve the invariant of unions: "no first line of a
// document in x is shorter than some first line of a document in y;
// or, equivalently, every first line in x is at least as long as
// every first line in y".
//
// The operator RLTable, defined in util.go, is properly careful about
// this.
//
// This document type is not exposed publicly because of the risk
// described above.
type pad struct {
	n int16
}

type keyword string

// Keyword is identical to Text except they are filtered by
// keywordTransform. The computed width is always len(s), regardless of
// the result of the result of the transform. This allows for things like
// coloring and other control characters in the output.
func Keyword(s string) Doc {
	return keyword(s)
}
