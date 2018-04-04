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

package lang

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
)

// The Optgen language compiler uses itself to generate its AST expressions.
// This is a form of compiler bootstrapping:
//   https://en.wikipedia.org/wiki/Bootstrapping_(compilers)
//
// In order to regenerate expr.og.go or operator.og.go, perform these steps
// from the optgen directory (and make sure that $GOROOT/bin is in your path):
//
//   cd cmd/langgen
//   go build && go install
//   cd ../../lang
//   go generate

//go:generate langgen -out expr.og.go exprs lang.opt
//go:generate langgen -out operator.og.go ops lang.opt
//go:generate stringer -type=Operator operator.og.go

// VisitFunc is called by the Visit method of an expression for each child of
// that expression. The function can return the expression as-is, with no
// changes, or it can construct and return a replacement expression that the
// visitor will graft into a replacement tree.
type VisitFunc func(e Expr) Expr

// Expr is implemented by all Optgen AST expressions, exposing its properties,
// children, and string representation.
type Expr interface {
	// Op returns the operator type of the expression (RuleOp, StringOp, etc).
	Op() Operator

	// ChildCount returns the number of children of the expression.
	ChildCount() int

	// Child returns the nth child of this expression.
	Child(nth int) Expr

	// ChildName returns the name of the nth child of this expression. If the
	// child has no name, or if there is no such child, then ChildName returns
	// the empty string.
	ChildName(nth int) string

	// Value returns the value stored by "leaf" expressions that are
	// represented as a primitive type like string or int. Other expression
	// types just return nil.
	Value() interface{}

	// Visit invokes the visit function for each child of the expression. The
	// function can return the child as-is, with no changes, or it can construct
	// and return a replacement expression. If any children have been replaced,
	// then Visit will construct a new instance of this expression that has the
	// new children. Callers can use the Visit function to traverse and rewrite
	// the expression tree, in either pre or post order. Here is a pre-order
	// example:
	//
	//   func myVisitFunc(e Expr) Expr {
	//     // Replace SomeOp, leave everything else as-is. This check is before
	//     // the call to Visit, so it's a pre-order traversal.
	//     if e.Op() == SomeOp {
	//       return &SomeOtherOp{}
	//     }
	//     return e.Visit(myVisitFunc)
	//   }
	//   newExpr := oldExpr.Visit(myVisitFunc)
	Visit(visit VisitFunc) Expr

	// Source returns the original source location of the expression, including
	// file name, line number, and column position. If the source location is
	// not available, then Source returns nil.
	Source() *SourceLoc

	// String returns a human-readable string representation of the expression
	// tree.
	String() string

	// Format writes the expression's string representation into the given
	// buffer, at the specified level of indentation.
	Format(buf *bytes.Buffer, level int)
}

// SourceLoc provides the original source location of an expression, including
// file name and line number and column position within that file. The file is
// the full path of the file, and the line and column locations are 0-based.
type SourceLoc struct {
	File string
	Line int
	Pos  int
}

// String returns the source location in "file:line:pos" format.
func (l SourceLoc) String() string {
	// Use short file name for string representation.
	file := filepath.Base(l.File)

	// Convert to 1-based line and position.
	return fmt.Sprintf("%s:%d:%d", file, l.Line+1, l.Pos+1)
}

// Contains returns true if the given tag is one of the tags in the collection.
func (e TagsExpr) Contains(tag string) bool {
	for _, existing := range e {
		if string(existing) == tag {
			return true
		}
	}
	return false
}

// WithTag returns the subset of defines in the set that have the given tag.
func (e DefineSetExpr) WithTag(tag string) DefineSetExpr {
	var defines DefineSetExpr
	for _, define := range e {
		if define.Tags.Contains(tag) {
			defines = append(defines, define)
		}
	}
	return defines
}

// WithoutTag returns the subset of defines in the set that do not have the
// given tag.
func (e DefineSetExpr) WithoutTag(tag string) DefineSetExpr {
	var defines DefineSetExpr
	for _, define := range e {
		if !define.Tags.Contains(tag) {
			defines = append(defines, define)
		}
	}
	return defines
}

// WithTag returns the subset of rules in the set that have the given tag.
func (e RuleSetExpr) WithTag(tag string) RuleSetExpr {
	var rules RuleSetExpr
	for _, rule := range e {
		if rule.Tags.Contains(tag) {
			rules = append(rules, rule)
		}
	}
	return rules
}

// visitChildren is a helper function called by the Visit function on AST
// expressions. It invokes the visit function on each child of the specified
// expression and returns the resulting children as a slice. If none of the
// children were replaced, then visitChildren returns nil.
func visitChildren(e Expr, visit VisitFunc) []Expr {
	var children []Expr

	for i := 0; i < e.ChildCount(); i++ {
		before := e.Child(i)
		after := visit(before)
		if children == nil && before != after {
			children = make([]Expr, e.ChildCount())
			for j := 0; j < i; j++ {
				children[j] = e.Child(j)
			}
		}

		if children != nil {
			children[i] = after
		}
	}

	return children
}

// formatExpr is a helper function called by the Format function on AST
// expressions. It recursively writes the string representation of the
// expression to the given buffer, at the specified level of indentation.
func formatExpr(e Expr, buf *bytes.Buffer, level int) {
	if e.Value() != nil {
		if e.Op() == StringOp {
			buf.WriteByte('"')
			buf.WriteString(e.Value().(string))
			buf.WriteByte('"')
		} else {
			buf.WriteString(fmt.Sprintf("%v", e.Value()))
		}
		return
	}

	opName := strings.Title(e.Op().String())
	opName = opName[:len(opName)-2]
	src := e.Source()

	nested := false
	for i := 0; i < e.ChildCount(); i++ {
		child := e.Child(i)
		if child.Value() == nil && child.ChildCount() != 0 {
			nested = true
			break
		}
	}

	if !nested {
		buf.WriteByte('(')
		buf.WriteString(opName)

		for i := 0; i < e.ChildCount(); i++ {
			buf.WriteByte(' ')

			name := e.ChildName(i)
			if name != "" {
				buf.WriteString(name)
				buf.WriteByte('=')
			}

			e.Child(i).Format(buf, level)
		}

		if src != nil {
			buf.WriteString(fmt.Sprintf(" Src=<%s>", src))
		}

		buf.WriteByte(')')
	} else {
		buf.WriteByte('(')
		buf.WriteString(opName)
		buf.WriteByte('\n')
		level++

		for i := 0; i < e.ChildCount(); i++ {
			writeIndent(buf, level)

			name := e.ChildName(i)
			if name != "" {
				buf.WriteString(name)
				buf.WriteByte('=')
			}

			e.Child(i).Format(buf, level)
			buf.WriteByte('\n')
		}

		if src != nil {
			writeIndent(buf, level)
			buf.WriteString(fmt.Sprintf("Src=<%s>\n", src))
		}

		level--
		writeIndent(buf, level)
		buf.WriteByte(')')
	}
}

func writeIndent(buf *bytes.Buffer, level int) {
	buf.WriteString(strings.Repeat("\t", level))
}
