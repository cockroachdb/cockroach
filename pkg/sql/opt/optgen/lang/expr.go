// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lang

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
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

	// InferredType describes the kind of data that will be returned when this
	// expression is evaluated. Type inference rules work top-down and bottom-
	// up to establish the type. For example:
	//
	//   define Select {
	//     Input  Node
	//     Filter Node
	//   }
	//
	//   define True {}
	//
	//   (Select $input:* $filter:(True)) => $input
	//
	// The type of the $input binding and ref would be inferred as Node, since
	// that's as specific as can be inferred. The type of $filter would be
	// inferred as TrueOp, since a more specific type than Node is possible to
	// infer in this case.
	//
	// The compiler uses this information to ensure that every match pattern has
	// a statically known set of ops it can match, and that every replace pattern
	// has a statically known set of ops it can construct. Code generators can
	// also use this information to generate strongly-typed code.
	InferredType() DataType

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

// Sort sorts the rules in the set, given a less function, while keeping the
// original order of equal elements. A rule that is "less than" another rule is
// stored stored earlier in the set.
//
// Note that the rule set is updated in place to reflect the sorted order.
func (e RuleSetExpr) Sort(less func(left, right *RuleExpr) bool) {
	sort.SliceStable(e, func(i, j int) bool {
		return less(e[i], e[j])
	})
}

// HasDynamicName returns true if this is a construction function which can
// construct several different operators; which it constructs is not known until
// runtime. For example:
//
//   (Select $input:(Left | InnerJoin $left:* $right:* $on))
//   =>
//   ((OpName $input) $left $right $on)
//
// The replace pattern uses a constructor function that dynamically constructs
// either a Left or InnerJoin operator.
func (e *FuncExpr) HasDynamicName() bool {
	switch e.Name.(type) {
	case *NameExpr, *NamesExpr:
		return false
	}
	return true
}

// SingleName returns the name of the function when there is exactly one choice.
// If there is zero or more than one choice, SingleName will panic.
func (e *FuncExpr) SingleName() string {
	if names, ok := e.Name.(*NamesExpr); ok {
		if len(*names) > 1 {
			panic("function cannot have more than one name")
		}
		return string((*names)[0])
	}
	return (string)(*e.Name.(*NameExpr))
}

// NameChoice returns the set of names that this function can have. Multiple
// choices are possible when this is a match function.
func (e *FuncExpr) NameChoice() NamesExpr {
	if names, ok := e.Name.(*NamesExpr); ok {
		return *names
	}
	return NamesExpr{*e.Name.(*NameExpr)}
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
		switch e.Op() {
		case StringOp:
			buf.WriteByte('"')
			buf.WriteString(e.Value().(string))
			buf.WriteByte('"')

		case NumberOp:
			fmt.Fprintf(buf, "%d", e.Value().(int64))

		default:
			fmt.Fprintf(buf, "%v", e.Value())
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

		typ := e.InferredType()
		if typ != nil && typ != AnyDataType {
			buf.WriteString(fmt.Sprintf(" Typ=%s", e.InferredType()))
		}

		if src != nil && e.ChildCount() != 0 {
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

		typ := e.InferredType()
		if typ != nil && typ != AnyDataType {
			writeIndent(buf, level)
			buf.WriteString(fmt.Sprintf("Typ=%s\n", e.InferredType()))
		}

		if src != nil && e.ChildCount() != 0 {
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
