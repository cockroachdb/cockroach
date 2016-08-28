// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copied from Go's text/template/parse package and modified for yacc.

// Parse nodes.

package yacc

// Pos represents a byte position in the original input text from which
// this template was parsed.
type Pos int

// Nodes.

// ProductionNode holds is a named production of multiple expressions.
type ProductionNode struct {
	Pos
	Name        string
	Expressions []*ExpressionNode
}

func newProduction(pos Pos, name string) *ProductionNode {
	return &ProductionNode{Pos: pos, Name: name}
}

// ExpressionNode hold a single expression.
type ExpressionNode struct {
	Pos
	Items   []Item
	Command string
}

func newExpression(pos Pos) *ExpressionNode {
	return &ExpressionNode{Pos: pos}
}

// Item hold an item.
type Item struct {
	Value string
	Typ   ItemTyp
}

// ItemTyp is the item type.
type ItemTyp int

const (
	// TypToken is the token type.
	TypToken ItemTyp = iota
	// TypLiteral is the literal type.
	TypLiteral
)
