package ast

import "fmt"

// EnumNode represents an enum declaration. Example:
//
//  enum Foo { BAR = 0; BAZ = 1 }
type EnumNode struct {
	compositeNode
	Keyword    *KeywordNode
	Name       *IdentNode
	OpenBrace  *RuneNode
	Decls      []EnumElement
	CloseBrace *RuneNode
}

func (*EnumNode) fileElement() {}
func (*EnumNode) msgElement()  {}

// NewEnumNode creates a new *EnumNode. All arguments must be non-nil. While
// it is technically allowed for decls to be nil or empty, the resulting node
// will not be a valid enum, which must have at least one value.
//  - keyword: The token corresponding to the "enum" keyword.
//  - name: The token corresponding to the enum's name.
//  - openBrace: The token corresponding to the "{" rune that starts the body.
//  - decls: All declarations inside the enum body.
//  - closeBrace: The token corresponding to the "}" rune that ends the body.
func NewEnumNode(keyword *KeywordNode, name *IdentNode, openBrace *RuneNode, decls []EnumElement, closeBrace *RuneNode) *EnumNode {
	if keyword == nil {
		panic("keyword is nil")
	}
	if name == nil {
		panic("name is nil")
	}
	if openBrace == nil {
		panic("openBrace is nil")
	}
	if closeBrace == nil {
		panic("closeBrace is nil")
	}
	children := make([]Node, 0, 4+len(decls))
	children = append(children, keyword, name, openBrace)
	for _, decl := range decls {
		children = append(children, decl)
	}
	children = append(children, closeBrace)

	for _, decl := range decls {
		switch decl.(type) {
		case *OptionNode, *EnumValueNode, *ReservedNode, *EmptyDeclNode:
		default:
			panic(fmt.Sprintf("invalid EnumElement type: %T", decl))
		}
	}

	return &EnumNode{
		compositeNode: compositeNode{
			children: children,
		},
		Keyword:    keyword,
		Name:       name,
		OpenBrace:  openBrace,
		CloseBrace: closeBrace,
		Decls:      decls,
	}
}

// EnumElement is an interface implemented by all AST nodes that can
// appear in the body of an enum declaration.
type EnumElement interface {
	Node
	enumElement()
}

var _ EnumElement = (*OptionNode)(nil)
var _ EnumElement = (*EnumValueNode)(nil)
var _ EnumElement = (*ReservedNode)(nil)
var _ EnumElement = (*EmptyDeclNode)(nil)

// EnumValueDeclNode is a placeholder interface for AST nodes that represent
// enum values. This allows NoSourceNode to be used in place of *EnumValueNode
// for some usages.
type EnumValueDeclNode interface {
	Node
	GetName() Node
	GetNumber() Node
}

var _ EnumValueDeclNode = (*EnumValueNode)(nil)
var _ EnumValueDeclNode = NoSourceNode{}

// EnumNode represents an enum declaration. Example:
//
//  UNSET = 0 [deprecated = true];
type EnumValueNode struct {
	compositeNode
	Name      *IdentNode
	Equals    *RuneNode
	Number    IntValueNode
	Options   *CompactOptionsNode
	Semicolon *RuneNode
}

func (*EnumValueNode) enumElement() {}

// NewEnumValueNode creates a new *EnumValueNode. All arguments must be non-nil
// except opts which is only non-nil if the declaration included options.
//  - name: The token corresponding to the enum value's name.
//  - equals: The token corresponding to the '=' rune after the name.
//  - number: The token corresponding to the enum value's number.
//  - opts: Optional set of enum value options.
//  - semicolon: The token corresponding to the ";" rune that ends the declaration.
func NewEnumValueNode(name *IdentNode, equals *RuneNode, number IntValueNode, opts *CompactOptionsNode, semicolon *RuneNode) *EnumValueNode {
	if name == nil {
		panic("name is nil")
	}
	if equals == nil {
		panic("equals is nil")
	}
	if number == nil {
		panic("number is nil")
	}
	if semicolon == nil {
		panic("semicolon is nil")
	}
	numChildren := 4
	if opts != nil {
		numChildren++
	}
	children := make([]Node, 0, numChildren)
	children = append(children, name, equals, number)
	if opts != nil {
		children = append(children, opts)
	}
	children = append(children, semicolon)
	return &EnumValueNode{
		compositeNode: compositeNode{
			children: children,
		},
		Name:      name,
		Equals:    equals,
		Number:    number,
		Options:   opts,
		Semicolon: semicolon,
	}
}

func (e *EnumValueNode) GetName() Node {
	return e.Name
}

func (e *EnumValueNode) GetNumber() Node {
	return e.Number
}
