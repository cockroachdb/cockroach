package ast

import "fmt"

// FileDeclNode is a placeholder interface for AST nodes that represent files.
// This allows NoSourceNode to be used in place of *FileNode for some usages.
type FileDeclNode interface {
	Node
	GetSyntax() Node
}

var _ FileDeclNode = (*FileNode)(nil)
var _ FileDeclNode = NoSourceNode{}

// FileNode is the root of the AST hierarchy. It represents an entire
// protobuf source file.
type FileNode struct {
	compositeNode
	Syntax *SyntaxNode // nil if file has no syntax declaration
	Decls  []FileElement

	// Any comments that follow the last token in the file.
	FinalComments []Comment
	// Any whitespace at the end of the file (after the last token or
	// last comment in the file).
	FinalWhitespace string
}

// NewFileElement creates a new *FileNode. The syntax parameter is optional. If it
// is absent, it means the file had no syntax declaration.
//
// This function panics if the concrete type of any element of decls is not
// from this package.
func NewFileNode(syntax *SyntaxNode, decls []FileElement) *FileNode {
	numChildren := len(decls)
	if syntax != nil {
		numChildren++
	}
	children := make([]Node, 0, numChildren)
	if syntax != nil {
		children = append(children, syntax)
	}
	for _, decl := range decls {
		children = append(children, decl)
	}

	for _, decl := range decls {
		switch decl := decl.(type) {
		case *PackageNode, *ImportNode, *OptionNode, *MessageNode,
			*EnumNode, *ExtendNode, *ServiceNode, *EmptyDeclNode:
		default:
			panic(fmt.Sprintf("invalid FileElement type: %T", decl))
		}
	}

	return &FileNode{
		compositeNode: compositeNode{
			children: children,
		},
		Syntax: syntax,
		Decls:  decls,
	}
}

func NewEmptyFileNode(filename string) *FileNode {
	return &FileNode{
		compositeNode: compositeNode{
			children: []Node{NewNoSourceNode(filename)},
		},
	}
}

func (f *FileNode) GetSyntax() Node {
	return f.Syntax
}

// FileElement is an interface implemented by all AST nodes that are
// allowed as top-level declarations in the file.
type FileElement interface {
	Node
	fileElement()
}

var _ FileElement = (*ImportNode)(nil)
var _ FileElement = (*PackageNode)(nil)
var _ FileElement = (*OptionNode)(nil)
var _ FileElement = (*MessageNode)(nil)
var _ FileElement = (*EnumNode)(nil)
var _ FileElement = (*ExtendNode)(nil)
var _ FileElement = (*ServiceNode)(nil)
var _ FileElement = (*EmptyDeclNode)(nil)

// SyntaxNode represents a syntax declaration, which if present must be
// the first non-comment content. Example:
//
//  syntax = "proto2";
//
// Files that don't have a syntax node are assumed to use proto2 syntax.
type SyntaxNode struct {
	compositeNode
	Keyword   *KeywordNode
	Equals    *RuneNode
	Syntax    StringValueNode
	Semicolon *RuneNode
}

// NewSyntaxNode creates a new *SyntaxNode. All four arguments must be non-nil:
//  - keyword: The token corresponding to the "syntax" keyword.
//  - equals: The token corresponding to the "=" rune.
//  - syntax: The actual syntax value, e.g. "proto2" or "proto3".
//  - semicolon: The token corresponding to the ";" rune that ends the declaration.
func NewSyntaxNode(keyword *KeywordNode, equals *RuneNode, syntax StringValueNode, semicolon *RuneNode) *SyntaxNode {
	if keyword == nil {
		panic("keyword is nil")
	}
	if equals == nil {
		panic("equals is nil")
	}
	if syntax == nil {
		panic("syntax is nil")
	}
	if semicolon == nil {
		panic("semicolon is nil")
	}
	children := []Node{keyword, equals, syntax, semicolon}
	return &SyntaxNode{
		compositeNode: compositeNode{
			children: children,
		},
		Keyword:   keyword,
		Equals:    equals,
		Syntax:    syntax,
		Semicolon: semicolon,
	}
}

// ImportNode represents an import statement. Example:
//
//  import "google/protobuf/empty.proto";
type ImportNode struct {
	compositeNode
	Keyword *KeywordNode
	// Optional; if present indicates this is a public import
	Public *KeywordNode
	// Optional; if present indicates this is a weak import
	Weak      *KeywordNode
	Name      StringValueNode
	Semicolon *RuneNode
}

// NewImportNode creates a new *ImportNode. The public and weak arguments are optional
// and only one or the other (or neither) may be specified, not both. When public is
// non-nil, it indicates the "public" keyword in the import statement and means this is
// a public import. When weak is non-nil, it indicates the "weak" keyword in the import
// statement and means this is a weak import. When both are nil, this is a normal import.
// The other arguments must be non-nil:
//  - keyword: The token corresponding to the "import" keyword.
//  - public: The token corresponding to the optional "public" keyword.
//  - weak: The token corresponding to the optional "weak" keyword.
//  - name: The actual imported file name.
//  - semicolon: The token corresponding to the ";" rune that ends the declaration.
func NewImportNode(keyword *KeywordNode, public *KeywordNode, weak *KeywordNode, name StringValueNode, semicolon *RuneNode) *ImportNode {
	if keyword == nil {
		panic("keyword is nil")
	}
	if name == nil {
		panic("name is nil")
	}
	if semicolon == nil {
		panic("semicolon is nil")
	}
	numChildren := 3
	if public != nil || weak != nil {
		numChildren++
	}
	children := make([]Node, 0, numChildren)
	children = append(children, keyword)
	if public != nil {
		children = append(children, public)
	} else if weak != nil {
		children = append(children, weak)
	}
	children = append(children, name, semicolon)

	return &ImportNode{
		compositeNode: compositeNode{
			children: children,
		},
		Keyword:   keyword,
		Public:    public,
		Weak:      weak,
		Name:      name,
		Semicolon: semicolon,
	}
}

func (*ImportNode) fileElement() {}

// PackageNode represents a package declaration. Example:
//
//  package foobar.com;
type PackageNode struct {
	compositeNode
	Keyword   *KeywordNode
	Name      IdentValueNode
	Semicolon *RuneNode
}

func (*PackageNode) fileElement() {}

// NewPackageNode creates a new *PackageNode. All three arguments must be non-nil:
//  - keyword: The token corresponding to the "package" keyword.
//  - name: The package name declared for the file.
//  - semicolon: The token corresponding to the ";" rune that ends the declaration.
func NewPackageNode(keyword *KeywordNode, name IdentValueNode, semicolon *RuneNode) *PackageNode {
	if keyword == nil {
		panic("keyword is nil")
	}
	if name == nil {
		panic("name is nil")
	}
	if semicolon == nil {
		panic("semicolon is nil")
	}
	children := []Node{keyword, name, semicolon}
	return &PackageNode{
		compositeNode: compositeNode{
			children: children,
		},
		Keyword:   keyword,
		Name:      name,
		Semicolon: semicolon,
	}
}
