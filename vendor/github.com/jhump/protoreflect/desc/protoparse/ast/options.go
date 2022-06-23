package ast

import "fmt"

// OptionDeclNode is a placeholder interface for AST nodes that represent
// options. This allows NoSourceNode to be used in place of *OptionNode
// for some usages.
type OptionDeclNode interface {
	Node
	GetName() Node
	GetValue() ValueNode
}

var _ OptionDeclNode = (*OptionNode)(nil)
var _ OptionDeclNode = NoSourceNode{}

// OptionNode represents the declaration of a single option for an element.
// It is used both for normal option declarations (start with "option" keyword
// and end with semicolon) and for compact options found in fields, enum values,
// and extension ranges. Example:
//
//  option (custom.option) = "foo";
type OptionNode struct {
	compositeNode
	Keyword   *KeywordNode // absent for compact options
	Name      *OptionNameNode
	Equals    *RuneNode
	Val       ValueNode
	Semicolon *RuneNode // absent for compact options
}

func (e *OptionNode) fileElement()    {}
func (e *OptionNode) msgElement()     {}
func (e *OptionNode) oneOfElement()   {}
func (e *OptionNode) enumElement()    {}
func (e *OptionNode) serviceElement() {}
func (e *OptionNode) methodElement()  {}

// NewOptionNode creates a new *OptionNode for a full option declaration (as
// used in files, messages, oneofs, enums, services, and methods). All arguments
// must be non-nil. (Also see NewCompactOptionNode.)
//  - keyword: The token corresponding to the "option" keyword.
//  - name: The token corresponding to the name of the option.
//  - equals: The token corresponding to the "=" rune after the name.
//  - val: The token corresponding to the option value.
//  - semicolon: The token corresponding to the ";" rune that ends the declaration.
func NewOptionNode(keyword *KeywordNode, name *OptionNameNode, equals *RuneNode, val ValueNode, semicolon *RuneNode) *OptionNode {
	if keyword == nil {
		panic("keyword is nil")
	}
	if name == nil {
		panic("name is nil")
	}
	if equals == nil {
		panic("equals is nil")
	}
	if val == nil {
		panic("val is nil")
	}
	if semicolon == nil {
		panic("semicolon is nil")
	}
	children := []Node{keyword, name, equals, val, semicolon}
	return &OptionNode{
		compositeNode: compositeNode{
			children: children,
		},
		Keyword:   keyword,
		Name:      name,
		Equals:    equals,
		Val:       val,
		Semicolon: semicolon,
	}
}

// NewCompactOptionNode creates a new *OptionNode for a full compact declaration
// (as used in fields, enum values, and extension ranges). All arguments must be
// non-nil.
//  - name: The token corresponding to the name of the option.
//  - equals: The token corresponding to the "=" rune after the name.
//  - val: The token corresponding to the option value.
func NewCompactOptionNode(name *OptionNameNode, equals *RuneNode, val ValueNode) *OptionNode {
	if name == nil {
		panic("name is nil")
	}
	if equals == nil {
		panic("equals is nil")
	}
	if val == nil {
		panic("val is nil")
	}
	children := []Node{name, equals, val}
	return &OptionNode{
		compositeNode: compositeNode{
			children: children,
		},
		Name:   name,
		Equals: equals,
		Val:    val,
	}
}

func (n *OptionNode) GetName() Node {
	return n.Name
}

func (n *OptionNode) GetValue() ValueNode {
	return n.Val
}

// OptionNameNode represents an option name or even a traversal through message
// types to name a nested option field. Example:
//
//   (foo.bar).baz.(bob)
type OptionNameNode struct {
	compositeNode
	Parts []*FieldReferenceNode
	// Dots represent the separating '.' characters between name parts. The
	// length of this slice must be exactly len(Parts)-1, each item in Parts
	// having a corresponding item in this slice *except the last* (since a
	// trailing dot is not allowed).
	//
	// These do *not* include dots that are inside of an extension name. For
	// example: (foo.bar).baz.(bob) has three parts:
	//    1. (foo.bar)  - an extension name
	//    2. baz        - a regular field in foo.bar
	//    3. (bob)      - an extension field in baz
	// Note that the dot in foo.bar will thus not be present in Dots but is
	// instead in Parts[0].
	Dots []*RuneNode
}

// NewOptionNameNode creates a new *OptionNameNode. The dots arg must have a
// length that is one less than the length of parts. The parts arg must not be
// empty.
func NewOptionNameNode(parts []*FieldReferenceNode, dots []*RuneNode) *OptionNameNode {
	if len(parts) == 0 {
		panic("must have at least one part")
	}
	if len(dots) != len(parts)-1 {
		panic(fmt.Sprintf("%d parts requires %d dots, not %d", len(parts), len(parts)-1, len(dots)))
	}
	children := make([]Node, 0, len(parts)*2-1)
	for i, part := range parts {
		if part == nil {
			panic(fmt.Sprintf("parts[%d] is nil", i))
		}
		if i > 0 {
			if dots[i-1] == nil {
				panic(fmt.Sprintf("dots[%d] is nil", i-1))
			}
			children = append(children, dots[i-1])
		}
		children = append(children, part)
	}
	return &OptionNameNode{
		compositeNode: compositeNode{
			children: children,
		},
		Parts: parts,
		Dots:  dots,
	}
}

// FieldReferenceNode is a reference to a field name. It can indicate a regular
// field (simple unqualified name) or an extension field (possibly-qualified
// name that is enclosed either in brackets or parentheses).
//
// This is used in options to indicate the names of custom options (which are
// actually extensions), in which case the name is enclosed in parentheses "("
// and ")". It is also used in message literals to set extension fields, in
// which case the name is enclosed in square brackets "[" and "]".
//
// Example:
//   (foo.bar)
type FieldReferenceNode struct {
	compositeNode
	Open  *RuneNode // only present for extension names
	Name  IdentValueNode
	Close *RuneNode // only present for extension names
}

// NewFieldReferenceNode creates a new *FieldReferenceNode for a regular field.
// The name arg must not be nil.
func NewFieldReferenceNode(name *IdentNode) *FieldReferenceNode {
	if name == nil {
		panic("name is nil")
	}
	children := []Node{name}
	return &FieldReferenceNode{
		compositeNode: compositeNode{
			children: children,
		},
		Name: name,
	}
}

// NewExtensionFieldReferenceNode creates a new *FieldReferenceNode for an
// extension field. All args must be non-nil. The openSym and closeSym runes
// should be "(" and ")" or "[" and "]".
func NewExtensionFieldReferenceNode(openSym *RuneNode, name IdentValueNode, closeSym *RuneNode) *FieldReferenceNode {
	if name == nil {
		panic("name is nil")
	}
	if openSym == nil {
		panic("openSym is nil")
	}
	if closeSym == nil {
		panic("closeSym is nil")
	}
	children := []Node{openSym, name, closeSym}
	return &FieldReferenceNode{
		compositeNode: compositeNode{
			children: children,
		},
		Open:  openSym,
		Name:  name,
		Close: closeSym,
	}
}

// IsExtension reports if this is an extension name or not (e.g. enclosed in
// punctuation, such as parentheses or brackets).
func (a *FieldReferenceNode) IsExtension() bool {
	return a.Open != nil
}

func (a *FieldReferenceNode) Value() string {
	if a.Open != nil {
		return string(a.Open.Rune) + string(a.Name.AsIdentifier()) + string(a.Close.Rune)
	} else {
		return string(a.Name.AsIdentifier())
	}
}

// CompactOptionsNode represents a compact options declaration, as used with
// fields, enum values, and extension ranges. Example:
//
//  [deprecated = true, json_name = "foo_bar"]
type CompactOptionsNode struct {
	compositeNode
	OpenBracket *RuneNode
	Options     []*OptionNode
	// Commas represent the separating ',' characters between options. The
	// length of this slice must be exactly len(Options)-1, with each item
	// in Options having a corresponding item in this slice *except the last*
	// (since a trailing comma is not allowed).
	Commas       []*RuneNode
	CloseBracket *RuneNode
}

// NewCompactOptionsNode creates a *CompactOptionsNode. All args must be
// non-nil. The commas arg must have a length that is one less than the
// length of opts. The opts arg must not be empty.
func NewCompactOptionsNode(openBracket *RuneNode, opts []*OptionNode, commas []*RuneNode, closeBracket *RuneNode) *CompactOptionsNode {
	if openBracket == nil {
		panic("openBracket is nil")
	}
	if closeBracket == nil {
		panic("closeBracket is nil")
	}
	if len(opts) == 0 {
		panic("must have at least one part")
	}
	if len(commas) != len(opts)-1 {
		panic(fmt.Sprintf("%d opts requires %d commas, not %d", len(opts), len(opts)-1, len(commas)))
	}
	children := make([]Node, 0, len(opts)*2+1)
	children = append(children, openBracket)
	for i, opt := range opts {
		if i > 0 {
			if commas[i-1] == nil {
				panic(fmt.Sprintf("commas[%d] is nil", i-1))
			}
			children = append(children, commas[i-1])
		}
		if opt == nil {
			panic(fmt.Sprintf("opts[%d] is nil", i))
		}
		children = append(children, opt)
	}
	children = append(children, closeBracket)

	return &CompactOptionsNode{
		compositeNode: compositeNode{
			children: children,
		},
		OpenBracket:  openBracket,
		Options:      opts,
		Commas:       commas,
		CloseBracket: closeBracket,
	}
}

func (e *CompactOptionsNode) GetElements() []*OptionNode {
	if e == nil {
		return nil
	}
	return e.Options
}
