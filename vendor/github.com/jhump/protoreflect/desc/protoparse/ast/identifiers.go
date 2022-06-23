package ast

import (
	"fmt"
	"strings"
)

// Identifier is a possibly-qualified name. This is used to distinguish
// ValueNode values that are references/identifiers vs. those that are
// string literals.
type Identifier string

// IdentValueNode is an AST node that represents an identifier.
type IdentValueNode interface {
	ValueNode
	AsIdentifier() Identifier
}

var _ IdentValueNode = (*IdentNode)(nil)
var _ IdentValueNode = (*CompoundIdentNode)(nil)

// IdentNode represents a simple, unqualified identifier. These are used to name
// elements declared in a protobuf file or to refer to elements. Example:
//
//  foobar
type IdentNode struct {
	terminalNode
	Val string
}

// NewIdentNode creates a new *IdentNode. The given val is the identifier text.
func NewIdentNode(val string, info TokenInfo) *IdentNode {
	return &IdentNode{
		terminalNode: info.asTerminalNode(),
		Val:          val,
	}
}

func (n *IdentNode) Value() interface{} {
	return n.AsIdentifier()
}

func (n *IdentNode) AsIdentifier() Identifier {
	return Identifier(n.Val)
}

// ToKeyword is used to convert identifiers to keywords. Since keywords are not
// reserved in the protobuf language, they are initially lexed as identifiers
// and then converted to keywords based on context.
func (n *IdentNode) ToKeyword() *KeywordNode {
	return (*KeywordNode)(n)
}

// CompoundIdentNode represents a qualified identifier. A qualified identifier
// has at least one dot and possibly multiple identifier names (all separated by
// dots). If the identifier has a leading dot, then it is a *fully* qualified
// identifier. Example:
//
//  .com.foobar.Baz
type CompoundIdentNode struct {
	compositeNode
	// Optional leading dot, indicating that the identifier is fully qualified.
	LeadingDot *RuneNode
	Components []*IdentNode
	// Dots[0] is the dot after Components[0]. The length of Dots is always
	// one less than the length of Components.
	Dots []*RuneNode
	// The text value of the identifier, with all components and dots
	// concatenated.
	Val string
}

// NewCompoundIdentNode creates a *CompoundIdentNode. The leadingDot may be nil.
// The dots arg must have a length that is one less than the length of
// components. The components arg must not be empty.
func NewCompoundIdentNode(leadingDot *RuneNode, components []*IdentNode, dots []*RuneNode) *CompoundIdentNode {
	if len(components) == 0 {
		panic("must have at least one component")
	}
	if len(dots) != len(components)-1 {
		panic(fmt.Sprintf("%d components requires %d dots, not %d", len(components), len(components)-1, len(dots)))
	}
	numChildren := len(components)*2 - 1
	if leadingDot != nil {
		numChildren++
	}
	children := make([]Node, 0, numChildren)
	var b strings.Builder
	if leadingDot != nil {
		children = append(children, leadingDot)
		b.WriteRune(leadingDot.Rune)
	}
	for i, comp := range components {
		if i > 0 {
			dot := dots[i-1]
			children = append(children, dot)
			b.WriteRune(dot.Rune)
		}
		children = append(children, comp)
		b.WriteString(comp.Val)
	}
	return &CompoundIdentNode{
		compositeNode: compositeNode{
			children: children,
		},
		LeadingDot: leadingDot,
		Components: components,
		Dots:       dots,
		Val:        b.String(),
	}
}

func (n *CompoundIdentNode) Value() interface{} {
	return n.AsIdentifier()
}

func (n *CompoundIdentNode) AsIdentifier() Identifier {
	return Identifier(n.Val)
}

// KeywordNode is an AST node that represents a keyword. Keywords are
// like identifiers, but they have special meaning in particular contexts.
// Example:
//
//  message
type KeywordNode IdentNode

// NewKeywordNode creates a new *KeywordNode. The given val is the keyword.
func NewKeywordNode(val string, info TokenInfo) *KeywordNode {
	return &KeywordNode{
		terminalNode: info.asTerminalNode(),
		Val:          val,
	}
}
