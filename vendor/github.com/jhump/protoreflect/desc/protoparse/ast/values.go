package ast

import (
	"fmt"
	"math"
	"strings"
)

// ValueNode is an AST node that represents a literal value.
//
// It also includes references (e.g. IdentifierValueNode), which can be
// used as values in some contexts, such as describing the default value
// for a field, which can refer to an enum value.
//
// This also allows NoSourceNode to be used in place of a real value node
// for some usages.
type ValueNode interface {
	Node
	// Value returns a Go representation of the value. For scalars, this
	// will be a string, int64, uint64, float64, or bool. This could also
	// be an Identifier (e.g. IdentValueNodes). It can also be a composite
	// literal:
	//   * For array literals, the type returned will be []ValueNode
	//   * For message literals, the type returned will be []*MessageFieldNode
	Value() interface{}
}

var _ ValueNode = (*IdentNode)(nil)
var _ ValueNode = (*CompoundIdentNode)(nil)
var _ ValueNode = (*StringLiteralNode)(nil)
var _ ValueNode = (*CompoundStringLiteralNode)(nil)
var _ ValueNode = (*UintLiteralNode)(nil)
var _ ValueNode = (*PositiveUintLiteralNode)(nil)
var _ ValueNode = (*NegativeIntLiteralNode)(nil)
var _ ValueNode = (*FloatLiteralNode)(nil)
var _ ValueNode = (*SpecialFloatLiteralNode)(nil)
var _ ValueNode = (*SignedFloatLiteralNode)(nil)
var _ ValueNode = (*BoolLiteralNode)(nil)
var _ ValueNode = (*ArrayLiteralNode)(nil)
var _ ValueNode = (*MessageLiteralNode)(nil)
var _ ValueNode = NoSourceNode{}

// StringValueNode is an AST node that represents a string literal.
// Such a node can be a single literal (*StringLiteralNode) or a
// concatenation of multiple literals (*CompoundStringLiteralNode).
type StringValueNode interface {
	ValueNode
	AsString() string
}

var _ StringValueNode = (*StringLiteralNode)(nil)
var _ StringValueNode = (*CompoundStringLiteralNode)(nil)

// StringLiteralNode represents a simple string literal. Example:
//
//  "proto2"
type StringLiteralNode struct {
	terminalNode
	// Val is the actual string value that the literal indicates.
	Val string
}

// NewStringLiteralNode creates a new *StringLiteralNode with the given val.
func NewStringLiteralNode(val string, info TokenInfo) *StringLiteralNode {
	return &StringLiteralNode{
		terminalNode: info.asTerminalNode(),
		Val:          val,
	}
}

func (n *StringLiteralNode) Value() interface{} {
	return n.AsString()
}

func (n *StringLiteralNode) AsString() string {
	return n.Val
}

// CompoundStringLiteralNode represents a compound string literal, which is
// the concatenaton of adjacent string literals. Example:
//
//  "this "  "is"   " all one "   "string"
type CompoundStringLiteralNode struct {
	compositeNode
	Val string
}

// NewCompoundLiteralStringNode creates a new *CompoundStringLiteralNode that
// consists of the given string components. The components argument may not be
// empty.
func NewCompoundLiteralStringNode(components ...*StringLiteralNode) *CompoundStringLiteralNode {
	if len(components) == 0 {
		panic("must have at least one component")
	}
	children := make([]Node, len(components))
	var b strings.Builder
	for i, comp := range components {
		children[i] = comp
		b.WriteString(comp.Val)
	}
	return &CompoundStringLiteralNode{
		compositeNode: compositeNode{
			children: children,
		},
		Val: b.String(),
	}
}

func (n *CompoundStringLiteralNode) Value() interface{} {
	return n.AsString()
}

func (n *CompoundStringLiteralNode) AsString() string {
	return n.Val
}

// IntValueNode is an AST node that represents an integer literal. If
// an integer literal is too large for an int64 (or uint64 for
// positive literals), it is represented instead by a FloatValueNode.
type IntValueNode interface {
	ValueNode
	AsInt64() (int64, bool)
	AsUint64() (uint64, bool)
}

// AsInt32 range checks the given int value and returns its value is
// in the range or 0, false if it is outside the range.
func AsInt32(n IntValueNode, min, max int32) (int32, bool) {
	i, ok := n.AsInt64()
	if !ok {
		return 0, false
	}
	if i < int64(min) || i > int64(max) {
		return 0, false
	}
	return int32(i), true
}

var _ IntValueNode = (*UintLiteralNode)(nil)
var _ IntValueNode = (*PositiveUintLiteralNode)(nil)
var _ IntValueNode = (*NegativeIntLiteralNode)(nil)

// UintLiteralNode represents a simple integer literal with no sign character.
type UintLiteralNode struct {
	terminalNode
	// Val is the numeric value indicated by the literal
	Val uint64
}

// NewUintLiteralNode creates a new *UintLiteralNode with the given val.
func NewUintLiteralNode(val uint64, info TokenInfo) *UintLiteralNode {
	return &UintLiteralNode{
		terminalNode: info.asTerminalNode(),
		Val:          val,
	}
}

func (n *UintLiteralNode) Value() interface{} {
	return n.Val
}

func (n *UintLiteralNode) AsInt64() (int64, bool) {
	if n.Val > math.MaxInt64 {
		return 0, false
	}
	return int64(n.Val), true
}

func (n *UintLiteralNode) AsUint64() (uint64, bool) {
	return n.Val, true
}

func (n *UintLiteralNode) AsFloat() float64 {
	return float64(n.Val)
}

// PositiveUintLiteralNode represents an integer literal with a positive (+) sign.
type PositiveUintLiteralNode struct {
	compositeNode
	Plus *RuneNode
	Uint *UintLiteralNode
	Val  uint64
}

// NewPositiveUintLiteralNode creates a new *PositiveUintLiteralNode. Both
// arguments must be non-nil.
func NewPositiveUintLiteralNode(sign *RuneNode, i *UintLiteralNode) *PositiveUintLiteralNode {
	if sign == nil {
		panic("sign is nil")
	}
	if i == nil {
		panic("i is nil")
	}
	children := []Node{sign, i}
	return &PositiveUintLiteralNode{
		compositeNode: compositeNode{
			children: children,
		},
		Plus: sign,
		Uint: i,
		Val:  i.Val,
	}
}

func (n *PositiveUintLiteralNode) Value() interface{} {
	return n.Val
}

func (n *PositiveUintLiteralNode) AsInt64() (int64, bool) {
	if n.Val > math.MaxInt64 {
		return 0, false
	}
	return int64(n.Val), true
}

func (n *PositiveUintLiteralNode) AsUint64() (uint64, bool) {
	return n.Val, true
}

// NegativeIntLiteralNode represents an integer literal with a negative (-) sign.
type NegativeIntLiteralNode struct {
	compositeNode
	Minus *RuneNode
	Uint  *UintLiteralNode
	Val   int64
}

// NewNegativeIntLiteralNode creates a new *NegativeIntLiteralNode. Both
// arguments must be non-nil.
func NewNegativeIntLiteralNode(sign *RuneNode, i *UintLiteralNode) *NegativeIntLiteralNode {
	if sign == nil {
		panic("sign is nil")
	}
	if i == nil {
		panic("i is nil")
	}
	children := []Node{sign, i}
	return &NegativeIntLiteralNode{
		compositeNode: compositeNode{
			children: children,
		},
		Minus: sign,
		Uint:  i,
		Val:   -int64(i.Val),
	}
}

func (n *NegativeIntLiteralNode) Value() interface{} {
	return n.Val
}

func (n *NegativeIntLiteralNode) AsInt64() (int64, bool) {
	return n.Val, true
}

func (n *NegativeIntLiteralNode) AsUint64() (uint64, bool) {
	if n.Val < 0 {
		return 0, false
	}
	return uint64(n.Val), true
}

// FloatValueNode is an AST node that represents a numeric literal with
// a floating point, in scientific notation, or too large to fit in an
// int64 or uint64.
type FloatValueNode interface {
	ValueNode
	AsFloat() float64
}

var _ FloatValueNode = (*FloatLiteralNode)(nil)
var _ FloatValueNode = (*SpecialFloatLiteralNode)(nil)
var _ FloatValueNode = (*UintLiteralNode)(nil)

// FloatLiteralNode represents a floating point numeric literal.
type FloatLiteralNode struct {
	terminalNode
	// Val is the numeric value indicated by the literal
	Val float64
}

// NewFloatLiteralNode creates a new *FloatLiteralNode with the given val.
func NewFloatLiteralNode(val float64, info TokenInfo) *FloatLiteralNode {
	return &FloatLiteralNode{
		terminalNode: info.asTerminalNode(),
		Val:          val,
	}
}

func (n *FloatLiteralNode) Value() interface{} {
	return n.AsFloat()
}

func (n *FloatLiteralNode) AsFloat() float64 {
	return n.Val
}

// SpecialFloatLiteralNode represents a special floating point numeric literal
// for "inf" and "nan" values.
type SpecialFloatLiteralNode struct {
	*KeywordNode
	Val float64
}

// NewSpecialFloatLiteralNode returns a new *SpecialFloatLiteralNode for the
// given keyword, which must be "inf" or "nan".
func NewSpecialFloatLiteralNode(name *KeywordNode) *SpecialFloatLiteralNode {
	var f float64
	if name.Val == "inf" {
		f = math.Inf(1)
	} else {
		f = math.NaN()
	}
	return &SpecialFloatLiteralNode{
		KeywordNode: name,
		Val:         f,
	}
}

func (n *SpecialFloatLiteralNode) Value() interface{} {
	return n.AsFloat()
}

func (n *SpecialFloatLiteralNode) AsFloat() float64 {
	return n.Val
}

// SignedFloatLiteralNode represents a signed floating point number.
type SignedFloatLiteralNode struct {
	compositeNode
	Sign  *RuneNode
	Float FloatValueNode
	Val   float64
}

// NewSignedFloatLiteralNode creates a new *SignedFloatLiteralNode. Both
// arguments must be non-nil.
func NewSignedFloatLiteralNode(sign *RuneNode, f FloatValueNode) *SignedFloatLiteralNode {
	if sign == nil {
		panic("sign is nil")
	}
	if f == nil {
		panic("f is nil")
	}
	children := []Node{sign, f}
	val := f.AsFloat()
	if sign.Rune == '-' {
		val = -val
	}
	return &SignedFloatLiteralNode{
		compositeNode: compositeNode{
			children: children,
		},
		Sign:  sign,
		Float: f,
		Val:   val,
	}
}

func (n *SignedFloatLiteralNode) Value() interface{} {
	return n.Val
}

func (n *SignedFloatLiteralNode) AsFloat() float64 {
	return n.Val
}

// BoolLiteralNode represents a boolean literal.
type BoolLiteralNode struct {
	*KeywordNode
	Val bool
}

// NewBoolLiteralNode returns a new *BoolLiteralNode for the given keyword,
// which must be "true" or "false".
func NewBoolLiteralNode(name *KeywordNode) *BoolLiteralNode {
	return &BoolLiteralNode{
		KeywordNode: name,
		Val:         name.Val == "true",
	}
}

func (n *BoolLiteralNode) Value() interface{} {
	return n.Val
}

// ArrayLiteralNode represents an array literal, which is only allowed inside of
// a MessageLiteralNode, to indicate values for a repeated field. Example:
//
//  ["foo", "bar", "baz"]
type ArrayLiteralNode struct {
	compositeNode
	OpenBracket *RuneNode
	Elements    []ValueNode
	// Commas represent the separating ',' characters between elements. The
	// length of this slice must be exactly len(Elements)-1, with each item
	// in Elements having a corresponding item in this slice *except the last*
	// (since a trailing comma is not allowed).
	Commas       []*RuneNode
	CloseBracket *RuneNode
}

// NewArrayLiteralNode creates a new *ArrayLiteralNode. The openBracket and
// closeBracket args must be non-nil and represent the "[" and "]" runes that
// surround the array values. The given commas arg must have a length that is
// one less than the length of the vals arg. However, vals may be empty, in
// which case commas must also be empty.
func NewArrayLiteralNode(openBracket *RuneNode, vals []ValueNode, commas []*RuneNode, closeBracket *RuneNode) *ArrayLiteralNode {
	if openBracket == nil {
		panic("openBracket is nil")
	}
	if closeBracket == nil {
		panic("closeBracket is nil")
	}
	if len(vals) == 0 && len(commas) != 0 {
		panic("vals is empty but commas is not")
	}
	if len(vals) > 0 && len(commas) != len(vals)-1 {
		panic(fmt.Sprintf("%d vals requires %d commas, not %d", len(vals), len(vals)-1, len(commas)))
	}
	children := make([]Node, 0, len(vals)*2+1)
	children = append(children, openBracket)
	for i, val := range vals {
		if i > 0 {
			if commas[i-1] == nil {
				panic(fmt.Sprintf("commas[%d] is nil", i-1))
			}
			children = append(children, commas[i-1])
		}
		if val == nil {
			panic(fmt.Sprintf("vals[%d] is nil", i))
		}
		children = append(children, val)
	}
	children = append(children, closeBracket)

	return &ArrayLiteralNode{
		compositeNode: compositeNode{
			children: children,
		},
		OpenBracket:  openBracket,
		Elements:     vals,
		Commas:       commas,
		CloseBracket: closeBracket,
	}
}

func (n *ArrayLiteralNode) Value() interface{} {
	return n.Elements
}

// MessageLiteralNode represents a message literal, which is compatible with the
// protobuf text format and can be used for custom options with message types.
// Example:
//
//   { foo:1 foo:2 foo:3 bar:<name:"abc" id:123> }
type MessageLiteralNode struct {
	compositeNode
	Open     *RuneNode // should be '{' or '<'
	Elements []*MessageFieldNode
	// Separator characters between elements, which can be either ','
	// or ';' if present. This slice must be exactly len(Elements) in
	// length, with each item in Elements having one corresponding item
	// in Seps. Separators in message literals are optional, so a given
	// item in this slice may be nil to indicate absence of a separator.
	Seps  []*RuneNode
	Close *RuneNode // should be '}' or '>', depending on Open
}

// NewMessageLiteralNode creates a new *MessageLiteralNode. The openSym and
// closeSym runes must not be nil and should be "{" and "}" or "<" and ">".
//
// Unlike separators (dots and commas) used for other AST nodes that represent
// a list of elements, the seps arg must be the SAME length as vals, and it may
// contain nil values to indicate absence of a separator (in fact, it could be
// all nils).
func NewMessageLiteralNode(openSym *RuneNode, vals []*MessageFieldNode, seps []*RuneNode, closeSym *RuneNode) *MessageLiteralNode {
	if openSym == nil {
		panic("openSym is nil")
	}
	if closeSym == nil {
		panic("closeSym is nil")
	}
	if len(seps) != len(vals) {
		panic(fmt.Sprintf("%d vals requires %d commas, not %d", len(vals), len(vals), len(seps)))
	}
	numChildren := len(vals) + 2
	for _, sep := range seps {
		if sep != nil {
			numChildren++
		}
	}
	children := make([]Node, 0, numChildren)
	children = append(children, openSym)
	for i, val := range vals {
		if val == nil {
			panic(fmt.Sprintf("vals[%d] is nil", i))
		}
		children = append(children, val)
		if seps[i] != nil {
			children = append(children, seps[i])
		}
	}
	children = append(children, closeSym)

	return &MessageLiteralNode{
		compositeNode: compositeNode{
			children: children,
		},
		Open:     openSym,
		Elements: vals,
		Seps:     seps,
		Close:    closeSym,
	}
}

func (n *MessageLiteralNode) Value() interface{} {
	return n.Elements
}

// MessageFieldNode represents a single field (name and value) inside of a
// message literal. Example:
//
//   foo:"bar"
type MessageFieldNode struct {
	compositeNode
	Name *FieldReferenceNode
	// Sep represents the ':' separator between the name and value. If
	// the value is a message literal (and thus starts with '<' or '{'),
	// then the separator is optional, and thus may be nil.
	Sep *RuneNode
	Val ValueNode
}

// NewMessageFieldNode creates a new *MessageFieldNode. All args except sep
// must be non-nil.
func NewMessageFieldNode(name *FieldReferenceNode, sep *RuneNode, val ValueNode) *MessageFieldNode {
	if name == nil {
		panic("name is nil")
	}
	if val == nil {
		panic("val is nil")
	}
	numChildren := 2
	if sep != nil {
		numChildren++
	}
	children := make([]Node, 0, numChildren)
	children = append(children, name)
	if sep != nil {
		children = append(children, sep)
	}
	children = append(children, val)

	return &MessageFieldNode{
		compositeNode: compositeNode{
			children: children,
		},
		Name: name,
		Sep:  sep,
		Val:  val,
	}
}
