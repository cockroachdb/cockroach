package ast

// Node is the interface implemented by all nodes in the AST. It
// provides information about the span of this AST node in terms
// of location in the source file. It also provides information
// about all prior comments (attached as leading comments) and
// optional subsequent comments (attached as trailing comments).
type Node interface {
	Start() *SourcePos
	End() *SourcePos
	LeadingComments() []Comment
	TrailingComments() []Comment
}

// TerminalNode represents a leaf in the AST. These represent
// the tokens/lexemes in the protobuf language. Comments and
// whitespace are accumulated by the lexer and associated with
// the following lexed token.
type TerminalNode interface {
	Node
	// PopLeadingComment removes the first leading comment from this
	// token and returns it. If the node has no leading comments then
	// this method will panic.
	PopLeadingComment() Comment
	// PushTrailingComment appends the given comment to the token's
	// trailing comments.
	PushTrailingComment(Comment)
	// LeadingWhitespace returns any whitespace between the prior comment
	// (last leading comment), if any, or prior lexed token and this token.
	LeadingWhitespace() string
	// RawText returns the raw text of the token as read from the source.
	RawText() string
}

var _ TerminalNode = (*StringLiteralNode)(nil)
var _ TerminalNode = (*UintLiteralNode)(nil)
var _ TerminalNode = (*FloatLiteralNode)(nil)
var _ TerminalNode = (*IdentNode)(nil)
var _ TerminalNode = (*BoolLiteralNode)(nil)
var _ TerminalNode = (*SpecialFloatLiteralNode)(nil)
var _ TerminalNode = (*KeywordNode)(nil)
var _ TerminalNode = (*RuneNode)(nil)

// TokenInfo represents state accumulated by the lexer to associated with a
// token (aka terminal node).
type TokenInfo struct {
	// The location of the token in the source file.
	PosRange
	// The raw text of the token.
	RawText string
	// Any comments encountered preceding this token.
	LeadingComments []Comment
	// Any leading whitespace immediately preceding this token.
	LeadingWhitespace string
	// Any trailing comments following this token. This is usually
	// empty as tokens are created by the lexer immediately and
	// trailing comments are accounted for afterwards, added using
	// the node's PushTrailingComment method.
	TrailingComments []Comment
}

func (t *TokenInfo) asTerminalNode() terminalNode {
	return terminalNode{
		posRange:          t.PosRange,
		leadingComments:   t.LeadingComments,
		leadingWhitespace: t.LeadingWhitespace,
		trailingComments:  t.TrailingComments,
		raw:               t.RawText,
	}
}

// CompositeNode represents any non-terminal node in the tree. These
// are interior or root nodes and have child nodes.
type CompositeNode interface {
	Node
	// All AST nodes that are immediate children of this one.
	Children() []Node
}

// terminalNode contains book-keeping shared by all TerminalNode
// implementations. It is embedded in all such node types in this
// package. It provides the implementation of the TerminalNode
// interface.
type terminalNode struct {
	posRange          PosRange
	leadingComments   []Comment
	leadingWhitespace string
	trailingComments  []Comment
	raw               string
}

func (n *terminalNode) Start() *SourcePos {
	return &n.posRange.Start
}

func (n *terminalNode) End() *SourcePos {
	return &n.posRange.End
}

func (n *terminalNode) LeadingComments() []Comment {
	return n.leadingComments
}

func (n *terminalNode) TrailingComments() []Comment {
	return n.trailingComments
}

func (n *terminalNode) PopLeadingComment() Comment {
	c := n.leadingComments[0]
	n.leadingComments = n.leadingComments[1:]
	return c
}

func (n *terminalNode) PushTrailingComment(c Comment) {
	n.trailingComments = append(n.trailingComments, c)
}

func (n *terminalNode) LeadingWhitespace() string {
	return n.leadingWhitespace
}

func (n *terminalNode) RawText() string {
	return n.raw
}

// compositeNode contains book-keeping shared by all CompositeNode
// implementations. It is embedded in all such node types in this
// package. It provides the implementation of the CompositeNode
// interface.
type compositeNode struct {
	children []Node
}

func (n *compositeNode) Children() []Node {
	return n.children
}

func (n *compositeNode) Start() *SourcePos {
	return n.children[0].Start()
}

func (n *compositeNode) End() *SourcePos {
	return n.children[len(n.children)-1].End()
}

func (n *compositeNode) LeadingComments() []Comment {
	return n.children[0].LeadingComments()
}

func (n *compositeNode) TrailingComments() []Comment {
	return n.children[len(n.children)-1].TrailingComments()
}

// RuneNode represents a single rune in protobuf source. Runes
// are typically collected into tokens, but some runes stand on
// their own, such as punctuation/symbols like commas, semicolons,
// equals signs, open and close symbols (braces, brackets, angles,
// and parentheses), and periods/dots.
type RuneNode struct {
	terminalNode
	Rune rune
}

// NewRuneNode creates a new *RuneNode with the given properties.
func NewRuneNode(r rune, info TokenInfo) *RuneNode {
	return &RuneNode{
		terminalNode: info.asTerminalNode(),
		Rune:         r,
	}
}

// EmptyDeclNode represents an empty declaration in protobuf source.
// These amount to extra semicolons, with no actual content preceding
// the semicolon.
type EmptyDeclNode struct {
	compositeNode
	Semicolon *RuneNode
}

// NewEmptyDeclNode creates a new *EmptyDeclNode. The one argument must
// be non-nil.
func NewEmptyDeclNode(semicolon *RuneNode) *EmptyDeclNode {
	if semicolon == nil {
		panic("semicolon is nil")
	}
	return &EmptyDeclNode{
		compositeNode: compositeNode{
			children: []Node{semicolon},
		},
		Semicolon: semicolon,
	}
}

func (e *EmptyDeclNode) fileElement()    {}
func (e *EmptyDeclNode) msgElement()     {}
func (e *EmptyDeclNode) extendElement()  {}
func (e *EmptyDeclNode) oneOfElement()   {}
func (e *EmptyDeclNode) enumElement()    {}
func (e *EmptyDeclNode) serviceElement() {}
func (e *EmptyDeclNode) methodElement()  {}
