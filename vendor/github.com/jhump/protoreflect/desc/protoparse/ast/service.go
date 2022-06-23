package ast

import "fmt"

// ServiceNode represents a service declaration. Example:
//
//  service Foo {
//    rpc Bar (Baz) returns (Bob);
//    rpc Frobnitz (stream Parts) returns (Gyzmeaux);
//  }
type ServiceNode struct {
	compositeNode
	Keyword    *KeywordNode
	Name       *IdentNode
	OpenBrace  *RuneNode
	Decls      []ServiceElement
	CloseBrace *RuneNode
}

func (*ServiceNode) fileElement() {}

// NewServiceNode creates a new *ServiceNode. All arguments must be non-nil.
//  - keyword: The token corresponding to the "service" keyword.
//  - name: The token corresponding to the service's name.
//  - openBrace: The token corresponding to the "{" rune that starts the body.
//  - decls: All declarations inside the service body.
//  - closeBrace: The token corresponding to the "}" rune that ends the body.
func NewServiceNode(keyword *KeywordNode, name *IdentNode, openBrace *RuneNode, decls []ServiceElement, closeBrace *RuneNode) *ServiceNode {
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
		switch decl := decl.(type) {
		case *OptionNode, *RPCNode, *EmptyDeclNode:
		default:
			panic(fmt.Sprintf("invalid ServiceElement type: %T", decl))
		}
	}

	return &ServiceNode{
		compositeNode: compositeNode{
			children: children,
		},
		Keyword:    keyword,
		Name:       name,
		OpenBrace:  openBrace,
		Decls:      decls,
		CloseBrace: closeBrace,
	}
}

// ServiceElement is an interface implemented by all AST nodes that can
// appear in the body of a service declaration.
type ServiceElement interface {
	Node
	serviceElement()
}

var _ ServiceElement = (*OptionNode)(nil)
var _ ServiceElement = (*RPCNode)(nil)
var _ ServiceElement = (*EmptyDeclNode)(nil)

// RPCDeclNode is a placeholder interface for AST nodes that represent RPC
// declarations. This allows NoSourceNode to be used in place of *RPCNode
// for some usages.
type RPCDeclNode interface {
	Node
	GetInputType() Node
	GetOutputType() Node
}

var _ RPCDeclNode = (*RPCNode)(nil)
var _ RPCDeclNode = NoSourceNode{}

// RPCNode represents an RPC declaration. Example:
//
//  rpc Foo (Bar) returns (Baz);
type RPCNode struct {
	compositeNode
	Keyword    *KeywordNode
	Name       *IdentNode
	Input      *RPCTypeNode
	Returns    *KeywordNode
	Output     *RPCTypeNode
	Semicolon  *RuneNode
	OpenBrace  *RuneNode
	Decls      []RPCElement
	CloseBrace *RuneNode
}

func (n *RPCNode) serviceElement() {}

// NewRPCNode creates a new *RPCNode with no body. All arguments must be non-nil.
//  - keyword: The token corresponding to the "rpc" keyword.
//  - name: The token corresponding to the RPC's name.
//  - input: The token corresponding to the RPC input message type.
//  - returns: The token corresponding to the "returns" keyword that precedes the output type.
//  - output: The token corresponding to the RPC output message type.
//  - semicolon: The token corresponding to the ";" rune that ends the declaration.
func NewRPCNode(keyword *KeywordNode, name *IdentNode, input *RPCTypeNode, returns *KeywordNode, output *RPCTypeNode, semicolon *RuneNode) *RPCNode {
	if keyword == nil {
		panic("keyword is nil")
	}
	if name == nil {
		panic("name is nil")
	}
	if input == nil {
		panic("input is nil")
	}
	if returns == nil {
		panic("returns is nil")
	}
	if output == nil {
		panic("output is nil")
	}
	if semicolon == nil {
		panic("semicolon is nil")
	}
	children := []Node{keyword, name, input, returns, output, semicolon}
	return &RPCNode{
		compositeNode: compositeNode{
			children: children,
		},
		Keyword:   keyword,
		Name:      name,
		Input:     input,
		Returns:   returns,
		Output:    output,
		Semicolon: semicolon,
	}
}

// NewRPCNodeWithBody creates a new *RPCNode that includes a body (and possibly
// options). All arguments must be non-nil.
//  - keyword: The token corresponding to the "rpc" keyword.
//  - name: The token corresponding to the RPC's name.
//  - input: The token corresponding to the RPC input message type.
//  - returns: The token corresponding to the "returns" keyword that precedes the output type.
//  - output: The token corresponding to the RPC output message type.
//  - openBrace: The token corresponding to the "{" rune that starts the body.
//  - decls: All declarations inside the RPC body.
//  - closeBrace: The token corresponding to the "}" rune that ends the body.
func NewRPCNodeWithBody(keyword *KeywordNode, name *IdentNode, input *RPCTypeNode, returns *KeywordNode, output *RPCTypeNode, openBrace *RuneNode, decls []RPCElement, closeBrace *RuneNode) *RPCNode {
	if keyword == nil {
		panic("keyword is nil")
	}
	if name == nil {
		panic("name is nil")
	}
	if input == nil {
		panic("input is nil")
	}
	if returns == nil {
		panic("returns is nil")
	}
	if output == nil {
		panic("output is nil")
	}
	if openBrace == nil {
		panic("openBrace is nil")
	}
	if closeBrace == nil {
		panic("closeBrace is nil")
	}
	children := make([]Node, 0, 7+len(decls))
	children = append(children, keyword, name, input, returns, output, openBrace)
	for _, decl := range decls {
		children = append(children, decl)
	}
	children = append(children, closeBrace)

	for _, decl := range decls {
		switch decl := decl.(type) {
		case *OptionNode, *EmptyDeclNode:
		default:
			panic(fmt.Sprintf("invalid RPCElement type: %T", decl))
		}
	}

	return &RPCNode{
		compositeNode: compositeNode{
			children: children,
		},
		Keyword:    keyword,
		Name:       name,
		Input:      input,
		Returns:    returns,
		Output:     output,
		OpenBrace:  openBrace,
		Decls:      decls,
		CloseBrace: closeBrace,
	}
}

func (n *RPCNode) GetInputType() Node {
	return n.Input.MessageType
}

func (n *RPCNode) GetOutputType() Node {
	return n.Output.MessageType
}

// RPCElement is an interface implemented by all AST nodes that can
// appear in the body of an rpc declaration (aka method).
type RPCElement interface {
	Node
	methodElement()
}

var _ RPCElement = (*OptionNode)(nil)
var _ RPCElement = (*EmptyDeclNode)(nil)

// RPCTypeNode represents the declaration of a request or response type for an
// RPC. Example:
//
//  (stream foo.Bar)
type RPCTypeNode struct {
	compositeNode
	OpenParen   *RuneNode
	Stream      *KeywordNode
	MessageType IdentValueNode
	CloseParen  *RuneNode
}

// NewRPCTypeNode creates a new *RPCTypeNode. All arguments must be non-nil
// except stream, which may be nil.
//  - openParen: The token corresponding to the "(" rune that starts the declaration.
//  - stream: The token corresponding to the "stream" keyword or nil if not present.
//  - msgType: The token corresponding to the message type's name.
//  - closeParen: The token corresponding to the ")" rune that ends the declaration.
func NewRPCTypeNode(openParen *RuneNode, stream *KeywordNode, msgType IdentValueNode, closeParen *RuneNode) *RPCTypeNode {
	if openParen == nil {
		panic("openParen is nil")
	}
	if msgType == nil {
		panic("msgType is nil")
	}
	if closeParen == nil {
		panic("closeParen is nil")
	}
	var children []Node
	if stream != nil {
		children = []Node{openParen, stream, msgType, closeParen}
	} else {
		children = []Node{openParen, msgType, closeParen}
	}

	return &RPCTypeNode{
		compositeNode: compositeNode{
			children: children,
		},
		OpenParen:   openParen,
		Stream:      stream,
		MessageType: msgType,
		CloseParen:  closeParen,
	}
}
