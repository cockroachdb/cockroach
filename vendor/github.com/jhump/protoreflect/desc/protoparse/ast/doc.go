// Package ast defines types for modeling the AST (Abstract Syntax
// Tree) for the protocol buffers source language.
//
// All nodes of the tree implement the Node interface. Leaf nodes in the
// tree implement TerminalNode and all others implement CompositeNode.
// The root of the tree for a proto source file is a *FileNode.
//
// Comments are not represented as nodes in the tree. Instead, they are
// attached to all terminal nodes in the tree. So, when lexing, comments
// are accumulated until the next non-comment token is found. The AST
// model in this package thus provides access to all comments in the
// file, regardless of location (unlike the SourceCodeInfo present in
// descriptor protos, which are lossy). The comments associated with a
// a non-leaf/non-token node (i.e. a CompositeNode) come from the first
// and last nodes in its sub-tree.
//
// Creation of AST nodes should use the factory functions in this
// package instead of struct literals. Some factory functions accept
// optional arguments, which means the arguments can be nil. If nil
// values are provided for other (non-optional) arguments, the resulting
// node may be invalid and cause panics later in the program.
//
// This package defines numerous interfaces. However, user code should
// not attempt to implement any of them. Most consumers of an AST will
// not work correctly if they encounter concrete implementations other
// than the ones defined in this package.
package ast
