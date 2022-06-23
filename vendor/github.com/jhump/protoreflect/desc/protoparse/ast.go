package protoparse

import "github.com/jhump/protoreflect/desc/protoparse/ast"

// SourcePos is the same as ast.SourcePos. This alias exists for
// backwards compatibility (SourcePos used to be defined in this package.)
type SourcePos = ast.SourcePos

// the types below are accumulator types: linked lists that are
// constructed during parsing and then converted to slices of AST nodes
// once the whole list has been parsed

type compactOptionList struct {
	option *ast.OptionNode
	comma  *ast.RuneNode
	next   *compactOptionList
}

func (list *compactOptionList) toNodes() ([]*ast.OptionNode, []*ast.RuneNode) {
	l := 0
	for cur := list; cur != nil; cur = cur.next {
		l++
	}
	opts := make([]*ast.OptionNode, l)
	commas := make([]*ast.RuneNode, l-1)
	for cur, i := list, 0; cur != nil; cur, i = cur.next, i+1 {
		opts[i] = cur.option
		if cur.comma != nil {
			commas[i] = cur.comma
		}
	}
	return opts, commas
}

type stringList struct {
	str  *ast.StringLiteralNode
	next *stringList
}

func (list *stringList) toStringValueNode() ast.StringValueNode {
	if list.next == nil {
		// single name
		return list.str
	}

	l := 0
	for cur := list; cur != nil; cur = cur.next {
		l++
	}
	strs := make([]*ast.StringLiteralNode, l)
	for cur, i := list, 0; cur != nil; cur, i = cur.next, i+1 {
		strs[i] = cur.str
	}
	return ast.NewCompoundLiteralStringNode(strs...)
}

type nameList struct {
	name  ast.StringValueNode
	comma *ast.RuneNode
	next  *nameList
}

func (list *nameList) toNodes() ([]ast.StringValueNode, []*ast.RuneNode) {
	l := 0
	for cur := list; cur != nil; cur = cur.next {
		l++
	}
	names := make([]ast.StringValueNode, l)
	commas := make([]*ast.RuneNode, l-1)
	for cur, i := list, 0; cur != nil; cur, i = cur.next, i+1 {
		names[i] = cur.name
		if cur.comma != nil {
			commas[i] = cur.comma
		}
	}
	return names, commas
}

type rangeList struct {
	rng   *ast.RangeNode
	comma *ast.RuneNode
	next  *rangeList
}

func (list *rangeList) toNodes() ([]*ast.RangeNode, []*ast.RuneNode) {
	l := 0
	for cur := list; cur != nil; cur = cur.next {
		l++
	}
	ranges := make([]*ast.RangeNode, l)
	commas := make([]*ast.RuneNode, l-1)
	for cur, i := list, 0; cur != nil; cur, i = cur.next, i+1 {
		ranges[i] = cur.rng
		if cur.comma != nil {
			commas[i] = cur.comma
		}
	}
	return ranges, commas
}

type valueList struct {
	val   ast.ValueNode
	comma *ast.RuneNode
	next  *valueList
}

func (list *valueList) toNodes() ([]ast.ValueNode, []*ast.RuneNode) {
	if list == nil {
		return nil, nil
	}
	l := 0
	for cur := list; cur != nil; cur = cur.next {
		l++
	}
	vals := make([]ast.ValueNode, l)
	commas := make([]*ast.RuneNode, l-1)
	for cur, i := list, 0; cur != nil; cur, i = cur.next, i+1 {
		vals[i] = cur.val
		if cur.comma != nil {
			commas[i] = cur.comma
		}
	}
	return vals, commas
}

type fieldRefList struct {
	ref  *ast.FieldReferenceNode
	dot  *ast.RuneNode
	next *fieldRefList
}

func (list *fieldRefList) toNodes() ([]*ast.FieldReferenceNode, []*ast.RuneNode) {
	l := 0
	for cur := list; cur != nil; cur = cur.next {
		l++
	}
	refs := make([]*ast.FieldReferenceNode, l)
	dots := make([]*ast.RuneNode, l-1)
	for cur, i := list, 0; cur != nil; cur, i = cur.next, i+1 {
		refs[i] = cur.ref
		if cur.dot != nil {
			dots[i] = cur.dot
		}
	}

	return refs, dots
}

type identList struct {
	ident *ast.IdentNode
	dot   *ast.RuneNode
	next  *identList
}

func (list *identList) toIdentValueNode(leadingDot *ast.RuneNode) ast.IdentValueNode {
	if list.next == nil && leadingDot == nil {
		// single name
		return list.ident
	}

	l := 0
	for cur := list; cur != nil; cur = cur.next {
		l++
	}
	idents := make([]*ast.IdentNode, l)
	dots := make([]*ast.RuneNode, l-1)
	for cur, i := list, 0; cur != nil; cur, i = cur.next, i+1 {
		idents[i] = cur.ident
		if cur.dot != nil {
			dots[i] = cur.dot
		}
	}

	return ast.NewCompoundIdentNode(leadingDot, idents, dots)
}

type messageFieldEntry struct {
	field     *ast.MessageFieldNode
	delimiter *ast.RuneNode
}

type messageFieldList struct {
	field *messageFieldEntry
	next  *messageFieldList
}

func (list *messageFieldList) toNodes() ([]*ast.MessageFieldNode, []*ast.RuneNode) {
	if list == nil {
		return nil, nil
	}
	l := 0
	for cur := list; cur != nil; cur = cur.next {
		l++
	}
	fields := make([]*ast.MessageFieldNode, l)
	delimiters := make([]*ast.RuneNode, l)
	for cur, i := list, 0; cur != nil; cur, i = cur.next, i+1 {
		fields[i] = cur.field.field
		if cur.field.delimiter != nil {
			delimiters[i] = cur.field.delimiter
		}
	}

	return fields, delimiters
}
