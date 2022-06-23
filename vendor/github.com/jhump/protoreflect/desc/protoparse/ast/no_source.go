package ast

// UnknownPos is a placeholder position when only the source file
// name is known.
func UnknownPos(filename string) *SourcePos {
	return &SourcePos{Filename: filename}
}

// NoSourceNode is a placeholder AST node that implements numerous
// interfaces in this package. It can be used to represent an AST
// element for a file whose source is not available.
type NoSourceNode struct {
	pos *SourcePos
}

// NewNoSourceNode creates a new NoSourceNode for the given filename.
func NewNoSourceNode(filename string) NoSourceNode {
	return NoSourceNode{pos: UnknownPos(filename)}
}

func (n NoSourceNode) Start() *SourcePos {
	return n.pos
}

func (n NoSourceNode) End() *SourcePos {
	return n.pos
}

func (n NoSourceNode) LeadingComments() []Comment {
	return nil
}

func (n NoSourceNode) TrailingComments() []Comment {
	return nil
}

func (n NoSourceNode) GetSyntax() Node {
	return n
}

func (n NoSourceNode) GetName() Node {
	return n
}

func (n NoSourceNode) GetValue() ValueNode {
	return n
}

func (n NoSourceNode) FieldLabel() Node {
	return n
}

func (n NoSourceNode) FieldName() Node {
	return n
}

func (n NoSourceNode) FieldType() Node {
	return n
}

func (n NoSourceNode) FieldTag() Node {
	return n
}

func (n NoSourceNode) FieldExtendee() Node {
	return n
}

func (n NoSourceNode) GetGroupKeyword() Node {
	return n
}

func (n NoSourceNode) GetOptions() *CompactOptionsNode {
	return nil
}

func (n NoSourceNode) RangeStart() Node {
	return n
}

func (n NoSourceNode) RangeEnd() Node {
	return n
}

func (n NoSourceNode) GetNumber() Node {
	return n
}

func (n NoSourceNode) MessageName() Node {
	return n
}

func (n NoSourceNode) GetInputType() Node {
	return n
}

func (n NoSourceNode) GetOutputType() Node {
	return n
}

func (n NoSourceNode) Value() interface{} {
	return nil
}
