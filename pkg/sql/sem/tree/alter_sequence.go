package tree

import "bytes"

// AlterSequence represents an ALTER SEQUENCE statement, except in the case of
// ALTER SEQUENCE <seqName> RENAME TO <newSeqName>, which is represented by a
// RenameTable node.
type AlterSequence struct {
	IfExists bool
	Name     NormalizableTableName
	Options  SequenceOptions
}

func (node *AlterSequence) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ALTER SEQUENCE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, &node.Name)
	FormatNode(buf, f, node.Options)
}
