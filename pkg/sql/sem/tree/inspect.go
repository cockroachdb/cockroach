// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "fmt"

// InspectType describes the INSPECT statement operation.
type InspectType int

const (
	// InspectTable describes the INSPECT operation INSPECT TABLE.
	InspectTable InspectType = iota
	// InspectDatabase describes the INSPECT operation INSPECT DATABASE.
	InspectDatabase
)

// Inspect represents an INSPECT statement.
type Inspect struct {
	Typ     InspectType
	Options InspectOptions
	// Table is only set during INSPECT TABLE statements.
	Table *UnresolvedObjectName
	// Database is only set during INSPECT DATABASE statements.
	Database Name
	AsOf     AsOfClause
}

// Format implements the NodeFormatter interface.
func (n *Inspect) Format(ctx *FmtCtx) {
	ctx.WriteString("INSPECT ")
	switch n.Typ {
	case InspectTable:
		ctx.WriteString("TABLE ")
		ctx.FormatNode(n.Table)
	case InspectDatabase:
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&n.Database)
	default:
		panic("Unhandled InspectType")
	}

	if n.AsOf.Expr != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(&n.AsOf)
	}

	if len(n.Options) > 0 {
		ctx.WriteString(" WITH OPTIONS ")
		ctx.FormatNode(&n.Options)
	}
}

// InspectOptions corresponds to a comma-delimited list of inspect options.
type InspectOptions []InspectOption

// Format implements the NodeFormatter interface.
func (n *InspectOptions) Format(ctx *FmtCtx) {
	for i, option := range *n {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(option)
	}
}

func (n *InspectOptions) String() string { return AsString(n) }

// InspectOption represents an inspect option.
type InspectOption interface {
	fmt.Stringer
	NodeFormatter

	inspectOptionType()
}

// InspectOptionIndex implements the InspectOption interface
func (*InspectOptionIndex) inspectOptionType() {}

func (n *InspectOptionIndex) String() string { return AsString(n) }

// InspectOptionIndex represents an INDEX inspect check.
type InspectOptionIndex struct {
	IndexNames TableIndexNames
}

// Format implements the NodeFormatter interface.
func (n *InspectOptionIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("INDEX ")
	if n.IndexNames != nil {
		ctx.WriteByte('(')
		ctx.FormatNode(&n.IndexNames)
		ctx.WriteByte(')')
	} else {
		ctx.WriteString("ALL")
	}
}
