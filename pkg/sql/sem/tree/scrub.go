// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "fmt"

// ScrubType describes the SCRUB statement operation.
type ScrubType int

const (
	// ScrubTable describes the SCRUB operation SCRUB TABLE.
	ScrubTable = iota
	// ScrubDatabase describes the SCRUB operation SCRUB DATABASE.
	ScrubDatabase = iota
)

// Scrub represents a SCRUB statement.
type Scrub struct {
	Typ     ScrubType
	Options ScrubOptions
	// Table is only set during SCRUB TABLE statements.
	Table *UnresolvedObjectName
	// Database is only set during SCRUB DATABASE statements.
	Database Name
	AsOf     AsOfClause
}

// Format implements the NodeFormatter interface.
func (n *Scrub) Format(ctx *FmtCtx) {
	ctx.WriteString("EXPERIMENTAL SCRUB ")
	switch n.Typ {
	case ScrubTable:
		ctx.WriteString("TABLE ")
		ctx.FormatNode(n.Table)
	case ScrubDatabase:
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&n.Database)
	default:
		panic("Unhandled ScrubType")
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

// ScrubOptions corresponds to a comma-delimited list of scrub options.
type ScrubOptions []ScrubOption

// Format implements the NodeFormatter interface.
func (n *ScrubOptions) Format(ctx *FmtCtx) {
	for i, option := range *n {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(option)
	}
}

func (n *ScrubOptions) String() string { return AsString(n) }

// ScrubOption represents a scrub option.
type ScrubOption interface {
	fmt.Stringer
	NodeFormatter

	scrubOptionType()
}

// scrubOptionType implements the ScrubOption interface
func (*ScrubOptionIndex) scrubOptionType()      {}
func (*ScrubOptionPhysical) scrubOptionType()   {}
func (*ScrubOptionConstraint) scrubOptionType() {}

func (n *ScrubOptionIndex) String() string      { return AsString(n) }
func (n *ScrubOptionPhysical) String() string   { return AsString(n) }
func (n *ScrubOptionConstraint) String() string { return AsString(n) }

// ScrubOptionIndex represents an INDEX scrub check.
type ScrubOptionIndex struct {
	IndexNames NameList
}

// Format implements the NodeFormatter interface.
func (n *ScrubOptionIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("INDEX ")
	if n.IndexNames != nil {
		ctx.WriteByte('(')
		ctx.FormatNode(&n.IndexNames)
		ctx.WriteByte(')')
	} else {
		ctx.WriteString("ALL")
	}
}

// ScrubOptionPhysical represents a PHYSICAL scrub check.
type ScrubOptionPhysical struct{}

// Format implements the NodeFormatter interface.
func (n *ScrubOptionPhysical) Format(ctx *FmtCtx) {
	ctx.WriteString("PHYSICAL")
}

// ScrubOptionConstraint represents a CONSTRAINT scrub check.
type ScrubOptionConstraint struct {
	ConstraintNames NameList
}

// Format implements the NodeFormatter interface.
func (n *ScrubOptionConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString("CONSTRAINT ")
	if n.ConstraintNames != nil {
		ctx.WriteByte('(')
		ctx.FormatNode(&n.ConstraintNames)
		ctx.WriteByte(')')
	} else {
		ctx.WriteString("ALL")
	}
}
