// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

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
	Database *UnresolvedObjectName
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
		ctx.FormatNode(n.Database)
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

// NamedIndexes flattens the indexes named by option.
func (n *InspectOptions) NamedIndexes() TableIndexNames {
	var names TableIndexNames
	for _, option := range *n {
		if opt, ok := option.(*InspectOptionIndex); ok {
			names = append(names, opt.IndexNames...)
		}
	}

	return names
}

// HasIndexAll checks if the options include an INDEX ALL option.
func (n *InspectOptions) HasIndexAll() bool {
	for _, option := range *n {
		if _, ok := option.(*InspectOptionIndexAll); ok {
			return true
		}
	}
	return false
}

// Validate checks for internal consistency of the INSPECT command.
func (n *Inspect) Validate() error {
	if err := n.Options.validate(); err != nil {
		return err
	}

	// TODO(155056): Better validate index names from the options with the name
	// of the database or table from the command.
	// TODO(148365): Check for duplicated index names (including the name of the
	// database or table from the command).
	for _, index := range n.Options.NamedIndexes() {
		switch n.Typ {
		case InspectTable:
			if index.Table.ObjectName != "" && n.Table.Object() != index.Table.Object() {
				return pgerror.Newf(pgcode.InvalidName, "index %q does not belong to table %q", index.String(), n.Table.String())
			}
		case InspectDatabase:
			if index.Table.ExplicitCatalog && n.Database.Object() != index.Table.Catalog() {
				return pgerror.Newf(pgcode.InvalidName, "index %q does not belong to database %q", index.String(), n.Database.String())
			}
		}
	}

	return nil
}

// validate checks for internal consistency of options on the INSPECT command.
func (n *InspectOptions) validate() error {
	var hasOptionIndex, hasOptionIndexAll bool

	for _, option := range *n {
		switch option.(type) {
		case *InspectOptionIndex:
			hasOptionIndex = true
		case *InspectOptionIndexAll:
			hasOptionIndexAll = true
		default:
			return fmt.Errorf("unknown inspect option: %T", option)
		}
	}

	// INDEX and INDEX ALL are mutually exclusive.
	if hasOptionIndex && hasOptionIndexAll {
		return pgerror.Newf(pgcode.Syntax,
			"conflicting INSPECT options: INDEX and INDEX ALL cannot be used together")
	}

	return nil
}

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
func (n *InspectOptionIndex) String() string   { return AsString(n) }

// InspectOptionIndex represents an INDEX inspect check.
type InspectOptionIndex struct {
	IndexNames TableIndexNames
}

// Format implements the NodeFormatter interface.
func (n *InspectOptionIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("INDEX (")
	ctx.FormatNode(&n.IndexNames)
	ctx.WriteByte(')')
}

// InspectOptionIndexAll implements the InspectOption interface
func (*InspectOptionIndexAll) inspectOptionType() {}
func (n *InspectOptionIndexAll) String() string   { return AsString(n) }

// InspectOptionIndexAll represents an `INDEX ALL` inspect option.
type InspectOptionIndexAll struct{}

// Format implements the NodeFormatter interface.
func (n *InspectOptionIndexAll) Format(ctx *FmtCtx) {
	ctx.WriteString("INDEX ALL")
}
