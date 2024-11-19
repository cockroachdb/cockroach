// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// CopyFrom represents a COPY FROM statement.
type CopyFrom struct {
	Table   TableName
	Columns NameList
	Stdin   bool
	Options CopyOptions
}

// CopyTo represents a COPY TO statement.
type CopyTo struct {
	Table     TableName
	Columns   NameList
	Statement Statement
	Options   CopyOptions
}

// Format implements the NodeFormatter interface.
func (node *CopyTo) Format(ctx *FmtCtx) {
	ctx.WriteString("COPY ")
	if node.Statement != nil {
		ctx.WriteString("(")
		ctx.FormatNode(node.Statement)
		ctx.WriteString(")")
	} else {
		ctx.FormatNode(&node.Table)
		if len(node.Columns) > 0 {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.Columns)
			ctx.WriteString(")")
		}
	}
	ctx.WriteString(" TO STDOUT")
	if !node.Options.IsDefault() {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// CopyOptions describes options for COPY execution.
type CopyOptions struct {
	Destination Expr
	CopyFormat  CopyFormat
	Delimiter   Expr
	Null        Expr
	Escape      *StrVal
	Header      bool
	Quote       *StrVal
	Encoding    *StrVal

	// Additional flags are needed to keep track of whether explicit default
	// values were already set.
	HasFormat bool
	HasHeader bool
}

var _ NodeFormatter = &CopyOptions{}

// Format implements the NodeFormatter interface.
func (node *CopyFrom) Format(ctx *FmtCtx) {
	ctx.WriteString("COPY ")
	ctx.FormatNode(&node.Table)
	if len(node.Columns) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(&node.Columns)
		ctx.WriteString(")")
	}
	ctx.WriteString(" FROM ")
	if node.Stdin {
		ctx.WriteString("STDIN")
	}
	if !node.Options.IsDefault() {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// Format implements the NodeFormatter interface
func (o *CopyOptions) Format(ctx *FmtCtx) {
	var addSep bool
	maybeAddSep := func() {
		if addSep {
			ctx.WriteString(", ")
		}
		addSep = true
	}
	ctx.WriteString("(")
	if o.HasFormat {
		maybeAddSep()
		switch o.CopyFormat {
		case CopyFormatBinary:
			ctx.WriteString("FORMAT BINARY")
		case CopyFormatCSV:
			ctx.WriteString("FORMAT CSV")
		case CopyFormatText:
			ctx.WriteString("FORMAT TEXT")
		}
	}
	if o.Delimiter != nil {
		maybeAddSep()
		ctx.WriteString("DELIMITER ")
		ctx.FormatNode(o.Delimiter)
		addSep = true
	}
	if o.Encoding != nil {
		maybeAddSep()
		ctx.WriteString("ENCODING ")
		ctx.FormatNode(o.Encoding)
		addSep = true
	}
	if o.Null != nil {
		maybeAddSep()
		ctx.WriteString("NULL ")
		ctx.FormatNode(o.Null)
		addSep = true
	}
	if o.Destination != nil {
		maybeAddSep()
		// Lowercase because that's what has historically been produced
		// by copy_file_upload.go, so this will provide backward
		// compatibility with older servers.
		ctx.WriteString("DESTINATION ")
		ctx.FormatURI(o.Destination)
		addSep = true
	}
	if o.Escape != nil {
		maybeAddSep()
		ctx.WriteString("ESCAPE ")
		ctx.FormatNode(o.Escape)
	}
	if o.HasHeader {
		maybeAddSep()
		ctx.WriteString("HEADER ")
		if o.Header {
			ctx.WriteString("true")
		} else {
			ctx.WriteString("false")
		}
	}
	if o.Quote != nil {
		maybeAddSep()
		ctx.WriteString("QUOTE ")
		ctx.FormatNode(o.Quote)
	}
	ctx.WriteString(")")
}

// IsDefault returns true if this struct has default value.
func (o CopyOptions) IsDefault() bool {
	return o == CopyOptions{}
}

// CombineWith merges other options into this struct. An error is returned if
// the same option merged multiple times.
func (o *CopyOptions) CombineWith(other *CopyOptions) error {
	if other.Destination != nil {
		if o.Destination != nil {
			return pgerror.Newf(pgcode.Syntax, "destination option specified multiple times")
		}
		o.Destination = other.Destination
	}
	if other.HasFormat {
		if o.HasFormat {
			return pgerror.Newf(pgcode.Syntax, "format option specified multiple times")
		}
		o.CopyFormat = other.CopyFormat
		o.HasFormat = true
	}
	if other.Delimiter != nil {
		if o.Delimiter != nil {
			return pgerror.Newf(pgcode.Syntax, "delimiter option specified multiple times")
		}
		o.Delimiter = other.Delimiter
	}
	if other.Encoding != nil {
		if o.Encoding != nil {
			return pgerror.Newf(pgcode.Syntax, "encoding option specified multiple times")
		}
		o.Encoding = other.Encoding
	}
	if other.Null != nil {
		if o.Null != nil {
			return pgerror.Newf(pgcode.Syntax, "null option specified multiple times")
		}
		o.Null = other.Null
	}
	if other.Escape != nil {
		if o.Escape != nil {
			return pgerror.Newf(pgcode.Syntax, "escape option specified multiple times")
		}
		o.Escape = other.Escape
	}
	if other.HasHeader {
		if o.HasHeader {
			return pgerror.Newf(pgcode.Syntax, "header option specified multiple times")
		}
		o.Header = other.Header
		o.HasHeader = true
	}
	if other.Quote != nil {
		if o.Quote != nil {
			return pgerror.Newf(pgcode.Syntax, "quote option specified multiple times")
		}
		o.Quote = other.Quote
	}
	return nil
}

// CopyFormat identifies a COPY data format.
type CopyFormat int

// Valid values for CopyFormat.
const (
	CopyFormatText CopyFormat = iota
	CopyFormatBinary
	CopyFormatCSV
)
