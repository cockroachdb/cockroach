// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// CopyOptions describes options for COPY execution.
type CopyOptions struct {
	Destination Expr
	CopyFormat  CopyFormat
	Delimiter   Expr
	Null        Expr
	Escape      *StrVal
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
			ctx.WriteString(" ")
		}
		addSep = true
	}
	if o.CopyFormat != CopyFormatText {
		maybeAddSep()
		switch o.CopyFormat {
		case CopyFormatBinary:
			ctx.WriteString("BINARY")
		case CopyFormatCSV:
			ctx.WriteString("CSV")
		}
	}
	if o.Delimiter != nil {
		maybeAddSep()
		ctx.WriteString("DELIMITER ")
		ctx.FormatNode(o.Delimiter)
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
		ctx.WriteString("destination = ")
		ctx.FormatNode(o.Destination)
		addSep = true
	}
	if o.Escape != nil {
		maybeAddSep()
		ctx.WriteString("ESCAPE ")
		ctx.FormatNode(o.Escape)
	}
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
	if other.CopyFormat != CopyFormatText {
		if o.CopyFormat != CopyFormatText {
			return pgerror.Newf(pgcode.Syntax, "format option specified multiple times")
		}
		o.CopyFormat = other.CopyFormat
	}
	if other.Delimiter != nil {
		if o.Delimiter != nil {
			return pgerror.Newf(pgcode.Syntax, "delimiter option specified multiple times")
		}
		o.Delimiter = other.Delimiter
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
