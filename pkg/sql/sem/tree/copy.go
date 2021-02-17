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

import "github.com/cockroachdb/errors"

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
			return errors.New("destination option specified multiple times")
		}
		o.Destination = other.Destination
	}
	if other.CopyFormat != CopyFormatText {
		if o.CopyFormat != CopyFormatText {
			return errors.New("format option specified multiple times")
		}
		o.CopyFormat = other.CopyFormat
	}
	if other.Delimiter != nil {
		if o.Delimiter != nil {
			return errors.New("delimiter option specified multiple times")
		}
		o.Delimiter = other.Delimiter
	}
	if other.Null != nil {
		if o.Null != nil {
			return errors.New("null option specified multiple times")
		}
		o.Null = other.Null
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
