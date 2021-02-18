// Copyright 2021 The Cockroach Authors.
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

// ReplicationOptions describes options for streaming replication.
type ReplicationOptions struct {
	Cursor   Expr
	Detached bool
}

var _ NodeFormatter = &ReplicationOptions{}

// Format implements the NodeFormatter interface
func (o *ReplicationOptions) Format(ctx *FmtCtx) {
	var addSep bool
	maybeAddSep := func() {
		if addSep {
			ctx.WriteString(", ")
		}
		addSep = true
	}
	if o.Cursor != nil {
		ctx.WriteString("CURSOR=")
		o.Cursor.Format(ctx)
		addSep = true
	}

	if o.Detached {
		maybeAddSep()
		ctx.WriteString("DETACHED")
	}
}

// CombineWith merges other backup options into this backup options struct.
// An error is returned if the same option merged multiple times.
func (o *ReplicationOptions) CombineWith(other *ReplicationOptions) error {
	if o.Cursor != nil {
		if other.Cursor != nil {
			return errors.New("CURSOR option specified multiple times")
		}
	} else {
		o.Cursor = other.Cursor
	}

	if o.Detached {
		if other.Detached {
			return errors.New("detached option specified multiple times")
		}
	} else {
		o.Detached = other.Detached
	}

	return nil
}

// IsDefault returns true if this backup options struct has default value.
func (o ReplicationOptions) IsDefault() bool {
	options := ReplicationOptions{}
	return o.Cursor == options.Cursor && o.Detached == options.Detached
}

// ReplicationStream represents a CREATE REPLICATION STREAM statement.
type ReplicationStream struct {
	Targets TargetList
	SinkURI Expr
	Options ReplicationOptions
}

var _ Statement = &ReplicationStream{}

// Format implements the NodeFormatter interface.
func (n *ReplicationStream) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE REPLICATION STREAM FOR ")
	ctx.FormatNode(&n.Targets)

	if n.SinkURI != nil {
		ctx.WriteString(" INTO ")
		ctx.FormatNode(n.SinkURI)
	}
	if !n.Options.IsDefault() {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&n.Options)
	}
}
