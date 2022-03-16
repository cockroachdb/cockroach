// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// CreateChangefeed represents a CREATE CHANGEFEED statement.
type CreateChangefeed struct {
	Targets ChangefeedTargets
	SinkURI Expr
	Options KVOptions
}

var _ Statement = &CreateChangefeed{}

// Format implements the NodeFormatter interface.
func (node *CreateChangefeed) Format(ctx *FmtCtx) {
	if node.SinkURI != nil {
		ctx.WriteString("CREATE ")
	} else {
		// Sinkless feeds don't really CREATE anything, so the syntax omits the
		// prefix. They're also still EXPERIMENTAL, so they get marked as such.
		ctx.WriteString("EXPERIMENTAL ")
	}
	ctx.WriteString("CHANGEFEED FOR ")
	ctx.FormatNode(&node.Targets)
	if node.SinkURI != nil {
		ctx.WriteString(" INTO ")
		ctx.FormatNode(node.SinkURI)
	}
	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// ChangefeedTarget represents a database object to be watched by a changefeed.
type ChangefeedTarget struct {
	TableName  TablePattern
	FamilyName Name
}

// Format implements the NodeFormatter interface.
func (ct *ChangefeedTarget) Format(ctx *FmtCtx) {
	ctx.WriteString("TABLE ")
	ctx.FormatNode(ct.TableName)
	if ct.FamilyName != "" {
		ctx.WriteString(" FAMILY ")
		ctx.FormatNode(&ct.FamilyName)
	}
}

// ChangefeedTargets represents a list of database objects to be watched by a changefeed.
type ChangefeedTargets []ChangefeedTarget

// Format implements the NodeFormatter interface.
func (cts *ChangefeedTargets) Format(ctx *FmtCtx) {
	for i, ct := range *cts {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&ct)
	}
}
