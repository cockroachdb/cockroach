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

// ChangefeedExportSpec configures changefeed to run the export
// table instead of regular operation.  Export table mode
// runs backfill until specified AsOf time.
// If AsOf is unspecified, export runs to the time when the changefeed
// export started.
type ChangefeedExportSpec struct {
	AsOf AsOfClause
}

// CreateChangefeed represents a CREATE CHANGEFEED statement.
type CreateChangefeed struct {
	Targets    TargetList
	SinkURI    Expr
	Options    KVOptions
	ExportSpec *ChangefeedExportSpec
}

var _ Statement = &CreateChangefeed{}

func (node *CreateChangefeed) formatForExport(ctx *FmtCtx) {
	ctx.WriteString("CREATE EXPORT CHANGEFEED FOR ")
	ctx.FormatNode(&node.Targets)
	if node.ExportSpec.AsOf.Expr != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.ExportSpec.AsOf)
	}
	ctx.WriteString(" INTO ")
	ctx.FormatNode(node.SinkURI)
	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// Format implements the NodeFormatter interface.
func (node *CreateChangefeed) Format(ctx *FmtCtx) {
	if node.IsExportTable() {
		node.formatForExport(ctx)
		return
	}

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

// IsExportTable returns true is this changefeed is used for export table.
func (node *CreateChangefeed) IsExportTable() bool {
	return node.ExportSpec != nil
}
