// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// AlterType represents an ALTER TYPE statement.
type AlterType struct {
	Type *UnresolvedObjectName
	Cmd  AlterTypeCmd
}

// Format implements the NodeFormatter interface.
func (node *AlterType) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TYPE ")
	ctx.FormatNode(node.Type)
	ctx.FormatNode(node.Cmd)
}

// AlterTypeCmd represents a type modification operation.
type AlterTypeCmd interface {
	NodeFormatter
	alterTypeCmd()
	// TelemetryName returns the counter name to use for telemetry purposes.
	TelemetryName() string
}

func (*AlterTypeAddValue) alterTypeCmd()    {}
func (*AlterTypeRenameValue) alterTypeCmd() {}
func (*AlterTypeRename) alterTypeCmd()      {}
func (*AlterTypeSetSchema) alterTypeCmd()   {}
func (*AlterTypeOwner) alterTypeCmd()       {}
func (*AlterTypeDropValue) alterTypeCmd()   {}

var _ AlterTypeCmd = &AlterTypeAddValue{}
var _ AlterTypeCmd = &AlterTypeRenameValue{}
var _ AlterTypeCmd = &AlterTypeRename{}
var _ AlterTypeCmd = &AlterTypeSetSchema{}
var _ AlterTypeCmd = &AlterTypeOwner{}
var _ AlterTypeCmd = &AlterTypeDropValue{}

// AlterTypeAddValue represents an ALTER TYPE ADD VALUE command.
type AlterTypeAddValue struct {
	NewVal      EnumValue
	IfNotExists bool
	Placement   *AlterTypeAddValuePlacement
}

// Format implements the NodeFormatter interface.
func (node *AlterTypeAddValue) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD VALUE ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.NewVal)
	if node.Placement != nil {
		if node.Placement.Before {
			ctx.WriteString(" BEFORE ")
		} else {
			ctx.WriteString(" AFTER ")
		}
		ctx.FormatNode(&node.Placement.ExistingVal)
	}
}

// TelemetryName implements the AlterTypeCmd interface.
func (node *AlterTypeAddValue) TelemetryName() string {
	return "add_value"
}

// AlterTypeAddValuePlacement represents the placement clause for an ALTER
// TYPE ADD VALUE command ([BEFORE | AFTER] value).
type AlterTypeAddValuePlacement struct {
	Before      bool
	ExistingVal EnumValue
}

// AlterTypeRenameValue represents an ALTER TYPE RENAME VALUE command.
type AlterTypeRenameValue struct {
	OldVal EnumValue
	NewVal EnumValue
}

// Format implements the NodeFormatter interface.
func (node *AlterTypeRenameValue) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME VALUE ")
	ctx.FormatNode(&node.OldVal)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewVal)
}

// TelemetryName implements the AlterTypeCmd interface.
func (node *AlterTypeRenameValue) TelemetryName() string {
	return "rename_value"
}

// AlterTypeDropValue represents an ALTER TYPE DROP VALUE command.
type AlterTypeDropValue struct {
	Val EnumValue
}

// Format implements the NodeFormatter interface.
func (node *AlterTypeDropValue) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP VALUE ")
	ctx.FormatNode(&node.Val)
}

// TelemetryName implements the AlterTypeCmd interface.
func (node *AlterTypeDropValue) TelemetryName() string {
	return "drop_value"
}

// AlterTypeRename represents an ALTER TYPE RENAME command.
type AlterTypeRename struct {
	NewName Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTypeRename) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.NewName)
}

// TelemetryName implements the AlterTypeCmd interface.
func (node *AlterTypeRename) TelemetryName() string {
	return "rename"
}

// AlterTypeSetSchema represents an ALTER TYPE SET SCHEMA command.
type AlterTypeSetSchema struct {
	Schema Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTypeSetSchema) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET SCHEMA ")
	ctx.FormatNode(&node.Schema)
}

// TelemetryName implements the AlterTypeCmd interface.
func (node *AlterTypeSetSchema) TelemetryName() string {
	return "set_schema"
}

// AlterTypeOwner represents an ALTER TYPE OWNER TO command.
type AlterTypeOwner struct {
	Owner RoleSpec
}

// Format implements the NodeFormatter interface.
func (node *AlterTypeOwner) Format(ctx *FmtCtx) {
	ctx.WriteString(" OWNER TO ")
	ctx.FormatNode(&node.Owner)
}

// TelemetryName implements the AlterTypeCmd interface.
func (node *AlterTypeOwner) TelemetryName() string {
	return "owner"
}
