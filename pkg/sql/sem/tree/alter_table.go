// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

// AlterTable represents an ALTER TABLE statement.
type AlterTable struct {
	IfExists bool
	Table    NormalizableTableName
	Cmds     AlterTableCmds
}

// Format implements the NodeFormatter interface.
func (node *AlterTable) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TABLE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Table)
	ctx.FormatNode(&node.Cmds)
}

// AlterTableCmds represents a list of table alterations.
type AlterTableCmds []AlterTableCmd

// Format implements the NodeFormatter interface.
func (node *AlterTableCmds) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(",")
		}
		ctx.FormatNode(n)
	}
}

// AlterTableCmd represents a table modification operation.
type AlterTableCmd interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types
	// (AlterTable*) conform to the AlterTableCmd interface.
	alterTableCmd()
}

func (*AlterTableAddColumn) alterTableCmd()          {}
func (*AlterTableAddConstraint) alterTableCmd()      {}
func (*AlterTableDropColumn) alterTableCmd()         {}
func (*AlterTableDropConstraint) alterTableCmd()     {}
func (*AlterTableDropNotNull) alterTableCmd()        {}
func (*AlterTableSetAudit) alterTableCmd()           {}
func (*AlterTableSetDefault) alterTableCmd()         {}
func (*AlterTableValidateConstraint) alterTableCmd() {}
func (*AlterTablePartitionBy) alterTableCmd()        {}

var _ AlterTableCmd = &AlterTableAddColumn{}
var _ AlterTableCmd = &AlterTableAddConstraint{}
var _ AlterTableCmd = &AlterTableDropColumn{}
var _ AlterTableCmd = &AlterTableDropConstraint{}
var _ AlterTableCmd = &AlterTableDropNotNull{}
var _ AlterTableCmd = &AlterTableSetAudit{}
var _ AlterTableCmd = &AlterTableSetDefault{}
var _ AlterTableCmd = &AlterTableValidateConstraint{}
var _ AlterTableCmd = &AlterTablePartitionBy{}

// ColumnMutationCmd is the subset of AlterTableCmds that modify an
// existing column.
type ColumnMutationCmd interface {
	AlterTableCmd
	GetColumn() Name
}

// AlterTableAddColumn represents an ADD COLUMN command.
type AlterTableAddColumn struct {
	ColumnKeyword bool
	IfNotExists   bool
	ColumnDef     *ColumnTableDef
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAddColumn) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD ")
	if node.ColumnKeyword {
		ctx.WriteString("COLUMN ")
	}
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(node.ColumnDef)
}

// ValidationBehavior specifies whether or not a constraint is validated.
type ValidationBehavior int

const (
	// ValidationDefault is the default validation behavior (immediate).
	ValidationDefault ValidationBehavior = iota
	// ValidationSkip skips validation of any existing data.
	ValidationSkip
)

// AlterTableAddConstraint represents an ADD CONSTRAINT command.
type AlterTableAddConstraint struct {
	ConstraintDef      ConstraintTableDef
	ValidationBehavior ValidationBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAddConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD ")
	ctx.FormatNode(node.ConstraintDef)
	if node.ValidationBehavior == ValidationSkip {
		ctx.WriteString(" NOT VALID")
	}
}

// AlterTableDropColumn represents a DROP COLUMN command.
type AlterTableDropColumn struct {
	ColumnKeyword bool
	IfExists      bool
	Column        Name
	DropBehavior  DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropColumn) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP ")
	if node.ColumnKeyword {
		ctx.WriteString("COLUMN ")
	}
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Column)
	if node.DropBehavior != DropDefault {
		ctx.Printf(" %s", node.DropBehavior)
	}
}

// AlterTableDropConstraint represents a DROP CONSTRAINT command.
type AlterTableDropConstraint struct {
	IfExists     bool
	Constraint   Name
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP CONSTRAINT ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Constraint)
	if node.DropBehavior != DropDefault {
		ctx.Printf(" %s", node.DropBehavior)
	}
}

// AlterTableValidateConstraint represents a VALIDATE CONSTRAINT command.
type AlterTableValidateConstraint struct {
	Constraint Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTableValidateConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" VALIDATE CONSTRAINT ")
	ctx.FormatNode(&node.Constraint)
}

// AlterTableSetDefault represents an ALTER COLUMN SET DEFAULT
// or DROP DEFAULT command.
type AlterTableSetDefault struct {
	ColumnKeyword bool
	Column        Name
	Default       Expr
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableSetDefault) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetDefault) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER ")
	if node.ColumnKeyword {
		ctx.WriteString("COLUMN ")
	}
	ctx.FormatNode(&node.Column)
	if node.Default == nil {
		ctx.WriteString(" DROP DEFAULT")
	} else {
		ctx.WriteString(" SET DEFAULT ")
		ctx.FormatNode(node.Default)
	}
}

// AlterTableDropNotNull represents an ALTER COLUMN DROP NOT NULL
// command.
type AlterTableDropNotNull struct {
	ColumnKeyword bool
	Column        Name
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableDropNotNull) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropNotNull) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER ")
	if node.ColumnKeyword {
		ctx.WriteString("COLUMN ")
	}
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" DROP NOT NULL")
}

// AlterTablePartitionBy represents an ALTER TABLE PARTITION BY
// command.
type AlterTablePartitionBy struct {
	*PartitionBy
}

// Format implements the NodeFormatter interface.
func (node *AlterTablePartitionBy) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.PartitionBy)
}

// AuditMode represents a table audit mode
type AuditMode int

const (
	// AuditModeDisable is the default mode - no audit.
	AuditModeDisable AuditMode = iota
	// AuditModeReadWrite enables audit on read or write statements.
	AuditModeReadWrite
)

var auditModeName = [...]string{
	AuditModeDisable:   "OFF",
	AuditModeReadWrite: "READ WRITE",
}

func (m AuditMode) String() string {
	return auditModeName[m]
}

// AlterTableSetAudit represents an ALTER TABLE AUDIT SET statement.
type AlterTableSetAudit struct {
	Mode AuditMode
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetAudit) Format(ctx *FmtCtx) {
	ctx.WriteString(" EXPERIMENTAL_AUDIT SET ")
	ctx.WriteString(node.Mode.String())
}
