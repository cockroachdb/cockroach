// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/types"

// AlterTable represents an ALTER TABLE statement.
type AlterTable struct {
	IfExists bool
	Table    *UnresolvedObjectName
	Cmds     AlterTableCmds
}

// Format implements the NodeFormatter interface.
func (node *AlterTable) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TABLE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Table)
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
func (*AlterTableAlterColumnType) alterTableCmd()    {}
func (*AlterTableAlterPrimaryKey) alterTableCmd()    {}
func (*AlterTableDropColumn) alterTableCmd()         {}
func (*AlterTableDropConstraint) alterTableCmd()     {}
func (*AlterTableDropNotNull) alterTableCmd()        {}
func (*AlterTableDropStored) alterTableCmd()         {}
func (*AlterTableSetNotNull) alterTableCmd()         {}
func (*AlterTableRenameColumn) alterTableCmd()       {}
func (*AlterTableRenameConstraint) alterTableCmd()   {}
func (*AlterTableRenameTable) alterTableCmd()        {}
func (*AlterTableSetAudit) alterTableCmd()           {}
func (*AlterTableSetDefault) alterTableCmd()         {}
func (*AlterTableValidateConstraint) alterTableCmd() {}
func (*AlterTablePartitionBy) alterTableCmd()        {}
func (*AlterTableInjectStats) alterTableCmd()        {}

var _ AlterTableCmd = &AlterTableAddColumn{}
var _ AlterTableCmd = &AlterTableAddConstraint{}
var _ AlterTableCmd = &AlterTableAlterColumnType{}
var _ AlterTableCmd = &AlterTableDropColumn{}
var _ AlterTableCmd = &AlterTableDropConstraint{}
var _ AlterTableCmd = &AlterTableDropNotNull{}
var _ AlterTableCmd = &AlterTableDropStored{}
var _ AlterTableCmd = &AlterTableSetNotNull{}
var _ AlterTableCmd = &AlterTableRenameColumn{}
var _ AlterTableCmd = &AlterTableRenameConstraint{}
var _ AlterTableCmd = &AlterTableRenameTable{}
var _ AlterTableCmd = &AlterTableSetAudit{}
var _ AlterTableCmd = &AlterTableSetDefault{}
var _ AlterTableCmd = &AlterTableValidateConstraint{}
var _ AlterTableCmd = &AlterTablePartitionBy{}
var _ AlterTableCmd = &AlterTableInjectStats{}

// ColumnMutationCmd is the subset of AlterTableCmds that modify an
// existing column.
type ColumnMutationCmd interface {
	AlterTableCmd
	GetColumn() Name
}

// AlterTableAddColumn represents an ADD COLUMN command.
type AlterTableAddColumn struct {
	IfNotExists bool
	ColumnDef   *ColumnTableDef
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAddColumn) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD COLUMN ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(node.ColumnDef)
}

// HoistAddColumnConstraints converts column constraints in ADD COLUMN commands,
// stored in node.Cmds, into top-level commands to add those constraints.
// Currently, this only applies to checks. For example, the ADD COLUMN in
//
//     ALTER TABLE t ADD COLUMN a INT CHECK (a < 1)
//
// is transformed into two commands, as in
//
//     ALTER TABLE t ADD COLUMN a INT, ADD CONSTRAINT check_a CHECK (a < 1)
//
// (with an auto-generated name).
//
// Note that some SQL databases require that a constraint attached to a column
// to refer only to the column it is attached to. We follow Postgres' behavior,
// however, in omitting this restriction by blindly hoisting all column
// constraints. For example, the following statement is accepted in
// CockroachDB and Postgres, but not necessarily other SQL databases:
//
//     ALTER TABLE t ADD COLUMN a INT CHECK (a < b)
//
func (node *AlterTable) HoistAddColumnConstraints() {
	var normalizedCmds AlterTableCmds

	for _, cmd := range node.Cmds {
		normalizedCmds = append(normalizedCmds, cmd)

		if t, ok := cmd.(*AlterTableAddColumn); ok {
			d := t.ColumnDef
			for _, checkExpr := range d.CheckExprs {
				normalizedCmds = append(normalizedCmds,
					&AlterTableAddConstraint{
						ConstraintDef: &CheckConstraintTableDef{
							Expr: checkExpr.Expr,
							Name: checkExpr.ConstraintName,
						},
						ValidationBehavior: ValidationDefault,
					},
				)
			}
			d.CheckExprs = nil
		}
	}
	node.Cmds = normalizedCmds
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

// AlterTableAlterColumnType represents an ALTER TABLE ALTER COLUMN TYPE command.
type AlterTableAlterColumnType struct {
	Collation string
	Column    Name
	ToType    *types.T
	Using     Expr
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAlterColumnType) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" SET DATA TYPE ")
	ctx.WriteString(node.ToType.SQLString())
	if len(node.Collation) > 0 {
		ctx.WriteString(" COLLATE ")
		ctx.WriteString(node.Collation)
	}
	if node.Using != nil {
		ctx.WriteString(" USING ")
		ctx.FormatNode(node.Using)
	}
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableAlterColumnType) GetColumn() Name {
	return node.Column
}

// AlterTableAlterPrimaryKey represents an ALTER TABLE ALTER PRIMARY KEY command.
type AlterTableAlterPrimaryKey struct {
	TableIndex TableIndexName
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAlterPrimaryKey) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER PRIMARY KEY USING INDEX ")
	ctx.FormatNode(&node.TableIndex)
}

// AlterTableDropColumn represents a DROP COLUMN command.
type AlterTableDropColumn struct {
	IfExists     bool
	Column       Name
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropColumn) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP COLUMN ")
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

// AlterTableRenameTable represents an ALTE RTABLE RENAME TO command.
type AlterTableRenameTable struct {
	NewName TableName
}

// Format implements the NodeFormatter interface.
func (node *AlterTableRenameTable) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.NewName)
}

// AlterTableRenameColumn represents an ALTER TABLE RENAME [COLUMN] command.
type AlterTableRenameColumn struct {
	Column  Name
	NewName Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTableRenameColumn) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewName)
}

// AlterTableRenameConstraint represents an ALTER TABLE RENAME CONSTRAINT command.
type AlterTableRenameConstraint struct {
	Constraint Name
	NewName    Name
}

// Format implements the NodeFormatter interface.
func (node *AlterTableRenameConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME CONSTRAINT ")
	ctx.FormatNode(&node.Constraint)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewName)
}

// AlterTableSetDefault represents an ALTER COLUMN SET DEFAULT
// or DROP DEFAULT command.
type AlterTableSetDefault struct {
	Column  Name
	Default Expr
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableSetDefault) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetDefault) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	if node.Default == nil {
		ctx.WriteString(" DROP DEFAULT")
	} else {
		ctx.WriteString(" SET DEFAULT ")
		ctx.FormatNode(node.Default)
	}
}

// AlterTableSetNotNull represents an ALTER COLUMN SET NOT NULL
// command.
type AlterTableSetNotNull struct {
	Column Name
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableSetNotNull) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetNotNull) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" SET NOT NULL")
}

// AlterTableDropNotNull represents an ALTER COLUMN DROP NOT NULL
// command.
type AlterTableDropNotNull struct {
	Column Name
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableDropNotNull) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropNotNull) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" DROP NOT NULL")
}

// AlterTableDropStored represents an ALTER COLUMN DROP STORED command
// to remove the computed-ness from a column.
type AlterTableDropStored struct {
	Column Name
}

// GetColumn implemnets the ColumnMutationCmd interface.
func (node *AlterTableDropStored) GetColumn() Name {
	return node.Column
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropStored) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" DROP STORED")
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

// AlterTableInjectStats represents an ALTER TABLE INJECT STATISTICS statement.
type AlterTableInjectStats struct {
	Stats Expr
}

// Format implements the NodeFormatter interface.
func (node *AlterTableInjectStats) Format(ctx *FmtCtx) {
	ctx.WriteString(" INJECT STATISTICS ")
	ctx.FormatNode(node.Stats)
}
