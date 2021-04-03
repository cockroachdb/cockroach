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

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

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
	// TelemetryCounter returns the telemetry counter to increment
	// when this command is used.
	TelemetryCounter() telemetry.Counter
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
func (*AlterTableSetAudit) alterTableCmd()           {}
func (*AlterTableSetDefault) alterTableCmd()         {}
func (*AlterTableSetVisible) alterTableCmd()         {}
func (*AlterTableValidateConstraint) alterTableCmd() {}
func (*AlterTablePartitionByTable) alterTableCmd()   {}
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
var _ AlterTableCmd = &AlterTableSetAudit{}
var _ AlterTableCmd = &AlterTableSetDefault{}
var _ AlterTableCmd = &AlterTableSetVisible{}
var _ AlterTableCmd = &AlterTableValidateConstraint{}
var _ AlterTableCmd = &AlterTablePartitionByTable{}
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

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableAddColumn) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "add_column")
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
			if d.HasFKConstraint() {
				var targetCol NameList
				if d.References.Col != "" {
					targetCol = append(targetCol, d.References.Col)
				}
				fk := &ForeignKeyConstraintTableDef{
					Table:    *d.References.Table,
					FromCols: NameList{d.Name},
					ToCols:   targetCol,
					Name:     d.References.ConstraintName,
					Actions:  d.References.Actions,
					Match:    d.References.Match,
				}
				constraint := &AlterTableAddConstraint{
					ConstraintDef:      fk,
					ValidationBehavior: ValidationDefault,
				}
				normalizedCmds = append(normalizedCmds, constraint)
				d.References.Table = nil
				telemetry.Inc(sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "add_column.references"))
			}
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

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableAddConstraint) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "add_constraint")
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
	ToType    ResolvableTypeReference
	Using     Expr
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableAlterColumnType) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "alter_column_type")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAlterColumnType) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" SET DATA TYPE ")
	ctx.WriteString(node.ToType.SQLString())
	if len(node.Collation) > 0 {
		ctx.WriteString(" COLLATE ")
		lex.EncodeLocaleName(&ctx.Buffer, node.Collation)
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
	Columns    IndexElemList
	Interleave *InterleaveDef
	Sharded    *ShardedIndexDef
	Name       Name
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableAlterPrimaryKey) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "alter_primary_key")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAlterPrimaryKey) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER PRIMARY KEY USING COLUMNS (")
	ctx.FormatNode(&node.Columns)
	ctx.WriteString(")")
	if node.Sharded != nil {
		ctx.FormatNode(node.Sharded)
	}
	if node.Interleave != nil {
		ctx.FormatNode(node.Interleave)
	}
}

// AlterTableDropColumn represents a DROP COLUMN command.
type AlterTableDropColumn struct {
	IfExists     bool
	Column       Name
	DropBehavior DropBehavior
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableDropColumn) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "drop_column")
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

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableDropConstraint) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "drop_constraint")
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

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableValidateConstraint) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "validate_constraint")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableValidateConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" VALIDATE CONSTRAINT ")
	ctx.FormatNode(&node.Constraint)
}

// AlterTableRenameColumn represents an ALTER TABLE RENAME [COLUMN] command.
type AlterTableRenameColumn struct {
	Column  Name
	NewName Name
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableRenameColumn) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "rename_column")
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

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableRenameConstraint) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "rename_constraint")
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

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableSetDefault) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "set_default")
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

// AlterTableSetVisible represents an ALTER COLUMN SET VISIBLE or NOT VISIBLE command.
type AlterTableSetVisible struct {
	Column  Name
	Visible bool
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableSetVisible) GetColumn() Name {
	return node.Column
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableSetVisible) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "set_visible")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetVisible) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" SET ")
	if !node.Visible {
		ctx.WriteString("NOT ")
	}
	ctx.WriteString("VISIBLE")
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

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableSetNotNull) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "set_not_null")
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

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableDropNotNull) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "drop_not_null")
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

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableDropStored) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "drop_stored")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropStored) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" DROP STORED")
}

// AlterTablePartitionByTable represents an ALTER TABLE PARTITION [ALL]
// BY command.
type AlterTablePartitionByTable struct {
	*PartitionByTable
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTablePartitionByTable) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "partition_by")
}

// Format implements the NodeFormatter interface.
func (node *AlterTablePartitionByTable) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.PartitionByTable)
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

// TelemetryName returns a friendly string for use in telemetry that represents
// the AuditMode.
func (m AuditMode) TelemetryName() string {
	return strings.ReplaceAll(strings.ToLower(m.String()), " ", "_")
}

// AlterTableSetAudit represents an ALTER TABLE AUDIT SET statement.
type AlterTableSetAudit struct {
	Mode AuditMode
}

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableSetAudit) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "set_audit")
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

// TelemetryCounter implements the AlterTableCmd interface.
func (node *AlterTableInjectStats) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "inject_stats")
}

// Format implements the NodeFormatter interface.
func (node *AlterTableInjectStats) Format(ctx *FmtCtx) {
	ctx.WriteString(" INJECT STATISTICS ")
	ctx.FormatNode(node.Stats)
}

// AlterTableLocality represents an ALTER TABLE LOCALITY command.
type AlterTableLocality struct {
	Name     *UnresolvedObjectName
	IfExists bool
	Locality *Locality
}

var _ Statement = &AlterTableLocality{}

// Format implements the NodeFormatter interface.
func (node *AlterTableLocality) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TABLE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Name)
	ctx.WriteString(" SET ")
	ctx.FormatNode(node.Locality)
}

// AlterTableSetSchema represents an ALTER TABLE SET SCHEMA command.
type AlterTableSetSchema struct {
	Name           *UnresolvedObjectName
	Schema         Name
	IfExists       bool
	IsView         bool
	IsMaterialized bool
	IsSequence     bool
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetSchema) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER")
	if node.IsView {
		if node.IsMaterialized {
			ctx.WriteString(" MATERIALIZED")
		}
		ctx.WriteString(" VIEW ")
	} else if node.IsSequence {
		ctx.WriteString(" SEQUENCE ")
	} else {
		ctx.WriteString(" TABLE ")
	}
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Name)
	ctx.WriteString(" SET SCHEMA ")
	ctx.FormatNode(&node.Schema)
}

// TelemetryCounter returns the telemetry counter to increment
// when this command is used.
func (node *AlterTableSetSchema) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra(
		GetTableType(node.IsSequence, node.IsView, node.IsMaterialized),
		"set_schema")
}

// AlterTableOwner represents an ALTER TABLE OWNER TO command.
type AlterTableOwner struct {
	Name *UnresolvedObjectName
	// TODO(solon): Adjust this, see
	// https://github.com/cockroachdb/cockroach/issues/54696
	Owner          security.SQLUsername
	IfExists       bool
	IsView         bool
	IsMaterialized bool
	IsSequence     bool
}

// TelemetryCounter returns the telemetry counter to increment
// when this command is used.
func (node *AlterTableOwner) TelemetryCounter() telemetry.Counter {
	return sqltelemetry.SchemaChangeAlterCounterWithExtra(
		GetTableType(node.IsSequence, node.IsView, node.IsMaterialized),
		"owner_to",
	)
}

// Format implements the NodeFormatter interface.
func (node *AlterTableOwner) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER")
	if node.IsView {
		if node.IsMaterialized {
			ctx.WriteString(" MATERIALIZED")
		}
		ctx.WriteString(" VIEW ")
	} else if node.IsSequence {
		ctx.WriteString(" SEQUENCE ")
	} else {
		ctx.WriteString(" TABLE ")
	}
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Name)
	ctx.WriteString(" OWNER TO ")
	ctx.FormatUsername(node.Owner)
}

// GetTableType returns a string representing the type of table the command
// is operating on.
// It is assumed if the table is not a sequence or a view, then it is a
// regular table.
func GetTableType(isSequence bool, isView bool, isMaterialized bool) string {
	tableType := "table"
	if isSequence {
		tableType = "sequence"
	} else if isView {
		if isMaterialized {
			tableType = "materialized_view"
		} else {
			tableType = "view"
		}
	}

	return tableType
}
