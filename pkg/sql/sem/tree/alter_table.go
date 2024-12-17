// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
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
	// TelemetryName returns the counter name to use for telemetry purposes.
	TelemetryName() string
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
func (*AlterTableSetOnUpdate) alterTableCmd()        {}
func (*AlterTableSetVisible) alterTableCmd()         {}
func (*AlterTableValidateConstraint) alterTableCmd() {}
func (*AlterTablePartitionByTable) alterTableCmd()   {}
func (*AlterTableInjectStats) alterTableCmd()        {}
func (*AlterTableSetStorageParams) alterTableCmd()   {}
func (*AlterTableResetStorageParams) alterTableCmd() {}
func (*AlterTableAddIdentity) alterTableCmd()        {}
func (*AlterTableSetIdentity) alterTableCmd()        {}
func (*AlterTableIdentity) alterTableCmd()           {}
func (*AlterTableDropIdentity) alterTableCmd()       {}
func (*AlterTableSetRLSMode) alterTableCmd()         {}

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
var _ AlterTableCmd = &AlterTableSetOnUpdate{}
var _ AlterTableCmd = &AlterTableSetVisible{}
var _ AlterTableCmd = &AlterTableValidateConstraint{}
var _ AlterTableCmd = &AlterTablePartitionByTable{}
var _ AlterTableCmd = &AlterTableInjectStats{}
var _ AlterTableCmd = &AlterTableSetStorageParams{}
var _ AlterTableCmd = &AlterTableResetStorageParams{}
var _ AlterTableCmd = &AlterTableAddIdentity{}
var _ AlterTableCmd = &AlterTableSetIdentity{}
var _ AlterTableCmd = &AlterTableIdentity{}
var _ AlterTableCmd = &AlterTableDropIdentity{}
var _ AlterTableCmd = &AlterTableSetRLSMode{}

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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableAddColumn) TelemetryName() string {
	return "add_column"
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
//	ALTER TABLE t ADD COLUMN a INT CHECK (a < 1)
//
// is transformed into two commands, as in
//
//	ALTER TABLE t ADD COLUMN a INT, ADD CONSTRAINT check_a CHECK (a < 1)
//
// (with an auto-generated name).
//
// Note that some SQL databases require that a constraint attached to a column
// to refer only to the column it is attached to. We follow Postgres' behavior,
// however, in omitting this restriction by blindly hoisting all column
// constraints. For example, the following statement is accepted in
// CockroachDB and Postgres, but not necessarily other SQL databases:
//
//	ALTER TABLE t ADD COLUMN a INT CHECK (a < b)
func (node *AlterTable) HoistAddColumnConstraints(onHoistedFKConstraint func()) {
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
				onHoistedFKConstraint()
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableAddConstraint) TelemetryName() string {
	return "add_constraint"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableAlterColumnType) TelemetryName() string {
	return "alter_column_type"
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAlterColumnType) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" SET DATA TYPE ")
	ctx.FormatTypeReference(node.ToType)
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
	Columns       IndexElemList
	Sharded       *ShardedIndexDef
	Name          Name
	StorageParams StorageParams
}

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableAlterPrimaryKey) TelemetryName() string {
	return "alter_primary_key"
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAlterPrimaryKey) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER PRIMARY KEY USING COLUMNS (")
	ctx.FormatNode(&node.Columns)
	ctx.WriteString(")")
	if node.Sharded != nil {
		ctx.FormatNode(node.Sharded)
	}
	if node.StorageParams != nil {
		ctx.WriteString(" WITH (")
		ctx.FormatNode(&node.StorageParams)
		ctx.WriteString(")")
	}
}

// AlterTableDropColumn represents a DROP COLUMN command.
type AlterTableDropColumn struct {
	IfExists     bool
	Column       Name
	DropBehavior DropBehavior
}

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableDropColumn) TelemetryName() string {
	return "drop_column"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableDropConstraint) TelemetryName() string {
	return "drop_constraint"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableValidateConstraint) TelemetryName() string {
	return "validate_constraint"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableRenameColumn) TelemetryName() string {
	return "rename_column"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableRenameConstraint) TelemetryName() string {
	return "rename_constraint"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableSetDefault) TelemetryName() string {
	return "set_default"
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

// AlterTableSetOnUpdate represents an ALTER COLUMN ON UPDATE SET
// or DROP ON UPDATE command.
type AlterTableSetOnUpdate struct {
	Column Name
	Expr   Expr
}

// GetColumn implements the ColumnMutationCmd interface.
func (node *AlterTableSetOnUpdate) GetColumn() Name {
	return node.Column
}

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableSetOnUpdate) TelemetryName() string {
	return "set_on_update"
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetOnUpdate) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	if node.Expr == nil {
		ctx.WriteString(" DROP ON UPDATE")
	} else {
		ctx.WriteString(" SET ON UPDATE ")
		ctx.FormatNode(node.Expr)
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableSetVisible) TelemetryName() string {
	return "set_visible"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableSetNotNull) TelemetryName() string {
	return "set_not_null"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableDropNotNull) TelemetryName() string {
	return "drop_not_null"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableDropStored) TelemetryName() string {
	return "drop_stored"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTablePartitionByTable) TelemetryName() string {
	return "partition_by"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableSetAudit) TelemetryName() string {
	return "set_audit"
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

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableInjectStats) TelemetryName() string {
	return "inject_stats"
}

// Format implements the NodeFormatter interface.
func (node *AlterTableInjectStats) Format(ctx *FmtCtx) {
	ctx.WriteString(" INJECT STATISTICS ")
	ctx.FormatNode(node.Stats)
}

// AlterTableSetStorageParams represents a ALTER TABLE SET command.
type AlterTableSetStorageParams struct {
	StorageParams StorageParams
}

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableSetStorageParams) TelemetryName() string {
	return "set_storage_param"
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetStorageParams) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET (")
	ctx.FormatNode(&node.StorageParams)
	ctx.WriteString(")")
}

// AlterTableResetStorageParams represents a ALTER TABLE RESET command.
type AlterTableResetStorageParams struct {
	Params []string
}

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableResetStorageParams) TelemetryName() string {
	return "set_storage_param"
}

// Format implements the NodeFormatter interface.
func (node *AlterTableResetStorageParams) Format(ctx *FmtCtx) {
	buf, f := &ctx.Buffer, ctx.flags
	ctx.WriteString(" RESET (")
	for i, param := range node.Params {
		if i > 0 {
			ctx.WriteString(", ")
		}
		lexbase.EncodeSQLStringWithFlags(buf, param, f.EncodeFlags())
	}
	ctx.WriteString(")")
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

// TelemetryName returns the telemetry counter to increment
// when this command is used.
func (node *AlterTableSetSchema) TelemetryName() string {
	return "set_schema"
}

// AlterTableOwner represents an ALTER TABLE OWNER TO command.
type AlterTableOwner struct {
	Name           *UnresolvedObjectName
	Owner          RoleSpec
	IfExists       bool
	IsView         bool
	IsMaterialized bool
	IsSequence     bool
}

// TelemetryName returns the telemetry counter to increment
// when this command is used.
func (node *AlterTableOwner) TelemetryName() string {
	return "owner_to"
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
	ctx.FormatNode(&node.Owner)
}

// AlterTableAddIdentity represents commands to alter a column to an identity.
type AlterTableAddIdentity struct {
	Column        Name
	Qualification ColumnQualification
}

// GetColumn implemnets the ColumnMutationCmd interface.
func (node *AlterTableAddIdentity) GetColumn() Name {
	return node.Column
}

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableAddIdentity) TelemetryName() string {
	return "add_identity"
}

// Format implements the NodeFormatter interface.
func (node *AlterTableAddIdentity) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	var options SequenceOptions
	switch t := (node.Qualification).(type) {
	case *GeneratedAlwaysAsIdentity:
		ctx.WriteString(" ADD GENERATED ALWAYS AS IDENTITY")
		options = t.SeqOptions
	case *GeneratedByDefAsIdentity:
		ctx.WriteString(" ADD GENERATED BY DEFAULT AS IDENTITY")
		options = t.SeqOptions
	}
	if len(options) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(&options)
		ctx.WriteString(" )")
	}
}

// AlterTableSetIdentity represents commands to alter a column identity type.
type AlterTableSetIdentity struct {
	Column                  Name
	GeneratedAsIdentityType GeneratedIdentityType
}

// GetColumn implemnets the ColumnMutationCmd interface.
func (node *AlterTableSetIdentity) GetColumn() Name {
	return node.Column
}

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableSetIdentity) TelemetryName() string {
	return "set_identity"
}

// Format implements the NodeFormatter interface.
func (node *AlterTableSetIdentity) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	switch node.GeneratedAsIdentityType {
	case GeneratedAlways:
		ctx.WriteString(" SET GENERATED ALWAYS")
	case GeneratedByDefault:
		ctx.WriteString(" SET GENERATED BY DEFAULT")
	}
}

// AlterTableIdentity represents commands to alter a column identity
// sequence options.
type AlterTableIdentity struct {
	Column     Name
	SeqOptions SequenceOptions
}

// GetColumn implemnets the ColumnMutationCmd interface.
func (node *AlterTableIdentity) GetColumn() Name {
	return node.Column
}

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableIdentity) TelemetryName() string {
	return "alter_identity"
}

// Format implements the NodeFormatter interface.
func (node *AlterTableIdentity) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	for i := range node.SeqOptions {
		option := node.SeqOptions[i]
		if option.Name != SeqOptRestart {
			ctx.WriteString(" SET")
		}
		ctx.FormatNode(&SequenceOptions{option})
	}

}

// AlterTableDropIdentity represents an ALTER COLUMN DROP IDENTITY [ IF EXISTS ].
type AlterTableDropIdentity struct {
	Column   Name
	IfExists bool
}

// GetColumn implemnets the ColumnMutationCmd interface.
func (node *AlterTableDropIdentity) GetColumn() Name {
	return node.Column
}

// TelemetryName implements the AlterTableCmd interface.
func (node *AlterTableDropIdentity) TelemetryName() string {
	return "drop_identity"
}

// Format implements the NodeFormatter interface.
func (node *AlterTableDropIdentity) Format(ctx *FmtCtx) {
	ctx.WriteString(" ALTER COLUMN ")
	ctx.FormatNode(&node.Column)
	ctx.WriteString(" DROP IDENTITY")
	if node.IfExists {
		ctx.WriteString(" IF EXISTS")
	}
}

// TableRLSMode represents different modes that the table can be in for
// row-level security.
type TableRLSMode int

// TableRLSMode values
const (
	TableRLSEnable TableRLSMode = iota
	TableRLSDisable
	TableRLSForce
	TableRLSNoForce
)

func (r TableRLSMode) String() string {
	var rlsTableModeName = [...]string{
		TableRLSEnable:  "ENABLE",
		TableRLSDisable: "DISABLE",
		TableRLSForce:   "FORCE",
		TableRLSNoForce: "NO FORCE",
	}
	return rlsTableModeName[r]
}

// TelemetryName implements the AlterTableCmd interface.
func (r TableRLSMode) TelemetryName() string {
	return strings.ReplaceAll(strings.ToLower(r.String()), " ", "_")
}

// SafeValue implements the redact.SafeValue interface.
func (r TableRLSMode) SafeValue() {}

// AlterTableSetRLSMode represents the following alter table command:
// {ENABLE | DISABLE | FORCE | NO FORCE} ROW LEVEL SECURITY
type AlterTableSetRLSMode struct {
	Mode TableRLSMode
}

// TelemetryName implements the AlterTableCmd interface
func (node *AlterTableSetRLSMode) TelemetryName() string {
	return fmt.Sprintf("%s_row_level_security", node.Mode.TelemetryName())
}

// Format implements the NodeFormatter interface
func (node *AlterTableSetRLSMode) Format(ctx *FmtCtx) {
	ctx.WriteString(" ")
	ctx.WriteString(node.Mode.String())
	ctx.WriteString(" ROW LEVEL SECURITY")
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
