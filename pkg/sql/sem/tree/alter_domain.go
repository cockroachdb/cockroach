// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// AlterDomain represents an ALTER DOMAIN statement.
type AlterDomain struct {
	Domain *UnresolvedObjectName
	Cmd    AlterDomainCmd
}

// Format implements the NodeFormatter interface.
func (node *AlterDomain) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DOMAIN ")
	ctx.FormatNode(node.Domain)
	ctx.FormatNode(node.Cmd)
}

// AlterDomainCmd represents a domain modification operation.
type AlterDomainCmd interface {
	NodeFormatter
	alterDomainCmd()
	// TelemetryName returns the counter name to use for telemetry purposes.
	TelemetryName() string
}

func (*AlterDomainSetDefault) alterDomainCmd()         {}
func (*AlterDomainDropDefault) alterDomainCmd()        {}
func (*AlterDomainSetNotNull) alterDomainCmd()         {}
func (*AlterDomainDropNotNull) alterDomainCmd()        {}
func (*AlterDomainAddConstraint) alterDomainCmd()      {}
func (*AlterDomainDropConstraint) alterDomainCmd()     {}
func (*AlterDomainRenameConstraint) alterDomainCmd()   {}
func (*AlterDomainValidateConstraint) alterDomainCmd() {}
func (*AlterDomainOwner) alterDomainCmd()              {}
func (*AlterDomainRename) alterDomainCmd()             {}
func (*AlterDomainSetSchema) alterDomainCmd()          {}

var _ AlterDomainCmd = &AlterDomainSetDefault{}
var _ AlterDomainCmd = &AlterDomainDropDefault{}
var _ AlterDomainCmd = &AlterDomainSetNotNull{}
var _ AlterDomainCmd = &AlterDomainDropNotNull{}
var _ AlterDomainCmd = &AlterDomainAddConstraint{}
var _ AlterDomainCmd = &AlterDomainDropConstraint{}
var _ AlterDomainCmd = &AlterDomainRenameConstraint{}
var _ AlterDomainCmd = &AlterDomainValidateConstraint{}
var _ AlterDomainCmd = &AlterDomainOwner{}
var _ AlterDomainCmd = &AlterDomainRename{}
var _ AlterDomainCmd = &AlterDomainSetSchema{}

// AlterDomainSetDefault represents an ALTER DOMAIN SET DEFAULT command.
type AlterDomainSetDefault struct {
	Default Expr
}

// Format implements the NodeFormatter interface.
func (node *AlterDomainSetDefault) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET DEFAULT ")
	ctx.FormatNode(node.Default)
}

// TelemetryName implements the AlterDomainCmd interface.
func (node *AlterDomainSetDefault) TelemetryName() string {
	return "set_default"
}

// AlterDomainDropDefault represents an ALTER DOMAIN DROP DEFAULT command.
type AlterDomainDropDefault struct{}

// Format implements the NodeFormatter interface.
func (node *AlterDomainDropDefault) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP DEFAULT")
}

// TelemetryName implements the AlterDomainCmd interface.
func (node *AlterDomainDropDefault) TelemetryName() string {
	return "drop_default"
}

// AlterDomainSetNotNull represents an ALTER DOMAIN SET NOT NULL command.
type AlterDomainSetNotNull struct{}

// Format implements the NodeFormatter interface.
func (node *AlterDomainSetNotNull) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET NOT NULL")
}

// TelemetryName implements the AlterDomainCmd interface.
func (node *AlterDomainSetNotNull) TelemetryName() string {
	return "set_not_null"
}

// AlterDomainDropNotNull represents an ALTER DOMAIN DROP NOT NULL command.
type AlterDomainDropNotNull struct{}

// Format implements the NodeFormatter interface.
func (node *AlterDomainDropNotNull) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP NOT NULL")
}

// TelemetryName implements the AlterDomainCmd interface.
func (node *AlterDomainDropNotNull) TelemetryName() string {
	return "drop_not_null"
}

// AlterDomainAddConstraint represents an ALTER DOMAIN ADD CONSTRAINT command.
type AlterDomainAddConstraint struct {
	// Name is the optional constraint name.
	Name Name
	// NotNull indicates this is a NOT NULL constraint (rather than CHECK).
	NotNull bool
	// Check is the CHECK expression (nil if NotNull is true).
	Check Expr
	// NotValid indicates the constraint should not be validated.
	NotValid bool
}

// Format implements the NodeFormatter interface.
func (node *AlterDomainAddConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD")
	if node.Name != "" {
		ctx.WriteString(" CONSTRAINT ")
		ctx.FormatNode(&node.Name)
	}
	if node.NotNull {
		ctx.WriteString(" NOT NULL")
	} else {
		ctx.WriteString(" CHECK (")
		ctx.FormatNode(node.Check)
		ctx.WriteByte(')')
	}
	if node.NotValid {
		ctx.WriteString(" NOT VALID")
	}
}

// TelemetryName implements the AlterDomainCmd interface.
func (node *AlterDomainAddConstraint) TelemetryName() string {
	return "add_constraint"
}

// AlterDomainDropConstraint represents an ALTER DOMAIN DROP CONSTRAINT command.
type AlterDomainDropConstraint struct {
	ConstraintName Name
	IfExists       bool
	DropBehavior   DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *AlterDomainDropConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" DROP CONSTRAINT ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.ConstraintName)
	switch node.DropBehavior {
	case DropCascade:
		ctx.WriteString(" CASCADE")
	case DropRestrict:
		ctx.WriteString(" RESTRICT")
	}
}

// TelemetryName implements the AlterDomainCmd interface.
func (node *AlterDomainDropConstraint) TelemetryName() string {
	return "drop_constraint"
}

// AlterDomainRenameConstraint represents an ALTER DOMAIN RENAME CONSTRAINT
// command.
type AlterDomainRenameConstraint struct {
	ConstraintName    Name
	NewConstraintName Name
}

// Format implements the NodeFormatter interface.
func (node *AlterDomainRenameConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME CONSTRAINT ")
	ctx.FormatNode(&node.ConstraintName)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.NewConstraintName)
}

// TelemetryName implements the AlterDomainCmd interface.
func (node *AlterDomainRenameConstraint) TelemetryName() string {
	return "rename_constraint"
}

// AlterDomainValidateConstraint represents an ALTER DOMAIN VALIDATE CONSTRAINT
// command.
type AlterDomainValidateConstraint struct {
	ConstraintName Name
}

// Format implements the NodeFormatter interface.
func (node *AlterDomainValidateConstraint) Format(ctx *FmtCtx) {
	ctx.WriteString(" VALIDATE CONSTRAINT ")
	ctx.FormatNode(&node.ConstraintName)
}

// TelemetryName implements the AlterDomainCmd interface.
func (node *AlterDomainValidateConstraint) TelemetryName() string {
	return "validate_constraint"
}

// AlterDomainOwner represents an ALTER DOMAIN OWNER TO command.
type AlterDomainOwner struct {
	Owner RoleSpec
}

// Format implements the NodeFormatter interface.
func (node *AlterDomainOwner) Format(ctx *FmtCtx) {
	ctx.WriteString(" OWNER TO ")
	ctx.FormatNode(&node.Owner)
}

// TelemetryName implements the AlterDomainCmd interface.
func (node *AlterDomainOwner) TelemetryName() string {
	return "owner"
}

// AlterDomainRename represents an ALTER DOMAIN RENAME TO command.
type AlterDomainRename struct {
	NewName Name
}

// Format implements the NodeFormatter interface.
func (node *AlterDomainRename) Format(ctx *FmtCtx) {
	ctx.WriteString(" RENAME TO ")
	ctx.FormatNode(&node.NewName)
}

// TelemetryName implements the AlterDomainCmd interface.
func (node *AlterDomainRename) TelemetryName() string {
	return "rename"
}

// AlterDomainSetSchema represents an ALTER DOMAIN SET SCHEMA command.
type AlterDomainSetSchema struct {
	Schema Name
}

// Format implements the NodeFormatter interface.
func (node *AlterDomainSetSchema) Format(ctx *FmtCtx) {
	ctx.WriteString(" SET SCHEMA ")
	ctx.FormatNode(&node.Schema)
}

// TelemetryName implements the AlterDomainCmd interface.
func (node *AlterDomainSetSchema) TelemetryName() string {
	return "set_schema"
}
