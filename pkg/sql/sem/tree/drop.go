// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This code was derived from https://github.com/youtube/vitess.

package tree

// DropBehavior represents options for dropping schema elements.
type DropBehavior int

// DropBehavior values.
const (
	DropDefault DropBehavior = iota
	DropRestrict
	DropCascade
)

var dropBehaviorName = [...]string{
	DropDefault:  "",
	DropRestrict: "RESTRICT",
	DropCascade:  "CASCADE",
}

func (d DropBehavior) String() string {
	return dropBehaviorName[d]
}

// DropDatabase represents a DROP DATABASE statement.
type DropDatabase struct {
	Name         Name
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP DATABASE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropIndex represents a DROP INDEX statement.
type DropIndex struct {
	IndexList    TableIndexNames
	IfExists     bool
	DropBehavior DropBehavior
	Concurrently bool
}

// Format implements the NodeFormatter interface.
func (node *DropIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP INDEX ")
	if node.Concurrently {
		ctx.WriteString("CONCURRENTLY ")
	}
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.IndexList)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropTable represents a DROP TABLE statement.
type DropTable struct {
	Names        TableNames
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropTable) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP TABLE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropView represents a DROP VIEW statement.
type DropView struct {
	Names          TableNames
	IfExists       bool
	DropBehavior   DropBehavior
	IsMaterialized bool
}

// Format implements the NodeFormatter interface.
func (node *DropView) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP ")
	if node.IsMaterialized {
		ctx.WriteString("MATERIALIZED ")
	}
	ctx.WriteString("VIEW ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropSequence represents a DROP SEQUENCE statement.
type DropSequence struct {
	Names        TableNames
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP SEQUENCE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropRole represents a DROP ROLE statement
type DropRole struct {
	Names    RoleSpecList
	IsRole   bool
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *DropRole) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP")
	if node.IsRole {
		ctx.WriteString(" ROLE ")
	} else {
		ctx.WriteString(" USER ")
	}
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
}

// DropProvisionedRolesOptions describes filter options for the
// DROP PROVISIONED ROLES statement.
type DropProvisionedRolesOptions struct {
	// Source filters users by their PROVISIONSRC role option value.
	Source Expr
	// LastLoginBefore filters users whose estimated last login
	// is before this timestamp.
	LastLoginBefore Expr
}

// Format implements the NodeFormatter interface.
func (o *DropProvisionedRolesOptions) Format(ctx *FmtCtx) {
	var addSep bool
	maybeAddSep := func() {
		if addSep {
			ctx.WriteString(", ")
		}
		addSep = true
	}

	if o.Source != nil {
		maybeAddSep()
		ctx.WriteString("SOURCE = ")
		ctx.FormatNode(o.Source)
	}
	if o.LastLoginBefore != nil {
		maybeAddSep()
		ctx.WriteString("LAST LOGIN BEFORE ")
		ctx.FormatNode(o.LastLoginBefore)
	}
}

// CombineWith merges other DropProvisionedRolesOptions into this
// struct. An error is returned if the same option is specified
// multiple times.
func (o *DropProvisionedRolesOptions) CombineWith(other *DropProvisionedRolesOptions) error {
	var err error
	o.Source, err = combineExpr(o.Source, other.Source, "SOURCE")
	if err != nil {
		return err
	}
	o.LastLoginBefore, err = combineExpr(
		o.LastLoginBefore, other.LastLoginBefore,
		"LAST LOGIN BEFORE",
	)
	return err
}

// IsDefault returns true if no options are set.
func (o DropProvisionedRolesOptions) IsDefault() bool {
	return o.Source == nil && o.LastLoginBefore == nil
}

var _ NodeFormatter = &DropProvisionedRolesOptions{}

// DropProvisionedRoles represents a DROP PROVISIONED ROLES statement that
// bulk-drops provisioned users matching filter criteria.
//
//	DROP PROVISIONED ROLES
//	  WITH SOURCE = 'ldap:ldap.example.com',
//	       LAST LOGIN BEFORE '2025-01-01'
//	  LIMIT 10
type DropProvisionedRoles struct {
	Options *DropProvisionedRolesOptions
	Limit   *Limit
}

var _ Statement = &DropProvisionedRoles{}

// Format implements the NodeFormatter interface.
func (node *DropProvisionedRoles) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP PROVISIONED ROLES")
	if node.Options != nil && !node.Options.IsDefault() {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(node.Options)
	}
	if node.Limit != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Limit)
	}
}

// DropType represents a DROP TYPE command.
type DropType struct {
	Names        []*UnresolvedObjectName
	IfExists     bool
	DropBehavior DropBehavior
}

var _ Statement = &DropType{}

// Format implements the NodeFormatter interface.
func (node *DropType) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP TYPE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	for i := range node.Names {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(node.Names[i])
	}
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropSchema represents a DROP SCHEMA command.
type DropSchema struct {
	Names        ObjectNamePrefixList
	IfExists     bool
	DropBehavior DropBehavior
}

var _ Statement = &DropSchema{}

// Format implements the NodeFormatter interface.
func (node *DropSchema) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP SCHEMA ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
	if node.DropBehavior != DropDefault {
		ctx.WriteString(" ")
		ctx.WriteString(node.DropBehavior.String())
	}
}

// DropExternalConnection represents a DROP EXTERNAL CONNECTION statement.
type DropExternalConnection struct {
	ConnectionLabel Expr
}

var _ Statement = &DropExternalConnection{}

// Format implements the Statement interface.
func (node *DropExternalConnection) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP EXTERNAL CONNECTION")

	if node.ConnectionLabel != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(node.ConnectionLabel)
	}
}

// DropTenant represents a DROP VIRTUAL CLUSTER command.
type DropTenant struct {
	TenantSpec *TenantSpec
	IfExists   bool
	Immediate  bool
}

var _ Statement = &DropTenant{}

// Format implements the NodeFormatter interface.
func (node *DropTenant) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP VIRTUAL CLUSTER ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.TenantSpec)
	if node.Immediate {
		ctx.WriteString(" IMMEDIATE")
	}
}
