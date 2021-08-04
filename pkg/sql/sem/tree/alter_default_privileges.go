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

import (
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/errors"
)

// AlterDefaultPrivileges represents an ALTER DEFAULT PRIVILEGES statement.
type AlterDefaultPrivileges struct {
	Roles NameList
	// True if `ALTER DEFAULT PRIVILEGES FOR ALL ROLES` is executed.
	ForAllRoles bool
	// If Schema is not specified, ALTER DEFAULT PRIVILEGES is being
	// run on the current database.
	Schemas ObjectNamePrefixList

	// Database is only used when converting a granting / revoking incompatible
	// database privileges to an alter default privileges statement.
	// If it is not set, the current database is used.
	Database *Name

	// Only one of Grant or Revoke should be set. IsGrant is used to determine
	// which one is set.
	IsGrant bool
	Grant   AbbreviatedGrant
	Revoke  AbbreviatedRevoke
}

// Format implements the NodeFormatter interface.
func (n *AlterDefaultPrivileges) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DEFAULT PRIVILEGES ")
	if len(n.Roles) > 0 {
		ctx.WriteString("FOR ROLE ")
		for i, role := range n.Roles {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.FormatNode(&role)
		}
		ctx.WriteString(" ")
	}
	if len(n.Schemas) > 0 {
		ctx.WriteString("IN SCHEMA ")
		ctx.FormatNode(n.Schemas)
		ctx.WriteString(" ")
	}
	if n.IsGrant {
		n.Grant.Format(ctx)
	} else {
		n.Revoke.Format(ctx)
	}
}

// AlterDefaultPrivilegesTargetObject represents the type of object that is
// having it's default privileges altered.
type AlterDefaultPrivilegesTargetObject uint32

// ToPrivilegeObjectType returns the privilege.ObjectType corresponding to
// the AlterDefaultPrivilegesTargetObject.
func (t AlterDefaultPrivilegesTargetObject) ToPrivilegeObjectType() privilege.ObjectType {
	targetObjectToPrivilegeObject := map[AlterDefaultPrivilegesTargetObject]privilege.ObjectType{
		Tables:    privilege.Table,
		Sequences: privilege.Table,
		Schemas:   privilege.Schema,
		Types:     privilege.Type,
	}
	return targetObjectToPrivilegeObject[t]
}

// The numbers are explicitly assigned since the DefaultPrivilegesPerObject
// map defined in the DefaultPrivilegesPerRole proto requires the key value
// for the object type to remain unchanged.
const (
	Tables    AlterDefaultPrivilegesTargetObject = 1
	Sequences AlterDefaultPrivilegesTargetObject = 2
	Types     AlterDefaultPrivilegesTargetObject = 3
	Schemas   AlterDefaultPrivilegesTargetObject = 4
)

// GetAlterDefaultPrivilegesTargetObjects returns a slice of all the
// AlterDefaultPrivilegesTargetObjects.
func GetAlterDefaultPrivilegesTargetObjects() []AlterDefaultPrivilegesTargetObject {
	return []AlterDefaultPrivilegesTargetObject{
		Tables,
		Sequences,
		Types,
		Schemas,
	}
}

func (t AlterDefaultPrivilegesTargetObject) String() string {
	switch t {
	case Tables:
		return "tables"
	case Sequences:
		return "sequences"
	case Types:
		return "types"
	case Schemas:
		return "schemas"
	default:
		panic(errors.AssertionFailedf("unknown AlterDefaultPrivilegesTargetObject value: %d", t))
	}
}

// AbbreviatedGrant represents an the GRANT part of an
// ALTER DEFAULT PRIVILEGES statement.
type AbbreviatedGrant struct {
	Privileges      privilege.List
	Target          AlterDefaultPrivilegesTargetObject
	Grantees        NameList
	WithGrantOption bool
}

// Format implements the NodeFormatter interface.
func (n *AbbreviatedGrant) Format(ctx *FmtCtx) {
	ctx.WriteString("GRANT ")
	n.Privileges.Format(&ctx.Buffer)
	ctx.WriteString(" ON ")
	switch n.Target {
	case Tables:
		ctx.WriteString("TABLES ")
	case Sequences:
		ctx.WriteString("SEQUENCES ")
	case Types:
		ctx.WriteString("TYPES ")
	case Schemas:
		ctx.WriteString("SCHEMAS ")
	}
	ctx.WriteString("TO ")
	n.Grantees.Format(ctx)
	if n.WithGrantOption {
		ctx.WriteString(" WITH GRANT OPTION")
	}
}

// AbbreviatedRevoke represents an the REVOKE part of an
// ALTER DEFAULT PRIVILEGES statement.
type AbbreviatedRevoke struct {
	Privileges     privilege.List
	Target         AlterDefaultPrivilegesTargetObject
	Grantees       NameList
	GrantOptionFor bool
}

// Format implements the NodeFormatter interface.
func (n *AbbreviatedRevoke) Format(ctx *FmtCtx) {
	ctx.WriteString("REVOKE ")
	if n.GrantOptionFor {
		ctx.WriteString("GRANT OPTION FOR ")
	}
	n.Privileges.Format(&ctx.Buffer)
	ctx.WriteString(" ON ")
	switch n.Target {
	case Tables:
		ctx.WriteString("TABLES ")
	case Sequences:
		ctx.WriteString("SEQUENCES ")
	case Types:
		ctx.WriteString("TYPES ")
	case Schemas:
		ctx.WriteString("SCHEMAS ")
	}
	ctx.WriteString(" FROM ")
	n.Grantees.Format(ctx)
}
