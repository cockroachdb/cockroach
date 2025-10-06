// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/privilege"

// AlterDefaultPrivileges represents an ALTER DEFAULT PRIVILEGES statement.
type AlterDefaultPrivileges struct {
	Roles RoleSpecList
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
	if n.ForAllRoles {
		ctx.WriteString("FOR ALL ROLES ")
	} else if len(n.Roles) > 0 {
		ctx.WriteString("FOR ROLE ")
		for i := range n.Roles {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.FormatNode(&n.Roles[i])
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

// AbbreviatedGrant represents the GRANT part of an
// ALTER DEFAULT PRIVILEGES statement.
type AbbreviatedGrant struct {
	Privileges      privilege.List
	Target          privilege.TargetObjectType
	Grantees        RoleSpecList
	WithGrantOption bool
}

// Format implements the NodeFormatter interface.
func (n *AbbreviatedGrant) Format(ctx *FmtCtx) {
	ctx.WriteString("GRANT ")
	n.Privileges.FormatNames(&ctx.Buffer)
	ctx.WriteString(" ON ")
	switch n.Target {
	case privilege.Tables:
		ctx.WriteString("TABLES ")
	case privilege.Sequences:
		ctx.WriteString("SEQUENCES ")
	case privilege.Types:
		ctx.WriteString("TYPES ")
	case privilege.Schemas:
		ctx.WriteString("SCHEMAS ")
	case privilege.Routines:
		ctx.WriteString("FUNCTIONS ")
	}
	ctx.WriteString("TO ")
	n.Grantees.Format(ctx)
	if n.WithGrantOption {
		ctx.WriteString(" WITH GRANT OPTION")
	}
}

// AbbreviatedRevoke represents the REVOKE part of an
// ALTER DEFAULT PRIVILEGES statement.
type AbbreviatedRevoke struct {
	Privileges     privilege.List
	Target         privilege.TargetObjectType
	Grantees       RoleSpecList
	GrantOptionFor bool
}

// Format implements the NodeFormatter interface.
func (n *AbbreviatedRevoke) Format(ctx *FmtCtx) {
	ctx.WriteString("REVOKE ")
	if n.GrantOptionFor {
		ctx.WriteString("GRANT OPTION FOR ")
	}
	n.Privileges.FormatNames(&ctx.Buffer)
	ctx.WriteString(" ON ")
	switch n.Target {
	case privilege.Tables:
		ctx.WriteString("TABLES ")
	case privilege.Sequences:
		ctx.WriteString("SEQUENCES ")
	case privilege.Types:
		ctx.WriteString("TYPES ")
	case privilege.Schemas:
		ctx.WriteString("SCHEMAS ")
	case privilege.Routines:
		ctx.WriteString("FUNCTIONS ")
	}
	ctx.WriteString(" FROM ")
	n.Grantees.Format(ctx)
}
