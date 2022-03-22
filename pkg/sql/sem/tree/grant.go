// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This code was derived from https://github.com/youtube/vitess.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/privilege"

// Grant represents a GRANT statement.
type Grant struct {
	Privileges      privilege.List
	Targets         TargetList
	Grantees        RoleSpecList
	WithGrantOption bool
}

// TargetList represents a list of targets.
// Only one field may be non-nil.
type TargetList struct {
	Databases NameList
	Schemas   ObjectNamePrefixList
	Tables    TablePatterns
	TenantID  TenantID
	Types     []*UnresolvedObjectName
	// If the target is for all tables in a set of schemas.
	AllTablesInSchema bool
	// Whether the target is only system users and roles_members table
	SystemUser bool

	// ForRoles and Roles are used internally in the parser and not used
	// in the AST. Therefore they do not participate in pretty-printing,
	// etc.
	ForRoles bool
	Roles    RoleSpecList
}

// Format implements the NodeFormatter interface.
func (tl *TargetList) Format(ctx *FmtCtx) {
	if tl.Databases != nil {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&tl.Databases)
	} else if tl.AllTablesInSchema {
		ctx.WriteString("ALL TABLES IN SCHEMA ")
		ctx.FormatNode(&tl.Schemas)
	} else if tl.Schemas != nil {
		ctx.WriteString("SCHEMA ")
		ctx.FormatNode(&tl.Schemas)
	} else if tl.TenantID.Specified {
		ctx.WriteString("TENANT ")
		ctx.FormatNode(&tl.TenantID)
	} else if tl.Types != nil {
		ctx.WriteString("TYPE ")
		for i, typ := range tl.Types {
			if i != 0 {
				ctx.WriteString(", ")
			}
			ctx.FormatNode(typ)
		}
	} else {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(&tl.Tables)
	}
}

// Format implements the NodeFormatter interface.
func (node *Grant) Format(ctx *FmtCtx) {
	ctx.WriteString("GRANT ")
	node.Privileges.Format(&ctx.Buffer)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&node.Targets)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.Grantees)
}

// GrantRole represents a GRANT <role> statement.
type GrantRole struct {
	Roles       NameList
	Members     RoleSpecList
	AdminOption bool
}

// Format implements the NodeFormatter interface.
func (node *GrantRole) Format(ctx *FmtCtx) {
	ctx.WriteString("GRANT ")
	ctx.FormatNode(&node.Roles)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.Members)
	if node.AdminOption {
		ctx.WriteString(" WITH ADMIN OPTION")
	}
}
