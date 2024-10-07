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

import "github.com/cockroachdb/cockroach/pkg/sql/privilege"

// Grant represents a GRANT statement.
type Grant struct {
	Privileges      privilege.List
	Targets         GrantTargetList
	Grantees        RoleSpecList
	WithGrantOption bool
}

// GrantTargetList represents a list of targets.
// Only one field may be non-nil.
type GrantTargetList struct {
	Databases  NameList
	Schemas    ObjectNamePrefixList
	Tables     TableAttrs
	Types      []*UnresolvedObjectName
	Functions  RoutineObjs
	Procedures RoutineObjs
	// If the target is for all sequences in a set of schemas.
	AllSequencesInSchema bool
	// If the target is for all tables in a set of schemas.
	AllTablesInSchema bool
	// If the target is for all functions in a set of schemas.
	AllFunctionsInSchema bool
	// If the target is for all procedures in a set of schemas.
	AllProceduresInSchema bool
	// If the target is system.
	System bool
	// If the target is External Connection.
	ExternalConnections NameList

	// ForRoles and Roles are used internally in the parser and not used
	// in the AST. Therefore they do not participate in pretty-printing,
	// etc.
	ForRoles bool
	Roles    RoleSpecList
}

// Format implements the NodeFormatter interface.
func (tl *GrantTargetList) Format(ctx *FmtCtx) {
	if tl.Databases != nil {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&tl.Databases)
	} else if tl.AllSequencesInSchema {
		ctx.WriteString("ALL SEQUENCES IN SCHEMA ")
		ctx.FormatNode(&tl.Schemas)
	} else if tl.AllTablesInSchema {
		ctx.WriteString("ALL TABLES IN SCHEMA ")
		ctx.FormatNode(&tl.Schemas)
	} else if tl.AllFunctionsInSchema {
		ctx.WriteString("ALL FUNCTIONS IN SCHEMA ")
		ctx.FormatNode(&tl.Schemas)
	} else if tl.AllProceduresInSchema {
		ctx.WriteString("ALL PROCEDURES IN SCHEMA ")
		ctx.FormatNode(&tl.Schemas)
	} else if tl.Schemas != nil {
		ctx.WriteString("SCHEMA ")
		ctx.FormatNode(&tl.Schemas)
	} else if tl.Types != nil {
		ctx.WriteString("TYPE ")
		for i, typ := range tl.Types {
			if i != 0 {
				ctx.WriteString(", ")
			}
			ctx.FormatNode(typ)
		}
	} else if tl.ExternalConnections != nil {
		ctx.WriteString("EXTERNAL CONNECTION ")
		ctx.FormatNode(&tl.ExternalConnections)
	} else if tl.Functions != nil {
		ctx.WriteString("FUNCTION ")
		ctx.FormatNode(tl.Functions)
	} else if tl.Procedures != nil {
		ctx.WriteString("PROCEDURE ")
		ctx.FormatNode(tl.Procedures)
	} else {
		if tl.Tables.SequenceOnly {
			ctx.WriteString("SEQUENCE ")
		} else {
			ctx.WriteString("TABLE ")
		}
		ctx.FormatNode(&tl.Tables.TablePatterns)
	}
}

// Format implements the NodeFormatter interface.
func (node *Grant) Format(ctx *FmtCtx) {
	ctx.WriteString("GRANT ")
	if node.Targets.System {
		ctx.WriteString(" SYSTEM ")
	}
	node.Privileges.FormatNames(&ctx.Buffer)
	if !node.Targets.System {
		ctx.WriteString(" ON ")
		ctx.FormatNode(&node.Targets)
	}
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
