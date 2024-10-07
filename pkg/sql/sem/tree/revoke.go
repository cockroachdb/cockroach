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

// Revoke represents a REVOKE statement.
// PrivilegeList and TargetList are defined in grant.go
type Revoke struct {
	Privileges     privilege.List
	Targets        GrantTargetList
	Grantees       RoleSpecList
	GrantOptionFor bool
}

// Format implements the NodeFormatter interface.
func (node *Revoke) Format(ctx *FmtCtx) {
	ctx.WriteString("REVOKE ")
	if node.Targets.System {
		ctx.WriteString(" SYSTEM ")
	}
	// NB: we cannot use FormatNode() here because node.Privileges is
	// not an AST node. This is OK, because a privilege list cannot
	// contain sensitive information.
	node.Privileges.FormatNames(&ctx.Buffer)
	if !node.Targets.System {
		ctx.WriteString(" ON ")
		ctx.FormatNode(&node.Targets)
	}
	ctx.WriteString(" FROM ")
	ctx.FormatNode(&node.Grantees)
}

// RevokeRole represents a REVOKE <role> statement.
type RevokeRole struct {
	Roles       NameList
	Members     RoleSpecList
	AdminOption bool
}

// Format implements the NodeFormatter interface.
func (node *RevokeRole) Format(ctx *FmtCtx) {
	ctx.WriteString("REVOKE ")
	if node.AdminOption {
		ctx.WriteString("ADMIN OPTION FOR ")
	}
	ctx.FormatNode(&node.Roles)
	ctx.WriteString(" FROM ")
	ctx.FormatNode(&node.Members)
}
