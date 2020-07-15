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

// Revoke represents a REVOKE statement.
// PrivilegeList and TargetList are defined in grant.go
type Revoke struct {
	Privileges privilege.List
	Targets    TargetList
	Grantees   NameList
}

// Format implements the NodeFormatter interface.
func (node *Revoke) Format(ctx *FmtCtx) {
	ctx.WriteString("REVOKE ")
	node.Privileges.Format(&ctx.Buffer)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&node.Targets)
	ctx.WriteString(" FROM ")
	ctx.FormatNode(&node.Grantees)
}

// RevokeRole represents a REVOKE <role> statement.
type RevokeRole struct {
	Roles       NameList
	Members     NameList
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

// RevokeOnType represents a REVOKE <privileges...> on <type_names...> statement.
type RevokeOnType struct {
	Privileges privilege.List
	Targets    TargetList
	Grantees   NameList
}

// Format implements the NodeFormatter interface.
func (node *RevokeOnType) Format(ctx *FmtCtx) {
	ctx.WriteString("REVOKE ")
	for i, priv := range node.Privileges {
		if i != 0 {
			ctx.WriteString(", ")
		}
		ctx.WriteString(priv.String())
	}
	ctx.WriteString(" ON TYPE ")
	for i, typ := range node.Targets.Types {
		if i != 0 {
			ctx.WriteString(", ")
		}
		ctx.WriteString(typ.String())
	}
	ctx.WriteString(" FROM ")
	ctx.FormatNode(&node.Grantees)
}
