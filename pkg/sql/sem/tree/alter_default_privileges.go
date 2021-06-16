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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// AlterDefaultPrivileges represents an ALTER DEFAULT PRIVILEGES statement.
type AlterDefaultPrivileges struct {
	Role *security.SQLUsername
	// If Schema is not specified, ALTER DEFAULT PRIVILEGES is being
	// run on the current database.
	Schema *ObjectNamePrefix

	// Only one of Grant or Revoke should be set. IsGrant is used to determine
	// which one is set.
	IsGrant bool
	Grant   AbbreviatedGrant
	Revoke  AbbreviatedRevoke
}

func (n *AlterDefaultPrivileges) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER DEFAULT PRIVILEGES ")
	if n.Role != nil {
		ctx.WriteString("FOR ROLE ")
		ctx.FormatUsername(*n.Role)
		ctx.WriteString(" ")
	}
	if n.Schema != nil {
		ctx.WriteString("IN SCHEMA ")
		ctx.FormatNode(n.Schema)
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
type AlterDefaultPrivilegesTargetObject int

const (
	Tables    AlterDefaultPrivilegesTargetObject = 1
	Sequences AlterDefaultPrivilegesTargetObject = 2
	Types     AlterDefaultPrivilegesTargetObject = 3
	Schemas   AlterDefaultPrivilegesTargetObject = 4
)

func (a AlterDefaultPrivilegesTargetObject) ToUInt32() uint32 {
	return uint32(a)
}

type AbbreviatedGrant struct {
	Privileges      privilege.List
	Target          AlterDefaultPrivilegesTargetObject
	Grantees        NameList
	WithGrantOption bool
}

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

type AbbreviatedRevoke struct {
	Privileges     privilege.List
	Target         AlterDefaultPrivilegesTargetObject
	Grantees       NameList
	GrantOptionFor bool
	Drop           DropBehavior
}

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
