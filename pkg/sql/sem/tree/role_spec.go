// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
)

// RoleSpecType represents whether the RoleSpec is represented by
// string name or if the spec is CURRENT_USER or SESSION_USER.
type RoleSpecType int

const (
	// RoleName represents if a RoleSpec is defined using an IDENT or
	// unreserved_keyword in the grammar.
	RoleName RoleSpecType = iota
	// CurrentUser represents if a RoleSpec is defined using CURRENT_USER.
	CurrentUser
	// SessionUser represents if a RoleSpec is defined using SESSION_USER.
	SessionUser
)

func (r RoleSpecType) String() string {
	switch r {
	case RoleName:
		return "ROLE_NAME"
	case CurrentUser:
		return "CURRENT_USER"
	case SessionUser:
		return "SESSION_USER"
	default:
		panic(fmt.Sprintf("unknown role spec type: %d", r))
	}
}

// RoleSpecList is a list of RoleSpec.
type RoleSpecList []RoleSpec

// RoleSpec represents a role.
// Name should only be populated if RoleSpecType is RoleName.
type RoleSpec struct {
	RoleSpecType RoleSpecType
	Name         string
}

// MakeRoleSpecWithRoleName creates a RoleSpec using a RoleName.
func MakeRoleSpecWithRoleName(name string) RoleSpec {
	return RoleSpec{RoleSpecType: RoleName, Name: name}
}

// Undefined returns if RoleSpec is undefined.
func (r RoleSpec) Undefined() bool {
	return r.RoleSpecType == RoleName && len(r.Name) == 0
}

// Format implements the NodeFormatter interface.
func (r *RoleSpec) Format(ctx *FmtCtx) {
	f := ctx.flags
	if f.HasFlags(FmtAnonymize) && !isArityIndicatorString(r.Name) {
		ctx.WriteByte('_')
	} else {
		switch r.RoleSpecType {
		case RoleName:
			lexbase.EncodeRestrictedSQLIdent(&ctx.Buffer, r.Name, f.EncodeFlags())
			return
		case CurrentUser, SessionUser:
			ctx.WriteString(r.RoleSpecType.String())
		}
	}
}

// Format implements the NodeFormatter interface.
func (l *RoleSpecList) Format(ctx *FmtCtx) {
	for i := range *l {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*l)[i])
	}
}
