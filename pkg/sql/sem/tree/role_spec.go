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
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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

// ToSQLUsername converts a RoleSpec to a security.SQLUsername.
func (r RoleSpec) ToSQLUsername(
	sessionData *sessiondata.SessionData,
) (security.SQLUsername, error) {
	if r.RoleSpecType == CurrentUser {
		return sessionData.User(), nil
	} else if r.RoleSpecType == SessionUser {
		return sessionData.SessionUser(), nil
	}
	return security.MakeSQLUsernameFromUserInput(r.Name, security.UsernameValidation)
}

// ToSQLUsernames converts a RoleSpecList to a slice of security.SQLUsername.
func (l RoleSpecList) ToSQLUsernames(
	sessionData *sessiondata.SessionData,
) ([]security.SQLUsername, error) {
	targetRoles := make([]security.SQLUsername, len(l))
	for i, role := range l {
		user, err := role.ToSQLUsername(sessionData)
		if err != nil {
			return nil, err
		}
		targetRoles[i] = user
	}
	return targetRoles, nil
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
		case CurrentUser:
			ctx.WriteString("CURRENT_USER")
		case SessionUser:
			ctx.WriteString("SESSION_USER")
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
