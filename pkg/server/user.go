// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
)

// UserSQLRoles return a list of the logged in SQL user roles.
func (s *baseStatusServer) UserSQLRoles(
	ctx context.Context, req *serverpb.UserSQLRolesRequest,
) (_ *serverpb.UserSQLRolesResponse, retErr error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	username, isAdmin, err := s.privilegeChecker.GetUserAndRole(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	var resp serverpb.UserSQLRolesResponse
	if !isAdmin {
		for _, privKind := range privilege.GlobalPrivileges {
			hasPriv, err := s.privilegeChecker.HasGlobalPrivilege(ctx, username, privKind)
			if err != nil {
				return nil, srverrors.ServerError(ctx, err)
			}
			privName := privKind.DisplayName()
			if hasPriv {
				resp.Roles = append(resp.Roles, string(privName))
				continue
			}
			roleOpt, ok := roleoption.ByName[string(privName)]
			if !ok {
				continue
			}
			hasRole, err := s.privilegeChecker.HasRoleOption(ctx, username, roleOpt)
			if err != nil {
				return nil, srverrors.ServerError(ctx, err)
			}
			if hasRole {
				resp.Roles = append(resp.Roles, string(privName))
			}
		}
	} else {
		resp.Roles = append(resp.Roles, "ADMIN")
	}
	return &resp, nil
}
