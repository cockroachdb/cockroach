// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
)

// UserSQLRoles return a list of the logged in SQL user roles.
func (s *baseStatusServer) UserSQLRoles(
	ctx context.Context, req *serverpb.UserSQLRolesRequest,
) (_ *serverpb.UserSQLRolesResponse, retErr error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	username, isAdmin, err := s.privilegeChecker.getUserAndRole(ctx)
	if err != nil {
		return nil, err
	}

	var resp serverpb.UserSQLRolesResponse
	if !isAdmin {
		for _, privKind := range privilege.GlobalPrivileges {
			privName := privKind.String()
			hasPriv := s.privilegeChecker.checkHasGlobalPrivilege(ctx, username, privKind)
			if hasPriv {
				resp.Roles = append(resp.Roles, privName)
				continue
			}
			hasRole, err := s.privilegeChecker.hasRoleOption(ctx, username, roleoption.ByName[privName])
			if err != nil {
				return nil, err
			}
			if hasRole {
				resp.Roles = append(resp.Roles, privName)
			}
		}
	} else {
		resp.Roles = append(resp.Roles, "ADMIN")
	}
	return &resp, nil
}
