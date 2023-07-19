// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package privchecker

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// adminPrivilegeChecker is a helper struct to check whether given usernames
// have admin privileges.
type adminPrivilegeChecker struct {
	ie isql.Executor
	st *cluster.Settings

	// makeAuthzAccessor is a function that calls NewInternalPlanner to
	// make a sql.AuthorizationAccessor outside of the sql package. This
	// is a hack to get around a Go package dependency cycle. See
	// comment in pkg/scheduledjobs/env.go on planHookMaker. It should
	// be cast to AuthorizationAccessor in order to use privilege
	// checking functions.
	makeAuthzAccessor func(opName string) (sql.AuthorizationAccessor, func())
}

// RequireAdminUser is part of the CheckerForRPCHandlers interface.
func (c *adminPrivilegeChecker) RequireAdminUser(
	ctx context.Context,
) (userName username.SQLUsername, err error) {
	userName, isAdmin, err := c.GetUserAndRole(ctx)
	if err != nil {
		return userName, srverrors.ServerError(ctx, err)
	}
	if !isAdmin {
		return userName, ErrRequiresAdmin
	}
	return userName, nil
}

// RequireViewActivityPermission is part of the CheckerForRPCHandlers interface.
func (c *adminPrivilegeChecker) RequireViewActivityPermission(ctx context.Context) (err error) {
	userName, isAdmin, err := c.GetUserAndRole(ctx)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if isAdmin {
		return nil
	}
	if hasView, err := c.HasGlobalPrivilege(ctx, userName, privilege.VIEWACTIVITY); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasView {
		return nil
	}
	if hasView, err := c.HasRoleOption(ctx, userName, roleoption.VIEWACTIVITY); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasView {
		return nil
	}
	return grpcstatus.Errorf(
		codes.PermissionDenied, "this operation requires the %s system privilege",
		roleoption.VIEWACTIVITY)
}

// RequireViewActivityOrViewActivityRedactedPermission's error return is a gRPC error.
func (c *adminPrivilegeChecker) RequireViewActivityOrViewActivityRedactedPermission(
	ctx context.Context,
) (err error) {
	userName, isAdmin, err := c.GetUserAndRole(ctx)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if isAdmin {
		return nil
	}
	if hasView, err := c.HasGlobalPrivilege(ctx, userName, privilege.VIEWACTIVITY); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasView {
		return nil
	}
	if hasViewRedacted, err := c.HasGlobalPrivilege(ctx, userName, privilege.VIEWACTIVITYREDACTED); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasViewRedacted {
		return nil
	}
	if hasView, err := c.HasRoleOption(ctx, userName, roleoption.VIEWACTIVITY); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasView {
		return nil
	}
	if hasViewRedacted, err := c.HasRoleOption(ctx, userName, roleoption.VIEWACTIVITYREDACTED); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasViewRedacted {
		return nil
	}
	return grpcstatus.Errorf(
		codes.PermissionDenied, "this operation requires the %s or %s system privileges",
		roleoption.VIEWACTIVITY, roleoption.VIEWACTIVITYREDACTED)
}

// RequireViewClusterSettingOrModifyClusterSettingPermission's error return is a gRPC error.
func (c *adminPrivilegeChecker) RequireViewClusterSettingOrModifyClusterSettingPermission(
	ctx context.Context,
) (err error) {
	userName, isAdmin, err := c.GetUserAndRole(ctx)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if isAdmin {
		return nil
	}
	if hasView, err := c.HasGlobalPrivilege(ctx, userName, privilege.VIEWCLUSTERSETTING); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasView {
		return nil
	}
	if hasModify, err := c.HasGlobalPrivilege(ctx, userName, privilege.MODIFYCLUSTERSETTING); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasModify {
		return nil
	}
	if hasView, err := c.HasRoleOption(ctx, userName, roleoption.VIEWCLUSTERSETTING); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasView {
		return nil
	}
	if hasModify, err := c.HasRoleOption(ctx, userName, roleoption.MODIFYCLUSTERSETTING); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasModify {
		return nil
	}
	return grpcstatus.Errorf(
		codes.PermissionDenied, "this operation requires the %s or %s system privileges",
		privilege.VIEWCLUSTERSETTING, privilege.MODIFYCLUSTERSETTING)
}

// RequireViewActivityAndNoViewActivityRedactedPermission requires
// that the user have the VIEWACTIVITY role, but does not have the
// VIEWACTIVITYREDACTED role. This function's error return is a gRPC
// error.
func (c *adminPrivilegeChecker) RequireViewActivityAndNoViewActivityRedactedPermission(
	ctx context.Context,
) (err error) {
	userName, isAdmin, err := c.GetUserAndRole(ctx)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}

	if !isAdmin {
		hasViewRedacted, err := c.HasGlobalPrivilege(ctx, userName, privilege.VIEWACTIVITYREDACTED)
		if err != nil {
			return srverrors.ServerError(ctx, err)
		}
		if !hasViewRedacted {
			hasViewRedacted, err := c.HasRoleOption(ctx, userName, roleoption.VIEWACTIVITYREDACTED)
			if err != nil {
				return srverrors.ServerError(ctx, err)
			}
			if hasViewRedacted {
				return grpcstatus.Errorf(
					codes.PermissionDenied, "this operation requires %s role option and is not allowed for %s role option",
					roleoption.VIEWACTIVITY, roleoption.VIEWACTIVITYREDACTED)
			}
		} else {
			return grpcstatus.Errorf(
				codes.PermissionDenied, "this operation requires %s system privilege and is not allowed for %s system privilege",
				privilege.VIEWACTIVITY, privilege.VIEWACTIVITYREDACTED)
		}
		return c.RequireViewActivityPermission(ctx)
	}
	return nil
}

// RequireViewClusterMetadataPermission requires the user have admin
// or the VIEWCLUSTERMETADATA system privilege and returns an error if
// the user does not have it.
func (c *adminPrivilegeChecker) RequireViewClusterMetadataPermission(
	ctx context.Context,
) (err error) {
	userName, isAdmin, err := c.GetUserAndRole(ctx)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if isAdmin {
		return nil
	}
	if hasViewClusterMetadata, err := c.HasGlobalPrivilege(ctx, userName, privilege.VIEWCLUSTERMETADATA); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasViewClusterMetadata {
		return nil
	}
	return grpcstatus.Errorf(
		codes.PermissionDenied, "this operation requires the %s system privilege",
		privilege.VIEWCLUSTERMETADATA)
}

// RequireViewDebugPermission requires the user have admin or the
// VIEWDEBUG system privilege and returns an error if the user does
// not have it.
func (c *adminPrivilegeChecker) RequireViewDebugPermission(ctx context.Context) (err error) {
	userName, isAdmin, err := c.GetUserAndRole(ctx)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if isAdmin {
		return nil
	}
	if hasViewDebug, err := c.HasGlobalPrivilege(ctx, userName, privilege.VIEWDEBUG); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasViewDebug {
		return nil
	}
	return grpcstatus.Errorf(
		codes.PermissionDenied, "this operation requires the %s system privilege",
		privilege.VIEWDEBUG)
}

// GetUserAndRole is part of the CheckerForRPCHandlers interface.
func (c *adminPrivilegeChecker) GetUserAndRole(
	ctx context.Context,
) (userName username.SQLUsername, isAdmin bool, err error) {
	userName, err = authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return userName, false, err
	}
	isAdmin, err = c.HasAdminRole(ctx, userName)
	return userName, isAdmin, err
}

// HasAdminRole is part of the SQLPrivilegeChecker interface.
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to serverErrors.
func (c *adminPrivilegeChecker) HasAdminRole(
	ctx context.Context, user username.SQLUsername,
) (bool, error) {
	if user.IsRootUser() {
		// Shortcut.
		return true, nil
	}
	row, err := c.ie.QueryRowEx(
		ctx, "check-is-admin", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: user},
		"SELECT crdb_internal.is_admin()")
	if err != nil {
		return false, err
	}
	if row == nil {
		return false, errors.AssertionFailedf("hasAdminRole: expected 1 row, got 0")
	}
	if len(row) != 1 {
		return false, errors.AssertionFailedf("hasAdminRole: expected 1 column, got %d", len(row))
	}
	dbDatum, ok := tree.AsDBool(row[0])
	if !ok {
		return false, errors.AssertionFailedf("hasAdminRole: expected bool, got %T", row[0])
	}
	return bool(dbDatum), nil
}

// HasRoleOptions is part of the SQLPrivilegeChecker interface.
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to serverErrors.
func (c *adminPrivilegeChecker) HasRoleOption(
	ctx context.Context, user username.SQLUsername, roleOption roleoption.Option,
) (bool, error) {
	if user.IsRootUser() {
		// Shortcut.
		return true, nil
	}
	row, err := c.ie.QueryRowEx(
		ctx, "check-role-option", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: user},
		"SELECT crdb_internal.has_role_option($1)", roleOption.String())
	if err != nil {
		return false, err
	}
	if row == nil {
		return false, errors.AssertionFailedf("hasRoleOption: expected 1 row, got 0")
	}
	if len(row) != 1 {
		return false, errors.AssertionFailedf("hasRoleOption: expected 1 column, got %d", len(row))
	}
	dbDatum, ok := tree.AsDBool(row[0])
	if !ok {
		return false, errors.AssertionFailedf("hasRoleOption: expected bool, got %T", row[0])
	}
	return bool(dbDatum), nil
}

// HasGlobalPrivilege is a helper function which calls
// CheckPrivilege and returns a true/false based on the returned
// result.
func (c *adminPrivilegeChecker) HasGlobalPrivilege(
	ctx context.Context, user username.SQLUsername, privilege privilege.Kind,
) (bool, error) {
	aa, cleanup := c.makeAuthzAccessor("check-system-privilege")
	defer cleanup()
	return aa.HasPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege, user)
}

// TestingSetPlannerFn is used in tests only.
func (c *adminPrivilegeChecker) SetAuthzAccessorFactory(
	fn func(opName string) (sql.AuthorizationAccessor, func()),
) {
	c.makeAuthzAccessor = fn
}
