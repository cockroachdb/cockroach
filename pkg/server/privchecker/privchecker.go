// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/redact"
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
	makeAuthzAccessor func(opName redact.SafeString) (sql.AuthorizationAccessor, func())
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
	hasView, err := c.HasPrivilegeOrRoleOption(ctx, userName, privilege.VIEWACTIVITY)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if hasView {
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
	hasView, err := c.HasPrivilegeOrRoleOption(ctx, userName, privilege.VIEWACTIVITY)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if hasView {
		return nil
	}
	hasViewRedacted, err := c.HasPrivilegeOrRoleOption(ctx, userName, privilege.VIEWACTIVITYREDACTED)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if hasViewRedacted {
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
	hasView, err := c.HasPrivilegeOrRoleOption(ctx, userName, privilege.VIEWCLUSTERSETTING)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if hasView {
		return nil
	}
	hasModify, err := c.HasPrivilegeOrRoleOption(ctx, userName, privilege.MODIFYCLUSTERSETTING)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if hasModify {
		return nil
	}
	return grpcstatus.Errorf(
		codes.PermissionDenied, "this operation requires the %s or %s system privileges",
		privilege.VIEWCLUSTERSETTING.DisplayName(), privilege.MODIFYCLUSTERSETTING.DisplayName())
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
				privilege.VIEWACTIVITY.DisplayName(), privilege.VIEWACTIVITYREDACTED.DisplayName())
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
		privilege.VIEWCLUSTERMETADATA.DisplayName())
}

// RequireRepairClusterPermission requires the user have admin
// or the REPAIRCLUSTER system privilege and returns an error if
// the user does not have it.
func (c *adminPrivilegeChecker) RequireRepairClusterPermission(ctx context.Context) (err error) {
	userName, isAdmin, err := c.GetUserAndRole(ctx)
	if err != nil {
		return srverrors.ServerError(ctx, err)
	}
	if isAdmin {
		return nil
	}
	if hasRepairCluster, err := c.HasGlobalPrivilege(ctx, userName, privilege.REPAIRCLUSTER); err != nil {
		return srverrors.ServerError(ctx, err)
	} else if hasRepairCluster {
		return nil
	}
	return grpcstatus.Errorf(
		codes.PermissionDenied, "this operation requires the %s system privilege",
		privilege.REPAIRCLUSTER.DisplayName())
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
		privilege.VIEWDEBUG.DisplayName())
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
	aa, cleanup := c.makeAuthzAccessor("check-admin-role")
	defer cleanup()
	return aa.UserHasAdminRole(ctx, user)
}

// HasRoleOption is part of the SQLPrivilegeChecker interface.
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to serverErrors.
func (c *adminPrivilegeChecker) HasRoleOption(
	ctx context.Context, user username.SQLUsername, roleOption roleoption.Option,
) (bool, error) {
	aa, cleanup := c.makeAuthzAccessor("check-role-option")
	defer cleanup()
	return aa.UserHasRoleOption(ctx, user, roleOption)
}

// HasPrivilegeOrRoleOption is a helper function which calls both HasGlobalPrivilege and HasRoleOption.
func (c *adminPrivilegeChecker) HasPrivilegeOrRoleOption(
	ctx context.Context, username username.SQLUsername, privilege privilege.Kind,
) (bool, error) {
	if privilegeName, err := c.HasGlobalPrivilege(ctx, username, privilege); err != nil {
		return false, err
	} else if privilegeName {
		return true, nil
	}
	maybeRoleOptionName := string(privilege.DisplayName())
	roleOption, ok := roleoption.ByName[maybeRoleOptionName]
	if !ok {
		return false, nil
	}
	if hasRoleOption, err := c.HasRoleOption(ctx, username, roleOption); err != nil {
		return false, err
	} else if hasRoleOption {
		return true, nil
	}
	return false, nil
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

func (c *adminPrivilegeChecker) SetAuthzAccessorFactory(
	fn func(opName redact.SafeString) (sql.AuthorizationAccessor, func()),
) {
	c.makeAuthzAccessor = fn
}
