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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// CheckerForRPCHandlers describes a helper for checking privileges.
//
// Note: this interface is intended for use inside RPC handlers in the
// 'server' package, where the identity of the current user is carried
// by the context.Context as per the 'authserver' protocol.
type CheckerForRPCHandlers interface {
	SQLPrivilegeChecker

	// GetUserAndRole returns the current user's name and whether the
	// user is an admin.
	//
	// Note that the function returns plain errors, and it is the caller's
	// responsibility to convert them through srverrors.ServerError.
	GetUserAndRole(ctx context.Context) (userName username.SQLUsername, isAdmin bool, err error)

	// RequireAdminUser validates the current user is
	// an admin user. It returns the current user's name.
	// Its error return is a gRPC error.
	RequireAdminUser(ctx context.Context) (userName username.SQLUsername, err error)

	// RequireAdminRole validates the current user has the VIEWACTIVITY
	// privilege or role option.
	// Its error return is a gRPC error.
	RequireViewActivityPermission(ctx context.Context) error

	RequireViewActivityOrViewActivityRedactedPermission(ctx context.Context) error
	RequireViewClusterSettingOrModifyClusterSettingPermission(ctx context.Context) error
	RequireViewActivityAndNoViewActivityRedactedPermission(ctx context.Context) error
	RequireViewClusterMetadataPermission(ctx context.Context) error
	RequireViewDebugPermission(ctx context.Context) error
}

// SQLPrivilegeChecker is the part of the privilege checker that can
// be used outside of RPC handlers, because it takes the identity as
// explicit argument.
type SQLPrivilegeChecker interface {
	// HasAdminRole checks if the user has the admin role.
	// Note that the function returns plain errors, and it is the
	// caller's responsibility to convert them through
	// srverrors.ServerError.
	HasAdminRole(ctx context.Context, user username.SQLUsername) (bool, error)

	// HasRoleOptions checks if the user has the given role option.
	// Note that the function returns plain errors, and it is the
	// caller's responsibility to convert them through
	// srverrors.ServerError.
	HasRoleOption(ctx context.Context, user username.SQLUsername, roleOption roleoption.Option) (bool, error)

	// SetSQLAuthzAccessorFactory sets the accessor factory that can be
	// used by HasGlobalPrivilege.
	SetAuthzAccessorFactory(factory func(opName string) (sql.AuthorizationAccessor, func()))

	// HasGlobalPrivilege is a convenience wrapper
	HasGlobalPrivilege(ctx context.Context, user username.SQLUsername, privilege privilege.Kind) (bool, error)
}

// NewChecker constructs a new CheckerForRPCHandlers.
func NewChecker(ie isql.Executor, st *cluster.Settings) CheckerForRPCHandlers {
	return &adminPrivilegeChecker{
		ie: ie,
		st: st,
	}
}

// ErrRequiresAdmin is returned when the admin role is required by an API.
var ErrRequiresAdmin = grpcstatus.Error(codes.PermissionDenied, "this operation requires admin privilege")

// ErrRequiresRoleOption can be used to construct an error that tells the user
// a given role option or privilege is required.
func ErrRequiresRoleOption(option roleoption.Option) error {
	return grpcstatus.Errorf(
		codes.PermissionDenied, "this operation requires %s privilege", option)
}
