// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admin

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/admin/types"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/query"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
)

// Controller is the admin controller for administrative operations.
type Controller struct {
	*controllers.Controller
	authService authtypes.IService
	handlers    []controllers.IControllerHandler
}

// NewController creates a new admin controller.
func NewController(authService authtypes.IService) *Controller {
	ctrl := &Controller{
		Controller:  controllers.NewDefaultController(),
		authService: authService,
	}

	ctrl.handlers = []controllers.IControllerHandler{
		// Token management
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/tokens",
			Func:   ctrl.ListAllTokens,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionTokensViewAll,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/users/:userId/tokens/:tokenId",
			Func:   ctrl.RevokeUserToken,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionTokensRevokeAll,
				},
			},
		},
		// Group Permissions
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/group-permissions",
			Func:   ctrl.ListGroupPermissions,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/group-permissions",
			Func:   ctrl.CreateGroupPermission,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "PUT",
			Path:   types.ControllerPath + "/group-permissions/:id",
			Func:   ctrl.UpdateGroupPermission,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/group-permissions/:id",
			Func:   ctrl.DeleteGroupPermission,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "PUT",
			Path:   types.ControllerPath + "/group-permissions",
			Func:   ctrl.ReplaceGroupPermissions,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		// ==================== User Management ====================
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/users",
			Func:   ctrl.ListUsers,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/users/:userId",
			Func:   ctrl.GetUser,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/users",
			Func:   ctrl.CreateUser,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "PUT",
			Path:   types.ControllerPath + "/users/:userId",
			Func:   ctrl.ReplaceUser,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "PATCH",
			Path:   types.ControllerPath + "/users/:userId",
			Func:   ctrl.PatchUser,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/users/:userId",
			Func:   ctrl.DeleteUser,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		// ==================== Group Management ====================
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/groups",
			Func:   ctrl.ListGroups,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "GET",
			Path:   types.ControllerPath + "/groups/:id",
			Func:   ctrl.GetGroup,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "POST",
			Path:   types.ControllerPath + "/groups",
			Func:   ctrl.CreateGroup,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "PUT",
			Path:   types.ControllerPath + "/groups/:id",
			Func:   ctrl.ReplaceGroup,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "PATCH",
			Path:   types.ControllerPath + "/groups/:id",
			Func:   ctrl.PatchGroup,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
		&controllers.ControllerHandler{
			Method: "DELETE",
			Path:   types.ControllerPath + "/groups/:id",
			Func:   ctrl.DeleteGroup,
			Authorization: &auth.AuthorizationRequirement{
				RequiredPermissions: []string{
					authtypes.PermissionScimManageUser,
				},
			},
		},
	}
	return ctrl
}

// GetControllerHandlers returns the controller's handlers.
func (ctrl *Controller) GetControllerHandlers() []controllers.IControllerHandler {
	return ctrl.handlers
}

// ListAllTokens returns all tokens in the system (admin view).
// GET /api/v1/admin/tokens
func (ctrl *Controller) ListAllTokens(c *gin.Context) {
	ctx := c.Request.Context()
	l := ctrl.GetRequestLogger(c)

	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputListTokensDTO
	if err := c.ShouldBindWith(&inputDTO, stripe.StripeQuery); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Convert to FilterSet and add pagination/sorting
	filterSet := inputDTO.ToFilterSet()
	pagination, sort := query.ParseQueryParams(c, -1) // default: unlimited
	filterSet.Pagination = pagination
	filterSet.Sort = sort

	// Validate FilterSet
	if err := filterSet.Validate(); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	tokens, totalCount, err := ctrl.authService.ListAllTokens(ctx, l, principal, authtypes.InputListTokensDTO{
		Filters: filterSet,
	})

	ctrl.Render(c, types.NewTokensListResult(
		tokens,
		totalCount,
		filterSet.Pagination,
		err,
	))
}

// RevokeUserToken revokes a specific token belonging to a user.
// DELETE /api/v1/admin/users/:userId/tokens/:tokenId
func (ctrl *Controller) RevokeUserToken(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse user ID from URL
	userIDStr := c.Param("userId")
	userID, err := uuid.FromString(userIDStr)
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid user ID"),
		})
		return
	}

	// Parse token ID from URL
	tokenIDStr := c.Param("tokenId")
	tokenID, err := uuid.FromString(tokenIDStr)
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid token ID"),
		})
		return
	}

	if err := ctrl.authService.RevokeUserToken(ctx, ctrl.GetRequestLogger(c), principal, userID, tokenID); err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildRevokeTokenResponse(), nil))
}

// ==================== Group Permissions ====================

// ListGroupPermissions returns all group permissions with filtering/pagination.
// GET /api/v1/admin/group-permissions
func (ctrl *Controller) ListGroupPermissions(c *gin.Context) {
	ctx := c.Request.Context()
	l := ctrl.GetRequestLogger(c)

	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputListGroupPermissionsDTO
	// Bind Stripe-style query parameters using our custom binding
	if err := c.ShouldBindWith(&inputDTO, stripe.StripeQuery); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Convert to FilterSet and add pagination/sorting
	filterSet := inputDTO.ToFilterSet()
	pagination, sort := query.ParseQueryParams(c, -1) // default: unlimited
	filterSet.Pagination = pagination
	filterSet.Sort = sort

	// Validate FilterSet
	if err := filterSet.Validate(); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Call service layer
	permissions, totalCount, err := ctrl.authService.ListGroupPermissions(
		ctx,
		l,
		principal,
		authtypes.InputListGroupPermissionsDTO{Filters: filterSet},
	)

	ctrl.Render(c, types.NewGroupPermissionsListResult(
		permissions,
		totalCount,
		filterSet.Pagination,
		err,
	))
}

// CreateGroupPermission creates a new group permission.
// POST /api/v1/admin/group-permissions
func (ctrl *Controller) CreateGroupPermission(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	var req types.CreateGroupPermissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	permission := &authmodels.GroupPermission{
		ID:         uuid.MakeV4(),
		GroupName:  req.GroupName,
		Scope:      req.Scope,
		Permission: req.Permission,
	}

	if err := ctrl.authService.CreateGroupPermission(ctx, ctrl.GetRequestLogger(c), principal, permission); err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildGroupPermissionResponse(permission), nil))
}

// UpdateGroupPermission updates an existing group permission.
// PUT /api/v1/admin/group-permissions/:id
func (ctrl *Controller) UpdateGroupPermission(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	// Parse permission ID from URL
	id, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid permission ID"),
		})
		return
	}

	var req types.UpdateGroupPermissionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Build permission with updated fields
	permission := &authmodels.GroupPermission{
		ID:         id,
		GroupName:  req.GroupName,
		Scope:      req.Scope,
		Permission: req.Permission,
	}

	if err := ctrl.authService.UpdateGroupPermission(ctx, ctrl.GetRequestLogger(c), principal, permission); err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildGroupPermissionResponse(permission), nil))
}

// DeleteGroupPermission deletes a group permission.
// DELETE /api/v1/admin/group-permissions/:id
func (ctrl *Controller) DeleteGroupPermission(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	// Parse permission ID from URL
	id, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid permission ID"),
		})
		return
	}

	if err := ctrl.authService.DeleteGroupPermission(ctx, ctrl.GetRequestLogger(c), principal, id); err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildDeleteGroupPermissionResponse(), nil))
}

// ReplaceGroupPermissions replaces all group permissions.
// PUT /api/v1/admin/group-permissions
func (ctrl *Controller) ReplaceGroupPermissions(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	var req types.ReplaceGroupPermissionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Convert request to models
	permissions := make([]*authmodels.GroupPermission, len(req.Permissions))
	for i, p := range req.Permissions {
		permissions[i] = &authmodels.GroupPermission{
			ID:         uuid.MakeV4(),
			GroupName:  p.GroupName,
			Scope:      p.Scope,
			Permission: p.Permission,
		}
	}

	if err := ctrl.authService.ReplaceGroupPermissions(ctx, ctrl.GetRequestLogger(c), principal, permissions); err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildReplaceGroupPermissionsResponse(), nil))
}
