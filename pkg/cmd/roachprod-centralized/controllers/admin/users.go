// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admin

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/admin/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/query"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
)

// ListUsers returns a list of users with optional filtering and pagination.
// GET /api/v1/admin/users
func (ctrl *Controller) ListUsers(c *gin.Context) {
	ctx := c.Request.Context()
	l := ctrl.GetRequestLogger(c)
	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputListUsersDTO
	if err := c.ShouldBindWith(&inputDTO, stripe.StripeQuery); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	filterSet := inputDTO.ToFilterSet()
	pagination, sort := query.ParseQueryParams(c, -1)
	filterSet.Pagination = pagination
	filterSet.Sort = sort

	if err := filterSet.Validate(); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	users, totalCount, err := ctrl.authService.ListUsers(ctx, l, principal, authtypes.InputListUsersDTO{
		Filters: filterSet,
	})

	ctrl.Render(c, types.NewUsersListResult(users, totalCount, filterSet.Pagination, err))
}

// GetUser retrieves a specific user by ID.
// GET /api/v1/admin/users/:id
func (ctrl *Controller) GetUser(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	userIDStr := c.Param("userId")
	userID, err := uuid.FromString(userIDStr)
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid user ID"),
		})
		return
	}

	user, err := ctrl.authService.GetUser(ctx, ctrl.GetRequestLogger(c), principal, userID)
	if err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildUserResponse(user), nil))
}

// CreateUser creates a new user.
// POST /api/v1/admin/users
func (ctrl *Controller) CreateUser(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	var req types.CreateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Default active to true if not specified
	active := true
	if req.Active != nil {
		active = *req.Active
	}

	newUser := &auth.User{
		OktaUserID:  req.OktaUserID,
		Email:       req.Email,
		FullName:    req.FullName,
		SlackHandle: req.SlackHandle,
		Active:      active,
	}

	if err := ctrl.authService.CreateUser(ctx, ctrl.GetRequestLogger(c), principal, newUser); err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildUserResponse(newUser), nil))
}

// ReplaceUser completely replaces an existing user's information.
// PUT /api/v1/admin/users/:id
func (ctrl *Controller) ReplaceUser(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	userID, err := uuid.FromString(c.Param("userId"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid user ID"),
		})
		return
	}

	var req types.ReplaceUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	input := authtypes.ReplaceUserInput{
		ExternalID: req.OktaUserID,
		UserName:   req.Email,
		FullName:   req.FullName,
		Active:     req.Active,
	}

	user, err := ctrl.authService.ReplaceUser(ctx, ctrl.GetRequestLogger(c), principal, userID, input)
	if err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildUserResponse(user), nil))
}

// PatchUser partially updates an existing user's information.
// PATCH /api/v1/admin/users/:id
func (ctrl *Controller) PatchUser(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	userID, err := uuid.FromString(c.Param("userId"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid user ID"),
		})
		return
	}

	var req types.PatchUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Convert admin patch request to service patch operations
	operations := ctrl.convertUserPatchToOperations(req)
	if len(operations) == 0 {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("no fields to update"),
		})
		return
	}

	input := authtypes.PatchUserInput{
		Operations: operations,
	}

	user, err := ctrl.authService.PatchUser(ctx, ctrl.GetRequestLogger(c), principal, userID, input)
	if err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildUserResponse(user), nil))
}

// convertUserPatchToOperations converts an admin PatchUserRequest to service layer patch operations.
func (ctrl *Controller) convertUserPatchToOperations(
	req types.PatchUserRequest,
) []authtypes.UserPatchOperation {
	var operations []authtypes.UserPatchOperation

	if req.Active != nil {
		operations = append(operations, authtypes.UserPatchOperation{
			Op:    "replace",
			Path:  "active",
			Value: *req.Active,
		})
	}
	if req.Email != nil {
		operations = append(operations, authtypes.UserPatchOperation{
			Op:    "replace",
			Path:  "userName",
			Value: *req.Email,
		})
	}
	if req.FullName != nil {
		operations = append(operations, authtypes.UserPatchOperation{
			Op:    "replace",
			Path:  "name.formatted",
			Value: *req.FullName,
		})
	}
	if req.OktaUserID != nil {
		operations = append(operations, authtypes.UserPatchOperation{
			Op:    "replace",
			Path:  "externalId",
			Value: *req.OktaUserID,
		})
	}
	// Note: SlackHandle is not supported by the service layer PatchUser

	return operations
}

// DeleteUser removes a user from the system.
// DELETE /api/v1/admin/users/:id
func (ctrl *Controller) DeleteUser(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	userID, err := uuid.FromString(c.Param("userId"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid user ID"),
		})
		return
	}

	if err := ctrl.authService.DeleteUser(ctx, ctrl.GetRequestLogger(c), principal, userID); err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildDeleteUserResponse(), nil))
}
