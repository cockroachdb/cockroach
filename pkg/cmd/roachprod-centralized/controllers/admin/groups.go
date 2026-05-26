// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admin

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/admin/types"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/stripe"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/query"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
)

// ListGroups returns a list of groups with optional filtering and pagination.
// GET /api/v1/admin/groups
func (ctrl *Controller) ListGroups(c *gin.Context) {
	ctx := c.Request.Context()
	l := ctrl.GetRequestLogger(c)
	principal, _ := controllers.GetPrincipal(c)

	var inputDTO types.InputListGroupsDTO
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

	groups, totalCount, err := ctrl.authService.ListGroups(ctx, l, principal, authtypes.InputListGroupsDTO{
		Filters: filterSet,
	})

	ctrl.Render(c, types.NewGroupsListResult(groups, totalCount, filterSet.Pagination, err))
}

// GetGroup retrieves a specific group by ID, including its members.
// GET /api/v1/admin/groups/:id
func (ctrl *Controller) GetGroup(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	groupID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid group ID"),
		})
		return
	}

	group, members, err := ctrl.authService.GetGroupWithMembers(ctx, ctrl.GetRequestLogger(c), principal, groupID)
	if err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildGroupResponse(group, members), nil))
}

// CreateGroup creates a new group with optional initial members.
// POST /api/v1/admin/groups
func (ctrl *Controller) CreateGroup(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	var req types.CreateGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Parse member UUIDs
	memberIDs, err := parseUUIDs(req.MemberIDs)
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.Wrap(err, "invalid member ID"),
		})
		return
	}

	input := authtypes.CreateGroupInput{
		ExternalID:  req.ExternalID,
		DisplayName: req.DisplayName,
		Members:     memberIDs,
	}

	group, members, err := ctrl.authService.CreateGroupWithMembers(ctx, ctrl.GetRequestLogger(c), principal, input)
	if err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildGroupResponse(group, members), nil))
}

// ReplaceGroup completely replaces an existing group's information.
// PUT /api/v1/admin/groups/:id
func (ctrl *Controller) ReplaceGroup(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	groupID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid group ID"),
		})
		return
	}

	var req types.ReplaceGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Parse member UUIDs
	memberIDs, err := parseUUIDs(req.MemberIDs)
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.Wrap(err, "invalid member ID"),
		})
		return
	}

	input := authtypes.ReplaceGroupInput{
		ExternalID:  req.ExternalID,
		DisplayName: req.DisplayName,
		Members:     memberIDs,
	}

	group, members, err := ctrl.authService.ReplaceGroup(ctx, ctrl.GetRequestLogger(c), principal, groupID, input)
	if err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildGroupResponse(group, members), nil))
}

// PatchGroup partially updates an existing group's information.
// PATCH /api/v1/admin/groups/:id
func (ctrl *Controller) PatchGroup(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	groupID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid group ID"),
		})
		return
	}

	var req types.PatchGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	// Convert admin patch request to service patch operations
	operations, err := convertGroupPatchToOperations(req)
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{Error: err})
		return
	}

	if len(operations) == 0 {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("no fields to update"),
		})
		return
	}

	input := authtypes.PatchGroupInput{
		Operations: operations,
	}

	output, err := ctrl.authService.PatchGroup(ctx, ctrl.GetRequestLogger(c), principal, groupID, input)
	if err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildGroupResponse(output.Group, output.Members), nil))
}

// convertGroupPatchToOperations converts an admin PatchGroupRequest to service layer patch operations.
func convertGroupPatchToOperations(
	req types.PatchGroupRequest,
) ([]authtypes.GroupPatchOperation, error) {
	var operations []authtypes.GroupPatchOperation

	if req.DisplayName != nil {
		operations = append(operations, authtypes.GroupPatchOperation{
			Op:    "replace",
			Path:  "displayName",
			Value: *req.DisplayName,
		})
	}
	if req.ExternalID != nil {
		operations = append(operations, authtypes.GroupPatchOperation{
			Op:    "replace",
			Path:  "externalId",
			Value: *req.ExternalID,
		})
	}

	// Handle member additions
	for _, memberIDStr := range req.AddMembers {
		memberID, err := uuid.FromString(memberIDStr)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid member ID: %s", memberIDStr)
		}
		operations = append(operations, authtypes.GroupPatchOperation{
			Op:   "add",
			Path: "members",
			Value: []map[string]string{
				{"value": memberID.String()},
			},
		})
	}

	// Handle member removals
	for _, memberIDStr := range req.RemoveMembers {
		memberID, err := uuid.FromString(memberIDStr)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid member ID: %s", memberIDStr)
		}
		operations = append(operations, authtypes.GroupPatchOperation{
			Op:   "remove",
			Path: "members",
			Value: []map[string]string{
				{"value": memberID.String()},
			},
		})
	}

	return operations, nil
}

// DeleteGroup removes a group from the system.
// DELETE /api/v1/admin/groups/:id
func (ctrl *Controller) DeleteGroup(c *gin.Context) {
	ctx := c.Request.Context()
	principal, _ := controllers.GetPrincipal(c)

	groupID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.Render(c, &controllers.BadRequestResult{
			Error: errors.New("invalid group ID"),
		})
		return
	}

	if err := ctrl.authService.DeleteGroup(ctx, ctrl.GetRequestLogger(c), principal, groupID); err != nil {
		ctrl.Render(c, types.NewAdminResult(nil, err))
		return
	}

	ctrl.Render(c, types.NewAdminResult(types.BuildDeleteGroupResponse(), nil))
}

// parseUUIDs parses a slice of UUID strings into a slice of uuid.UUID.
func parseUUIDs(ids []string) ([]uuid.UUID, error) {
	result := make([]uuid.UUID, 0, len(ids))
	for _, idStr := range ids {
		id, err := uuid.FromString(idStr)
		if err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, nil
}
