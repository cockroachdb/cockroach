// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scim

import (
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/scim/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	scimbindings "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/bindings/scim"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/api/query"
	filters "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
)

// ListGroups returns a list of groups with optional filtering, pagination, and sorting.
// GET /scim/v2/Groups
func (ctrl *Controller) ListGroups(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse SCIM filter query parameter
	filterStr := c.Query("filter")

	var filterSet filtertypes.FilterSet

	if filterStr != "" {
		parsedFilterSet, err := scimbindings.ParseSCIMFilter(filterStr)
		if err != nil {
			ctrl.renderIdentityResult(c, nil, errors.Wrap(err, "invalid SCIM filter"))
			return
		}
		filterSet = parsedFilterSet
	} else {
		filterSet = *filters.NewFilterSet()
	}

	// Parse pagination and sorting parameters
	pagination, sort := query.ParseQueryParams(c, -1)
	filterSet.Pagination = pagination
	filterSet.Sort = sort

	// Translate SCIM field names to domain model field names
	if !filterSet.IsEmpty() || filterSet.Sort != nil {
		filterSet = scimbindings.TranslateGroupFilterFields(filterSet)
	}

	if err := filterSet.Validate(); err != nil {
		ctrl.renderIdentityResult(c, nil, errors.Wrap(err, "invalid filter"))
		return
	}

	inputDTO := authtypes.InputListGroupsDTO{
		Filters: filterSet,
	}

	groups, totalCount, err := ctrl.authService.ListGroups(ctx, ctrl.GetRequestLogger(c), principal, inputDTO)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	// Convert to SCIM format
	groupResources := make([]types.GroupResource, 0, len(groups))
	for _, group := range groups {
		groupResources = append(groupResources, ctrl.toGroupResource(group, nil))
	}

	startIndex := 1
	if filterSet.Pagination != nil {
		startIndex = filterSet.Pagination.StartIndex
	}

	response := types.BuildSCIMListResponse(
		groupResources,
		totalCount,
		startIndex,
		[]string{types.SchemaListResponse},
	)

	ctrl.renderIdentityResult(c, response, nil)
}

// GetGroup retrieves a specific group by ID.
// GET /scim/v2/Groups/:id
func (ctrl *Controller) GetGroup(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	groupIDStr := c.Param("id")

	groupID, err := uuid.FromString(groupIDStr)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrInvalidUUID)
		return
	}

	group, members, err := ctrl.authService.GetGroupWithMembers(ctx, ctrl.GetRequestLogger(c), principal, groupID)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	ctrl.renderIdentityResult(c, ctrl.toGroupResource(group, members), nil)
}

// CreateGroup creates a new group.
// POST /scim/v2/Groups
func (ctrl *Controller) CreateGroup(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	var req types.CreateGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrRequiredField)
		return
	}

	// Convert SCIM member refs to UUIDs
	memberIDs := make([]uuid.UUID, 0, len(req.Members))
	for _, member := range req.Members {
		userID, err := uuid.FromString(member.Value)
		if err != nil {
			ctrl.renderIdentityResult(c, nil, authtypes.ErrInvalidUUID)
			return
		}
		memberIDs = append(memberIDs, userID)
	}

	input := authtypes.CreateGroupInput{
		ExternalID:  req.ExternalID,
		DisplayName: req.DisplayName,
		Members:     memberIDs,
	}

	group, members, err := ctrl.authService.CreateGroupWithMembers(ctx, ctrl.GetRequestLogger(c), principal, input)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	ctrl.renderIdentityResult(c, ctrl.toGroupResource(group, members), nil)
}

// ReplaceGroup completely replaces an existing group's information.
// PUT /scim/v2/Groups/:id
func (ctrl *Controller) ReplaceGroup(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	groupIDStr := c.Param("id")

	groupID, err := uuid.FromString(groupIDStr)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrInvalidUUID)
		return
	}

	var req types.CreateGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrRequiredField)
		return
	}

	// Convert SCIM member refs to UUIDs
	memberIDs := make([]uuid.UUID, 0, len(req.Members))
	for _, member := range req.Members {
		userID, err := uuid.FromString(member.Value)
		if err != nil {
			ctrl.renderIdentityResult(c, nil, authtypes.ErrInvalidUUID)
			return
		}
		memberIDs = append(memberIDs, userID)
	}

	input := authtypes.ReplaceGroupInput{
		ExternalID:  req.ExternalID,
		DisplayName: req.DisplayName,
		Members:     memberIDs,
	}

	group, members, err := ctrl.authService.ReplaceGroup(ctx, ctrl.GetRequestLogger(c), principal, groupID, input)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	ctrl.renderIdentityResult(c, ctrl.toGroupResource(group, members), nil)
}

// PatchGroup partially updates an existing group's information.
// PATCH /scim/v2/Groups/:id
func (ctrl *Controller) PatchGroup(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	groupIDStr := c.Param("id")

	groupID, err := uuid.FromString(groupIDStr)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrInvalidUUID)
		return
	}

	var req map[string]interface{}
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrRequiredField)
		return
	}

	// Parse SCIM PATCH operations and convert to service DTOs
	operations, err := ctrl.parseSCIMPatchOperations(req)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrRequiredField)
		return
	}

	input := authtypes.PatchGroupInput{
		Operations: operations,
	}

	output, err := ctrl.authService.PatchGroup(ctx, ctrl.GetRequestLogger(c), principal, groupID, input)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	ctrl.renderIdentityResult(c, ctrl.toGroupResource(output.Group, output.Members), nil)
}

// parseSCIMPatchOperations parses SCIM PATCH operations from the request body.
// Handles SCIM filter paths like members[filter-expr] by extracting the filter
// and converting it to a FilterSet for the service layer.
func (ctrl *Controller) parseSCIMPatchOperations(
	req map[string]interface{},
) ([]authtypes.GroupPatchOperation, error) {
	var operations []authtypes.GroupPatchOperation

	ops, ok := req["Operations"].([]interface{})
	if !ok {
		return nil, errors.New("missing or invalid Operations field")
	}

	for _, op := range ops {
		operation, ok := op.(map[string]interface{})
		if !ok {
			continue
		}

		opType, _ := operation["op"].(string)
		path, _ := operation["path"].(string)
		value := operation["value"]

		// Handle SCIM filter paths: members[filter-expression]
		// Extract the filter and convert to a FilterSet for the service layer
		if strings.HasPrefix(path, "members[") && strings.HasSuffix(path, "]") {
			filterExpr := path[len("members[") : len(path)-1]
			filterSet, err := scimbindings.ParseSCIMFilter(filterExpr)
			if err == nil {
				// Translate SCIM field names (e.g., "value") to database column names (e.g., "user_id")
				translatedFilterSet := scimbindings.TranslateGroupMemberFilterFields(filterSet)
				operations = append(operations, authtypes.GroupPatchOperation{
					Op:      opType,
					Path:    "members",
					Filters: &translatedFilterSet,
				})
				continue
			}
			// If filter parsing fails, fall through to include raw path
		}

		// Normalize member values to []map[string]string for consistent service layer handling.
		// This only applies to path == "members" (exact match), not filter paths like "members[...]".
		if path == "members" && value != nil {
			value = normalizeMemberValue(value)
		}

		operations = append(operations, authtypes.GroupPatchOperation{
			Op:    opType,
			Path:  path,
			Value: value,
		})
	}

	return operations, nil
}

// normalizeMemberValue converts SCIM member values from interface{} types to []map[string]string.
// This normalizes the JSON-unmarshaled types to a consistent format for the service layer.
func normalizeMemberValue(value interface{}) []map[string]string {
	var result []map[string]string

	switch v := value.(type) {
	case []interface{}:
		for _, item := range v {
			if member, ok := item.(map[string]interface{}); ok {
				normalized := make(map[string]string)
				for key, val := range member {
					if strVal, ok := val.(string); ok {
						normalized[key] = strVal
					}
				}
				if len(normalized) > 0 {
					result = append(result, normalized)
				}
			}
		}
	case map[string]interface{}:
		normalized := make(map[string]string)
		for key, val := range v {
			if strVal, ok := val.(string); ok {
				normalized[key] = strVal
			}
		}
		if len(normalized) > 0 {
			result = append(result, normalized)
		}
	case []map[string]string:
		// Already normalized
		return v
	case map[string]string:
		// Single member, wrap in slice
		return []map[string]string{v}
	}

	return result
}

// DeleteGroup removes a group from the identity management system.
// DELETE /scim/v2/Groups/:id
func (ctrl *Controller) DeleteGroup(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	groupID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrInvalidUUID)
		return
	}

	if err := ctrl.authService.DeleteGroup(ctx, ctrl.GetRequestLogger(c), principal, groupID); err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	ctrl.renderIdentityResult(c, nil, nil)
}

// toGroupResource converts an auth.Group and its members to SCIM GroupResource format.
func (ctrl *Controller) toGroupResource(
	group *auth.Group, members []*auth.GroupMember,
) types.GroupResource {
	memberRefs := make([]types.GroupMemberRef, 0, len(members))
	for _, member := range members {
		memberRefs = append(memberRefs, types.GroupMemberRef{
			Value: member.UserID.String(),
			Ref:   types.ControllerPath + "/Users/" + member.UserID.String(),
		})
	}

	externalId := ""
	if group.ExternalID != nil {
		externalId = *group.ExternalID
	}

	return types.GroupResource{
		Schemas:     []string{types.SchemaGroup},
		ID:          group.ID.String(),
		ExternalID:  externalId,
		DisplayName: group.DisplayName,
		Members:     memberRefs,
		Meta: map[string]string{
			"resourceType": "Group",
			"created":      group.CreatedAt.Format(time.RFC3339),
			"lastModified": group.UpdatedAt.Format(time.RFC3339),
			"location":     types.ControllerPath + "/Groups/" + group.ID.String(),
		},
	}
}
