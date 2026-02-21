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

// ListUsers returns a list of users with optional filtering, pagination, and sorting.
// GET /scim/v2/Users
func (ctrl *Controller) ListUsers(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse SCIM filter query parameter (SCIM 2.0 spec section 3.4.2.2)
	filterStr := c.Query("filter")

	var filterSet filtertypes.FilterSet

	if filterStr != "" {
		// Parse SCIM filter and convert to FilterSet
		parsedFilterSet, err := scimbindings.ParseSCIMFilter(filterStr)
		if err != nil {
			ctrl.renderIdentityResult(c, nil, errors.Wrap(err, "invalid SCIM filter"))
			return
		}
		filterSet = parsedFilterSet
	} else {
		// No filter - create empty FilterSet
		filterSet = *filters.NewFilterSet()
	}

	// Parse pagination and sorting parameters (SCIM 2.0 spec section 3.4.2.4)
	// Default to unlimited (-1) if not specified
	pagination, sort := query.ParseQueryParams(c, -1)
	filterSet.Pagination = pagination
	filterSet.Sort = sort

	// Translate SCIM field names to domain model field names
	if !filterSet.IsEmpty() || filterSet.Sort != nil {
		filterSet = scimbindings.TranslateUserFilterFields(filterSet)
	}

	// Validate the complete filter set with pagination and sorting
	if err := filterSet.Validate(); err != nil {
		ctrl.renderIdentityResult(c, nil, errors.Wrap(err, "invalid filter"))
		return
	}

	inputDTO := authtypes.InputListUsersDTO{
		Filters: filterSet,
	}

	users, totalCount, err := ctrl.authService.ListUsers(ctx, ctrl.GetRequestLogger(c), principal, inputDTO)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	// Convert to cross-domain identity management format
	userResources := make([]types.UserResource, 0, len(users))
	for _, user := range users {
		userResources = append(userResources, ctrl.toUserResource(user))
	}

	// Build SCIM response with pagination metadata
	startIndex := 1
	if filterSet.Pagination != nil {
		startIndex = filterSet.Pagination.StartIndex
	}

	response := types.BuildSCIMListResponse(
		userResources,
		totalCount,
		startIndex,
		[]string{types.SchemaListResponse},
	)

	ctrl.renderIdentityResult(c, response, nil)
}

// GetUser retrieves a specific user by ID.
// GET /scim/v2/Users/:id
func (ctrl *Controller) GetUser(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse UUID from string
	userID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrInvalidUUID)
		return
	}

	user, err := ctrl.authService.GetUser(ctx, ctrl.GetRequestLogger(c), principal, userID)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	ctrl.renderIdentityResult(c, ctrl.toUserResource(user), nil)
}

// CreateUser creates a new user in the identity management system.
// POST /scim/v2/Users
func (ctrl *Controller) CreateUser(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	var req types.CreateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrRequiredField)
		return
	}

	// Default active to true if not specified
	active := true
	if req.Active != nil {
		active = *req.Active
	}

	newUser := &auth.User{
		OktaUserID: req.ExternalID,
		Email:      req.UserName,
		FullName:   extractFullName(req.Name),
		Active:     active,
	}

	err := ctrl.authService.CreateUser(ctx, ctrl.GetRequestLogger(c), principal, newUser)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	// CreateUser mutates newUser with ID and timestamps
	ctrl.renderIdentityResult(c, ctrl.toUserResource(newUser), nil)
}

// ReplaceUser completely replaces an existing user's information.
// PUT /scim/v2/Users/:id
func (ctrl *Controller) ReplaceUser(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	userID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrInvalidUUID)
		return
	}

	var req types.CreateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrRequiredField)
		return
	}

	active := true
	if req.Active != nil {
		active = *req.Active
	}

	input := authtypes.ReplaceUserInput{
		ExternalID: req.ExternalID,
		UserName:   req.UserName,
		FullName:   extractFullName(req.Name),
		Active:     active,
	}

	user, err := ctrl.authService.ReplaceUser(ctx, ctrl.GetRequestLogger(c), principal, userID, input)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	ctrl.renderIdentityResult(c, ctrl.toUserResource(user), nil)
}

// PatchUser partially updates an existing user's information.
// PATCH /scim/v2/Users/:id
func (ctrl *Controller) PatchUser(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	userID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrInvalidUUID)
		return
	}

	var req map[string]interface{}
	if err := c.ShouldBindJSON(&req); err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrRequiredField)
		return
	}

	// Parse SCIM PATCH operations and convert to service DTO
	operations, err := ctrl.parseSCIMUserPatchOperations(req)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrRequiredField)
		return
	}

	input := authtypes.PatchUserInput{
		Operations: operations,
	}

	user, err := ctrl.authService.PatchUser(ctx, ctrl.GetRequestLogger(c), principal, userID, input)
	if err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	ctrl.renderIdentityResult(c, ctrl.toUserResource(user), nil)
}

// parseSCIMUserPatchOperations parses SCIM PATCH operations from the request body.
func (ctrl *Controller) parseSCIMUserPatchOperations(
	req map[string]interface{},
) ([]authtypes.UserPatchOperation, error) {
	var operations []authtypes.UserPatchOperation

	// Handle SCIM 2.0 PATCH format with Operations array
	if ops, ok := req["Operations"].([]interface{}); ok {
		for _, op := range ops {
			operation, ok := op.(map[string]interface{})
			if !ok {
				continue
			}

			opType, _ := operation["op"].(string)
			path, _ := operation["path"].(string)
			value := operation["value"]

			operations = append(operations, authtypes.UserPatchOperation{
				Op:    opType,
				Path:  path,
				Value: value,
			})
		}
		return operations, nil
	}

	// Handle direct field updates (simplified SCIM PATCH)
	if active, ok := req["active"].(bool); ok {
		operations = append(operations, authtypes.UserPatchOperation{
			Op:    "replace",
			Path:  "active",
			Value: active,
		})
	}
	if userName, ok := req["userName"].(string); ok {
		operations = append(operations, authtypes.UserPatchOperation{
			Op:    "replace",
			Path:  "userName",
			Value: userName,
		})
	}
	if name, ok := req["name"].(map[string]interface{}); ok {
		// Extract full name from SCIM name components
		fullName := extractFullNameFromInterface(name)
		if fullName != "" {
			operations = append(operations, authtypes.UserPatchOperation{
				Op:    "replace",
				Path:  "name.formatted",
				Value: fullName,
			})
		}
	}

	return operations, nil
}

// DeleteUser removes a user from the identity management system.
// DELETE /scim/v2/Users/:id
func (ctrl *Controller) DeleteUser(c *gin.Context) {
	ctx := c.Request.Context()

	principal, _ := controllers.GetPrincipal(c)

	// Parse UUID from string
	userID, err := uuid.FromString(c.Param("id"))
	if err != nil {
		ctrl.renderIdentityResult(c, nil, authtypes.ErrInvalidUUID)
		return
	}

	if err := ctrl.authService.DeleteUser(ctx, ctrl.GetRequestLogger(c), principal, userID); err != nil {
		ctrl.renderIdentityResult(c, nil, err)
		return
	}

	ctrl.renderIdentityResult(c, nil, nil)
}

// extractFullName extracts or constructs a full name from SCIM name components.
// If "formatted" is present, it returns that. Otherwise, it constructs the name
// from givenName and familyName (per SCIM RFC 7643 section 4.1.1).
func extractFullName(name map[string]string) string {
	if name == nil {
		return ""
	}
	// Prefer formatted if explicitly provided
	if formatted := name["formatted"]; formatted != "" {
		return formatted
	}
	// Construct from components: givenName + familyName
	var parts []string
	if givenName := name["givenName"]; givenName != "" {
		parts = append(parts, givenName)
	}
	if familyName := name["familyName"]; familyName != "" {
		parts = append(parts, familyName)
	}
	return strings.Join(parts, " ")
}

// extractFullNameFromInterface is like extractFullName but for map[string]interface{}.
func extractFullNameFromInterface(name map[string]interface{}) string {
	if name == nil {
		return ""
	}
	// Prefer formatted if explicitly provided
	if formatted, ok := name["formatted"].(string); ok && formatted != "" {
		return formatted
	}
	// Construct from components: givenName + familyName
	var parts []string
	if givenName, ok := name["givenName"].(string); ok && givenName != "" {
		parts = append(parts, givenName)
	}
	if familyName, ok := name["familyName"].(string); ok && familyName != "" {
		parts = append(parts, familyName)
	}
	return strings.Join(parts, " ")
}

// toUserResource converts an auth.User to an identity management resource format.
func (ctrl *Controller) toUserResource(user *auth.User) types.UserResource {
	return types.UserResource{
		Schemas:    []string{types.SchemaUser},
		ID:         user.ID.String(),
		ExternalID: user.OktaUserID,
		UserName:   user.Email,
		Name: map[string]string{
			"formatted": user.FullName,
		},
		Emails: []map[string]interface{}{
			{
				"value":   user.Email,
				"primary": true,
			},
		},
		Active: user.Active,
		Meta: map[string]string{
			"resourceType": "User",
			"created":      user.CreatedAt.Format(time.RFC3339),
			"lastModified": user.UpdatedAt.Format(time.RFC3339),
			"location":     types.ControllerPath + "/Users/" + user.ID.String(),
		},
	}
}
