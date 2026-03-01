// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scim

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/scim/types"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/mocks"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// createSCIMTestContext creates a test Gin context with optional principal for SCIM tests.
func createSCIMTestContext(
	principal *pkgauth.Principal, method, path string, body io.Reader,
) (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, path, body)
	c.Request.Header.Set("Content-Type", "application/scim+json")
	if principal != nil {
		controllers.SetPrincipal(c, principal)
	}
	return c, w
}

// scimAdminPrincipal creates a principal with SCIM admin permissions.
func scimAdminPrincipal() *pkgauth.Principal {
	return &pkgauth.Principal{
		User: &authmodels.User{Email: "scim-admin@example.com"},
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{Permission: authtypes.PermissionScimManageUser},
		},
	}
}

// User Endpoint Tests

func TestSCIMController_ListUsers(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	userID1 := uuid.MakeV4()
	userID2 := uuid.MakeV4()
	now := timeutil.Now()

	users := []*authmodels.User{
		{
			ID:         userID1,
			Email:      "user1@example.com",
			FullName:   "User One",
			Active:     true,
			OktaUserID: "okta-123",
			CreatedAt:  now,
			UpdatedAt:  now,
		},
		{
			ID:         userID2,
			Email:      "user2@example.com",
			FullName:   "User Two",
			Active:     true,
			OktaUserID: "okta-456",
			CreatedAt:  now,
			UpdatedAt:  now,
		},
	}

	mockService.On("ListUsers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(users, 2, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Users", nil)
	ctrl.ListUsers(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{types.SchemaListResponse}, response["schemas"])
	assert.Equal(t, float64(2), response["totalResults"])
	assert.Equal(t, float64(1), response["startIndex"])

	resources := response["Resources"].([]interface{})
	assert.Len(t, resources, 2)

	user1 := resources[0].(map[string]interface{})
	assert.Equal(t, userID1.String(), user1["id"])
	assert.Equal(t, "user1@example.com", user1["userName"])
	assert.True(t, user1["active"].(bool))

	mockService.AssertExpectations(t)
}

func TestSCIMController_ListUsers_WithFilter(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	userID := uuid.MakeV4()
	now := timeutil.Now()

	users := []*authmodels.User{
		{
			ID:         userID,
			Email:      "alice@example.com",
			FullName:   "Alice Smith",
			Active:     true,
			OktaUserID: "okta-123",
			CreatedAt:  now,
			UpdatedAt:  now,
		},
	}

	mockService.On("ListUsers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(users, 1, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Users?filter=userName+eq+\"alice@example.com\"", nil)
	ctrl.ListUsers(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, float64(1), response["totalResults"])
	resources := response["Resources"].([]interface{})
	assert.Len(t, resources, 1)

	mockService.AssertExpectations(t)
}

func TestSCIMController_GetUser(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	userID := uuid.MakeV4()
	now := timeutil.Now()

	user := &authmodels.User{
		ID:         userID,
		Email:      "user@example.com",
		FullName:   "Test User",
		Active:     true,
		OktaUserID: "okta-123",
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	mockService.On("GetUser", mock.Anything, mock.Anything, mock.Anything, userID).
		Return(user, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Users/"+userID.String(), nil)
	c.Params = []gin.Param{{Key: "id", Value: userID.String()}}
	ctrl.GetUser(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response types.UserResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, userID.String(), response.ID)
	assert.Equal(t, "user@example.com", response.UserName)
	assert.Equal(t, "Test User", response.Name["formatted"])
	assert.True(t, response.Active)
	assert.Equal(t, []string{types.SchemaUser}, response.Schemas)

	mockService.AssertExpectations(t)
}

func TestSCIMController_GetUser_NotFound(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	userID := uuid.MakeV4()

	mockService.On("GetUser", mock.Anything, mock.Anything, mock.Anything, userID).
		Return((*authmodels.User)(nil), authtypes.ErrUserNotFound)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Users/"+userID.String(), nil)
	c.Params = []gin.Param{{Key: "id", Value: userID.String()}}
	ctrl.GetUser(c)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{types.SchemaError}, response["schemas"])
	assert.Equal(t, float64(404), response["status"])
	assert.NotEmpty(t, response["detail"])

	mockService.AssertExpectations(t)
}

func TestSCIMController_CreateUser(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	active := true
	requestBody := types.CreateUserRequest{
		Schemas:    []string{types.SchemaUser},
		ExternalID: "okta-789",
		UserName:   "newuser@example.com",
		Name: map[string]string{
			"formatted": "New User",
		},
		Active: &active,
	}

	bodyBytes, _ := json.Marshal(requestBody)

	mockService.On("CreateUser", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(u *authmodels.User) bool {
		return u.Email == "newuser@example.com" && u.FullName == "New User" && u.OktaUserID == "okta-789" && u.Active == true
	})).Run(func(args mock.Arguments) {
		user := args.Get(3).(*authmodels.User)
		user.ID = uuid.MakeV4()
		user.CreatedAt = timeutil.Now()
		user.UpdatedAt = timeutil.Now()
	}).Return(nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "POST", "/scim/v2/Users", bytes.NewReader(bodyBytes))
	ctrl.CreateUser(c)

	assert.Equal(t, http.StatusCreated, w.Code)

	var response types.UserResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "newuser@example.com", response.UserName)
	assert.Equal(t, "New User", response.Name["formatted"])
	assert.True(t, response.Active)
	assert.Equal(t, "okta-789", response.ExternalID)

	mockService.AssertExpectations(t)
}

func TestSCIMController_CreateUser_InvalidSchema(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	// Invalid JSON - missing required userName field
	requestBody := map[string]interface{}{
		"schemas": []string{types.SchemaUser},
		"active":  true,
	}

	bodyBytes, _ := json.Marshal(requestBody)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "POST", "/scim/v2/Users", bytes.NewReader(bodyBytes))
	ctrl.CreateUser(c)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{types.SchemaError}, response["schemas"])
	assert.Equal(t, float64(400), response["status"])

	mockService.AssertExpectations(t)
}

func TestSCIMController_ReplaceUser(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	userID := uuid.MakeV4()
	now := timeutil.Now()

	active := false
	requestBody := types.CreateUserRequest{
		Schemas:    []string{types.SchemaUser},
		ExternalID: "okta-999",
		UserName:   "updated@example.com",
		Name: map[string]string{
			"formatted": "Updated User",
		},
		Active: &active,
	}

	bodyBytes, _ := json.Marshal(requestBody)

	updatedUser := &authmodels.User{
		ID:         userID,
		Email:      "updated@example.com",
		FullName:   "Updated User",
		Active:     false,
		OktaUserID: "okta-999",
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	mockService.On("ReplaceUser", mock.Anything, mock.Anything, mock.Anything, userID, mock.MatchedBy(func(input authtypes.ReplaceUserInput) bool {
		return input.UserName == "updated@example.com" && input.FullName == "Updated User" && input.Active == false
	})).Return(updatedUser, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "PUT", "/scim/v2/Users/"+userID.String(), bytes.NewReader(bodyBytes))
	c.Params = []gin.Param{{Key: "id", Value: userID.String()}}
	ctrl.ReplaceUser(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response types.UserResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "updated@example.com", response.UserName)
	assert.False(t, response.Active)

	mockService.AssertExpectations(t)
}

func TestSCIMController_PatchUser(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	userID := uuid.MakeV4()
	now := timeutil.Now()

	requestBody := map[string]interface{}{
		"schemas": []string{types.SchemaPatchOp},
		"Operations": []interface{}{
			map[string]interface{}{
				"op":    "replace",
				"path":  "active",
				"value": false,
			},
		},
	}

	bodyBytes, _ := json.Marshal(requestBody)

	patchedUser := &authmodels.User{
		ID:         userID,
		Email:      "user@example.com",
		FullName:   "Test User",
		Active:     false,
		OktaUserID: "okta-123",
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	mockService.On("PatchUser", mock.Anything, mock.Anything, mock.Anything, userID, mock.MatchedBy(func(input authtypes.PatchUserInput) bool {
		return len(input.Operations) == 1 && input.Operations[0].Op == "replace" && input.Operations[0].Path == "active"
	})).Return(patchedUser, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "PATCH", "/scim/v2/Users/"+userID.String(), bytes.NewReader(bodyBytes))
	c.Params = []gin.Param{{Key: "id", Value: userID.String()}}
	ctrl.PatchUser(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response types.UserResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.False(t, response.Active)

	mockService.AssertExpectations(t)
}

func TestSCIMController_PatchUser_Deactivate(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	userID := uuid.MakeV4()
	now := timeutil.Now()

	requestBody := map[string]interface{}{
		"schemas": []string{types.SchemaPatchOp},
		"Operations": []interface{}{
			map[string]interface{}{
				"op":    "replace",
				"path":  "active",
				"value": false,
			},
		},
	}

	bodyBytes, _ := json.Marshal(requestBody)

	deactivatedUser := &authmodels.User{
		ID:         userID,
		Email:      "user@example.com",
		FullName:   "Test User",
		Active:     false,
		OktaUserID: "okta-123",
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	mockService.On("PatchUser", mock.Anything, mock.Anything, mock.Anything, userID, mock.Anything).
		Return(deactivatedUser, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "PATCH", "/scim/v2/Users/"+userID.String(), bytes.NewReader(bodyBytes))
	c.Params = []gin.Param{{Key: "id", Value: userID.String()}}
	ctrl.PatchUser(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response types.UserResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.False(t, response.Active)

	mockService.AssertExpectations(t)
}

func TestSCIMController_DeleteUser(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	userID := uuid.MakeV4()

	mockService.On("DeleteUser", mock.Anything, mock.Anything, mock.Anything, userID).
		Return(nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "DELETE", "/scim/v2/Users/"+userID.String(), nil)
	c.Params = []gin.Param{{Key: "id", Value: userID.String()}}
	ctrl.DeleteUser(c)

	assert.Equal(t, http.StatusNoContent, w.Code)

	mockService.AssertExpectations(t)
}

// Group Endpoint Tests

func TestSCIMController_ListGroups(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	groupID1 := uuid.MakeV4()
	groupID2 := uuid.MakeV4()
	now := timeutil.Now()

	externalIDGroup1 := "okta-group-123"
	externalIDGroup2 := "okta-group-456"
	groups := []*authmodels.Group{
		{
			ID:          groupID1,
			DisplayName: "Engineering",
			ExternalID:  &externalIDGroup1,
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		{
			ID:          groupID2,
			DisplayName: "Marketing",
			ExternalID:  &externalIDGroup2,
			CreatedAt:   now,
			UpdatedAt:   now,
		},
	}

	mockService.On("ListGroups", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(groups, 2, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Groups", nil)
	ctrl.ListGroups(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{types.SchemaListResponse}, response["schemas"])
	assert.Equal(t, float64(2), response["totalResults"])

	resources := response["Resources"].([]interface{})
	assert.Len(t, resources, 2)

	mockService.AssertExpectations(t)
}

func TestSCIMController_GetGroup(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	groupID := uuid.MakeV4()
	userID1 := uuid.MakeV4()
	userID2 := uuid.MakeV4()
	now := timeutil.Now()

	externalID := "okta-group-123"
	group := &authmodels.Group{
		ID:          groupID,
		DisplayName: "Engineering",
		ExternalID:  &externalID,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	members := []*authmodels.GroupMember{
		{
			ID:        uuid.MakeV4(),
			GroupID:   groupID,
			UserID:    userID1,
			CreatedAt: now,
		},
		{
			ID:        uuid.MakeV4(),
			GroupID:   groupID,
			UserID:    userID2,
			CreatedAt: now,
		},
	}

	mockService.On("GetGroupWithMembers", mock.Anything, mock.Anything, mock.Anything, groupID).
		Return(group, members, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Groups/"+groupID.String(), nil)
	c.Params = []gin.Param{{Key: "id", Value: groupID.String()}}
	ctrl.GetGroup(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response types.GroupResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, groupID.String(), response.ID)
	assert.Equal(t, "Engineering", response.DisplayName)
	assert.Len(t, response.Members, 2)
	assert.Equal(t, userID1.String(), response.Members[0].Value)

	mockService.AssertExpectations(t)
}

func TestSCIMController_CreateGroup(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	userID1 := uuid.MakeV4()
	userID2 := uuid.MakeV4()
	now := timeutil.Now()

	externalID := "okta-group-999"
	requestBody := types.CreateGroupRequest{
		Schemas:     []string{types.SchemaGroup},
		ExternalID:  &externalID,
		DisplayName: "New Team",
		Members: []types.GroupMemberRef{
			{Value: userID1.String()},
			{Value: userID2.String()},
		},
	}

	bodyBytes, _ := json.Marshal(requestBody)

	createdGroup := &authmodels.Group{
		ID:          uuid.MakeV4(),
		DisplayName: "New Team",
		ExternalID:  &externalID,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	members := []*authmodels.GroupMember{
		{ID: uuid.MakeV4(), GroupID: createdGroup.ID, UserID: userID1, CreatedAt: now},
		{ID: uuid.MakeV4(), GroupID: createdGroup.ID, UserID: userID2, CreatedAt: now},
	}

	mockService.On("CreateGroupWithMembers", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(input authtypes.CreateGroupInput) bool {
		return input.DisplayName == "New Team" && input.ExternalID != nil && *input.ExternalID == externalID && len(input.Members) == 2
	})).Return(createdGroup, members, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "POST", "/scim/v2/Groups", bytes.NewReader(bodyBytes))
	ctrl.CreateGroup(c)

	assert.Equal(t, http.StatusCreated, w.Code)

	var response types.GroupResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "New Team", response.DisplayName)
	assert.Len(t, response.Members, 2)

	mockService.AssertExpectations(t)
}

func TestSCIMController_ReplaceGroup(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	groupID := uuid.MakeV4()
	userID := uuid.MakeV4()
	now := timeutil.Now()

	externalID := "okta-group-999"
	requestBody := types.CreateGroupRequest{
		Schemas:     []string{types.SchemaGroup},
		ExternalID:  &externalID,
		DisplayName: "Updated Team",
		Members: []types.GroupMemberRef{
			{Value: userID.String()},
		},
	}

	bodyBytes, _ := json.Marshal(requestBody)

	updatedGroup := &authmodels.Group{
		ID:          groupID,
		DisplayName: "Updated Team",
		ExternalID:  &externalID,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	members := []*authmodels.GroupMember{
		{ID: uuid.MakeV4(), GroupID: groupID, UserID: userID, CreatedAt: now},
	}

	mockService.On("ReplaceGroup", mock.Anything, mock.Anything, mock.Anything, groupID, mock.Anything).
		Return(updatedGroup, members, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "PUT", "/scim/v2/Groups/"+groupID.String(), bytes.NewReader(bodyBytes))
	c.Params = []gin.Param{{Key: "id", Value: groupID.String()}}
	ctrl.ReplaceGroup(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response types.GroupResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "Updated Team", response.DisplayName)
	assert.Len(t, response.Members, 1)

	mockService.AssertExpectations(t)
}

func TestSCIMController_PatchGroup(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	groupID := uuid.MakeV4()
	now := timeutil.Now()

	requestBody := map[string]interface{}{
		"schemas": []string{types.SchemaPatchOp},
		"Operations": []interface{}{
			map[string]interface{}{
				"op":    "replace",
				"path":  "displayName",
				"value": "Patched Team Name",
			},
		},
	}

	bodyBytes, _ := json.Marshal(requestBody)

	externalID := "okta-group-123"
	patchedGroup := &authmodels.Group{
		ID:          groupID,
		DisplayName: "Patched Team Name",
		ExternalID:  &externalID,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	output := &authtypes.PatchGroupOutput{
		Group:   patchedGroup,
		Members: []*authmodels.GroupMember{},
	}

	mockService.On("PatchGroup", mock.Anything, mock.Anything, mock.Anything, groupID, mock.Anything).
		Return(output, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "PATCH", "/scim/v2/Groups/"+groupID.String(), bytes.NewReader(bodyBytes))
	c.Params = []gin.Param{{Key: "id", Value: groupID.String()}}
	ctrl.PatchGroup(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response types.GroupResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "Patched Team Name", response.DisplayName)

	mockService.AssertExpectations(t)
}

func TestSCIMController_PatchGroup_AddMember(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	groupID := uuid.MakeV4()
	userID := uuid.MakeV4()
	now := timeutil.Now()

	requestBody := map[string]interface{}{
		"schemas": []string{types.SchemaPatchOp},
		"Operations": []interface{}{
			map[string]interface{}{
				"op":   "add",
				"path": "members",
				"value": []interface{}{
					map[string]interface{}{
						"value": userID.String(),
					},
				},
			},
		},
	}

	bodyBytes, _ := json.Marshal(requestBody)

	externalID := "okta-group-123"
	group := &authmodels.Group{
		ID:          groupID,
		DisplayName: "Team",
		ExternalID:  &externalID,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	members := []*authmodels.GroupMember{
		{ID: uuid.MakeV4(), GroupID: groupID, UserID: userID, CreatedAt: now},
	}

	output := &authtypes.PatchGroupOutput{
		Group:   group,
		Members: members,
	}

	mockService.On("PatchGroup", mock.Anything, mock.Anything, mock.Anything, groupID, mock.Anything).
		Return(output, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "PATCH", "/scim/v2/Groups/"+groupID.String(), bytes.NewReader(bodyBytes))
	c.Params = []gin.Param{{Key: "id", Value: groupID.String()}}
	ctrl.PatchGroup(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response types.GroupResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Len(t, response.Members, 1)
	assert.Equal(t, userID.String(), response.Members[0].Value)

	mockService.AssertExpectations(t)
}

func TestSCIMController_PatchGroup_RemoveMember(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	groupID := uuid.MakeV4()
	userID := uuid.MakeV4()
	now := timeutil.Now()

	requestBody := map[string]interface{}{
		"schemas": []string{types.SchemaPatchOp},
		"Operations": []interface{}{
			map[string]interface{}{
				"op":   "remove",
				"path": "members[value eq \"" + userID.String() + "\"]",
			},
		},
	}

	bodyBytes, _ := json.Marshal(requestBody)

	externalID := "okta-group-123"
	group := &authmodels.Group{
		ID:          groupID,
		DisplayName: "Team",
		ExternalID:  &externalID,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	output := &authtypes.PatchGroupOutput{
		Group:   group,
		Members: []*authmodels.GroupMember{}, // Member removed
	}

	mockService.On("PatchGroup", mock.Anything, mock.Anything, mock.Anything, groupID, mock.Anything).
		Return(output, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "PATCH", "/scim/v2/Groups/"+groupID.String(), bytes.NewReader(bodyBytes))
	c.Params = []gin.Param{{Key: "id", Value: groupID.String()}}
	ctrl.PatchGroup(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response types.GroupResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Len(t, response.Members, 0)

	mockService.AssertExpectations(t)
}

func TestSCIMController_DeleteGroup(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	groupID := uuid.MakeV4()

	mockService.On("DeleteGroup", mock.Anything, mock.Anything, mock.Anything, groupID).
		Return(nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "DELETE", "/scim/v2/Groups/"+groupID.String(), nil)
	c.Params = []gin.Param{{Key: "id", Value: groupID.String()}}
	ctrl.DeleteGroup(c)

	assert.Equal(t, http.StatusNoContent, w.Code)

	mockService.AssertExpectations(t)
}

// Schema Discovery Tests

func TestSCIMController_ServiceProviderConfig(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/ServiceProviderConfig", nil)
	ctrl.ServiceProviderConfig(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{types.SchemaServiceProviderConfig}, response["schemas"])
	assert.NotNil(t, response["patch"])
	assert.NotNil(t, response["filter"])
	assert.NotNil(t, response["sort"])

	patch := response["patch"].(map[string]interface{})
	assert.True(t, patch["supported"].(bool))

	filter := response["filter"].(map[string]interface{})
	assert.True(t, filter["supported"].(bool))

	mockService.AssertExpectations(t)
}

func TestSCIMController_Schemas(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Schemas", nil)
	ctrl.Schemas(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{types.SchemaListResponse}, response["schemas"])
	assert.Equal(t, float64(2), response["totalResults"])

	resources := response["Resources"].([]interface{})
	assert.Len(t, resources, 2)

	// Verify User schema
	userSchema := resources[0].(map[string]interface{})
	assert.Equal(t, types.SchemaUser, userSchema["id"])
	assert.Equal(t, "User", userSchema["name"])

	// Verify Group schema
	groupSchema := resources[1].(map[string]interface{})
	assert.Equal(t, types.SchemaGroup, groupSchema["id"])
	assert.Equal(t, "Group", groupSchema["name"])

	mockService.AssertExpectations(t)
}

func TestSCIMController_ResourceTypes(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/ResourceTypes", nil)
	ctrl.ResourceTypes(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{types.SchemaListResponse}, response["schemas"])
	assert.Equal(t, float64(2), response["totalResults"])

	resources := response["Resources"].([]interface{})
	assert.Len(t, resources, 2)

	// Verify User resource type
	userRT := resources[0].(map[string]interface{})
	assert.Equal(t, "User", userRT["id"])
	assert.Equal(t, "/Users", userRT["endpoint"])

	// Verify Group resource type
	groupRT := resources[1].(map[string]interface{})
	assert.Equal(t, "Group", groupRT["id"])
	assert.Equal(t, "/Groups", groupRT["endpoint"])

	mockService.AssertExpectations(t)
}

// Error Handling Tests

func TestSCIMController_ErrorFormat(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	userID := uuid.MakeV4()

	mockService.On("GetUser", mock.Anything, mock.Anything, mock.Anything, userID).
		Return((*authmodels.User)(nil), authtypes.ErrUserNotFound)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Users/"+userID.String(), nil)
	c.Params = []gin.Param{{Key: "id", Value: userID.String()}}
	ctrl.GetUser(c)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	// Verify RFC 7644 error format
	assert.Equal(t, []interface{}{types.SchemaError}, response["schemas"])
	assert.Equal(t, float64(404), response["status"])
	assert.NotEmpty(t, response["detail"])

	mockService.AssertExpectations(t)
}

func TestSCIMController_InvalidUUID(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Users/invalid-uuid", nil)
	c.Params = []gin.Param{{Key: "id", Value: "invalid-uuid"}}
	ctrl.GetUser(c)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{types.SchemaError}, response["schemas"])
	assert.Equal(t, float64(400), response["status"])

	mockService.AssertExpectations(t)
}

func TestSCIMController_Unauthorized(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	// Create principal without SCIM permission
	principal := &pkgauth.Principal{
		User: &authmodels.User{Email: "regular-user@example.com"},
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{Permission: "some:other:permission"},
		},
	}

	// Note: In the real app, the authorization middleware would catch this before reaching the handler
	// This test demonstrates that the handler itself doesn't enforce authorization - that's done by middleware

	users := []*authmodels.User{}
	mockService.On("ListUsers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(users, 0, nil)

	c, w := createSCIMTestContext(principal, "GET", "/scim/v2/Users", nil)
	ctrl.ListUsers(c)

	// Without authorization middleware, this would succeed (middleware isn't tested here)
	assert.Equal(t, http.StatusOK, w.Code)

	mockService.AssertExpectations(t)
}

// Additional Edge Case Tests

func TestSCIMController_ListUsers_EmptyResult(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	mockService.On("ListUsers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]*authmodels.User{}, 0, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Users", nil)
	ctrl.ListUsers(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, float64(0), response["totalResults"])
	resources := response["Resources"].([]interface{})
	assert.Len(t, resources, 0)

	mockService.AssertExpectations(t)
}

func TestSCIMController_CreateUser_DefaultActive(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	requestBody := types.CreateUserRequest{
		Schemas:    []string{types.SchemaUser},
		ExternalID: "okta-789",
		UserName:   "newuser@example.com",
		Name: map[string]string{
			"formatted": "New User",
		},
		// Active not specified - should default to true
	}

	bodyBytes, _ := json.Marshal(requestBody)

	mockService.On("CreateUser", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(u *authmodels.User) bool {
		return u.Active == true // Verify default is true
	})).Run(func(args mock.Arguments) {
		user := args.Get(3).(*authmodels.User)
		user.ID = uuid.MakeV4()
		user.CreatedAt = timeutil.Now()
		user.UpdatedAt = timeutil.Now()
	}).Return(nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "POST", "/scim/v2/Users", bytes.NewReader(bodyBytes))
	ctrl.CreateUser(c)

	assert.Equal(t, http.StatusCreated, w.Code)

	var response types.UserResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.True(t, response.Active)

	mockService.AssertExpectations(t)
}

func TestSCIMController_ListUsers_WithPagination(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	userID := uuid.MakeV4()
	now := timeutil.Now()

	users := []*authmodels.User{
		{
			ID:         userID,
			Email:      "user@example.com",
			FullName:   "Test User",
			Active:     true,
			OktaUserID: "okta-123",
			CreatedAt:  now,
			UpdatedAt:  now,
		},
	}

	mockService.On("ListUsers", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(dto authtypes.InputListUsersDTO) bool {
		return dto.Filters.Pagination != nil &&
			dto.Filters.Pagination.StartIndex == 11 &&
			dto.Filters.Pagination.Count == 10
	})).Return(users, 1, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Users?startIndex=11&count=10", nil)
	ctrl.ListUsers(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, float64(11), response["startIndex"])

	mockService.AssertExpectations(t)
}

func TestSCIMController_GetGroup_NotFound(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	groupID := uuid.MakeV4()

	mockService.On("GetGroupWithMembers", mock.Anything, mock.Anything, mock.Anything, groupID).
		Return((*authmodels.Group)(nil), []*authmodels.GroupMember(nil), authtypes.ErrGroupNotFound)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Groups/"+groupID.String(), nil)
	c.Params = []gin.Param{{Key: "id", Value: groupID.String()}}
	ctrl.GetGroup(c)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{types.SchemaError}, response["schemas"])
	assert.Equal(t, float64(404), response["status"])

	mockService.AssertExpectations(t)
}

func TestSCIMController_CreateGroup_EmptyMembers(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	now := timeutil.Now()

	externalID := "okta-group-999"
	requestBody := types.CreateGroupRequest{
		Schemas:     []string{types.SchemaGroup},
		ExternalID:  &externalID,
		DisplayName: "Empty Team",
		Members:     []types.GroupMemberRef{}, // No members
	}

	bodyBytes, _ := json.Marshal(requestBody)

	createdGroup := &authmodels.Group{
		ID:          uuid.MakeV4(),
		DisplayName: "Empty Team",
		ExternalID:  &externalID,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	mockService.On("CreateGroupWithMembers", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(input authtypes.CreateGroupInput) bool {
		return len(input.Members) == 0
	})).Return(createdGroup, []*authmodels.GroupMember{}, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "POST", "/scim/v2/Groups", bytes.NewReader(bodyBytes))
	ctrl.CreateGroup(c)

	assert.Equal(t, http.StatusCreated, w.Code)

	var response types.GroupResource
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "Empty Team", response.DisplayName)
	assert.Len(t, response.Members, 0)

	mockService.AssertExpectations(t)
}

func TestSCIMController_ListGroups_WithFilter(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	groupID := uuid.MakeV4()
	now := timeutil.Now()

	externalID := "okta-group-123"
	groups := []*authmodels.Group{
		{
			ID:          groupID,
			DisplayName: "Engineering",
			ExternalID:  &externalID,
			CreatedAt:   now,
			UpdatedAt:   now,
		},
	}

	mockService.On("ListGroups", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(dto authtypes.InputListGroupsDTO) bool {
		// Verify that filter is parsed correctly
		return !dto.Filters.IsEmpty()
	})).Return(groups, 1, nil)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Groups?filter=displayName+eq+\"Engineering\"", nil)
	ctrl.ListGroups(c)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, float64(1), response["totalResults"])

	mockService.AssertExpectations(t)
}

func TestSCIMController_CreateGroup_InvalidMemberID(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	externalID := "okta-group-999"
	requestBody := types.CreateGroupRequest{
		Schemas:     []string{types.SchemaGroup},
		ExternalID:  &externalID,
		DisplayName: "New Team",
		Members: []types.GroupMemberRef{
			{Value: "invalid-uuid"}, // Invalid UUID
		},
	}

	bodyBytes, _ := json.Marshal(requestBody)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "POST", "/scim/v2/Groups", bytes.NewReader(bodyBytes))
	ctrl.CreateGroup(c)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{types.SchemaError}, response["schemas"])
	assert.Equal(t, float64(400), response["status"])

	mockService.AssertExpectations(t)
}

func TestSCIMController_PatchGroup_MissingOperations(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	groupID := uuid.MakeV4()

	// Missing Operations field
	requestBody := map[string]interface{}{
		"schemas": []string{types.SchemaPatchOp},
	}

	bodyBytes, _ := json.Marshal(requestBody)

	c, w := createSCIMTestContext(scimAdminPrincipal(), "PATCH", "/scim/v2/Groups/"+groupID.String(), bytes.NewReader(bodyBytes))
	c.Params = []gin.Param{{Key: "id", Value: groupID.String()}}
	ctrl.PatchGroup(c)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{types.SchemaError}, response["schemas"])
	assert.Equal(t, float64(400), response["status"])

	mockService.AssertExpectations(t)
}

func TestSCIMController_ListUsers_InvalidFilter(t *testing.T) {
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)

	// Invalid SCIM filter syntax
	c, w := createSCIMTestContext(scimAdminPrincipal(), "GET", "/scim/v2/Users?filter=invalid++syntax", nil)
	ctrl.ListUsers(c)

	// Should return an error for invalid filter
	assert.NotEqual(t, http.StatusOK, w.Code)

	mockService.AssertExpectations(t)
}
