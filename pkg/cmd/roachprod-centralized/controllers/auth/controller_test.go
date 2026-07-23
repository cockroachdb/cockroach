// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/auth/types"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/mocks"
	clusterstypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWhoAmI_BearerTokenUser tests the WhoAmI endpoint with a bearer token for a user.
func TestWhoAmI_BearerTokenUser(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Setup test data
	tokenID := uuid.MakeV4()
	userID := uuid.MakeV4()
	createdAt := timeutil.Now().Add(-24 * time.Hour)
	expiresAt := timeutil.Now().Add(6 * 24 * time.Hour)

	principal := &auth.Principal{
		Token: auth.TokenInfo{
			ID:        tokenID,
			Type:      authmodels.TokenTypeUser,
			CreatedAt: &createdAt,
			ExpiresAt: &expiresAt,
		},
		UserID: &userID,
		User: &authmodels.User{
			ID:       userID,
			Email:    "alice@example.com",
			FullName: "Alice Smith",
			Active:   true,
		},
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{
				Scope:      "gcp-engineering",
				Permission: clusterstypes.PermissionCreate,
			},
		},
	}

	// Create controller and test context
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)
	c, w := createTestGinContext(principal)

	// Execute
	ctrl.WhoAmI(c)

	// Verify HTTP status
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json; charset=utf-8", w.Header().Get("Content-Type"))

	// Parse response
	response := parseWhoAmIResponse(t, w.Body.Bytes())

	// Verify token info
	assert.Equal(t, tokenID.String(), response.Token.ID)
	assert.Equal(t, "user", response.Token.Type)
	assert.NotEmpty(t, response.Token.CreatedAt, "CreatedAt should be populated for bearer tokens")
	assert.NotEmpty(t, response.Token.ExpiresAt, "ExpiresAt should be populated for bearer tokens")

	// Verify timestamp format (ISO 8601)
	parsedCreatedAt, err := time.Parse("2006-01-02T15:04:05Z07:00", response.Token.CreatedAt)
	require.NoError(t, err, "CreatedAt should be in ISO 8601 format")
	assert.Equal(t, createdAt.Unix(), parsedCreatedAt.Unix())

	parsedExpiresAt, err := time.Parse("2006-01-02T15:04:05Z07:00", response.Token.ExpiresAt)
	require.NoError(t, err, "ExpiresAt should be in ISO 8601 format")
	assert.Equal(t, expiresAt.Unix(), parsedExpiresAt.Unix())

	// Verify user info
	require.NotNil(t, response.User)
	assert.Equal(t, userID.String(), response.User.ID)
	assert.Equal(t, "alice@example.com", response.User.Email)
	assert.Equal(t, "Alice Smith", response.User.Name)
	assert.True(t, response.User.Active)

	// Verify service account is not populated
	assert.Nil(t, response.ServiceAccount)

	// Verify permissions
	require.Len(t, response.Permissions, 1)
	assert.Equal(t, "gcp-engineering", response.Permissions[0].Scope)
	assert.Equal(t, clusterstypes.PermissionCreate, response.Permissions[0].Permission)
}

// TestWhoAmI_BearerTokenServiceAccount tests the WhoAmI endpoint with a service account token.
func TestWhoAmI_BearerTokenServiceAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Setup test data
	tokenID := uuid.MakeV4()
	saID := uuid.MakeV4()
	createdAt := timeutil.Now().Add(-24 * time.Hour)
	expiresAt := timeutil.Now().Add(365 * 24 * time.Hour)

	principal := &auth.Principal{
		Token: auth.TokenInfo{
			ID:        tokenID,
			Type:      authmodels.TokenTypeServiceAccount,
			CreatedAt: &createdAt,
			ExpiresAt: &expiresAt,
		},
		ServiceAccountID: &saID,
		ServiceAccount: &authmodels.ServiceAccount{
			ID:          saID,
			Name:        "ci-bot",
			Description: "CI/CD automation service account",
			Enabled:     true,
		},
		Permissions: []authmodels.Permission{
			&authmodels.ServiceAccountPermission{
				Scope:      "gcp-engineering",
				Permission: clusterstypes.PermissionViewAll,
			},
		},
	}

	// Create controller and test context
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)
	c, w := createTestGinContext(principal)

	// Execute
	ctrl.WhoAmI(c)

	// Verify HTTP status
	assert.Equal(t, http.StatusOK, w.Code)

	// Parse response
	response := parseWhoAmIResponse(t, w.Body.Bytes())

	// Verify token info
	assert.Equal(t, tokenID.String(), response.Token.ID)
	assert.Equal(t, "service-account", response.Token.Type)
	assert.NotEmpty(t, response.Token.CreatedAt)
	assert.NotEmpty(t, response.Token.ExpiresAt)

	// Verify service account info
	require.NotNil(t, response.ServiceAccount)
	assert.Equal(t, saID.String(), response.ServiceAccount.ID)
	assert.Equal(t, "ci-bot", response.ServiceAccount.Name)
	assert.Equal(t, "CI/CD automation service account", response.ServiceAccount.Description)
	assert.True(t, response.ServiceAccount.Enabled)

	// Verify user is not populated
	assert.Nil(t, response.User)

	// Verify permissions
	require.Len(t, response.Permissions, 1)
	assert.Equal(t, clusterstypes.PermissionViewAll, response.Permissions[0].Permission)
}

// TestWhoAmI_JWTToken tests the WhoAmI endpoint with JWT token (with timestamps from claims).
func TestWhoAmI_JWTToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Setup test data - JWT tokens have zero UUID and timestamps from iat/exp claims
	now := timeutil.Now()
	createdAt := now.Add(-1 * time.Hour)
	expiresAt := now.Add(1 * time.Hour)

	principal := &auth.Principal{
		Token: auth.TokenInfo{
			ID:        uuid.UUID{}, // Zero UUID for JWT tokens
			Type:      authmodels.TokenTypeUser,
			CreatedAt: &createdAt, // From iat claim
			ExpiresAt: &expiresAt, // From exp claim
		},
		User: &authmodels.User{
			Email:    "user@example.com",
			FullName: "JWT User",
			Active:   true,
		},
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{
				Scope:      "*",
				Permission: "*",
			},
		},
	}

	// Create controller and test context
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)
	c, w := createTestGinContext(principal)

	// Execute
	ctrl.WhoAmI(c)

	// Verify HTTP status
	assert.Equal(t, http.StatusOK, w.Code)

	// Parse response
	response := parseWhoAmIResponse(t, w.Body.Bytes())

	// Verify token info - JWT has zero UUID
	assert.Equal(t, "00000000-0000-0000-0000-000000000000", response.Token.ID)
	assert.Equal(t, "user", response.Token.Type)

	// JWT should now include timestamps from iat/exp claims
	assert.NotEmpty(t, response.Token.CreatedAt, "JWT should include created_at from iat claim")
	assert.NotEmpty(t, response.Token.ExpiresAt, "JWT should include expires_at from exp claim")

	// Verify timestamp format
	parsedCreatedAt, err := time.Parse("2006-01-02T15:04:05Z07:00", response.Token.CreatedAt)
	require.NoError(t, err)
	assert.Equal(t, createdAt.Unix(), parsedCreatedAt.Unix())

	parsedExpiresAt, err := time.Parse("2006-01-02T15:04:05Z07:00", response.Token.ExpiresAt)
	require.NoError(t, err)
	assert.Equal(t, expiresAt.Unix(), parsedExpiresAt.Unix())

	// Verify user info
	require.NotNil(t, response.User)
	assert.Equal(t, "user@example.com", response.User.Email)
	assert.Equal(t, "JWT User", response.User.Name)

	// Verify wildcard permissions
	require.Len(t, response.Permissions, 1)
	assert.Equal(t, "*", response.Permissions[0].Permission)
}

// TestWhoAmI_DisabledAuth tests the WhoAmI endpoint with disabled auth (nil timestamps).
func TestWhoAmI_DisabledAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Setup test data - disabled auth has nil timestamps
	tokenID := uuid.MakeV4()

	principal := &auth.Principal{
		Token: auth.TokenInfo{
			ID:        tokenID,
			Type:      authmodels.TokenTypeUser,
			CreatedAt: nil, // Disabled auth doesn't populate timestamps
			ExpiresAt: nil, // Disabled auth doesn't populate timestamps
		},
		User: &authmodels.User{
			ID:       uuid.MakeV4(),
			Email:    "dev@localhost",
			FullName: "Development User",
			Active:   true,
		},
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{
				Scope:      "*",
				Permission: "*",
			},
		},
	}

	// Create controller and test context
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)
	c, w := createTestGinContext(principal)

	// Execute
	ctrl.WhoAmI(c)

	// Verify HTTP status
	assert.Equal(t, http.StatusOK, w.Code)

	// Parse response
	var apiResponse struct {
		Data types.WhoAmIResponse `json:"data"`
	}
	err := json.Unmarshal(w.Body.Bytes(), &apiResponse)
	require.NoError(t, err)

	response := apiResponse.Data

	// Verify token info
	assert.Equal(t, tokenID.String(), response.Token.ID)
	assert.Equal(t, "user", response.Token.Type)

	// Disabled auth should NOT include timestamps (omitted from JSON)
	assert.Empty(t, response.Token.CreatedAt, "disabled auth should omit created_at")
	assert.Empty(t, response.Token.ExpiresAt, "disabled auth should omit expires_at")

	// Verify the JSON response doesn't include these fields at all
	rawJSON := w.Body.String()
	assert.NotContains(t, rawJSON, "\"created_at\"", "created_at should be omitted from JSON response")
	assert.NotContains(t, rawJSON, "\"expires_at\"", "expires_at should be omitted from JSON response")
}

// TestWhoAmI_Unauthenticated tests the WhoAmI endpoint without authentication.
func TestWhoAmI_Unauthenticated(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create controller and test context WITHOUT principal
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)
	c, w := createTestGinContext(nil) // No principal

	// Execute
	ctrl.WhoAmI(c)

	// Verify HTTP 401 Unauthorized
	assert.Equal(t, http.StatusUnauthorized, w.Code)

	// Parse error response
	var apiResponse struct {
		Error string `json:"error"`
	}
	err := json.Unmarshal(w.Body.Bytes(), &apiResponse)
	require.NoError(t, err)

	assert.Contains(t, apiResponse.Error, "not authenticated")
}

// TestWhoAmI_MultiplePermissions tests the WhoAmI endpoint with multiple permissions.
func TestWhoAmI_MultiplePermissions(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Setup test data with multiple permissions
	tokenID := uuid.MakeV4()
	userID := uuid.MakeV4()
	createdAt := timeutil.Now().Add(-1 * time.Hour)
	expiresAt := timeutil.Now().Add(24 * time.Hour)

	principal := &auth.Principal{
		Token: auth.TokenInfo{
			ID:        tokenID,
			Type:      authmodels.TokenTypeUser,
			CreatedAt: &createdAt,
			ExpiresAt: &expiresAt,
		},
		UserID: &userID,
		User: &authmodels.User{
			ID:       userID,
			Email:    "poweruser@example.com",
			FullName: "Power User",
			Active:   true,
		},
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{
				Scope:      "gcp-engineering",
				Permission: clusterstypes.PermissionViewAll,
			},
			&authmodels.UserPermission{
				Scope:      "gcp-engineering",
				Permission: clusterstypes.PermissionCreate,
			},
			&authmodels.UserPermission{
				Scope:      "gcp-staging",
				Permission: clusterstypes.PermissionViewOwn,
			},
			&authmodels.UserPermission{
				Scope:      "aws-production",
				Permission: clusterstypes.PermissionUpdateAll,
			},
		},
	}

	// Create controller and test context
	mockService := &authmock.IService{}
	ctrl := NewController(mockService)
	c, w := createTestGinContext(principal)

	// Execute
	ctrl.WhoAmI(c)

	// Verify HTTP status
	assert.Equal(t, http.StatusOK, w.Code)

	// Parse response
	response := parseWhoAmIResponse(t, w.Body.Bytes())

	// Verify all permissions are serialized correctly
	require.Len(t, response.Permissions, 4)

	// Verify first permission
	assert.Equal(t, "gcp-engineering", response.Permissions[0].Scope)
	assert.Equal(t, clusterstypes.PermissionViewAll, response.Permissions[0].Permission)

	// Verify second permission
	assert.Equal(t, "gcp-engineering", response.Permissions[1].Scope)
	assert.Equal(t, clusterstypes.PermissionCreate, response.Permissions[1].Permission)

	// Verify third permission
	assert.Equal(t, "gcp-staging", response.Permissions[2].Scope)
	assert.Equal(t, clusterstypes.PermissionViewOwn, response.Permissions[2].Permission)

	// Verify fourth permission
	assert.Equal(t, "aws-production", response.Permissions[3].Scope)
	assert.Equal(t, clusterstypes.PermissionUpdateAll, response.Permissions[3].Permission)
}

// TestWhoAmI_EmptyPermissions tests that an empty permissions slice serializes as
// [] in the JSON response, not null.
func TestWhoAmI_EmptyPermissions(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tokenID := uuid.MakeV4()
	userID := uuid.MakeV4()
	createdAt := timeutil.Now().Add(-1 * time.Hour)
	expiresAt := timeutil.Now().Add(24 * time.Hour)

	principal := &auth.Principal{
		Token: auth.TokenInfo{
			ID:        tokenID,
			Type:      authmodels.TokenTypeUser,
			CreatedAt: &createdAt,
			ExpiresAt: &expiresAt,
		},
		UserID: &userID,
		User: &authmodels.User{
			ID:       userID,
			Email:    "noperm@example.com",
			FullName: "No Permissions User",
			Active:   true,
		},
		Permissions: []authmodels.Permission{},
	}

	mockService := &authmock.IService{}
	ctrl := NewController(mockService)
	c, w := createTestGinContext(principal)

	ctrl.WhoAmI(c)

	assert.Equal(t, http.StatusOK, w.Code)

	response := parseWhoAmIResponse(t, w.Body.Bytes())
	require.NotNil(t, response.Permissions)
	assert.Empty(t, response.Permissions)

	// Verify JSON contains "permissions":[] not "permissions":null
	rawJSON := w.Body.String()
	assert.Contains(t, rawJSON, `"permissions":[]`)
	assert.NotContains(t, rawJSON, `"permissions":null`)
}

// Helper functions

// createTestGinContext creates a Gin context for testing with optional principal.
func createTestGinContext(principal *auth.Principal) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/v1/auth/whoami", nil)

	if principal != nil {
		controllers.SetPrincipal(c, principal)
	}

	return c, w
}

// parseWhoAmIResponse parses the JSON response into WhoAmIResponse.
func parseWhoAmIResponse(t *testing.T, body []byte) types.WhoAmIResponse {
	var apiResponse struct {
		Data types.WhoAmIResponse `json:"data"`
	}
	err := json.Unmarshal(body, &apiResponse)
	require.NoError(t, err, "response should be valid JSON")
	return apiResponse.Data
}
