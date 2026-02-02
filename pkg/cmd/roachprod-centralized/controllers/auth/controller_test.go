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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			CreatedAt: &createdAt,  // From iat claim
			ExpiresAt: &expiresAt,  // From exp claim
		},
	}

	// Create controller and test context
	ctrl := NewController()
	c, w := createTestGinContext(principal)

	// Execute
	ctrl.WhoAmI(c)

	// Verify HTTP status
	assert.Equal(t, http.StatusOK, w.Code)

	// Parse response
	response := parseWhoAmIResponse(t, w.Body.Bytes())

	// Verify token info - JWT has zero UUID
	assert.Equal(t, "00000000-0000-0000-0000-000000000000", response.Token.ID)

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
}

// TestWhoAmI_DisabledAuth tests the WhoAmI endpoint with disabled auth (nil timestamps).
func TestWhoAmI_DisabledAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Setup test data - disabled auth has nil timestamps
	tokenID := uuid.MakeV4()

	principal := &auth.Principal{
		Token: auth.TokenInfo{
			ID:        tokenID,
			CreatedAt: nil, // Disabled auth doesn't populate timestamps
			ExpiresAt: nil, // Disabled auth doesn't populate timestamps
		},
	}

	// Create controller and test context
	ctrl := NewController()
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
	ctrl := NewController()
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
