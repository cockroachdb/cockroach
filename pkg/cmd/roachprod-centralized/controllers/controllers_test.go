// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package controllers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/idtoken"
)

type MockResultDTO struct {
	Data                 any
	Error                error
	AssociatedStatusCode int
}

func (r MockResultDTO) GetData() any {
	return r.Data
}
func (r MockResultDTO) GetError() error {
	return r.Error
}
func (r MockResultDTO) GetAssociatedStatusCode() int {
	return r.AssociatedStatusCode
}

func TestApiResponse_deduceAndFillDataType(t *testing.T) {
	tests := []struct {
		name           string
		response       *ApiResponse
		expectedResult string
	}{
		{
			name:           "nil data",
			response:       &ApiResponse{Data: nil},
			expectedResult: "",
		},
		{
			name:           "string data",
			response:       &ApiResponse{Data: "test"},
			expectedResult: "string",
		},
		{
			name:           "struct data",
			response:       &ApiResponse{Data: struct{ Name string }{"test"}},
			expectedResult: "struct { Name string }",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.response.deduceAndFillDataType()
			assert.Equal(t, tt.expectedResult, tt.response.ResultType)
		})
	}
}

func TestController_Render(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name              string
		dto               IResultDTO
		expectedStatus    int
		expectedPublicErr string
		expectedDataType  string
	}{
		{
			name: "success response",
			dto: &MockResultDTO{
				Data: "test",
			},
			expectedStatus:    http.StatusOK,
			expectedPublicErr: "",
			expectedDataType:  "string",
		},
		{
			name: "public error response",
			dto: &BadRequestResult{
				Error: fmt.Errorf("bad request"),
			},
			expectedStatus:    http.StatusBadRequest,
			expectedPublicErr: "bad request",
		},
		{
			name: "private error response",
			dto: MockResultDTO{
				Error:                fmt.Errorf("internal database error or something"),
				AssociatedStatusCode: http.StatusInternalServerError,
			},
			expectedStatus:    http.StatusInternalServerError,
			expectedPublicErr: ErrInternalServerError.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)

			ctrl := &Controller{}

			ctrl.Render(c, tt.dto)

			assert.Equal(t, tt.expectedStatus, w.Code)
			if tt.expectedPublicErr != "" {
				assert.Contains(t, w.Body.String(), fmt.Sprintf(`"error":"%s"`, tt.expectedPublicErr))
			}
		})
	}
}

func TestControllerHandler_GetHandlers(t *testing.T) {
	handler := func(c *gin.Context) {}
	extra := []gin.HandlerFunc{func(c *gin.Context) {}}

	ch := &ControllerHandler{
		Func:  handler,
		Extra: extra,
	}

	handlers := ch.GetHandlers()
	assert.Equal(t, len(extra)+1, len(handlers))
}

func TestControllerHandler_GetMethod(t *testing.T) {
	ch := &ControllerHandler{
		Method: http.MethodGet,
	}
	assert.Equal(t, http.MethodGet, ch.GetMethod())
}

func TestControllerHandler_GetPath(t *testing.T) {
	ch := &ControllerHandler{
		Path: "/test",
	}
	assert.Equal(t, "/test", ch.GetPath())
}

func TestControllerHandler_GetAuthenticationType(t *testing.T) {
	for _, at := range []AuthenticationType{
		AuthenticationTypeNone,
		AuthenticationTypeRequired,
	} {
		t.Run(string(at), func(t *testing.T) {
			ch := &ControllerHandler{
				Authentication: at,
			}
			assert.Equal(t, at, ch.GetAuthenticationType())
		})
	}
}

func TestController_Authentication(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name         string
		authDisabled bool
		authHeader   string
		setupHeader  func(*gin.Context)
		validateFn   func(context.Context, string, string) (*idtoken.Payload, error)
		wantStatus   int
		wantUserID   string
		wantEmail    string
	}{
		{
			name:         "authentication disabled",
			authDisabled: true,
			wantStatus:   http.StatusOK,
		},
		{
			name: "missing jwt token",
			validateFn: func(ctx context.Context, token, audience string) (*idtoken.Payload, error) {
				return nil, fmt.Errorf("missing token")
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid jwt token",
			setupHeader: func(c *gin.Context) {
				c.Request.Header.Set("X-Goog-IAP-JWT-Assertion", "invalid-token")
			},
			validateFn: func(ctx context.Context, token, audience string) (*idtoken.Payload, error) {
				return nil, fmt.Errorf("invalid token")
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "valid jwt token without claims",
			setupHeader: func(c *gin.Context) {
				c.Request.Header.Set("X-Goog-IAP-JWT-Assertion", "valid-token")
			},
			validateFn: func(ctx context.Context, token, audience string) (*idtoken.Payload, error) {
				return &idtoken.Payload{
					Claims: map[string]interface{}{},
				}, nil
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "valid jwt token with claims",
			setupHeader: func(c *gin.Context) {
				c.Request.Header.Set("X-Goog-IAP-JWT-Assertion", "valid-token")
			},
			validateFn: func(ctx context.Context, token, audience string) (*idtoken.Payload, error) {
				return &idtoken.Payload{
					Claims: map[string]interface{}{
						"sub":   "test-user-id",
						"email": "test@example.com",
					},
				}, nil
			},
			wantStatus: http.StatusOK,
			wantUserID: "test-user-id",
			wantEmail:  "test@example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			fmt.Printf("Running test: %s\n", tt.name)

			// Setup
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("GET", "/", nil)

			if tt.setupHeader != nil {
				tt.setupHeader(c)
			}

			// Create a test controller and mock the idtoken.Validate function.
			ctrl := NewControllerWithTokenValidator(tt.validateFn)

			// Test the authentication middleware
			ctrl.Authentication(c, tt.authDisabled, "X-Goog-IAP-JWT-Assertion", "test-audience")

			// Verify response status
			assert.Equal(t, tt.wantStatus, w.Code)

			// Verify user claims if expected
			if tt.wantUserID != "" {
				userID, exists := c.Get(SessionUserID)
				assert.True(t, exists)
				assert.Equal(t, tt.wantUserID, userID)
			}
			if tt.wantEmail != "" {
				email, exists := c.Get(SessionUserEmail)
				assert.True(t, exists)
				assert.Equal(t, tt.wantEmail, email)
			}
		})
	}
}
