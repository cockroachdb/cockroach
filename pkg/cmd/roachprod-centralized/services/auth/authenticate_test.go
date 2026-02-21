// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"testing"
	"time"

	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	authmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth/mocks"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestService_AuthenticateToken_ValidUserToken tests successful authentication for a user token
func TestService_AuthenticateToken_ValidUserToken(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})
	l := logger.NewLogger("error")

	userID := uuid.MakeV4()
	tokenID := uuid.MakeV4()
	createdAt := timeutil.Now().Add(-1 * time.Hour)
	expiresAt := timeutil.Now().Add(6 * 24 * time.Hour)

	// Setup mock expectations
	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(&authmodels.ApiToken{
			ID:          tokenID,
			TokenType:   authmodels.TokenTypeUser,
			UserID:      &userID,
			Status:      authmodels.TokenStatusValid,
			TokenSuffix: "rp$user$1$****test1234",
			CreatedAt:   createdAt,
			ExpiresAt:   expiresAt,
		}, nil)

	mockRepo.On("GetUser", mock.Anything, mock.Anything, userID).
		Return(&authmodels.User{
			ID:       userID,
			Email:    "test@example.com",
			FullName: "Test User",
			Active:   true,
		}, nil)

	mockRepo.On("GetUserPermissionsFromGroups", mock.Anything, mock.Anything, userID).
		Return([]*authmodels.GroupPermission{
			{ID: uuid.MakeV4(), GroupName: "devs", Scope: "gcp-engineering", Permission: "clusters:create"},
		}, nil)

	// Execute
	principal, err := service.AuthenticateToken(context.Background(), l, "valid-token", "127.0.0.1")

	// Verify
	require.NoError(t, err)
	require.NotNil(t, principal)
	assert.Equal(t, tokenID, principal.Token.ID)
	assert.Equal(t, authmodels.TokenTypeUser, principal.Token.Type)
	assert.NotNil(t, principal.Token.CreatedAt)
	assert.NotNil(t, principal.Token.ExpiresAt)
	assert.NotNil(t, principal.User)
	assert.Equal(t, "test@example.com", principal.User.Email)
	assert.Equal(t, "Test User", principal.User.FullName)
	assert.Len(t, principal.Permissions, 1)
	assert.Equal(t, "gcp-engineering", principal.Permissions[0].GetScope())
	assert.Equal(t, "clusters:create", principal.Permissions[0].GetPermission())
	mockRepo.AssertExpectations(t)
}

// TestService_AuthenticateToken_UserNotProvisioned tests authentication when user is not in the database
func TestService_AuthenticateToken_UserNotProvisioned(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})
	l := logger.NewLogger("error")

	userID := uuid.MakeV4()
	tokenID := uuid.MakeV4()

	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(&authmodels.ApiToken{
			ID:          tokenID,
			TokenType:   authmodels.TokenTypeUser,
			UserID:      &userID,
			Status:      authmodels.TokenStatusValid,
			TokenSuffix: "rp$user$1$****test1234",
			ExpiresAt:   timeutil.Now().Add(1 * time.Hour),
		}, nil)

	mockRepo.On("GetUser", mock.Anything, mock.Anything, userID).
		Return(nil, rauth.ErrNotFound)

	principal, err := service.AuthenticateToken(context.Background(), l, "token", "127.0.0.1")

	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrUserNotProvisioned), "expected ErrUserNotProvisioned, got: %v", err)
	mockRepo.AssertExpectations(t)
}

// TestService_AuthenticateToken_UserDeactivated tests authentication when user is deactivated
func TestService_AuthenticateToken_UserDeactivated(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})
	l := logger.NewLogger("error")

	userID := uuid.MakeV4()
	tokenID := uuid.MakeV4()

	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(&authmodels.ApiToken{
			ID:          tokenID,
			TokenType:   authmodels.TokenTypeUser,
			UserID:      &userID,
			Status:      authmodels.TokenStatusValid,
			TokenSuffix: "rp$user$1$****test1234",
			ExpiresAt:   timeutil.Now().Add(1 * time.Hour),
		}, nil)

	mockRepo.On("GetUser", mock.Anything, mock.Anything, userID).
		Return(&authmodels.User{ID: userID, Email: "test@example.com", Active: false}, nil)

	principal, err := service.AuthenticateToken(context.Background(), l, "token", "127.0.0.1")

	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrUserDeactivated), "expected ErrUserDeactivated, got: %v", err)
	mockRepo.AssertExpectations(t)
}

// TestService_AuthenticateToken_ValidServiceAccountToken tests successful authentication for a service account token
func TestService_AuthenticateToken_ValidServiceAccountToken(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})
	l := logger.NewLogger("error")

	saID := uuid.MakeV4()
	tokenID := uuid.MakeV4()

	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(&authmodels.ApiToken{
			ID:               tokenID,
			TokenType:        authmodels.TokenTypeServiceAccount,
			ServiceAccountID: &saID,
			Status:           authmodels.TokenStatusValid,
			TokenSuffix:      "rp$sa$1$****test1234",
			ExpiresAt:        timeutil.Now().Add(30 * 24 * time.Hour),
		}, nil)

	mockRepo.On("ListServiceAccountOrigins", mock.Anything, mock.Anything, saID, mock.Anything).
		Return([]*authmodels.ServiceAccountOrigin{}, 0, nil) // No IP restrictions
	mockRepo.On("GetServiceAccount", mock.Anything, mock.Anything, saID).
		Return(&authmodels.ServiceAccount{ID: saID, Name: "ci-bot", Description: "CI automation", Enabled: true}, nil)
	mockRepo.On("ListServiceAccountPermissions", mock.Anything, mock.Anything, saID, mock.Anything).
		Return([]*authmodels.ServiceAccountPermission{
			{ID: uuid.MakeV4(), ServiceAccountID: saID, Scope: "*", Permission: "clusters:create"},
		}, 0, nil)

	principal, err := service.AuthenticateToken(context.Background(), l, "sa-token", "127.0.0.1")

	require.NoError(t, err)
	require.NotNil(t, principal)
	assert.Equal(t, tokenID, principal.Token.ID)
	assert.Equal(t, authmodels.TokenTypeServiceAccount, principal.Token.Type)
	assert.NotNil(t, principal.ServiceAccount)
	assert.Equal(t, "ci-bot", principal.ServiceAccount.Name)
	assert.Len(t, principal.Permissions, 1)
	assert.Equal(t, "clusters:create", principal.Permissions[0].GetPermission())
	mockRepo.AssertExpectations(t)
}

// TestService_AuthenticateToken_ServiceAccountDisabled tests authentication when service account is disabled
func TestService_AuthenticateToken_ServiceAccountDisabled(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})
	l := logger.NewLogger("error")

	saID := uuid.MakeV4()
	tokenID := uuid.MakeV4()

	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(&authmodels.ApiToken{
			ID:               tokenID,
			TokenType:        authmodels.TokenTypeServiceAccount,
			ServiceAccountID: &saID,
			Status:           authmodels.TokenStatusValid,
			TokenSuffix:      "rp$sa$1$****test1234",
			ExpiresAt:        timeutil.Now().Add(1 * time.Hour),
		}, nil)

	mockRepo.On("ListServiceAccountOrigins", mock.Anything, mock.Anything, saID, mock.Anything).
		Return([]*authmodels.ServiceAccountOrigin{}, 0, nil)
	mockRepo.On("GetServiceAccount", mock.Anything, mock.Anything, saID).
		Return(&authmodels.ServiceAccount{ID: saID, Name: "ci-bot", Enabled: false}, nil)

	principal, err := service.AuthenticateToken(context.Background(), l, "token", "127.0.0.1")

	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrServiceAccountDisabled), "expected ErrServiceAccountDisabled, got: %v", err)
	mockRepo.AssertExpectations(t)
}

// TestService_AuthenticateToken_ServiceAccountNotFound tests authentication when service account is deleted
func TestService_AuthenticateToken_ServiceAccountNotFound(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})
	l := logger.NewLogger("error")

	saID := uuid.MakeV4()
	tokenID := uuid.MakeV4()

	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(&authmodels.ApiToken{
			ID:               tokenID,
			TokenType:        authmodels.TokenTypeServiceAccount,
			ServiceAccountID: &saID,
			Status:           authmodels.TokenStatusValid,
			TokenSuffix:      "rp$sa$1$****test1234",
			ExpiresAt:        timeutil.Now().Add(1 * time.Hour),
		}, nil)

	mockRepo.On("ListServiceAccountOrigins", mock.Anything, mock.Anything, saID, mock.Anything).
		Return([]*authmodels.ServiceAccountOrigin{}, 0, nil)
	mockRepo.On("GetServiceAccount", mock.Anything, mock.Anything, saID).
		Return(nil, rauth.ErrNotFound)

	principal, err := service.AuthenticateToken(context.Background(), l, "token", "127.0.0.1")

	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrServiceAccountNotFound), "expected ErrServiceAccountNotFound, got: %v", err)
	mockRepo.AssertExpectations(t)
}

// TestService_AuthenticateToken_UserWithMultiplePermissions tests that all user permissions are loaded
func TestService_AuthenticateToken_UserWithMultiplePermissions(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})
	l := logger.NewLogger("error")

	userID := uuid.MakeV4()
	tokenID := uuid.MakeV4()

	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(&authmodels.ApiToken{
			ID:          tokenID,
			TokenType:   authmodels.TokenTypeUser,
			UserID:      &userID,
			Status:      authmodels.TokenStatusValid,
			TokenSuffix: "rp$user$1$****test1234",
			ExpiresAt:   timeutil.Now().Add(1 * time.Hour),
		}, nil)

	mockRepo.On("GetUser", mock.Anything, mock.Anything, userID).
		Return(&authmodels.User{ID: userID, Email: "poweruser@example.com", Active: true}, nil)
	mockRepo.On("GetUserPermissionsFromGroups", mock.Anything, mock.Anything, userID).
		Return([]*authmodels.GroupPermission{
			{ID: uuid.MakeV4(), Scope: "gcp-engineering", Permission: "clusters:create"},
			{ID: uuid.MakeV4(), Scope: "gcp-engineering", Permission: "clusters:delete"},
			{ID: uuid.MakeV4(), Scope: "aws-staging", Permission: "clusters:view"},
		}, nil)

	principal, err := service.AuthenticateToken(context.Background(), l, "token", "127.0.0.1")

	require.NoError(t, err)
	require.NotNil(t, principal)
	assert.Len(t, principal.Permissions, 3)
	mockRepo.AssertExpectations(t)
}
