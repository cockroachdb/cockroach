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

// TestService_ValidateToken_InvalidToken tests that a token not found in the database returns ErrInvalidToken
func TestService_ValidateToken_InvalidToken(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})
	l := logger.NewLogger("error")

	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, rauth.ErrNotFound)

	token, err := service.validateToken(context.Background(), l, "invalid-token", "127.0.0.1")

	require.Error(t, err)
	assert.Nil(t, token)
	assert.True(t, errors.Is(err, authtypes.ErrInvalidToken), "expected ErrInvalidToken, got: %v", err)
	mockRepo.AssertExpectations(t)
}

// TestService_ValidateToken_TokenExpired tests that an expired token returns ErrTokenExpired
func TestService_ValidateToken_TokenExpired(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})
	l := logger.NewLogger("error")

	tokenID := uuid.MakeV4()
	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(&authmodels.ApiToken{
			ID:          tokenID,
			Status:      authmodels.TokenStatusValid,
			TokenSuffix: "rp$user$1$****test1234",
			ExpiresAt:   timeutil.Now().Add(-1 * time.Hour), // Expired
		}, nil)

	token, err := service.validateToken(context.Background(), l, "expired-token", "127.0.0.1")

	require.Error(t, err)
	assert.Nil(t, token)
	assert.True(t, errors.Is(err, authtypes.ErrTokenExpired), "expected ErrTokenExpired, got: %v", err)
	mockRepo.AssertExpectations(t)
}

// TestService_ValidateToken_RevokedStatus tests that a revoked token returns ErrInvalidToken
func TestService_ValidateToken_RevokedStatus(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})
	l := logger.NewLogger("error")

	tokenID := uuid.MakeV4()
	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(&authmodels.ApiToken{
			ID:          tokenID,
			Status:      authmodels.TokenStatusRevoked, // Revoked
			TokenSuffix: "rp$user$1$****test1234",
			ExpiresAt:   timeutil.Now().Add(1 * time.Hour),
		}, nil)

	token, err := service.validateToken(context.Background(), l, "revoked-token", "127.0.0.1")

	require.Error(t, err)
	assert.Nil(t, token)
	assert.True(t, errors.Is(err, authtypes.ErrInvalidToken), "expected ErrInvalidToken, got: %v", err)
	mockRepo.AssertExpectations(t)
}

// TestService_ValidateToken_ValidUserToken tests that a valid user token passes validation
func TestService_ValidateToken_ValidUserToken(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})
	l := logger.NewLogger("error")

	tokenID := uuid.MakeV4()
	userID := uuid.MakeV4()

	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(&authmodels.ApiToken{
			ID:          tokenID,
			TokenType:   authmodels.TokenTypeUser,
			UserID:      &userID,
			Status:      authmodels.TokenStatusValid,
			TokenSuffix: "rp$user$1$****test1234",
			ExpiresAt:   timeutil.Now().Add(1 * time.Hour),
		}, nil)
	mockRepo.On("UpdateTokenLastUsed", mock.Anything, mock.Anything, tokenID).Return(nil)

	token, err := service.validateToken(context.Background(), l, "valid-token", "127.0.0.1")

	require.NoError(t, err)
	require.NotNil(t, token)
	assert.Equal(t, tokenID, token.ID)
	assert.Equal(t, authmodels.TokenTypeUser, token.TokenType)
	mockRepo.AssertExpectations(t)
}

// TestService_ValidateToken_IPAllowlist_Allowed tests that a client IP within allowed CIDR passes
func TestService_ValidateToken_IPAllowlist_Allowed(t *testing.T) {
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
		Return([]*authmodels.ServiceAccountOrigin{
			{ID: uuid.MakeV4(), ServiceAccountID: saID, CIDR: "10.0.0.0/8"},
			{ID: uuid.MakeV4(), ServiceAccountID: saID, CIDR: "192.168.1.0/24"},
		}, 0, nil)
	mockRepo.On("UpdateTokenLastUsed", mock.Anything, mock.Anything, tokenID).Return(nil)

	// Client IP within 10.0.0.0/8
	token, err := service.validateToken(context.Background(), l, "sa-token", "10.1.2.3")

	require.NoError(t, err)
	require.NotNil(t, token)
	assert.Equal(t, tokenID, token.ID)
	mockRepo.AssertExpectations(t)
}

// TestService_ValidateToken_IPAllowlist_Denied tests that a client IP outside allowed CIDR is denied
func TestService_ValidateToken_IPAllowlist_Denied(t *testing.T) {
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
		Return([]*authmodels.ServiceAccountOrigin{
			{ID: uuid.MakeV4(), ServiceAccountID: saID, CIDR: "10.0.0.0/8"}, // Only allow 10.x.x.x
		}, 0, nil)

	// Client IP outside allowed range
	token, err := service.validateToken(context.Background(), l, "sa-token", "192.168.1.100")

	require.Error(t, err)
	assert.Nil(t, token)
	assert.True(t, errors.Is(err, authtypes.ErrIPNotAllowed), "expected ErrIPNotAllowed, got: %v", err)
	mockRepo.AssertExpectations(t)
}

// TestService_ValidateToken_IPv6Support tests that IPv6 addresses are handled correctly
func TestService_ValidateToken_IPv6Support(t *testing.T) {
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
		Return([]*authmodels.ServiceAccountOrigin{
			{ID: uuid.MakeV4(), ServiceAccountID: saID, CIDR: "2001:db8::/32"},
		}, 0, nil)
	mockRepo.On("UpdateTokenLastUsed", mock.Anything, mock.Anything, tokenID).Return(nil)

	// IPv6 address within allowed range
	token, err := service.validateToken(context.Background(), l, "sa-token", "2001:db8::1")

	require.NoError(t, err)
	require.NotNil(t, token)
	mockRepo.AssertExpectations(t)
}

// TestService_ValidateToken_NoOrigins tests that service accounts without origins allow any IP
func TestService_ValidateToken_NoOrigins(t *testing.T) {
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
		Return([]*authmodels.ServiceAccountOrigin{}, 0, nil) // No origins configured
	mockRepo.On("UpdateTokenLastUsed", mock.Anything, mock.Anything, tokenID).Return(nil)

	// Any IP should be allowed when no origins are configured
	token, err := service.validateToken(context.Background(), l, "sa-token", "1.2.3.4")

	require.NoError(t, err)
	require.NotNil(t, token)
	mockRepo.AssertExpectations(t)
}

// TestService_ValidateToken_InvalidClientIP tests that an invalid client IP format is rejected
func TestService_ValidateToken_InvalidClientIP(t *testing.T) {
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
		Return([]*authmodels.ServiceAccountOrigin{
			{ID: uuid.MakeV4(), ServiceAccountID: saID, CIDR: "10.0.0.0/8"},
		}, 0, nil)

	// Invalid IP format
	token, err := service.validateToken(context.Background(), l, "sa-token", "not-an-ip")

	require.Error(t, err)
	assert.Nil(t, token)
	assert.True(t, errors.Is(err, authtypes.ErrIPNotAllowed), "expected ErrIPNotAllowed, got: %v", err)
	mockRepo.AssertExpectations(t)
}
