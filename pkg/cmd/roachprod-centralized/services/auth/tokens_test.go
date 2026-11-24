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

	token, err := service.validateToken(context.Background(), l, "valid-token", "127.0.0.1")

	require.NoError(t, err)
	require.NotNil(t, token)
	assert.Equal(t, tokenID, token.ID)
	assert.Equal(t, authmodels.TokenTypeUser, token.TokenType)
	mockRepo.AssertExpectations(t)
}

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

	token, err := service.validateToken(context.Background(), l, "sa-token", "10.1.2.3")

	require.NoError(t, err)
	require.NotNil(t, token)
	assert.Equal(t, tokenID, token.ID)
	mockRepo.AssertExpectations(t)
}

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

	token, err := service.validateToken(context.Background(), l, "sa-token", "192.168.1.100")

	require.Error(t, err)
	assert.Nil(t, token)
	assert.True(t, errors.Is(err, authtypes.ErrIPNotAllowed), "expected ErrIPNotAllowed, got: %v", err)
	mockRepo.AssertExpectations(t)
}

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

	token, err := service.validateToken(context.Background(), l, "sa-token", "2001:db8::1")

	require.NoError(t, err)
	require.NotNil(t, token)
	mockRepo.AssertExpectations(t)
}

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

	token, err := service.validateToken(context.Background(), l, "sa-token", "1.2.3.4")

	require.NoError(t, err)
	require.NotNil(t, token)
	mockRepo.AssertExpectations(t)
}

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

	token, err := service.validateToken(context.Background(), l, "sa-token", "not-an-ip")

	require.Error(t, err)
	assert.Nil(t, token)
	assert.True(t, errors.Is(err, authtypes.ErrIPNotAllowed), "expected ErrIPNotAllowed, got: %v", err)
	mockRepo.AssertExpectations(t)
}

// TestService_ValidateToken_AsyncLastUsed tests that successful token validation
// queues the token ID to the async last-used channel instead of updating synchronously.
func TestService_ValidateToken_AsyncLastUsed(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{
		TokenLastUsedBufferSize: 4,
	})
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

	// Validate the token — should succeed and queue the update
	token, err := service.validateToken(context.Background(), l, "valid-token", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, token)

	// Verify the token ID was queued to the channel
	select {
	case id := <-service.tokenLastUsedCh:
		assert.Equal(t, tokenID, id)
	default:
		t.Fatal("expected token ID in channel")
	}

	mockRepo.AssertExpectations(t)
}

// TestService_ValidateToken_AsyncLastUsed_ChannelFull tests that token validation
// succeeds even when the last-used update buffer is full (update is silently dropped).
func TestService_ValidateToken_AsyncLastUsed_ChannelFull(t *testing.T) {
	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{
		TokenLastUsedBufferSize: 1,
	})
	l := logger.NewLogger("error")

	tokenID1 := uuid.MakeV4()
	tokenID2 := uuid.MakeV4()
	userID := uuid.MakeV4()

	// Fill the channel
	service.tokenLastUsedCh <- tokenID1

	mockRepo.On("GetTokenByHash", mock.Anything, mock.Anything, mock.Anything).
		Return(&authmodels.ApiToken{
			ID:          tokenID2,
			TokenType:   authmodels.TokenTypeUser,
			UserID:      &userID,
			Status:      authmodels.TokenStatusValid,
			TokenSuffix: "rp$user$1$****test1234",
			ExpiresAt:   timeutil.Now().Add(1 * time.Hour),
		}, nil)

	// Should still validate successfully even though channel is full
	token, err := service.validateToken(context.Background(), l, "valid-token", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, token)

	// Channel still holds tokenID1 (tokenID2 was dropped)
	id := <-service.tokenLastUsedCh
	assert.Equal(t, tokenID1, id)

	mockRepo.AssertExpectations(t)
}
