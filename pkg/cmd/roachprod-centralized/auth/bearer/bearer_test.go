// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bearer

import (
	"context"
	"testing"
	"time"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/mocks"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBearerAuthenticator_Authenticate_EmptyToken(t *testing.T) {
	config := pkgauth.AuthConfig{}
	mockService := &authmock.IService{}
	mockMetrics := &authmock.IAuthMetricsRecorder{}
	l := logger.NewLogger("error")
	authenticator := NewBearerAuthenticator(config, mockService, mockMetrics, l)

	mockMetrics.On("RecordAuthentication", "error", "none", mock.Anything).Return()

	principal, err := authenticator.Authenticate(context.Background(), "", "127.0.0.1")
	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrNotAuthenticated), "expected ErrNotAuthenticated, got: %v", err)

	mockService.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestBearerAuthenticator_Authenticate_InvalidToken(t *testing.T) {
	config := pkgauth.AuthConfig{}
	mockService := &authmock.IService{}
	mockMetrics := &authmock.IAuthMetricsRecorder{}
	l := logger.NewLogger("error")
	authenticator := NewBearerAuthenticator(config, mockService, mockMetrics, l)

	mockService.On("AuthenticateToken", mock.Anything, mock.Anything, "invalid-token", "127.0.0.1").
		Return(nil, authtypes.ErrInvalidToken)
	mockMetrics.On("RecordAuthentication", "error", "bearer", mock.Anything).Return()

	principal, err := authenticator.Authenticate(context.Background(), "invalid-token", "127.0.0.1")
	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrInvalidToken), "expected ErrInvalidToken, got: %v", err)

	mockService.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestBearerAuthenticator_Authenticate_ExpiredToken(t *testing.T) {
	config := pkgauth.AuthConfig{}
	mockService := &authmock.IService{}
	mockMetrics := &authmock.IAuthMetricsRecorder{}
	l := logger.NewLogger("error")
	authenticator := NewBearerAuthenticator(config, mockService, mockMetrics, l)

	mockService.On("AuthenticateToken", mock.Anything, mock.Anything, "expired-token", "127.0.0.1").
		Return(nil, authtypes.ErrTokenExpired)
	mockMetrics.On("RecordAuthentication", "error", "bearer", mock.Anything).Return()

	principal, err := authenticator.Authenticate(context.Background(), "expired-token", "127.0.0.1")
	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrTokenExpired), "expected ErrTokenExpired, got: %v", err)

	mockService.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestBearerAuthenticator_Authenticate_IPNotAllowed(t *testing.T) {
	config := pkgauth.AuthConfig{}
	mockService := &authmock.IService{}
	mockMetrics := &authmock.IAuthMetricsRecorder{}
	l := logger.NewLogger("error")
	authenticator := NewBearerAuthenticator(config, mockService, mockMetrics, l)

	mockService.On("AuthenticateToken", mock.Anything, mock.Anything, "valid-token", "1.2.3.4").
		Return(nil, authtypes.ErrIPNotAllowed)
	mockMetrics.On("RecordAuthentication", "error", "bearer", mock.Anything).Return()

	principal, err := authenticator.Authenticate(context.Background(), "valid-token", "1.2.3.4")
	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrIPNotAllowed), "expected ErrIPNotAllowed, got: %v", err)

	mockService.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestBearerAuthenticator_Authenticate_ValidUserToken(t *testing.T) {
	config := pkgauth.AuthConfig{}
	mockService := &authmock.IService{}
	mockMetrics := &authmock.IAuthMetricsRecorder{}
	l := logger.NewLogger("error")
	authenticator := NewBearerAuthenticator(config, mockService, mockMetrics, l)

	userID := uuid.MakeV4()
	tokenID := uuid.MakeV4()
	createdAt := timeutil.Now().Add(-1 * time.Hour)
	expiresAt := timeutil.Now().Add(1 * time.Hour)

	expectedPrincipal := &pkgauth.Principal{
		Token: pkgauth.TokenInfo{
			ID:        tokenID,
			Type:      authmodels.TokenTypeUser,
			CreatedAt: &createdAt,
			ExpiresAt: &expiresAt,
		},
		UserID: &userID,
		User: &authmodels.User{
			ID:       userID,
			Email:    "test@example.com",
			FullName: "Test User",
			Active:   true,
		},
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{
				Scope:      "gcp-engineering",
				Permission: "clusters:read",
			},
		},
	}

	mockService.On("AuthenticateToken", mock.Anything, mock.Anything, "valid-user-token", "127.0.0.1").
		Return(expectedPrincipal, nil)
	mockMetrics.On("RecordAuthentication", "success", "user", mock.Anything).Return()

	principal, err := authenticator.Authenticate(context.Background(), "valid-user-token", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, principal)

	assert.Equal(t, tokenID, principal.Token.ID)
	assert.Equal(t, authmodels.TokenTypeUser, principal.Token.Type)
	assert.NotNil(t, principal.Token.CreatedAt)
	assert.NotNil(t, principal.Token.ExpiresAt)
	assert.Equal(t, createdAt, *principal.Token.CreatedAt)
	assert.Equal(t, expiresAt, *principal.Token.ExpiresAt)
	assert.NotNil(t, principal.User)
	assert.Equal(t, "test@example.com", principal.User.Email)
	assert.Equal(t, "Test User", principal.User.FullName)
	assert.Len(t, principal.Permissions, 1)
	assert.Equal(t, "gcp-engineering", principal.Permissions[0].GetScope())

	mockService.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestBearerAuthenticator_Authenticate_ValidServiceAccountToken(t *testing.T) {
	config := pkgauth.AuthConfig{}
	mockService := &authmock.IService{}
	mockMetrics := &authmock.IAuthMetricsRecorder{}
	l := logger.NewLogger("error")
	authenticator := NewBearerAuthenticator(config, mockService, mockMetrics, l)

	saID := uuid.MakeV4()
	tokenID := uuid.MakeV4()
	createdAt := timeutil.Now().Add(-2 * time.Hour)
	expiresAt := timeutil.Now().Add(30 * 24 * time.Hour) // 30 days

	expectedPrincipal := &pkgauth.Principal{
		Token: pkgauth.TokenInfo{
			ID:        tokenID,
			Type:      authmodels.TokenTypeServiceAccount,
			CreatedAt: &createdAt,
			ExpiresAt: &expiresAt,
		},
		ServiceAccountID: &saID,
		ServiceAccount: &authmodels.ServiceAccount{
			ID:          saID,
			Name:        "test-sa",
			Description: "Test Service Account",
			Enabled:     true,
		},
		Permissions: []authmodels.Permission{
			&authmodels.ServiceAccountPermission{
				Scope:      "gcp-engineering",
				Permission: "clusters:write",
			},
		},
	}

	mockService.On("AuthenticateToken", mock.Anything, mock.Anything, "valid-sa-token", "127.0.0.1").
		Return(expectedPrincipal, nil)
	mockMetrics.On("RecordAuthentication", "success", "service-account", mock.Anything).Return()

	principal, err := authenticator.Authenticate(context.Background(), "valid-sa-token", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, principal)

	assert.Equal(t, tokenID, principal.Token.ID)
	assert.Equal(t, authmodels.TokenTypeServiceAccount, principal.Token.Type)
	assert.NotNil(t, principal.Token.CreatedAt)
	assert.NotNil(t, principal.Token.ExpiresAt)
	assert.Equal(t, createdAt, *principal.Token.CreatedAt)
	assert.Equal(t, expiresAt, *principal.Token.ExpiresAt)
	assert.NotNil(t, principal.ServiceAccount)
	assert.Equal(t, "test-sa", principal.ServiceAccount.Name)
	assert.Len(t, principal.Permissions, 1)
	assert.Equal(t, "gcp-engineering", principal.Permissions[0].GetScope())

	mockService.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestBearerAuthenticator_Authenticate_UserNotProvisioned(t *testing.T) {
	config := pkgauth.AuthConfig{}
	mockService := &authmock.IService{}
	mockMetrics := &authmock.IAuthMetricsRecorder{}
	l := logger.NewLogger("error")
	authenticator := NewBearerAuthenticator(config, mockService, mockMetrics, l)

	mockService.On("AuthenticateToken", mock.Anything, mock.Anything, "valid-token", "127.0.0.1").
		Return(nil, authtypes.ErrUserNotProvisioned)
	mockMetrics.On("RecordAuthentication", "error", "bearer", mock.Anything).Return()

	principal, err := authenticator.Authenticate(context.Background(), "valid-token", "127.0.0.1")
	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrUserNotProvisioned), "expected ErrUserNotProvisioned, got: %v", err)

	mockService.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestBearerAuthenticator_Authenticate_UserDeactivated(t *testing.T) {
	config := pkgauth.AuthConfig{}
	mockService := &authmock.IService{}
	mockMetrics := &authmock.IAuthMetricsRecorder{}
	l := logger.NewLogger("error")
	authenticator := NewBearerAuthenticator(config, mockService, mockMetrics, l)

	mockService.On("AuthenticateToken", mock.Anything, mock.Anything, "user-token", "127.0.0.1").
		Return(nil, authtypes.ErrUserDeactivated)
	mockMetrics.On("RecordAuthentication", "error", "bearer", mock.Anything).Return()

	principal, err := authenticator.Authenticate(context.Background(), "user-token", "127.0.0.1")
	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrUserDeactivated), "expected ErrUserDeactivated, got: %v", err)

	mockService.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestBearerAuthenticator_Authenticate_ServiceAccountDisabled(t *testing.T) {
	config := pkgauth.AuthConfig{}
	mockService := &authmock.IService{}
	mockMetrics := &authmock.IAuthMetricsRecorder{}
	l := logger.NewLogger("error")
	authenticator := NewBearerAuthenticator(config, mockService, mockMetrics, l)

	mockService.On("AuthenticateToken", mock.Anything, mock.Anything, "sa-token", "127.0.0.1").
		Return(nil, authtypes.ErrServiceAccountDisabled)
	mockMetrics.On("RecordAuthentication", "error", "bearer", mock.Anything).Return()

	principal, err := authenticator.Authenticate(context.Background(), "sa-token", "127.0.0.1")
	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrServiceAccountDisabled), "expected ErrServiceAccountDisabled, got: %v", err)

	mockService.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}

func TestBearerAuthenticator_Authenticate_ServiceAccountNotFound(t *testing.T) {
	config := pkgauth.AuthConfig{}
	mockService := &authmock.IService{}
	mockMetrics := &authmock.IAuthMetricsRecorder{}
	l := logger.NewLogger("error")
	authenticator := NewBearerAuthenticator(config, mockService, mockMetrics, l)

	mockService.On("AuthenticateToken", mock.Anything, mock.Anything, "sa-token", "127.0.0.1").
		Return(nil, authtypes.ErrServiceAccountNotFound)
	mockMetrics.On("RecordAuthentication", "error", "bearer", mock.Anything).Return()

	principal, err := authenticator.Authenticate(context.Background(), "sa-token", "127.0.0.1")
	require.Error(t, err)
	assert.Nil(t, principal)
	assert.True(t, errors.Is(err, authtypes.ErrServiceAccountNotFound), "expected ErrServiceAccountNotFound, got: %v", err)

	mockService.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
}
