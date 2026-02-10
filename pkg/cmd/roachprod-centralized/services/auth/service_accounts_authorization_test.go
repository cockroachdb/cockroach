// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"testing"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth/mocks"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestCanGrantPermission_WildcardScopeGrantRequiresGlobalScope(t *testing.T) {
	tests := []struct {
		name      string
		principal *pkgauth.Principal
		grant     *authmodels.ServiceAccountPermission
		expected  bool
	}{
		{
			name: "denied when caller has permission only on narrow scope",
			principal: &pkgauth.Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{Scope: "gcp-engineering", Permission: "clusters:create"},
				},
			},
			grant:    &authmodels.ServiceAccountPermission{Scope: "*", Permission: "clusters:create"},
			expected: false,
		},
		{
			name: "allowed when caller already has global scope",
			principal: &pkgauth.Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{Scope: "*", Permission: "clusters:create"},
				},
			},
			grant:    &authmodels.ServiceAccountPermission{Scope: "*", Permission: "clusters:create"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, canGrantPermission(tt.principal, tt.grant))
		})
	}
}

func TestCanGrantPermission_PreventsPrivilegeEscalation(t *testing.T) {
	tests := []struct {
		name      string
		principal *pkgauth.Principal
		grant     *authmodels.ServiceAccountPermission
		expected  bool
	}{
		{
			name: "cannot grant permission not held by caller",
			principal: &pkgauth.Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{Scope: "*", Permission: "clusters:view:all"},
				},
			},
			grant:    &authmodels.ServiceAccountPermission{Scope: "*", Permission: "clusters:delete:all"},
			expected: false,
		},
		{
			name: "cannot grant all variant when caller only has own variant",
			principal: &pkgauth.Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{Scope: "gcp-engineering", Permission: "clusters:view:own"},
				},
			},
			grant:    &authmodels.ServiceAccountPermission{Scope: "gcp-engineering", Permission: "clusters:view:all"},
			expected: false,
		},
		{
			name: "cannot grant same permission on wider scope",
			principal: &pkgauth.Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{Scope: "gcp-engineering", Permission: "clusters:view:all"},
				},
			},
			grant:    &authmodels.ServiceAccountPermission{Scope: "aws-staging", Permission: "clusters:view:all"},
			expected: false,
		},
		{
			name: "all variant may grant own variant in same scope",
			principal: &pkgauth.Principal{
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{Scope: "gcp-engineering", Permission: "clusters:view:all"},
				},
			},
			grant:    &authmodels.ServiceAccountPermission{Scope: "gcp-engineering", Permission: "clusters:view:own"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, canGrantPermission(tt.principal, tt.grant))
		})
	}
}

func TestServiceAddServiceAccountPermission_DeniesEscalatingWildcardGrant(t *testing.T) {
	ctx := context.Background()
	l := logger.NewLogger("error")
	saID := uuid.MakeV4()

	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})

	principal := &pkgauth.Principal{
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{Scope: "*", Permission: authtypes.PermissionServiceAccountUpdateAll},
			&authmodels.UserPermission{Scope: "gcp-engineering", Permission: "clusters:create"},
		},
	}

	mockRepo.On("GetServiceAccount", mock.Anything, mock.Anything, saID).
		Return(&authmodels.ServiceAccount{ID: saID, Name: "target-sa", Enabled: true}, nil).
		Once()

	err := service.AddServiceAccountPermission(
		ctx,
		l,
		principal,
		saID,
		&authmodels.ServiceAccountPermission{
			Scope:      "*",
			Permission: "clusters:create",
		},
	)

	require.Error(t, err)
	assert.True(t, errors.Is(err, authtypes.ErrPermissionEscalation), "expected ErrPermissionEscalation, got: %v", err)
	mockRepo.AssertNotCalled(t, "ListServiceAccountPermissions", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mockRepo.AssertNotCalled(t, "AddServiceAccountPermission", mock.Anything, mock.Anything, mock.Anything)
	mockRepo.AssertExpectations(t)
}

func TestServiceUpdateServiceAccountPermissions_DeniesEscalatingPermissionSet(t *testing.T) {
	ctx := context.Background()
	l := logger.NewLogger("error")
	saID := uuid.MakeV4()

	mockRepo := &authmock.IAuthRepository{}
	service := NewService(mockRepo, nil, "test-instance", authtypes.Options{})

	principal := &pkgauth.Principal{
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{Scope: "*", Permission: authtypes.PermissionServiceAccountUpdateAll},
			&authmodels.UserPermission{Scope: "gcp-engineering", Permission: "clusters:create"},
		},
	}

	mockRepo.On("GetServiceAccount", mock.Anything, mock.Anything, saID).
		Return(&authmodels.ServiceAccount{ID: saID, Name: "target-sa", Enabled: true}, nil).
		Once()

	err := service.UpdateServiceAccountPermissions(
		ctx,
		l,
		principal,
		saID,
		[]*authmodels.ServiceAccountPermission{
			{Scope: "gcp-engineering", Permission: "clusters:create"},
			{Scope: "*", Permission: "clusters:create"}, // escalation attempt
		},
	)

	require.Error(t, err)
	assert.True(t, errors.Is(err, authtypes.ErrPermissionEscalation), "expected ErrPermissionEscalation, got: %v", err)
	mockRepo.AssertNotCalled(t, "UpdateServiceAccountPermissions", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mockRepo.AssertExpectations(t)
}
