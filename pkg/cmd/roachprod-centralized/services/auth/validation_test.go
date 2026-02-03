// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidateEmail tests email validation
func TestValidateEmail(t *testing.T) {
	tests := []struct {
		name      string
		email     string
		expectErr error
	}{
		{
			name:      "valid email",
			email:     "user@example.com",
			expectErr: nil,
		},
		{
			name:      "valid email with plus",
			email:     "user+tag@example.com",
			expectErr: nil,
		},
		{
			name:      "valid email with subdomain",
			email:     "user@sub.example.com",
			expectErr: nil,
		},
		{
			name:      "empty email",
			email:     "",
			expectErr: authtypes.ErrRequiredField,
		},
		{
			name:      "invalid email - no at sign",
			email:     "userexample.com",
			expectErr: authtypes.ErrInvalidEmail,
		},
		{
			name:      "invalid email - no domain",
			email:     "user@",
			expectErr: authtypes.ErrInvalidEmail,
		},
		{
			name:      "invalid email - spaces",
			email:     "user @example.com",
			expectErr: authtypes.ErrInvalidEmail,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateEmail(tt.email)
			if tt.expectErr == nil {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.expectErr), "expected %v, got %v", tt.expectErr, err)
			}
		})
	}
}

// TestValidateUser tests user validation
func TestValidateUser(t *testing.T) {
	tests := []struct {
		name      string
		user      *auth.User
		isUpdate  bool
		expectErr error
	}{
		{
			name: "valid user for creation",
			user: &auth.User{
				ID:         uuid.MakeV4(),
				Email:      "user@example.com",
				OktaUserID: "okta-123",
				FullName:   "Test User",
			},
			isUpdate:  false,
			expectErr: nil,
		},
		{
			name: "valid user for update",
			user: &auth.User{
				ID:       uuid.MakeV4(),
				Email:    "user@example.com",
				FullName: "Test User",
				// OktaUserID not required for updates
			},
			isUpdate:  true,
			expectErr: nil,
		},
		{
			name: "missing email",
			user: &auth.User{
				ID:         uuid.MakeV4(),
				OktaUserID: "okta-123",
			},
			isUpdate:  false,
			expectErr: authtypes.ErrRequiredField,
		},
		{
			name: "invalid email format",
			user: &auth.User{
				ID:         uuid.MakeV4(),
				Email:      "not-an-email",
				OktaUserID: "okta-123",
			},
			isUpdate:  false,
			expectErr: authtypes.ErrInvalidEmail,
		},
		{
			name: "missing okta_user_id on creation",
			user: &auth.User{
				ID:    uuid.MakeV4(),
				Email: "user@example.com",
			},
			isUpdate:  false,
			expectErr: authtypes.ErrRequiredField,
		},
		{
			name: "missing okta_user_id allowed on update",
			user: &auth.User{
				ID:    uuid.MakeV4(),
				Email: "user@example.com",
			},
			isUpdate:  true,
			expectErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUser(tt.user, tt.isUpdate)
			if tt.expectErr == nil {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.expectErr), "expected %v, got %v", tt.expectErr, err)
			}
		})
	}
}
