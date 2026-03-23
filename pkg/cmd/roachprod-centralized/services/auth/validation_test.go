// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"testing"
	"time"

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

// TestValidateServiceAccount tests service account validation
func TestValidateServiceAccount(t *testing.T) {
	tests := []struct {
		name      string
		sa        *auth.ServiceAccount
		isUpdate  bool
		expectErr error
	}{
		{
			name: "valid service account",
			sa: &auth.ServiceAccount{
				ID:          uuid.MakeV4(),
				Name:        "ci-bot",
				Description: "CI automation",
				Enabled:     true,
			},
			isUpdate:  false,
			expectErr: nil,
		},
		{
			name: "valid service account for update",
			sa: &auth.ServiceAccount{
				ID:      uuid.MakeV4(),
				Name:    "ci-bot",
				Enabled: true,
			},
			isUpdate:  true,
			expectErr: nil,
		},
		{
			name: "missing name",
			sa: &auth.ServiceAccount{
				ID:          uuid.MakeV4(),
				Description: "Description without name",
			},
			isUpdate:  false,
			expectErr: authtypes.ErrRequiredField,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateServiceAccount(tt.sa, tt.isUpdate)
			if tt.expectErr == nil {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.expectErr), "expected %v, got %v", tt.expectErr, err)
			}
		})
	}
}

// TestValidateTTL tests TTL validation
func TestValidateTTL(t *testing.T) {
	tests := []struct {
		name      string
		ttl       time.Duration
		expectErr error
	}{
		{
			name:      "valid TTL - 1 hour",
			ttl:       1 * time.Hour,
			expectErr: nil,
		},
		{
			name:      "valid TTL - 7 days",
			ttl:       7 * 24 * time.Hour,
			expectErr: nil,
		},
		{
			name:      "valid TTL - max allowed",
			ttl:       authtypes.MaxTokenTTL,
			expectErr: nil,
		},
		{
			name:      "invalid TTL - zero",
			ttl:       0,
			expectErr: authtypes.ErrInvalidTTL,
		},
		{
			name:      "invalid TTL - negative",
			ttl:       -1 * time.Hour,
			expectErr: authtypes.ErrInvalidTTL,
		},
		{
			name:      "invalid TTL - exceeds max",
			ttl:       authtypes.MaxTokenTTL + 1*time.Hour,
			expectErr: authtypes.ErrInvalidTTL,
		},
		{
			name:      "invalid TTL - way too long",
			ttl:       10 * 365 * 24 * time.Hour, // 10 years
			expectErr: authtypes.ErrInvalidTTL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTTL(tt.ttl)
			if tt.expectErr == nil {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.expectErr), "expected %v, got %v", tt.expectErr, err)
			}
		})
	}
}

// TestValidateCIDR tests CIDR validation
func TestValidateCIDR(t *testing.T) {
	tests := []struct {
		name      string
		cidr      string
		expectErr error
	}{
		{
			name:      "valid IPv4 CIDR",
			cidr:      "10.0.0.0/8",
			expectErr: nil,
		},
		{
			name:      "valid IPv4 CIDR - /24",
			cidr:      "192.168.1.0/24",
			expectErr: nil,
		},
		{
			name:      "valid IPv4 CIDR - single host",
			cidr:      "192.168.1.1/32",
			expectErr: nil,
		},
		{
			name:      "valid IPv6 CIDR",
			cidr:      "2001:db8::/32",
			expectErr: nil,
		},
		{
			name:      "valid IPv6 CIDR - /128",
			cidr:      "2001:db8::1/128",
			expectErr: nil,
		},
		{
			name:      "invalid - plain IP without prefix",
			cidr:      "10.0.0.1",
			expectErr: authtypes.ErrInvalidOriginCIDR,
		},
		{
			name:      "invalid - malformed",
			cidr:      "not-a-cidr",
			expectErr: authtypes.ErrInvalidOriginCIDR,
		},
		{
			name:      "invalid - empty",
			cidr:      "",
			expectErr: authtypes.ErrInvalidOriginCIDR,
		},
		{
			name:      "invalid - wrong prefix length",
			cidr:      "10.0.0.0/33",
			expectErr: authtypes.ErrInvalidOriginCIDR,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCIDR(tt.cidr)
			if tt.expectErr == nil {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.expectErr), "expected %v, got %v", tt.expectErr, err)
			}
		})
	}
}

// TestValidateOktaGroups tests Okta groups validation
func TestValidateOktaGroups(t *testing.T) {
	tests := []struct {
		name      string
		groups    []string
		expectErr bool
	}{
		{
			name:      "valid single group",
			groups:    []string{"engineering"},
			expectErr: false,
		},
		{
			name:      "valid multiple groups",
			groups:    []string{"engineering", "platform", "security"},
			expectErr: false,
		},
		{
			name:      "empty groups list",
			groups:    []string{},
			expectErr: true,
		},
		{
			name:      "nil groups list",
			groups:    nil,
			expectErr: true,
		},
		{
			name:      "group with empty name",
			groups:    []string{"engineering", "", "platform"},
			expectErr: true,
		},
		{
			name:      "only empty group name",
			groups:    []string{""},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOktaGroups(tt.groups)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
