// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOktaValidator_MissingIssuer(t *testing.T) {
	cfg := OktaValidatorConfig{
		Audience: "test-audience",
	}
	_, err := NewOktaValidator(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "issuer is required")
}

func TestNewOktaValidator_MissingAudience(t *testing.T) {
	cfg := OktaValidatorConfig{
		Issuer: "https://example.okta.com",
	}
	_, err := NewOktaValidator(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "audience is required")
}

func TestNewOktaValidator_InvalidIssuer(t *testing.T) {
	// Test with an invalid issuer URL that will fail OIDC discovery
	cfg := OktaValidatorConfig{
		Issuer:   "https://invalid.nonexistent.domain.example.com",
		Audience: "test-audience",
	}
	_, err := NewOktaValidator(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create OIDC provider")
}

func TestNewOktaValidator_MalformedIssuer(t *testing.T) {
	// Test with a malformed issuer URL
	cfg := OktaValidatorConfig{
		Issuer:   "not-a-valid-url",
		Audience: "test-audience",
	}
	_, err := NewOktaValidator(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create OIDC provider")
}
