// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/coreos/go-oidc/v3/oidc"
)

// OktaValidatorConfig holds the configuration for Okta token validation.
type OktaValidatorConfig struct {
	Issuer   string
	Audience string
}

// OktaValidator validates Okta ID tokens using OIDC.
type OktaValidator struct {
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
	config   OktaValidatorConfig
}

// NewOktaValidator creates a new Okta token validator function.
// It initializes an OIDC provider that fetches the JWKS from the issuer's
// .well-known/openid-configuration endpoint for automatic key management.
// Returns an error if the configuration is invalid or the OIDC provider
// cannot be initialized.
func NewOktaValidator(
	cfg OktaValidatorConfig,
) (func(context.Context, string) (string, string, error), error) {
	// Validate configuration
	if cfg.Issuer == "" {
		return nil, errors.New("Okta issuer is required")
	}
	if cfg.Audience == "" {
		return nil, errors.New("Okta audience is required")
	}

	// Create OIDC provider - this fetches the discovery document from
	// {issuer}/.well-known/openid-configuration and caches the JWKS
	ctx := context.Background()
	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create OIDC provider")
	}

	// Create verifier with audience validation
	// The ClientID config field is used as the expected audience
	verifier := provider.Verifier(&oidc.Config{
		ClientID: cfg.Audience,
	})

	validator := &OktaValidator{
		provider: provider,
		verifier: verifier,
		config:   cfg,
	}

	return validator.Validate, nil
}

// Validate verifies an Okta ID token and extracts user information.
// It validates the token signature using JWKS, checks expiration and audience,
// then extracts the user identifier and email from claims.
// Returns (oktaUserID, email, error).
func (v *OktaValidator) Validate(ctx context.Context, token string) (string, string, error) {
	// Verify the token signature and standard claims (exp, aud, iss)
	idToken, err := v.verifier.Verify(ctx, token)
	if err != nil {
		return "", "", errors.Wrap(err, "failed to verify token")
	}

	// Extract claims from the token
	var claims struct {
		Sub   string `json:"sub"`
		Email string `json:"email"`
		UID   string `json:"uid"` // Okta-specific user ID claim
	}
	if err := idToken.Claims(&claims); err != nil {
		return "", "", errors.Wrap(err, "failed to extract claims")
	}

	// Use 'uid' claim if present (Okta-specific), otherwise fall back to 'sub'
	oktaUserID := claims.UID
	if oktaUserID == "" {
		oktaUserID = claims.Sub
	}

	if oktaUserID == "" {
		return "", "", errors.New("token missing user identifier (uid or sub claim)")
	}

	return oktaUserID, claims.Email, nil
}
