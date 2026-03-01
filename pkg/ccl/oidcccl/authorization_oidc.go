// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package oidcccl

import (
	"context"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/jwthelper"
	secuser "github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"golang.org/x/oauth2"
)

// authorize reconciles SQL role memberships for the given user after a
// successful OIDC login.
//
// Called as a method on *oidcAuthenticationServer, it relies on the server’s
// configuration (`conf`) and execCfg to:
//
//   - Discover which OIDC authorization sources are enabled.
//   - Collect a "groups" claim from the ID token, the access token, or,
//     when configured: the provider's /userinfo endpoint.
//   - Convert the union of all group names into SQL roles and call
//     sql.EnsureUserOnlyBelongsToRoles so the user’s memberships reflect
//     the IdP state.
//
// Inputs:
//
//	ctx            – request context used for logging and database ops
//	rawIDToken     – verified ID token string
//	rawAccessToken – access token string (may be opaque)
//	username       – SQL username extracted from the ID token
//
// Return value:
//
//	nil   – roles synced and authorization succeeded
//	error – login must be denied; message explains whether no groups claim
//	        was found, the list was empty, or a database/error occurred.
//
// The function is a no-op (returns nil) when
// server.oidc_authentication.authorization.enabled is false, preserving the
// legacy "authentication-only" behavior.
func (oidcAuthentication *oidcAuthenticationServer) authorize(
	ctx context.Context, rawIDToken string, rawAccessToken string, username string,
) error {
	if !oidcAuthentication.conf.authZEnabled {
		return nil
	}

	// When cluster-level authorization is enabled we require an access token.
	if rawAccessToken == "" {
		log.Dev.Errorf(ctx, "OIDC: access_token missing while authorization is enabled")
		return errors.New("OIDC: required access_token missing from credentials")
	}

	var groups []string
	var err error

	tokenSources := []struct{ raw, name string }{
		{rawIDToken, "ID token"},
		{rawAccessToken, "access token"},
	}

	for _, ts := range tokenSources {
		if ts.raw == "" {
			continue // Nothing to parse, this token was not sent
		}

		if tok, parseErr := jwt.ParseInsecure([]byte(ts.raw)); parseErr == nil {
			claimGroups, err := jwthelper.ParseGroupsClaim(
				tok,
				oidcAuthentication.conf.groupClaim,
			)
			if err != nil {
				// A malformed claim is a warning, but we can still fall back to
				// the next token or to /userinfo.
				log.Ops.Warningf(ctx,
					"OIDC: failed to parse groups claim from %s, will try other sources: %v",
					ts.name, err)
				continue
			}
			groups = append(groups, claimGroups...)
		} else {
			// Parsing failed (for example, opaque access token).  This is a
			// warning rather than an error because we can continue with other
			// sources.  Use the OPS channel so operators notice repeated issues.
			log.Ops.VWarningf(ctx, 1, "OIDC: failed to parse %s: %v", ts.name, parseErr)
		}
	}

	// Remove case-insensitive duplicates
	groups = slices.CompactFunc(groups, func(a, b string) bool {
		return strings.EqualFold(a, b)
	})

	// If no groups in ID or access tokens, fall back to the userinfo endpoint
	if groups == nil && oidcAuthentication.conf.userinfoGroupKey != "" {
		// Re-create a minimal oauth2.Token for the UserInfo call.
		oauthTok := &oauth2.Token{AccessToken: rawAccessToken}
		userinfo, err := oidcAuthentication.manager.UserInfo(ctx, oauth2.StaticTokenSource(oauthTok))
		if err != nil {
			log.Dev.VErrorf(ctx, 1, "OIDC: failed to get userinfo for authorization: %v", err)
		} else {
			var userinfoJSON map[string]any
			if err := userinfo.Claims(&userinfoJSON); err == nil {
				if raw, ok := userinfoJSON[oidcAuthentication.conf.userinfoGroupKey]; ok {
					synthetic := jwt.New()
					if err := synthetic.Set(oidcAuthentication.conf.userinfoGroupKey, raw); err != nil {
						log.Dev.VErrorf(ctx, 1, "OIDC: failed to set synthetic claim for authorization: %v", err)
					}
					groups, err = jwthelper.ParseGroupsClaim(
						synthetic,
						oidcAuthentication.conf.userinfoGroupKey,
					)
					if err != nil {
						log.Ops.VWarningf(ctx, 1, "OIDC: failed to parse groups claim from userinfo: %v", err)
					}
				} else {
					log.Ops.VWarningf(ctx, 1, "OIDC: userinfo response did not contain group key: %s", oidcAuthentication.conf.userinfoGroupKey)
				}
			} else {
				log.Ops.VWarningf(ctx, 1, "OIDC: failed to parse userinfo claims: %v", err)
			}
		}
	}

	// Sync roles. An empty slice here means “explicitly no roles”, so we keep going.
	sqlRoles := make([]secuser.SQLUsername, 0, len(groups))
	for _, g := range groups {
		role, err := secuser.MakeSQLUsernameFromUserInput(g, secuser.PurposeValidation)
		if err != nil {
			log.Ops.VWarningf(ctx, 1, "OIDC authorization: error finding matching SQL role for group %s: %v", g, err)
			continue
		}
		sqlRoles = append(sqlRoles, role)
	}

	userSQL, err := secuser.MakeSQLUsernameFromUserInput(
		username, secuser.PurposeValidation,
	)
	if err != nil {
		return err
	}

	if err := sql.EnsureUserOnlyBelongsToRoles(
		ctx, oidcAuthentication.execCfg, userSQL, sqlRoles,
	); err != nil {
		return err
	}
	// If no groups claim was ever found
	// this is an authorization failure and the login must be denied
	// after all roles are revoked
	if groups == nil {
		log.Dev.Errorf(ctx, "OIDC: unable to locate any groups claim for user %s", username)
		return errors.Newf("OIDC: unable to complete authentication: no groups claim found for %s", username)
	}

	// As per policy, an empty list of groups is treated as
	// an authorization failure and the login must be denied
	// after all roles are revoked
	if len(groups) == 0 {
		log.Dev.Errorf(ctx, "OIDC: found empty list of groups for user %s", username)
		return errors.Newf("OIDC: unable to complete authentication: groups list was empty for %s", username)
	}

	return nil
}
