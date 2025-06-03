// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package oidcccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/securityccl/jwthelper"
	secuser "github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"golang.org/x/oauth2"
)

func (oidcAuthentication *oidcAuthenticationServer) authorize(
	ctx context.Context, rawIDToken string, rawAccessToken string, username string,
) error {
	if !oidcAuthentication.conf.authZEnabled {
		return nil
	}

	// When cluster-level authorization is enabled we require an access token.
	if rawAccessToken == "" {
		log.Errorf(ctx, "OIDC: access_token missing while authorization is enabled")
		return errors.New("OIDC: required access_token missing from credentials")
	}

	var groups []string
	var err error
	groupSet := make(map[string]struct{}) // collect unique groups from all tokens

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
			for _, g := range claimGroups {
				groupSet[g] = struct{}{}
			}
		} else {
			// Parsing failed (for example, opaque access token).  This is a
			// warning rather than an error because we can continue with other
			// sources.  Use the OPS channel so operators notice repeated issues.
			log.Ops.VWarningf(ctx, 1, "OIDC: failed to parse %s: %v", ts.name, parseErr)
		}
	}

	if len(groupSet) > 0 {
		groups = make([]string, 0, len(groupSet))
		for g := range groupSet {
			groups = append(groups, g)
		}
	}

	// If no groups in ID or access tokens, fall back to the userinfo endpoint
	if groups == nil && oidcAuthentication.conf.userinfoGroupKey != "" {
		// Re-create a minimal oauth2.Token for the UserInfo call.
		oauthTok := &oauth2.Token{AccessToken: rawAccessToken}
		userinfo, err := oidcAuthentication.manager.UserInfo(ctx, oauth2.StaticTokenSource(oauthTok))
		if err != nil {
			log.VErrorf(ctx, 1, "OIDC: failed to get userinfo for authorization: %v", err)
		} else {
			var userinfoJSON map[string]any
			if err := userinfo.Claims(&userinfoJSON); err == nil {
				if raw, ok := userinfoJSON[oidcAuthentication.conf.userinfoGroupKey]; ok {
					synthetic := jwt.New()
					if err := synthetic.Set(oidcAuthentication.conf.userinfoGroupKey, raw); err != nil {
						log.VErrorf(ctx, 1, "OIDC: failed to set synthetic claim for authorization: %v", err)
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
		log.Errorf(ctx, "OIDC: unable to locate any groups claim for user %s", username)
		return errors.Newf("OIDC: unable to complete authentication: no groups claim found for %s", username)
	}

	// As per policy, an empty list of groups is treated as
	// an authorization failure and the login must be denied
	// after all roles are revoked
	if len(groups) == 0 {
		log.Errorf(ctx, "OIDC: found empty list of groups for user %s", username)
		return errors.Newf("OIDC: unable to complete authentication: groups list was empty for %s", username)
	}

	return nil
}
