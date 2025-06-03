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

	var groups []string
	var err error

	// Look for the claim in the ID token.
	if tok, err := jwt.ParseInsecure([]byte(rawIDToken)); err == nil {
		groups, err = jwthelper.ParseGroupsClaim(
			tok,
			oidcAuthentication.conf.groupClaim,
		)
		if err != nil {
			// A malformed claim is an error, but we can still fall back.
			log.Warningf(ctx, "OIDC: failed to parse groups claim from ID token, will try other sources: %v", err)
		}
	} else if log.V(1) {
		log.Errorf(ctx, "OIDC: failed to parse ID token: %v", err)
	}

	// If no claim in ID token, try the access token (only if it is a JWT).
	if groups == nil {
		if atok, err := jwt.ParseInsecure([]byte(rawAccessToken)); err == nil {
			groups, err = jwthelper.ParseGroupsClaim(
				atok,
				oidcAuthentication.conf.groupClaim,
			)
			if err != nil {
				log.Warningf(ctx, "OIDC: failed to parse groups claim from access token, will try userinfo: %v", err)
				groups = nil
			}
		} else if log.V(1) {
			log.Errorf(ctx, "OIDC: failed to parse access token: %v", err)
		}
	}

	// If no groups in ID or access tokens, fall back to the userinfo endpoint
	if groups == nil && oidcAuthentication.conf.userinfoGroupKey != "" {
		// Re-create a minimal oauth2.Token for the UserInfo call.
		oauthTok := &oauth2.Token{AccessToken: rawAccessToken}
		userinfo, err := oidcAuthentication.manager.UserInfo(ctx, oauth2.StaticTokenSource(oauthTok))
		if err != nil {
			if log.V(1) {
				log.Errorf(ctx, "OIDC: failed to get userinfo: %v", err)
			}
		} else {
			var userinfoJSON map[string]any
			if err := userinfo.Claims(&userinfoJSON); err == nil {
				if raw, ok := userinfoJSON[oidcAuthentication.conf.userinfoGroupKey]; ok {
					synthetic := jwt.New()
					if err := synthetic.Set(oidcAuthentication.conf.userinfoGroupKey, raw); err != nil {
						if log.V(1) {
							log.Errorf(ctx, "OIDC: failed to set synthetic claim: %v", err)
						}
					}
					groups, err = jwthelper.ParseGroupsClaim(
						synthetic,
						oidcAuthentication.conf.userinfoGroupKey,
					)
					if err != nil && log.V(1) {
						log.Errorf(ctx, "OIDC: failed to parse groups claim from userinfo: %v", err)
					}
				} else if log.V(1) {
					log.Infof(ctx, "OIDC: userinfo response did not contain group key: %s", oidcAuthentication.conf.userinfoGroupKey)
				}
			} else if log.V(1) {
				log.Errorf(ctx, "OIDC: failed to parse userinfo claims: %v", err)
			}
		}
	}

	// Sync roles. An empty slice here means “explicitly no roles”, so we keep going.
	sqlRoles := make([]secuser.SQLUsername, 0, len(groups))
	for _, g := range groups {
		if role, err := secuser.MakeSQLUsernameFromUserInput(
			g, secuser.PurposeValidation,
		); err == nil {
			sqlRoles = append(sqlRoles, role)
		}
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
