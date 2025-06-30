// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package oidcccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/securityccl/jwthelper"
	secuser "github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/coreos/go-oidc"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"golang.org/x/oauth2"
)

func (oidcAuthentication *oidcAuthenticationServer) reconcileRoles(
	ctx context.Context, rawIDToken string, oauthTok *oauth2.Token, username string,
) error {
	if !oidcAuthentication.conf.authZEnabled {
		return nil
	}

	var groups []string

	// Look for the claim in the ID token
	if tok, err := jwt.ParseInsecure([]byte(rawIDToken)); err == nil {
		groups, _ = jwthelper.ParseGroupsClaim(
			tok,
			oidcAuthentication.conf.groupClaim,
		)
	}

	// If no claim in ID Token, try the access token (only if it is a JWT)
	if len(groups) == 0 {
		if atok, err := jwt.ParseInsecure([]byte(oauthTok.AccessToken)); err == nil {
			groups, _ = jwthelper.ParseGroupsClaim(
				atok,
				oidcAuthentication.conf.groupClaim,
			)
		}
	}

	// If no groups in ID or access tokens, fall back to the userinfo endpoint
	if len(groups) == 0 && oidcAuthentication.conf.userinfoGroupKey != "" {
		pctx := oidc.ClientContext(ctx, oidcAuthentication.conf.httpClient.Client)
		prov, err := oidc.NewProvider(pctx, oidcAuthentication.conf.providerURL)
		if err != nil {
			return err
		}

		ui, err := prov.UserInfo(pctx, oauth2.StaticTokenSource(oauthTok))
		if err != nil {
			return err
		}

		var userinfoJSON map[string]any
		if err := ui.Claims(&userinfoJSON); err == nil {
			if raw, ok := userinfoJSON[oidcAuthentication.conf.userinfoGroupKey]; ok {
				synthetic := jwt.New()
				_ = synthetic.Set(oidcAuthentication.conf.userinfoGroupKey, raw)
				groups, _ = jwthelper.ParseGroupsClaim(
					synthetic,
					oidcAuthentication.conf.userinfoGroupKey,
				)
			}
		}
	}

	// Sync roles if we found any groups
	if len(groups) != 0 {
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
	}

	return nil
}
