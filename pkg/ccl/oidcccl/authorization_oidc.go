// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package oidcccl

import (
	"context"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/securityccl/jwthelper"
	secuser "github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"golang.org/x/oauth2"
)

// authorize reconciles username's SQL role memberships against the IdP's groups
// after a successful OIDC login. conf and manager are a caller-supplied snapshot
// (see configSnapshot) so the call, including the /userinfo lookup via manager,
// runs lock-free. It collects the "groups" claim from the ID token, the access
// token, or the provider's /userinfo endpoint, then calls
// sql.EnsureUserOnlyBelongsToRoles. A non-nil error means the login must be
// denied; it is a no-op returning nil when authZEnabled is false.
func (oidcAuthentication *oidcAuthenticationServer) authorize(
	ctx context.Context,
	conf oidcAuthenticationConf,
	manager IOIDCManager,
	rawIDToken string,
	rawAccessToken string,
	username string,
) error {
	if !conf.authZEnabled {
		return nil
	}

	// When cluster-level authorization is enabled we require an access token.
	if rawAccessToken == "" {
		log.Dev.Errorf(ctx, "OIDC: access_token missing while authorization is enabled")
		return errors.New("OIDC: required access_token missing from credentials")
	}

	var groups []string
	var err error

	// groupsClaimFound records whether the configured groups claim was located and
	// parsed from any source (ID token, access token, or /userinfo), regardless of
	// how many groups it held. It separates "the IdP explicitly reported no groups"
	// (claim present but empty) from "no claim found at all" (a transient IdP error
	// or misconfiguration), which are handled differently below. A nil groups slice
	// alone cannot: appending an empty parse result to nil leaves it nil.
	groupsClaimFound := false

	tokenSources := []struct{ raw, name string }{
		{rawIDToken, "ID token"},
		{rawAccessToken, "access token"},
	}

	for _, ts := range tokenSources {
		if ts.raw == "" {
			continue // Nothing to parse, this token was not sent
		}

		// ParseInsecure extracts claims without re-verifying the token, which is
		// intentional: the ID token was already signature- and issuer-verified by
		// o.Verify before authorize() ran, and the access token was obtained over a
		// back-channel TLS exchange with the IdP (o.Exchange) that never passes
		// through the user, so trust is established at the transport layer. If this
		// ever accepts a token from an untrusted source, add signature and audience
		// validation here.
		if tok, parseErr := jwt.ParseInsecure([]byte(ts.raw)); parseErr == nil {
			claimGroups, err := jwthelper.ParseGroupsClaim(
				tok,
				conf.groupClaim,
			)
			if err != nil {
				// A malformed claim is a warning, but we can still fall back to
				// the next token or to /userinfo.
				log.Ops.Warningf(ctx,
					"OIDC: failed to parse groups claim from %s, will try other sources: %v",
					ts.name, err)
				continue
			}
			groupsClaimFound = true
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

	// If the tokens yielded no groups, fall back to the userinfo endpoint. This
	// includes a token that carried an explicitly empty groups claim: some IdPs
	// return an empty array in the token and populate the groups only at
	// /userinfo, so the endpoint is consulted whenever no groups were found yet.
	if len(groups) == 0 && conf.userinfoGroupKey != "" {
		// Re-create a minimal oauth2.Token for the UserInfo call.
		oauthTok := &oauth2.Token{AccessToken: rawAccessToken}
		userinfo, err := manager.UserInfo(ctx, oauth2.StaticTokenSource(oauthTok))
		if err != nil {
			log.Dev.VErrorf(ctx, 1, "OIDC: failed to get userinfo for authorization: %v", err)
		} else {
			var userinfoJSON map[string]any
			if err := userinfo.Claims(&userinfoJSON); err == nil {
				if raw, ok := userinfoJSON[conf.userinfoGroupKey]; ok {
					synthetic := jwt.New()
					if err := synthetic.Set(conf.userinfoGroupKey, raw); err != nil {
						log.Dev.VErrorf(ctx, 1, "OIDC: failed to set synthetic claim for authorization: %v", err)
					}
					groups, err = jwthelper.ParseGroupsClaim(
						synthetic,
						conf.userinfoGroupKey,
					)
					if err != nil {
						log.Ops.VWarningf(ctx, 1, "OIDC: failed to parse groups claim from userinfo: %v", err)
					} else {
						groupsClaimFound = true
					}
				} else {
					log.Ops.VWarningf(ctx, 1, "OIDC: userinfo response did not contain group key: %s", conf.userinfoGroupKey)
				}
			} else {
				log.Ops.VWarningf(ctx, 1, "OIDC: failed to parse userinfo claims: %v", err)
			}
		}
	}

	// If no groups claim was found from any source, deny login without touching
	// role memberships: a missing claim signals a transient IdP error or
	// misconfiguration, and syncing an empty role set would strip all the user's
	// roles as a side effect of a login that is denied anyway. This is distinct
	// from the IdP explicitly reporting no groups, handled after the sync below.
	if !groupsClaimFound {
		log.Dev.Errorf(ctx, "OIDC: unable to locate any groups claim for user %s", username)
		return errors.Newf("OIDC: unable to complete authentication: no groups claim found for %s", username)
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

	// As per policy, the IdP explicitly reporting an empty list of groups is
	// treated as an authorization failure: the login is denied after all roles
	// are revoked by the sync above.
	if len(groups) == 0 {
		log.Dev.Errorf(ctx, "OIDC: found empty list of groups for user %s", username)
		return errors.Newf("OIDC: unable to complete authentication: groups list was empty for %s", username)
	}

	return nil
}
