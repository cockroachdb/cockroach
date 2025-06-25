// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/securityccl/jwthelper"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"golang.org/x/oauth2"
)

const (
	beginAuthorizationCounter       = counterPrefix + "begin_authorization"
	authorizationTokenSuccessCtr    = counterPrefix + "authorization_token_success"
	authorizationUserinfoSuccessCtr = counterPrefix + "authorization_userinfo_success"
	authorizationUserinfoNoGrpCtr   = counterPrefix + "authorization_userinfo_no_groups"
	authorizationFailureCtr         = counterPrefix + "authorization_failure"
)

var (
	beginAuthorizationUseCounter       = telemetry.GetCounterOnce(beginAuthorizationCounter)
	authorizationTokenSuccessUseCtr    = telemetry.GetCounterOnce(authorizationTokenSuccessCtr)
	authorizationUserinfoSuccessUseCtr = telemetry.GetCounterOnce(authorizationUserinfoSuccessCtr)
	authorizationUserinfoNoGrpUseCtr   = telemetry.GetCounterOnce(authorizationUserinfoNoGrpCtr)
	authorizationFailureUseCtr         = telemetry.GetCounterOnce(authorizationFailureCtr)
)

// parseUserinfoGroups pulls the group list out of a decoded user-info payload,
// applies the same normalisation rules as jwthelper.ExtractGroups, and returns
// a deduped slice.
//
// A nil slice means “no groups field present”; callers decide whether this is
// fatal
func parseUserinfoGroups(
	payload map[string]any, userinfoGroupsKey string, claimName string,
) ([]string, error) {

	rawGroups, ok := payload[userinfoGroupsKey]
	if !ok {
		return nil, nil // no groups field
	}

	// Build a synthetic token that carries only the groups claim.
	groupsTok := jwt.New()
	if err := groupsTok.Set(claimName, rawGroups); err != nil {
		return nil, errors.Wrapf(err,
			"setting %q claim on synthetic groups token", claimName)
	}

	groups, err := jwthelper.ParseGroupsClaim(groupsTok, claimName)
	if err != nil {
		return nil, errors.Wrapf(err,
			"extracting groups from synthetic groups token")
	}
	return groups, nil
}

// ExtractGroups is part of the JWTVerifier interface in pgwire.
//   - If server.jwt_authentication.authorization.enabled is FALSE -> return (nil, nil)
//   - Else return the groups list from jwt
//     if groups field is absent from jwt, try fetching it from userinfo endpoint
//
// return value:
// nil, nil means feature is off. empty groups means all roles should be revoked.
func (a *jwtAuthenticator) ExtractGroups(
	ctx context.Context, st *cluster.Settings, tokenBytes []byte,
) ([]string, error) {
	telemetry.Inc(beginAuthorizationUseCounter)
	a.reloadConfig(ctx, st)

	if !JWTAuthZEnabled.Get(&st.SV) {
		return nil, nil
	}

	tok, err := jwt.ParseInsecure(tokenBytes)
	if err != nil {
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: invalid token"),
			"token parsing failed: %v", err)
	}

	groups, err := jwthelper.ParseGroupsClaim(tok, JWTAuthGroupClaim.Get(&st.SV))
	if err == nil {
		telemetry.Inc(authorizationTokenSuccessUseCtr)
		if groups == nil {
			groups = []string{}
		}
		return groups, nil
	}

	// Fallback to userinfo, now using the dynamic provider getter.
	issuer := tok.Issuer()
	if issuer == "" {
		return nil, errors.New("JWT authorization: token has no issuer claim, cannot perform userinfo lookup")
	}

	provider, err := a.getProviderForIssuer(ctx, issuer)
	if err != nil {
		telemetry.Inc(authorizationFailureUseCtr)
		clientErr := errors.New("JWT authorization: userinfo lookup failed")
		// The detailed internal error now comes from the provider initialization.
		return nil, errors.WithSecondaryError(clientErr, err)
	}

	userInfo, err := provider.UserInfo(ctx, oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: string(tokenBytes),
	}))
	if err != nil {
		telemetry.Inc(authorizationFailureUseCtr)
		internalErr := errors.Wrap(err, "JWT authorization: userinfo lookup failed")
		clientErr := errors.New("JWT authorization: userinfo lookup failed")
		return nil, errors.WithSecondaryError(clientErr, internalErr)
	}

	var claims map[string]interface{}
	if err := userInfo.Claims(&claims); err != nil {
		telemetry.Inc(authorizationFailureUseCtr)
		return nil, errors.Wrap(err, "JWT authorization: could not decode claims from userinfo")
	}

	groups, err = parseUserinfoGroups(
		claims,
		JWTAuthUserinfoGroupKey.Get(&st.SV),
		JWTAuthGroupClaim.Get(&st.SV),
	)
	if err != nil {
		telemetry.Inc(authorizationFailureUseCtr)
		return nil, errors.Wrap(err, "JWT authorization: could not parse groups from userinfo")
	}

	if groups == nil {
		telemetry.Inc(authorizationUserinfoNoGrpUseCtr)
		return []string{}, nil
	}

	telemetry.Inc(authorizationUserinfoSuccessUseCtr)
	return groups, nil
}
