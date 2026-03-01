// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/security/jwthelper"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwt"
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

// getUserinfoEndpoint returns <issuer>'s user-info URL as advertised in the
// OpenID-Connect discovery document. An empty string means the provider
// doesn’t expose one.
//
// It’s a free function instead of a (*jwtAuthenticator) method so that it
// remains mock-friendly in tests and also as it is part of the authz flow
// and not the authn flow.
// We still pass the authenticator so the code can reuse getHttpResponse
// (which already wraps authenticator.mu.conf.httpClient
// and therefore honours custom CAs).
func getUserinfoEndpoint(
	ctx context.Context, issuer string, authenticator *jwtAuthenticator,
) (string, error) {
	cfgURL := getOpenIdConfigEndpoint(issuer)

	body, err := getHttpResponse(ctx, cfgURL, authenticator)
	if err != nil {
		return "", errors.Wrapf(err, "discovery fetch failed: GET %s", cfgURL)
	}

	var oidcConfigResp struct {
		UserinfoEndpoint string `json:"userinfo_endpoint"`
	}
	if err := json.Unmarshal(body, &oidcConfigResp); err != nil {
		return "", errors.Wrap(err, "invalid discovery document")
	}
	return oidcConfigResp.UserinfoEndpoint, nil
}

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
	//This call grabs a writelock internally and therefore must happen
	//before we acquire readlock below to avoid deadlocking.
	a.reloadConfig(ctx, st)
	a.mu.RLock()
	defer a.mu.RUnlock()

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
		if groups == nil { // empty claim -> empty (non-nil) slice
			groups = []string{}
		}
		return groups, nil // found groups in jwt, early exit
	}

	// Fallback to userinfo
	groups, err = fetchGroupsFromUserinfo(
		ctx, st, tok.Issuer(), tokenBytes, a)
	if err != nil {
		telemetry.Inc(authorizationFailureUseCtr)
		internalErr := errors.Wrap(err, "JWT authorization: userinfo lookup failed")
		clientErr := errors.New("JWT authorization: userinfo lookup failed")
		return nil, errors.WithSecondaryError(clientErr, internalErr)
	}

	// Userinfo endpoint was explicitly ""
	if groups == nil {
		telemetry.Inc(authorizationUserinfoNoGrpUseCtr)
		internalErr := errors.Newf("JWT authorization: groups claim missing in token and there is no userinfo endpoint (issuer=%q subject=%q)",
			tok.Issuer(), tok.Subject())
		clientErr := errors.New("JWT authorization: No userinfo endpoint")

		return nil, errors.WithSecondaryError(clientErr, internalErr)
	}

	telemetry.Inc(authorizationUserinfoSuccessUseCtr)
	return groups, nil
}

// fetchGroupsFromUserinfo calls the user-info endpoint advertised by the
// provider, decodes the JSON payload, and returns the value of the field named
// by server.jwt_authentication.userinfo_group_key (default “groups”) as a
// normalised, deduped slice.
//
// Return values:
//   - (nil,  nil)  – userinfo endpoint explicitly set to ""
//   - ([] , nil)   – endpoint responded, key not present or value is an empty list.
//     Callers should treat this as “authorization failure” and reject login.
//   - (slice, nil) – one or more groups extracted successfully.
//   - (nil,  err)  – any parsing / type / network error (details wrapped with
//     errors.WithDetailf).
var fetchGroupsFromUserinfo = func(
	ctx context.Context,
	st *cluster.Settings,
	issuer string,
	bearer []byte,
	auth *jwtAuthenticator, // we need this both for HTTP client & custom CA
) ([]string, error) {

	//Discover the user-info endpoint from the OIDC configuration.
	userinfoEndpoint, err := getUserinfoEndpoint(ctx, issuer, auth)
	if err != nil {
		return nil, errors.Wrapf(err, "userinfo discovery failed")
	}
	if userinfoEndpoint == "" {
		// Provider's UserinfoEndpoint was explicitly set to ""
		// caller should decide what to do.
		return nil, nil
	}

	header := http.Header{}
	header.Set("Authorization", "Bearer "+string(bearer))

	body, err := getHttpResponse(ctx, userinfoEndpoint, auth, header)
	if err != nil {
		return nil, errors.Wrapf(err, "userinfo request failed")
	}

	//Decode the JSON payload.
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, errors.Wrapf(err, "invalid userinfo response")
	}

	// Extract and normalise groups
	groups, err := parseUserinfoGroups(
		payload,
		JWTAuthUserinfoGroupKey.Get(&st.SV),
		JWTAuthGroupClaim.Get(&st.SV),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid groups in user-info")
	}
	// if IdP doesn’t supply a groups key, better to assume that user is no
	// longer part of any roles. thus, return empty list
	if groups == nil {
		return []string{}, nil
	}
	return groups, nil
}
