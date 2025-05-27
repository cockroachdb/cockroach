// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/ccl/securityccl/jwthelper"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

const (
	authzTokenSuccessCounter    = counterPrefix + "authz_success"
	authzUserinfoSuccessCounter = counterPrefix + "authz_userinfo_success"
	authzUserinfoMissCounter    = counterPrefix + "authz_userinfo_miss"
	authzFailureCounter         = counterPrefix + "authz_failure"
)

var (
	authzTokenSuccessUseCounter    = telemetry.GetCounterOnce(authzTokenSuccessCounter)
	authzUserinfoSuccessUseCounter = telemetry.GetCounterOnce(authzUserinfoSuccessCounter)
	authzUserinfoMissUseCounter    = telemetry.GetCounterOnce(authzUserinfoMissCounter)
	authzFailureUseCounter         = telemetry.GetCounterOnce(authzFailureCounter)
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
		return "", errors.WithDetailf(
			errors.Newf("JWT authorization: discovery fetch failed"),
			"http GET %s: %v", cfgURL, err)
	}

	var oidcConfigResp struct {
		UserinfoEndpoint string `json:"userinfo_endpoint"`
	}
	if err := json.Unmarshal(body, &oidcConfigResp); err != nil {
		return "", errors.WithDetailf(
			errors.Newf("JWT authorization: invalid discovery document"),
			"failed to parse JSON: %v", err)
	}
	return oidcConfigResp.UserinfoEndpoint, nil
}

// ExtractGroups is part of the JWTVerifier interface in pgwire.
//   - If server.jwt_authentication.authorization.enabled is FALSE -> return (nil, nil)
//   - Else return the groups list from jwt
//     if groups field is absent from jwt, try fetching it from userinfo endpoint
//
// return value:
// nil groups means feature is off. empty groups means all roles should be revoked.
func (a *jwtAuthenticator) ExtractGroups(
	ctx context.Context, st *cluster.Settings, tokenBytes []byte,
) ([]string, error) {
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

	groups, err := jwthelper.ExtractGroups(tok, JWTAuthGroupClaim.Get(&st.SV))
	if err == nil {
		telemetry.Inc(authzTokenSuccessUseCounter)
		if groups == nil { // empty claim -> empty (non-nil) slice
			groups = []string{}
		}
		return groups, nil // found groups in jwt, early exit
	}

	// Fallback to userinfo
	groups, err = fetchGroupsFromUserinfo(
		ctx, st, tok.Issuer(), tokenBytes, a)
	if err != nil {
		telemetry.Inc(authzFailureUseCounter)
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: userinfo lookup failed"),
			"%v", err)
	}
	// Nil -> empty slice so callers can tell “no groups” apart from “feature off”.
	if groups == nil {
		groups = []string{}
	}

	if len(groups) == 0 {
		telemetry.Inc(authzUserinfoMissUseCounter)
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: missing groups"),
			"no groups claim in token nor in userinfo (issuer=%q subject=%q)",
			tok.Issuer(), tok.Subject())
	}
	telemetry.Inc(authzUserinfoSuccessUseCounter)
	return groups, nil
}

// fetchGroupsFromUserinfo calls the user-info endpoint advertised by the
// provider, decodes the JSON payload, and returns the value of the field named
// by server.jwt_authentication.userinfo_group_key (default “groups”) as a
// normalised, deduped, sorted slice.
//
// Every error that bubbles up is wrapped with errors.WithDetailf so callers can
// tag logs consistently.
//
// A provider that has no user-info endpoint, a network/HTTP failure, or a
// payload that lacks the configured groups key all yield (nil, nil). callers
// decide whether that is fatal.
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
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: user-info discovery failed"),
			"%v", err)
	}
	if userinfoEndpoint == "" {
		// Provider does not expose user-info – caller will decide what to do.
		return nil, nil
	}

	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+string(bearer))

	body, err := getHttpResponse(ctx, userinfoEndpoint, auth, hdr)
	if err != nil {
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: user-info request failed"),
			"%v", err)
	}

	//Decode the JSON payload.
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: invalid user-info response"),
			"failed to parse response %v", err)
	}

	// Extract and normalise the groups claim.
	userinfoGroupsKey := JWTAuthUserinfoGroupKey.Get(&st.SV)
	rawGroups, ok := payload[userinfoGroupsKey]
	if !ok {
		// No groups field – let caller decide (auth-Z may still succeed
		// if groups are optional for them).
		return nil, nil
	}

	claimName := JWTAuthGroupClaim.Get(&st.SV)

	// Synthesize a minimal token that only carries the groups claim so we can
	// reuse jwthelper.ExtractGroups().
	groupsTok := jwt.New()
	if err := groupsTok.Set(claimName, rawGroups); err != nil {
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: user-info claim synthesis failed"),
			"setting %q claim on the groups token: %v", claimName, err)
	}

	grps, err := jwthelper.ExtractGroups(groupsTok, claimName)
	if err != nil {
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: invalid groups in user-info"),
			"%v", err)
	}
	return grps, nil
}
