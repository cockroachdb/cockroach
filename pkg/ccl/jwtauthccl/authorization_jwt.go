// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/ccl/securityccl/jwthelper"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
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
func getUserinfoEndpoint(
	ctx context.Context, issuer string, httpClient *httputil.Client,
) (string, error) {
	cfgURL := getOpenIdConfigEndpoint(issuer)

	resp, err := httpClient.Get(ctx, cfgURL)
	if err != nil {
		return "", errors.WithDetailf(
			errors.Newf("JWT authorization: discovery fetch failed"),
			"http GET %s: %v", cfgURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.WithDetailf(
			errors.Newf("JWT authorization: invalid discovery response"),
			"failed to read body: %v", err)
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
func (a *jwtAuthenticator) ExtractGroups(
	st *cluster.Settings, tokenBytes []byte,
) ([]string, error) {
	if !JWTAuthZEnabled.Get(&st.SV) {
		return nil, nil
	}
	tok, err := jwt.ParseInsecure(tokenBytes)
	if err != nil {
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: invalid token"),
			"token parsing failed: %v", err)
	}

	groups, err := jwthelper.ExtractGroups(st, tok, JWTAuthGroupClaim.Get(&st.SV))
	if err == nil && len(groups) > 0 {
		telemetry.Inc(authzTokenSuccessUseCounter)
		return groups, nil // found groups in jwt, early exit
	}

	// Fallback to userinfo
	ctx := context.TODO()
	groups, err = fetchGroupsFromUserinfo(
		ctx, st, tok.Issuer(), tokenBytes, a.mu.conf.httpClient)
	if err != nil {
		telemetry.Inc(authzFailureUseCounter)
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: userinfo lookup failed"),
			"%v", err)
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

// fetchGroupsFromUserinfo calls the userinfo endpoint with the supplied Bearer
// token, decodes the JSON payload, and returns the value of the field named by
// server.jwt_authentication.userinfo_group_key (default "groups") as a
// normalised, deduped, sorted slice.
// A non-200 response or network error / missing groups field yields (nil, nil)
// so the caller can decide whether or not to fail the login.
var fetchGroupsFromUserinfo = func(
	ctx context.Context,
	st *cluster.Settings,
	issuer string,
	bearer []byte,
	httpClient *httputil.Client,
) ([]string, error) {
	userinfoEndpoint, err := getUserinfoEndpoint(ctx, issuer, httpClient)
	if err != nil {
		return nil, err
	}
	if userinfoEndpoint == "" {
		return nil, nil
	}

	req, _ := http.NewRequestWithContext(ctx, "GET", userinfoEndpoint, nil)
	req.Header.Set("Authorization", "Bearer "+string(bearer))

	resp, err := httpClient.Do(req)
	if err != nil { // network / TLS failure
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: userinfo fetch failed"),
			"request error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Newf("userinfo: %s", resp.Status)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: invalid userinfo response"),
			"failed to parse response: %v", err)
	}

	userinfoGroupsKey := JWTAuthUserinfoGroupKey.Get(&st.SV)
	userinfoGroupsRaw, ok := payload[userinfoGroupsKey]
	if !ok {
		return nil, nil
	}

	claimName := JWTAuthGroupClaim.Get(&st.SV)

	// synthesise a minimal token that only carries the groups claim so we
	// can reuse jwthelper.ExtractGroups().
	newTokenWithGroups := jwt.New()
	if err := newTokenWithGroups.Set(claimName, userinfoGroupsRaw); err != nil {
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: user-info claim synthesis failed"),
			"setting %q claim on dummy token: %v", claimName, err)
	}
	return jwthelper.ExtractGroups(st, newTokenWithGroups, claimName)

}
