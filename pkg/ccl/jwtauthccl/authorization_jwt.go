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

// getUserinfoEndpoint returns <issuer>'s user-info URL as advertised in the
// OpenID-Connect discovery document. An empty string means the provider
// doesn’t expose one.
func getUserinfoEndpoint(
	ctx context.Context, issuer string, httpClient *httputil.Client,
) (string, error) {
	cfgURL := getOpenIdConfigEndpoint(issuer)

	resp, err := httpClient.Get(ctx, cfgURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var cfg struct {
		UserinfoEndpoint string `json:"userinfo_endpoint"`
	}
	if err := json.Unmarshal(body, &cfg); err != nil {
		return "", err
	}
	return cfg.UserinfoEndpoint, nil
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
		return nil, err
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
		return nil, err
	}
	if len(groups) == 0 {
		telemetry.Inc(authzUserinfoMissUseCounter)
		return nil, errors.New("no groups claim in token nor userinfo")
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
	ep, err := getUserinfoEndpoint(ctx, issuer, httpClient)
	if err != nil {
		return nil, err
	}
	if ep == "" {
		return nil, nil
	}

	req, _ := http.NewRequestWithContext(ctx, "GET", ep, nil)
	req.Header.Set("Authorization", "Bearer "+string(bearer))

	resp, err := httpClient.Do(req)
	if err != nil { // network / TLS failure
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Newf("userinfo: %s", resp.Status)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	key := JWTAuthUserinfoGroupKey.Get(&st.SV)
	raw, ok := payload[key]
	if !ok {
		return nil, nil
	}

	claimName := JWTAuthGroupClaim.Get(&st.SV)
	dummy := jwt.New()
	_ = dummy.Set(claimName, raw)
	return jwthelper.ExtractGroups(st, dummy, claimName)
}
