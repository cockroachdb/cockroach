// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// extractGroupsFromJWT uses the name mentioned in server.jwt_authentication.group_claim
// to extract the groups of the user from the JWT
// groups can be either
//  1. JSON array  ["admins","interns"]
//  2. space or comma separated string  "admins interns" "admins, interns"
//
// In addition we also lower-case, dedupe amd sort before returning the slice
func extractGroupsFromJWT(st *cluster.Settings, tok jwt.Token) ([]string, error) {
	claimName := JWTAuthGroupClaim.Get(&st.SV)

	raw, ok := tok.Get(claimName)
	if !ok {
		return nil, errors.Newf("groups claim %q missing from token", claimName)
	}

	var groups []string
	switch v := raw.(type) {
	case []any: // ["A","B"]; go unmarshals json array as []any
		for _, x := range v {
			groups = append(groups, strings.ToLower(strings.TrimSpace(fmt.Sprint(x))))
		}
	case string: // "A,B" or "A B"
		sep := ","
		if !strings.Contains(v, ",") {
			sep = " "
		}
		for _, g := range strings.Split(v, sep) {
			g = strings.ToLower(strings.TrimSpace(g))
			if g != "" {
				groups = append(groups, g)
			}
		}
	default:
		return nil, errors.New("groups claim must be string or array")
	}

	// dedupe, sort
	uniq := make(map[string]struct{}, len(groups))
	for _, g := range groups {
		uniq[g] = struct{}{}
	}
	out := make([]string, 0, len(uniq))
	for g := range uniq {
		out = append(out, g)
	}
	sort.Strings(out)
	return out, nil
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

	dummy := jwt.New()
	_ = dummy.Set(key, raw)
	return extractGroupsFromJWT(st, dummy)
}
