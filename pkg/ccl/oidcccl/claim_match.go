// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package oidcccl

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// extractUsernameFromClaims uses a regex to strip out elements of the value
// corresponding to the token claim claimKey.
func extractUsernameFromClaims(
	ctx context.Context,
	claims map[string]json.RawMessage,
	claimKey string,
	principalRE *regexp.Regexp,
) (string, error) {
	var (
		principal  string
		principals []string
	)

	claimKeys := make([]string, len(claims))
	i := 0
	for k := range claims {
		claimKeys[i] = k
		i++
	}

	targetClaim, ok := claims[claimKey]
	if !ok {
		log.Errorf(
			ctx, "OIDC: failed to complete authentication: invalid JSON claim key: %s", claimKey,
		)
		log.Infof(ctx, "token payload includes the following claims: %s", strings.Join(claimKeys, ", "))
	}

	if err := json.Unmarshal(targetClaim, &principal); err != nil {
		// Try parsing assuming the claim value is a list and not a string.
		if log.V(1) {
			log.Infof(ctx,
				"failed parsing claim as string; attempting to parse as a list",
			)
		}
		if err = json.Unmarshal(targetClaim, &principals); err != nil {
			log.Errorf(ctx,
				"OIDC: failed to complete authentication: failed to parse value for the claim %s: %v",
				claimKey, err,
			)
			return "", err
		}
		if log.V(1) {
			log.Infof(ctx,
				"multiple principals in the claim found; selecting first matching principal",
			)
		}
	}

	if len(principals) == 0 {
		principals = []string{principal}
	}

	var match []string
	for _, principal := range principals {
		match = principalRE.FindStringSubmatch(principal)
		if len(match) == 2 {
			log.Infof(ctx,
				"extracted SQL username %s from the target claim %s", match[1], claimKey,
			)
			return match[1], nil
		}
	}

	// Error when there is not a match.
	err := errors.Newf("expected one group in regexp")
	log.Errorf(ctx, "OIDC: failed to complete authentication: %v", err)
	if log.V(1) {
		log.Infof(ctx,
			"token payload includes the following claims: %s\n"+
				"check OIDC cluster settings: %s, %s",
			strings.Join(claimKeys, ", "),
			OIDCClaimJSONKeySettingName, OIDCPrincipalRegexSettingName,
		)
	}
	return "", err
}
