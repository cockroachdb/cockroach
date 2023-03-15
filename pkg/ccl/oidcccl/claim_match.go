// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package oidcccl

import (
	"context"
	"encoding/json"
	"regexp"

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

	targetClaim, ok := claims[claimKey]
	if !ok {
		log.Errorf(
			ctx, "OIDC: failed to complete authentication: invalid JSON claim key: %s", claimKey,
		)
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
			return match[1], nil
		}
	}

	// Error when there is not a match.
	err := errors.Newf("expected one group in regexp")
	log.Errorf(ctx, "OIDC: failed to complete authentication: %v", err)
	if log.V(1) {
		log.Infof(ctx,
			"check OIDC cluster settings: %s, %s",
			OIDCPrincipalRegexSettingName, OIDCClaimJSONKeySettingName,
		)
	}
	return "", err
}
