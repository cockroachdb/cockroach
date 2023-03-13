package oidcccl

import (
	"context"
	"encoding/json"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const claimGroups = "groups"

// extractUsernameFromClaims uses a regex to strip out elements of the value
// corresponding to the token claim claimKey.
//
// Note: to be clear on the nomenclature, the "groups" claim should not be
// confused with the notion of groups in the context of regular expressions.
func extractUsernameFromClaims(
	ctx context.Context,
	claims map[string]json.RawMessage,
	claimKey string,
	principalRE *regexp.Regexp,
) (string, error) {
	targetClaim, ok := claims[claimKey]
	if !ok {
		log.Errorf(
			ctx, "OIDC: failed to complete authentication: invalid JSON claim key: %s", claimKey,
		)
	}

	if claimKey != claimGroups {
		var principal string
		if err := json.Unmarshal(targetClaim, &principal); err != nil {
			log.Errorf(ctx,
				"OIDC: failed to complete authentication: failed to parse value for the claim %s: %v",
				claimKey, err,
			)
			return "", err
		}

		match := principalRE.FindStringSubmatch(principal)
		numGroups := len(match)
		if numGroups != 2 {
			err := errors.Newf("expected one group in regexp, got %d", numGroups)
			log.Errorf(ctx, "OIDC: failed to complete authentication: %v", err)
			if log.V(1) {
				log.Infof(ctx,
					"check OIDC cluster settings: %s, %s",
					OIDCPrincipalRegexSettingName, OIDCClaimJSONKeySettingName,
				)
			}
			return "", err
		}

		return match[1], nil
	} else {
		// This is the case where the claim key specified is the "groups" claim.
		// The first matching principal is selected as the SQL username.
		if log.V(1) {
			log.Infof(ctx,
				"multiple principals in the claim found; selecting first matching principal...",
			)
		}
		var principals []string
		if err := json.Unmarshal(targetClaim, &principals); err != nil {
			log.Errorf(ctx,
				"OIDC: failed to complete authentication: failed to parse value for the claim %s: %v",
				claimGroups, err,
			)
			return "", err
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
}
