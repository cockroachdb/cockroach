// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwthelper

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// ParseGroupsClaim returns a deduplicated, lowercased list of groups
// taken from tok[claimName]. The claim may be a JSON array or a
// comma/space-separated string.
func ParseGroupsClaim(token jwt.Token, claim string) ([]string, error) {
	raw, ok := token.Get(claim)
	if !ok {
		return nil, errors.Newf(
			"groups claim %q missing in token (issuer=%q subject=%q)",
			claim, token.Issuer(), token.Subject())
	}
	groups, err := normalize(raw)
	if err != nil {
		return nil, err
	}
	return groups, nil
}

// dedupeStrings returns a slice that contains each element of `in` exactly
// once, preserving the original order of first appearance.
func dedupeStrings(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, dup := seen[s]; !dup {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func normalize(rawGroups any) ([]string, error) {
	appendGroup := func(dst []string, group string) []string {
		group = strings.ToLower(strings.TrimSpace(group))
		if group != "" {
			return append(dst, group)
		}
		return dst
	}

	groups := make([]string, 0) // always non-nil

	switch evaluatedGroups := rawGroups.(type) {
	case []any: // JSON array, e.g. ["A", "B"]
		for _, evaluatedGroup := range evaluatedGroups {
			groups = appendGroup(groups, fmt.Sprint(evaluatedGroup))
		}
	case string: // comma- or space-separated string, e.g "A, B" or "A B"
		separator := ","
		if !strings.Contains(evaluatedGroups, ",") {
			separator = " "
		}
		for _, evaluatedGroup := range strings.Split(evaluatedGroups, separator) {
			groups = appendGroup(groups, evaluatedGroup)
		}
	default:
		return nil, errors.Newf(
			"groups claim must be array or string (raw=%q)", evaluatedGroups)
	}

	return dedupeStrings(groups), nil
}
