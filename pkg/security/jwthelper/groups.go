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
// comma-separated string.
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
	case string: // comma-separated string, e.g. "A, B"
		// Only commas separate groups. Splitting on spaces would break a single
		// group whose name contains a space (e.g. "admin readers") into multiple
		// groups, which can grant unintended SQL role memberships. IdPs that
		// return multiple groups should encode them as a JSON array.
		for evaluatedGroup := range strings.SplitSeq(evaluatedGroups, ",") {
			groups = appendGroup(groups, evaluatedGroup)
		}
	default:
		return nil, errors.Newf(
			"groups claim must be array or string (raw=%q)", evaluatedGroups)
	}

	return dedupeStrings(groups), nil
}
