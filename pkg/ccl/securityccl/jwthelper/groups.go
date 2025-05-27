// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwthelper

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// ExtractGroups returns a sorted, deduplicated, lowercased list of groups
// taken from tok[claimName]. The claim may be a JSON array or a
// comma/space-separated string.
func ExtractGroups(tok jwt.Token, claim string) ([]string, error) {
	raw, ok := tok.Get(claim)
	if !ok {
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: missing groups"),
			"%q claim missing in token (issuer=%q subject=%q)",
			claim, tok.Issuer(), tok.Subject())
	}
	groups, err := normalize(raw)
	if err != nil {
		return nil, err
	}
	sort.Strings(groups) // deterministic GRANT order
	return groups, nil
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

	switch val := rawGroups.(type) {
	case []any: // JSON array, e.g. ["A", "B"]
		for _, element := range val {
			groups = appendGroup(groups, fmt.Sprint(element))
		}
	case string: // comma- or space-separated string
		separator := ","
		if !strings.Contains(val, ",") {
			separator = " "
		}
		for _, part := range strings.Split(val, separator) {
			groups = appendGroup(groups, part)
		}
	default:
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: invalid groups format"),
			"groups claim must be array or string (raw=%q)", val)
	}

	unique := map[string]struct{}{}
	for _, group := range groups {
		unique[group] = struct{}{}
	}

	normalized := make([]string, 0, len(unique))
	for group := range unique {
		normalized = append(normalized, group)
	}
	return normalized, nil
}
