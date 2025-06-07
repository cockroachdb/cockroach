// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwthelper

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// ExtractGroups returns a sorted, deduplicated, lowercased list of groups
// taken from tok[claimName]. The claim may be a JSON array or a
// comma/space-separated string.
func ExtractGroups(st *cluster.Settings, tok jwt.Token, claim string) ([]string, error) {
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

func normalize(v any) ([]string, error) {
	add := func(dst []string, s string) []string {
		s = strings.ToLower(strings.TrimSpace(s))
		if s != "" {
			return append(dst, s)
		}
		return dst
	}

	var out []string
	switch g := v.(type) {
	case []any: // ["A", "B"]
		for _, x := range g {
			out = add(out, fmt.Sprint(x))
		}
	case string: // "A,B" or "A B"
		sep := ","
		if !strings.Contains(g, ",") {
			sep = " "
		}
		for _, part := range strings.Split(g, sep) {
			out = add(out, part)
		}
	default:
		return nil, errors.WithDetailf(
			errors.Newf("JWT authorization: invalid groups format"),
			"groups claim must be array or string (raw=%q)", g)
	}

	uniq := map[string]struct{}{}
	for _, g := range out {
		uniq[g] = struct{}{}
	}
	out = out[:0]
	for g := range uniq {
		out = append(out, g)
	}
	return out, nil
}
