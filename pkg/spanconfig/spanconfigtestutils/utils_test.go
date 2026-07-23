// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigtestutils

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpanRe(t *testing.T) {
	for _, tc := range []struct {
		input            string
		expMatch         bool
		expTenant        bool
		expStart, expEnd string
	}{
		{"[a, b)", true, false, "a", "b"},
		// Multi character keys allowed.
		{"[acd, bfg)", true, false, "acd", "bfg"},
		// Separating space is optional.
		{"[a,b)", true, false, "a", "b"},
		// Tenant span.
		{"[/Tenant/10/a,/Tenant/10/b)", true, true, "/Tenant/10/a", "/Tenant/10/b"},
		// Extraneous spaces disallowed.
		{"[ a,b) ", false, false, "", ""},
		// Extraneous spaces disallowed.
		{"[a,b ) ", false, false, "", ""},
		// Only single comma allowed.
		{"[a,, b)", false, false, "", ""},
		// Need to start with '['.
		{" [a, b)", false, false, "", ""},
		// Need to end with ')'.
		{"[a,b)x", false, false, "", ""},
	} {
		require.Equalf(t, tc.expMatch, spanRe.MatchString(tc.input), "input = %s", tc.input)
		if !tc.expMatch {
			continue
		}

		matches := spanRe.FindStringSubmatch(tc.input)
		require.Len(t, matches, 5)
		start, end := matches[1], matches[3]

		const tenPrefix = "/Tenant/"
		if tc.expTenant {
			require.True(t, strings.HasPrefix(matches[2], tenPrefix))
			require.True(t, strings.HasPrefix(matches[4], tenPrefix))
		} else {
			require.Equal(t, 0, len(matches[2]))
			require.Equal(t, 0, len(matches[4]))
		}
		require.Equal(t, tc.expStart, start)
		require.Equal(t, tc.expEnd, end)
	}
}
