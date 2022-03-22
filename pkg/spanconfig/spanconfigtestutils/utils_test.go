// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigtestutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpanRe(t *testing.T) {
	for _, tc := range []struct {
		input            string
		expMatch         bool
		expStart, expEnd string
	}{
		{"[a, b)", true, "a", "b"},
		{"[acd, bfg)", true, "acd", "bfg"}, // multi character keys allowed
		{"[a,b)", true, "a", "b"},          // separating space is optional
		{"[ a,b) ", false, "", ""},         // extraneous spaces disallowed
		{"[a,b ) ", false, "", ""},         // extraneous spaces disallowed
		{"[a,, b)", false, "", ""},         // only single comma allowed
		{" [a, b)", false, "", ""},         // need to start with '['
		{"[a,b)x", false, "", ""},          // need to end with ')'
	} {
		require.Equalf(t, tc.expMatch, spanRe.MatchString(tc.input), "input = %s", tc.input)
		if !tc.expMatch {
			continue
		}

		matches := spanRe.FindStringSubmatch(tc.input)
		require.Len(t, matches, 3)
		start, end := matches[1], matches[2]
		require.Equal(t, tc.expStart, start)
		require.Equal(t, tc.expEnd, end)
	}
}
