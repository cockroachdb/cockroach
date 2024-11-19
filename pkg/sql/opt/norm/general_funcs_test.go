// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCommuteJoinFlags(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := [][2]memo.JoinFlags{
		{0, 0},

		{
			memo.DisallowLookupJoinIntoLeft,
			memo.DisallowLookupJoinIntoRight,
		},

		{
			memo.DisallowInvertedJoinIntoLeft,
			memo.DisallowInvertedJoinIntoRight,
		},

		{
			memo.PreferLookupJoinIntoLeft,
			memo.PreferLookupJoinIntoRight,
		},

		{
			memo.AllowOnlyMergeJoin,
			memo.AllowOnlyMergeJoin,
		},

		{
			memo.DisallowHashJoinStoreLeft | memo.DisallowMergeJoin | memo.DisallowLookupJoinIntoLeft | memo.DisallowLookupJoinIntoRight |
				memo.DisallowInvertedJoinIntoLeft | memo.DisallowInvertedJoinIntoRight,
			memo.DisallowHashJoinStoreRight | memo.DisallowMergeJoin | memo.DisallowLookupJoinIntoLeft | memo.DisallowLookupJoinIntoRight |
				memo.DisallowInvertedJoinIntoLeft | memo.DisallowInvertedJoinIntoRight,
		},

		{
			memo.DisallowHashJoinStoreLeft | memo.DisallowHashJoinStoreRight | memo.DisallowMergeJoin | memo.DisallowLookupJoinIntoLeft |
				memo.DisallowInvertedJoinIntoLeft | memo.DisallowInvertedJoinIntoRight,
			memo.DisallowHashJoinStoreLeft | memo.DisallowHashJoinStoreRight | memo.DisallowMergeJoin | memo.DisallowLookupJoinIntoRight |
				memo.DisallowInvertedJoinIntoLeft | memo.DisallowInvertedJoinIntoRight,
		},

		{
			memo.DisallowMergeJoin | memo.DisallowHashJoinStoreLeft | memo.DisallowLookupJoinIntoRight | memo.DisallowInvertedJoinIntoRight,
			memo.DisallowMergeJoin | memo.DisallowHashJoinStoreRight | memo.DisallowLookupJoinIntoLeft | memo.DisallowInvertedJoinIntoLeft,
		},
	}

	var funcs CustomFuncs
	for _, tc := range cases {
		// The result of commuting flags should be symmetrical, so test each case in
		// both directions.
		for dir := 0; dir <= 1; dir++ {
			in, out := tc[dir], tc[dir^1]
			res := funcs.CommuteJoinFlags(&memo.JoinPrivate{Flags: in})
			if res.Flags != out {
				t.Errorf("input: '%s'  expected: '%s'  got: '%s'", in, out, res.Flags)
			}
		}
	}
}
