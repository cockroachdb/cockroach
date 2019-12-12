// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCommuteJoinFlags(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := [][2]memo.JoinFlags{
		{0, 0},

		{memo.AllowLookupJoinIntoLeft, memo.AllowLookupJoinIntoRight},

		{
			memo.AllowLookupJoinIntoLeft | memo.AllowLookupJoinIntoRight,
			memo.AllowLookupJoinIntoLeft | memo.AllowLookupJoinIntoRight,
		},

		{memo.AllowHashJoinStoreLeft, memo.AllowHashJoinStoreRight},

		{
			memo.AllowHashJoinStoreLeft | memo.AllowHashJoinStoreRight,
			memo.AllowHashJoinStoreLeft | memo.AllowHashJoinStoreRight,
		},

		{
			memo.AllowMergeJoin | memo.AllowHashJoinStoreLeft | memo.AllowLookupJoinIntoRight,
			memo.AllowMergeJoin | memo.AllowHashJoinStoreRight | memo.AllowLookupJoinIntoLeft,
		},
	}

	var funcs CustomFuncs
	for _, tc := range cases {
		for dir := 0; dir <= 1; dir++ {
			in, out := tc[dir], tc[dir^1]
			res := funcs.CommuteJoinFlags(&memo.JoinPrivate{Flags: in})
			if res.Flags != out {
				t.Errorf("input: '%s'  expected: '%s'  got: '%s'", in, out, res.Flags)
			}
		}
	}
}
