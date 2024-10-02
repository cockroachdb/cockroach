// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestPlaceholderTypesIdentical(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		a, b      PlaceholderTypes
		identical bool
	}{
		{ // 0
			PlaceholderTypes{},
			PlaceholderTypes{},
			true,
		},
		{ // 1
			PlaceholderTypes{types.Int, types.Int},
			PlaceholderTypes{types.Int, types.Int},
			true,
		},
		{ // 2
			PlaceholderTypes{types.Int},
			PlaceholderTypes{types.Int, types.Int},
			false,
		},
		{ // 3
			PlaceholderTypes{types.Int, nil},
			PlaceholderTypes{types.Int, types.Int},
			false,
		},
		{ // 4
			PlaceholderTypes{types.Int, types.Int},
			PlaceholderTypes{types.Int, nil},
			false,
		},
		{ // 5
			PlaceholderTypes{types.Int, nil},
			PlaceholderTypes{types.Int, nil},
			true,
		},
		{ // 6
			PlaceholderTypes{types.Int},
			PlaceholderTypes{types.Int, nil},
			false,
		},
		{ // 7
			PlaceholderTypes{types.Int, nil},
			PlaceholderTypes{types.Int4, nil},
			false,
		},
		{ // 8
			PlaceholderTypes{types.Int},
			PlaceholderTypes{types.Int4},
			false,
		},
		{ // 9
			PlaceholderTypes{types.Int4},
			PlaceholderTypes{types.Int4},
			true,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			res := tc.a.Identical(tc.b)
			if res != tc.identical {
				t.Errorf("%v vs %v: expected %t, got %t", tc.a, tc.b, tc.identical, res)
			}
			res2 := tc.b.Identical(tc.a)
			if res != res2 {
				t.Errorf("%v vs %v: not commutative", tc.a, tc.b)
			}
		})
	}
}
