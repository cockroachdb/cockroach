// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colinfo

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestResultColumnsTypesEqual(t *testing.T) {
	tests := []struct {
		r, o  ResultColumns
		equal bool
	}{
		{
			r:     ResultColumns{{Typ: types.Int}},
			o:     ResultColumns{{Typ: types.Int}},
			equal: true,
		},
		{
			r:     ResultColumns{{Typ: types.Int}},
			o:     ResultColumns{{Typ: types.String}},
			equal: false,
		},
		{
			r:     ResultColumns{{Typ: types.Unknown}},
			o:     ResultColumns{{Typ: types.Int}},
			equal: false,
		},
		{
			r:     ResultColumns{{Typ: types.Int}},
			o:     ResultColumns{{Typ: types.Unknown}},
			equal: true,
		},
		{
			r:     ResultColumns{{Typ: types.Unknown}},
			o:     ResultColumns{{Typ: types.Unknown}},
			equal: true,
		},
		{
			r:     ResultColumns{{Typ: types.Int}, {Typ: types.Int}},
			o:     ResultColumns{{Typ: types.Int}},
			equal: false,
		},
		{
			r:     ResultColumns{},
			o:     ResultColumns{{Typ: types.Unknown}},
			equal: false,
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%v-%v", tc.r, tc.o), func(t *testing.T) {
			eq := tc.r.TypesEqual(tc.o)
			if eq != tc.equal {
				t.Fatalf("expected %v, got %v", tc.equal, eq)
			}
		})
	}
}
