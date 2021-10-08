// Copyright 2021 The Cockroach Authors.
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
	"github.com/stretchr/testify/require"
)

// TestCanHaveCompositeKeyEncoding tests that
func TestCanHaveCompositeKeyEncoding(t *testing.T) {
	for _, tc := range []struct {
		typ *types.T
		exp bool
	}{
		{types.Any, true},
		{types.AnyArray, true},
		{types.AnyCollatedString, true},
		{types.AnyEnum, false},
		{types.AnyTuple, true},
		{types.Bool, false},
		{types.BoolArray, false},
		{types.Box2D, false},
		{types.Bytes, false},
		{types.Date, false},
		{types.DateArray, false},
		{types.Decimal, true},
		{types.DecimalArray, true},
		{types.EmptyTuple, false},
		{types.Float, true},
		{types.Float4, true},
		{types.FloatArray, true},
		{types.Geography, false},
		{types.Geometry, false},
		{types.INet, false},
		{types.INetArray, false},
		{types.Int, false},
		{types.Int2, false},
		{types.Int2Vector, false},
		{types.Int4, false},
		{types.IntArray, false},
		{types.Interval, false},
		{types.IntervalArray, false},
		{types.Jsonb, false},
		{types.Name, false},
		{types.Oid, false},
		{types.String, false},
		{types.StringArray, false},
		{types.Time, false},
		{types.TimeArray, false},
		{types.TimeTZ, false},
		{types.TimeTZArray, false},
		{types.Timestamp, false},
		{types.TimestampArray, false},
		{types.TimestampTZ, false},
		{types.TimestampTZArray, false},
		{types.UUIDArray, false},
		{types.Unknown, true},
		{types.Uuid, false},
		{types.VarBit, false},
		{types.VarBitArray, false},
		{types.VarChar, false},
		{types.MakeTuple([]*types.T{types.Int, types.Date}), false},
		{types.MakeTuple([]*types.T{types.Float, types.Date}), true},
		// Test that a made up type with a bogus family will return true.
		{&types.T{InternalType: types.InternalType{Family: 1 << 29}}, true},
	} {
		// Note that sprint is used here because the bogus type family will
		// panic when formatting to a string and sprint will catch that.
		t.Run(fmt.Sprint(tc.typ), func(t *testing.T) {
			require.Equal(t, tc.exp, CanHaveCompositeKeyEncoding(tc.typ))
		})
	}
}
