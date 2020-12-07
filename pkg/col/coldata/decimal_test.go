// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"math/big"
	"testing"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/assert"
)

// Res is around just to make sure that the benchmark has to do some work.
var Res apd.Decimal

func BenchmarkSumDecimals(b *testing.B) {
	colFactory := defaultColumnFactory{}
	col := colFactory.MakeColumn(types.Decimal, 1024).(*Decimals)
	var ctx = &apd.Context{
		Precision:   20,
		Rounding:    apd.RoundHalfUp,
		MaxExponent: 2000,
		MinExponent: -2000,
		// Don't error on invalid operation, return NaN instead.
		Traps: apd.DefaultTraps &^ apd.InvalidOperation,
	}
	exactCtx := ctx.WithPrecision(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sum apd.Decimal
		l := col.Len()
		for j := 0; j < l; j++ {
			d := col.Get(j)
			_, err := exactCtx.Add(&sum, &sum, &d)
			if err != nil {
				b.Fatal(err)
			}
		}
		Res = sum
	}
}

func TestUnsafeSetNat(t *testing.T) {
	b := big.NewInt(1000)
	c := big.NewInt(2000)

	bSlice := b.Bits()
	cSlice := c.Bits()

	unsafeSetNat(b, cSlice)
	unsafeSetNat(c, bSlice)

	assert.Equal(t, b.Int64(), int64(2000))
	assert.Equal(t, c.Int64(), int64(1000))
}
