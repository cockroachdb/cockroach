// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package movr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCityDistributor(t *testing.T) {
	for numRows := len(cities); numRows < len(cities)*len(cities); numRows++ {
		d := cityDistributor{numRows: numRows}
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			cityIdx := d.cityForRow(rowIdx)
			if cityIdx < 0 || cityIdx >= len(cities) {
				t.Fatalf(`city must be in [0,%d) was %d`, len(cities), cityIdx)
			}
			min, max := d.rowsForCity(cityIdx)
			if rowIdx < min || rowIdx >= max {
				t.Fatalf(`row must be in [%d,%d) was %d`, min, max, rowIdx)
			}
		}
		for cityIdx := range cities {
			min, max := d.rowsForCity(cityIdx)
			if min < 0 || min > numRows {
				t.Fatalf(`min must be in [0,%d] was %d`, numRows, min)
			}
			if max < 0 || max > numRows {
				t.Fatalf(`max must be in [0,%d] was %d`, numRows, max)
			}
			if min > max {
				t.Fatalf(`min %d must be <= max %d`, min, max)
			}
			for row := min; row < max; row++ {
				require.Equal(t, cityIdx, d.cityForRow(row), "rows=%d row=%d", numRows, row)
			}
		}
	}
}
