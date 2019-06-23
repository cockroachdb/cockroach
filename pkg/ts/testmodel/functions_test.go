// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testmodel

import "testing"

func TestAggregates(t *testing.T) {
	tests := []struct {
		data DataSeries
		// Expected values
		sum      float64
		min      float64
		max      float64
		first    float64
		last     float64
		avg      float64
		variance float64
	}{
		// Single value.
		{
			data: DataSeries{
				dp(1, 23432),
			},
			sum:      23432,
			min:      23432,
			max:      23432,
			first:    23432,
			last:     23432,
			avg:      23432,
			variance: 0,
		},
		// Value is constant.
		{
			data: DataSeries{
				dp(1, 2),
				dp(2, 2),
				dp(3, 2),
				dp(4, 2),
				dp(5, 2),
			},
			sum:      10,
			min:      2,
			max:      2,
			first:    2,
			last:     2,
			avg:      2,
			variance: 0,
		},
		// Value is constant zero.
		{
			data: DataSeries{
				dp(1, 0),
				dp(2, 0),
				dp(3, 0),
				dp(4, 0),
				dp(5, 0),
			},
			sum:      0,
			min:      0,
			max:      0,
			first:    0,
			last:     0,
			avg:      0,
			variance: 0,
		},
		// Value not constant, compute variance.
		{
			data: DataSeries{
				dp(1, 2),
				dp(2, 4),
				dp(3, 6),
			},
			sum:      12,
			min:      2,
			max:      6,
			first:    2,
			last:     6,
			avg:      4,
			variance: 2.6666666666666665,
		},
		// Negative values, variance still positive.
		{
			data: DataSeries{
				dp(1, -3),
				dp(2, -9),
				dp(3, -6),
			},
			sum:      -18,
			min:      -9,
			max:      -3,
			first:    -3,
			last:     -6,
			avg:      -6,
			variance: 6,
		},
		// Some fairly random numbers, variance and average computed from an online
		// calculator. Gives good confidence that the functions are actually doing
		// the correct thing.
		{
			data: DataSeries{
				dp(1, 60),
				dp(2, 3),
				dp(3, 352),
				dp(4, 7),
				dp(5, 143),
			},
			sum:      565,
			min:      3,
			max:      352,
			first:    60,
			last:     143,
			avg:      113,
			variance: 16833.2,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if actual := AggregateSum(tt.data); actual != tt.sum {
				t.Errorf("AggregateSum() = %v, expected %v", actual, tt.sum)
			}
			if actual := AggregateMin(tt.data); actual != tt.min {
				t.Errorf("AggregateMin() = %v, expected %v", actual, tt.min)
			}
			if actual := AggregateMax(tt.data); actual != tt.max {
				t.Errorf("AggregateMax() = %v, expected %v", actual, tt.max)
			}
			if actual := AggregateFirst(tt.data); actual != tt.first {
				t.Errorf("AggregateFirst() = %v, expected %v", actual, tt.first)
			}
			if actual := AggregateLast(tt.data); actual != tt.last {
				t.Errorf("AggregateLast() = %v, expected %v", actual, tt.last)
			}
			if actual := AggregateAverage(tt.data); actual != tt.avg {
				t.Errorf("AggregateAverage() = %v, expected %v", actual, tt.avg)
			}
			if actual := AggregateVariance(tt.data); actual != tt.variance {
				t.Errorf("AggregateVariance() = %v, expected %v", actual, tt.variance)
			}
		})
	}
}
