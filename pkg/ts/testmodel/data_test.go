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

import (
	"reflect"
	"testing"
)

func TestDataSeriesTimeSlice(t *testing.T) {
	testData := DataSeries{
		dp(1, 5),
		dp(2, 5),
		dp(8, 10),
		dp(12, 10),
		dp(17, 5),
		dp(34, 50),
		dp(35, 75),
		dp(40, 10),
		dp(49, 10),
		dp(100, 500),
		dp(109, 50),
		dp(115, 99),
	}

	for _, tc := range []struct {
		start    int64
		end      int64
		expected DataSeries
	}{
		{
			start:    0,
			end:      120,
			expected: testData,
		},
		{
			start:    0,
			end:      0,
			expected: nil,
		},
		{
			start:    1000,
			end:      1001,
			expected: nil,
		},

		{
			start:    1,
			end:      2,
			expected: testData[:1],
		},
		{
			start:    -100,
			end:      36,
			expected: testData[:7],
		},
		{
			start:    15,
			end:      2000,
			expected: testData[4:],
		},
		{
			start: 15,
			end:   49,
			expected: DataSeries{
				dp(17, 5),
				dp(34, 50),
				dp(35, 75),
				dp(40, 10),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			results := testData.TimeSlice(tc.start, tc.end)
			if a, e := results, tc.expected; !reflect.DeepEqual(a, e) {
				t.Errorf("time slice got %v, wanted %v", a, e)
			}
		})
	}
}

func TestDataSeriesGroupByResolution(t *testing.T) {
	// Each test uses the same input data. The input data includes several scenarios
	// for missing/present data, such as gaps of different sizes and irregular
	// recording patterns
	testData := DataSeries{
		dp(1, 5),
		dp(2, 5),
		dp(8, 10),
		dp(12, 10),
		dp(17, 5),
		dp(34, 50),
		dp(35, 75),
		dp(40, 10),
		dp(41, 10),
		dp(42, 10),
		dp(43, 10),
		dp(44, 10),
		dp(45, 10),
		dp(46, 10),
		dp(47, 10),
		dp(48, 10),
		dp(49, 10),
		dp(100, 500),
		dp(109, 50),
		dp(115, 99),
	}

	for _, tc := range []struct {
		resolution int64
		aggFunc    aggFunc
		expected   DataSeries
	}{
		// Group by same resolution, aggregate add.
		{
			resolution: 1,
			aggFunc:    AggregateSum,
			expected:   testData,
		},
		// Group by 10 second resolution, aggregate add.
		{
			resolution: 10,
			aggFunc:    AggregateSum,
			expected: DataSeries{
				dp(0, 20),
				dp(10, 15),
				dp(30, 125),
				dp(40, 100),
				dp(100, 550),
				dp(110, 99),
			},
		},
		// Group by 10 second resolution, aggregate last.
		{
			resolution: 10,
			aggFunc:    AggregateLast,
			expected: DataSeries{
				dp(0, 10),
				dp(10, 5),
				dp(30, 75),
				dp(40, 10),
				dp(100, 50),
				dp(110, 99),
			},
		},
		// Group by 20 second resolution, aggregate last.
		{
			resolution: 20,
			aggFunc:    AggregateLast,
			expected: DataSeries{
				dp(0, 5),
				dp(20, 75),
				dp(40, 10),
				dp(100, 99),
			},
		},
		// Group by 100 second resolution, aggregate add.
		{
			resolution: 100,
			aggFunc:    AggregateSum,
			expected: DataSeries{
				dp(0, 260),
				dp(100, 649),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			results := testData.GroupByResolution(tc.resolution, tc.aggFunc)
			if a, e := results, tc.expected; !reflect.DeepEqual(a, e) {
				t.Errorf("group by resolution got %v, wanted %v", a, e)
			}
		})
	}
}

func TestDataSeriesFillByResolution(t *testing.T) {
	testData := DataSeries{
		dp(0, 10),
		dp(10, 5),
		dp(30, 75),
		dp(40, 10),
		dp(100, 70),
		dp(110, 99),
	}
	for _, tc := range []struct {
		resolution int64
		fillFunc   fillFunc
		expected   DataSeries
	}{
		// fill function that does nothing
		{
			resolution: 10,
			fillFunc: func(_ DataSeries, _ DataSeries, _ int64) DataSeries {
				return nil
			},
			expected: DataSeries{
				dp(0, 10),
				dp(10, 5),
				dp(30, 75),
				dp(40, 10),
				dp(100, 70),
				dp(110, 99),
			},
		},
		// fill function that returns a constant value
		{
			resolution: 10,
			fillFunc: func(before DataSeries, _ DataSeries, res int64) DataSeries {
				return DataSeries{
					dp(before[len(before)-1].TimestampNanos+res, 777),
				}
			},
			expected: DataSeries{
				dp(0, 10),
				dp(10, 5),
				dp(20, 777),
				dp(30, 75),
				dp(40, 10),
				dp(50, 777),
				dp(100, 70),
				dp(110, 99),
			},
		},
		// interpolation fill function
		{
			resolution: 10,
			fillFunc:   fillFuncLinearInterpolate,
			expected: DataSeries{
				dp(0, 10),
				dp(10, 5),
				dp(20, 40),
				dp(30, 75),
				dp(40, 10),
				dp(50, 20),
				dp(60, 30),
				dp(70, 40),
				dp(80, 50),
				dp(90, 60),
				dp(100, 70),
				dp(110, 99),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			results := testData.fillForResolution(tc.resolution, tc.fillFunc)
			if a, e := results, tc.expected; !reflect.DeepEqual(a, e) {
				t.Errorf("fill by resolution got %v, wanted %v", a, e)
			}
		})
	}
}

func TestDataSeriesRateOfChange(t *testing.T) {
	testData := DataSeries{
		dp(0, 10),
		dp(10, 5),
		dp(30, 75),
		dp(40, 10),
		dp(100, 70),
		dp(110, 99),
	}
	for _, tc := range []struct {
		period   int64
		expected DataSeries
	}{
		{
			period: 10,
			expected: DataSeries{
				dp(10, -5),
				dp(30, 35),
				dp(40, -65),
				dp(100, 10),
				dp(110, 29),
			},
		},
		{
			period: 1,
			expected: DataSeries{
				dp(10, -0.5),
				dp(30, 3.5),
				dp(40, -6.5),
				dp(100, 1),
				dp(110, 2.9),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			results := testData.rateOfChange(tc.period)
			if a, e := results, tc.expected; !reflect.DeepEqual(a, e) {
				t.Errorf("rate of change got %v, wanted %v", a, e)
			}
		})
	}
}

func TestDataSeriesNonNegative(t *testing.T) {
	for _, tc := range []struct {
		input    DataSeries
		expected DataSeries
	}{
		{
			input: DataSeries{
				dp(10, -5),
				dp(30, 35),
				dp(40, -65),
				dp(100, 10),
				dp(110, 29),
			},
			expected: DataSeries{
				dp(10, 0),
				dp(30, 35),
				dp(40, 0),
				dp(100, 10),
				dp(110, 29),
			},
		},
		{
			input: DataSeries{
				dp(10, -0.5),
			},
			expected: DataSeries{
				dp(10, 0),
			},
		},
		{
			input: DataSeries{
				dp(10, 0.001),
			},
			expected: DataSeries{
				dp(10, 0.001),
			},
		},
		{
			input:    DataSeries{},
			expected: DataSeries{},
		},
	} {
		t.Run("", func(t *testing.T) {
			results := tc.input.nonNegative()
			if a, e := results, tc.expected; !reflect.DeepEqual(a, e) {
				t.Errorf("rate of change got %v, wanted %v", a, e)
			}
		})
	}
}

func TestDataSeriesAdjustTimestamp(t *testing.T) {
	testData := DataSeries{
		dp(0, 10),
		dp(10, 5),
		dp(30, 75),
		dp(40, 10),
		dp(100, 70),
		dp(110, 99),
	}

	for _, tc := range []struct {
		offset   int64
		expected DataSeries
	}{
		{
			offset: 0,
			expected: DataSeries{
				dp(0, 10),
				dp(10, 5),
				dp(30, 75),
				dp(40, 10),
				dp(100, 70),
				dp(110, 99),
			},
		},
		{
			offset: 15,
			expected: DataSeries{
				dp(15, 10),
				dp(25, 5),
				dp(45, 75),
				dp(55, 10),
				dp(115, 70),
				dp(125, 99),
			},
		},
		{
			offset: -15,
			expected: DataSeries{
				dp(-15, 10),
				dp(-5, 5),
				dp(15, 75),
				dp(25, 10),
				dp(85, 70),
				dp(95, 99),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			results := testData.adjustTimestamps(tc.offset)
			if a, e := results, tc.expected; !reflect.DeepEqual(a, e) {
				t.Errorf("adjust timestamp got %v, wanted %v", a, e)
			}
		})
	}
}

func TestDataSeriesRemoveDuplicates(t *testing.T) {
	for _, tc := range []struct {
		input    DataSeries
		expected DataSeries
	}{
		{
			input: DataSeries{
				dp(10, 999),
				dp(10, 888),
				dp(10, 777),
			},
			expected: DataSeries{
				dp(10, 777),
			},
		},
		{
			input: DataSeries{
				dp(10, 999),
				dp(10, 888),
				dp(10, 777),
				dp(20, 100),
				dp(45, 300),
				dp(100, 1),
				dp(100, 2),
				dp(100, 3),
			},
			expected: DataSeries{
				dp(10, 777),
				dp(20, 100),
				dp(45, 300),
				dp(100, 3),
			},
		},
		{
			input: DataSeries{
				dp(10, 777),
				dp(20, 100),
				dp(45, 300),
				dp(100, 3),
			},
			expected: DataSeries{
				dp(10, 777),
				dp(20, 100),
				dp(45, 300),
				dp(100, 3),
			},
		},
		{
			input:    DataSeries{},
			expected: DataSeries{},
		},
	} {
		t.Run("", func(t *testing.T) {
			results := tc.input.removeDuplicates()
			if a, e := results, tc.expected; !reflect.DeepEqual(a, e) {
				t.Errorf("remove duplicates got %v, wanted %v", a, e)
			}
		})
	}
}

func TestDataSeriesGroupByTimestamp(t *testing.T) {
	testData1 := DataSeries{
		dp(10, 1),
		dp(20, 1),
		dp(30, 1),
		dp(140, 1),
		dp(777, 1),
	}
	testData2 := DataSeries{
		dp(10, 2),
		dp(20, 2),
		dp(30, 2),
		dp(140, 2),
		dp(777, 2),
	}
	testDataStaggered := DataSeries{
		dp(10, 5),
		dp(25, 5),
		dp(30, 5),
		dp(141, 5),
		dp(888, 5),
	}

	for _, tc := range []struct {
		inputs   []DataSeries
		aggFunc  aggFunc
		expected DataSeries
	}{
		{
			inputs:   nil,
			aggFunc:  AggregateSum,
			expected: nil,
		},
		{
			inputs:  []DataSeries{testData1},
			aggFunc: AggregateSum,
			expected: DataSeries{
				dp(10, 1),
				dp(20, 1),
				dp(30, 1),
				dp(140, 1),
				dp(777, 1),
			},
		},
		{
			inputs:  []DataSeries{testData1, testData2},
			aggFunc: AggregateSum,
			expected: DataSeries{
				dp(10, 3),
				dp(20, 3),
				dp(30, 3),
				dp(140, 3),
				dp(777, 3),
			},
		},
		{
			inputs:  []DataSeries{testData1, testData2},
			aggFunc: AggregateAverage,
			expected: DataSeries{
				dp(10, 1.5),
				dp(20, 1.5),
				dp(30, 1.5),
				dp(140, 1.5),
				dp(777, 1.5),
			},
		},
		{
			inputs:  []DataSeries{testData1, testData2, testDataStaggered},
			aggFunc: AggregateSum,
			expected: DataSeries{
				dp(10, 8),
				dp(20, 3),
				dp(25, 5),
				dp(30, 8),
				dp(140, 3),
				dp(141, 5),
				dp(777, 3),
				dp(888, 5),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			results := groupSeriesByTimestamp(tc.inputs, tc.aggFunc)
			if a, e := results, tc.expected; !reflect.DeepEqual(a, e) {
				t.Errorf("rate of change got %v, wanted %v", a, e)
			}
		})
	}
}

func TestDataSeriesIntersectTimestamps(t *testing.T) {
	testData := DataSeries{
		dp(10, 999),
		dp(11, 888),
		dp(12, 777),
		dp(13, 999),
		dp(14, 888),
		dp(15, 777),
		dp(16, 999),
		dp(17, 888),
		dp(18, 777),
	}

	for _, tc := range []struct {
		intersections []DataSeries
		expected      DataSeries
	}{
		{
			intersections: []DataSeries{
				{
					dp(10, 0),
				},
			},
			expected: DataSeries{
				dp(10, 999),
			},
		},
		{
			intersections: []DataSeries{},
			expected:      DataSeries{},
		},
		{
			intersections: []DataSeries{
				{
					dp(10, 0),
					dp(12, 0),
					dp(14, 0),
					dp(16, 0),
					dp(18, 0),
					dp(20, 0),
				},
				{
					dp(11, 0),
					dp(13, 0),
					dp(15, 0),
					dp(17, 0),
					dp(19, 0),
					dp(21, 0),
				},
			},
			expected: testData,
		},
		{
			intersections: []DataSeries{
				{
					dp(10, 0),
				},
				{
					dp(13, 0),
					dp(17, 0),
					dp(21, 0),
				},
				{
					dp(10, 0),
					dp(15, 0),
					dp(17, 0),
				},
			},
			expected: DataSeries{
				dp(10, 999),
				dp(13, 999),
				dp(15, 777),
				dp(17, 888),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			results := testData.intersectTimestamps(tc.intersections...)
			if a, e := results, tc.expected; !reflect.DeepEqual(a, e) {
				t.Errorf("intersect timestamps got %v, wanted %v", a, e)
			}
		})
	}
}
