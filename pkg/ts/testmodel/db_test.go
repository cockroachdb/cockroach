// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package testmodel

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

func TestModelDBQuery(t *testing.T) {
	db := NewModelDB()
	db.Record("testmetric", "source1", DataSeries{
		dp(0, 0.0),
		dp(100, 100.0),
		dp(200, 200.0),
		dp(400, 400.0),
	})
	db.Record("testmetric", "source2", DataSeries{
		dp(0, 0.0),
		dp(103, 50.0),
		dp(199, 150.0),
		dp(205, 400.0),
		dp(301, 600.0),
		dp(425, 800.0),
	})
	db.Record("othermetric", "source1", DataSeries{
		dp(150, 10000),
		dp(250, 10000),
		dp(600, 5000),
	})

	for _, tc := range []struct {
		seriesName         string
		sources            []string
		downsampler        tspb.TimeSeriesQueryAggregator
		aggregator         tspb.TimeSeriesQueryAggregator
		derivative         tspb.TimeSeriesQueryDerivative
		slabDuration       int64
		sampleDuration     int64
		start              int64
		end                int64
		interpolationLimit int64
		expected           DataSeries
	}{
		// Basic Query
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			expected: DataSeries{
				dp(50, 0.0),
				dp(150, 200.0),
				dp(250, 600.0),
				dp(350, 900.0),
				dp(450, 1200.0),
			},
		},
		// Different downsampler
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_MAX,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			expected: DataSeries{
				dp(50, 0.0),
				dp(150, 250.0),
				dp(250, 600.0),
				dp(350, 900.0),
				dp(450, 1200.0),
			},
		},
		// Different aggregator
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_MAX,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			expected: DataSeries{
				dp(50, 0.0),
				dp(150, 100.0),
				dp(250, 400.0),
				dp(350, 600.0),
				dp(450, 800.0),
			},
		},
		// Single-source Query
		{
			seriesName:         "testmetric",
			sources:            []string{"source2"},
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			expected: DataSeries{
				dp(50, 0.0),
				dp(150, 100.0),
				dp(250, 400.0),
				dp(350, 600.0),
				dp(450, 800.0),
			},
		},
		// Limited time.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              200,
			end:                300,
			interpolationLimit: 10000,
			expected: DataSeries{
				dp(250, 600.0),
				dp(350, 900.0),
			},
		},
		// No interpolation.
		{
			seriesName:         "testmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 1,
			expected: DataSeries{
				dp(50, 0.0),
				dp(150, 200.0),
				dp(250, 600.0),
				dp(350, 600.0),
				dp(450, 1200.0),
			},
		},
		// No data.
		{
			seriesName:         "wrongmetric",
			sources:            nil,
			downsampler:        tspb.TimeSeriesQueryAggregator_AVG,
			aggregator:         tspb.TimeSeriesQueryAggregator_SUM,
			derivative:         tspb.TimeSeriesQueryDerivative_NONE,
			slabDuration:       1000,
			sampleDuration:     100,
			start:              0,
			end:                10000,
			interpolationLimit: 10000,
			expected:           nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			result := db.Query(
				tc.seriesName,
				tc.sources,
				tc.downsampler,
				tc.aggregator,
				tc.derivative,
				tc.slabDuration,
				tc.sampleDuration,
				tc.start,
				tc.end,
				tc.interpolationLimit,
			)
			if a, e := result, tc.expected; !reflect.DeepEqual(a, e) {
				t.Errorf("query got result %v, wanted %v", a, e)
			}
		})
	}
}
