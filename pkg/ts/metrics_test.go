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

package ts

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestTimeSeriesWriteMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tm := newTestModel(t)
	tm.Start()
	defer tm.Stop()

	metrics := tm.DB.Metrics()

	tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
		{
			Name:   "test.multimetric",
			Source: "source1",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(1, 100),
				datapoint(15, 300),
				datapoint(17, 500),
				datapoint(52, 900),
			},
		},
		{
			Name:   "test.multimetric",
			Source: "source2",
			Datapoints: []tspb.TimeSeriesDatapoint{
				datapoint(5, 100),
				datapoint(16, 300),
				datapoint(22, 500),
				datapoint(82, 900),
			},
		},
	})
	tm.assertKeyCount(7)
	tm.assertModelCorrect()

	if a, e := metrics.WriteSamples.Count(), int64(8); a != e {
		t.Fatalf("samples written was %d, wanted %d", a, e)
	}

	originalBytes := metrics.WriteBytes.Count()
	if a, e := originalBytes, int64(0); a <= e {
		t.Fatalf("sample bytes written was %d, wanted more than %d", a, e)
	}

	if a, e := metrics.WriteErrors.Count(), int64(0); a != e {
		t.Fatalf("write error count was %d, wanted %d", a, e)
	}

	// Introduce an error into the db.
	if err := tm.DB.StoreData(context.TODO(), resolutionInvalid, []tspb.TimeSeriesData{
		{
			Name:   "test.multimetric",
			Source: "source3",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					Value:          1,
					TimestampNanos: 1,
				},
			},
		},
	}); err == nil {
		t.Fatal("StoreData for invalid resolution did not throw error, wanted an error")
	}

	if a, e := metrics.WriteSamples.Count(), int64(8); a != e {
		t.Fatalf("samples written was %d, wanted %d", a, e)
	}

	if a, e := metrics.WriteBytes.Count(), originalBytes; a != e {
		t.Fatalf("sample bytes written was %d, wanted %d", a, e)
	}

	if a, e := metrics.WriteErrors.Count(), int64(1); a != e {
		t.Fatalf("write error count was %d, wanted %d", a, e)
	}
}
